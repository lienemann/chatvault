"""Decrypt a Signal Android backup file into a plaintext SQLite database
plus media directories.

Implements the canonical Signal-Android backup format:

* file header: ``[uint32 BE length] [BackupFrame{header: {iv, salt, version}}]``
* key derivation: 250 000 manual SHA-512 rounds (``salt || hash || passphrase``)
  followed by HKDF-SHA256 with info ``"Backup Export"``; output split into
  32 bytes AES-256 cipher key + 32 bytes HMAC key
* per-frame layout:
  - version unset: ``[uint32 BE length] [ciphertext] [10-byte truncated HMAC]``
  - version == 1: length itself is AES-CTR encrypted; MAC covers the encrypted
    length followed by the ciphertext
* attachments / stickers / avatars: each control frame is followed by an
  encrypted payload (length given in the frame) plus its own 10-byte MAC
  (computed over IV || ciphertext)

Reference implementation: ``mossblaser/signal_for_android_decryption``
(decrypt_backup.py). We re-implement here so chatvault only takes
``cryptography`` as a new optional dependency.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import struct
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CTR
from cryptography.hazmat.primitives.hashes import SHA256, SHA512, Hash
from cryptography.hazmat.primitives.hmac import HMAC
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from .proto import BackupFrame, decode_backup_frame

log = logging.getLogger(__name__)


class SignalBackupError(Exception):
    """Base class for backup-decoding failures."""


class MACMismatchError(SignalBackupError):
    """HMAC verification failed — wrong passphrase or corrupt file."""


class UnsupportedVersionError(SignalBackupError):
    """Backup header advertises an unknown version."""


@dataclass(slots=True)
class DecryptStats:
    statements: int = 0
    attachments: int = 0
    stickers: int = 0
    avatars: int = 0
    skipped_statements: int = 0
    preferences: int = 0
    key_values: int = 0
    db_version: int | None = None
    bytes_read: int = 0


@dataclass(slots=True)
class Keys:
    cipher_key: bytes
    hmac_key: bytes


def _read_initial_header(stream: BinaryIO) -> BackupFrame:
    raw_len_bytes = stream.read(4)
    if len(raw_len_bytes) != 4:
        msg = "backup file is empty or truncated"
        raise SignalBackupError(msg)
    (length,) = struct.unpack(">I", raw_len_bytes)
    body = stream.read(length)
    if len(body) != length:
        msg = "truncated header frame"
        raise SignalBackupError(msg)
    frame = decode_backup_frame(body)
    if frame.header is None:
        msg = "first frame is not a Header"
        raise SignalBackupError(msg)
    return frame


def derive_keys(passphrase: str, salt: bytes) -> Keys:
    """Reproduce Signal Android's manual SHA-512 KDF + HKDF split."""
    passphrase_bytes = passphrase.replace(" ", "").encode("ascii")
    digest = passphrase_bytes
    for i in range(250_000):
        sha = Hash(SHA512())
        if i == 0:
            sha.update(salt)
        sha.update(digest)
        sha.update(passphrase_bytes)
        digest = sha.finalize()
    hkdf = HKDF(algorithm=SHA256(), length=64, salt=b"", info=b"Backup Export")
    keys = hkdf.derive(digest[:32])
    return Keys(cipher_key=keys[:32], hmac_key=keys[32:])


def _increment_iv(iv: bytes) -> bytes:
    counter = int.from_bytes(iv[:4], "big")
    counter = (counter + 1) & 0xFFFFFFFF
    return counter.to_bytes(4, "big") + iv[4:]


def _decrypt_control_frame(
    stream: BinaryIO,
    *,
    keys: Keys,
    iv: bytes,
    header_version: int | None,
) -> BackupFrame:
    hmac = HMAC(keys.hmac_key, SHA256())
    cipher = Cipher(AES(keys.cipher_key), CTR(iv))
    decryptor = cipher.decryptor()
    if header_version is None or header_version == 0:
        raw_len = stream.read(4)
        if len(raw_len) != 4:
            msg = "EOF before frame length"
            raise SignalBackupError(msg)
        (length,) = struct.unpack(">I", raw_len)
    elif header_version == 1:
        enc_len = stream.read(4)
        if len(enc_len) != 4:
            msg = "EOF before encrypted frame length"
            raise SignalBackupError(msg)
        hmac.update(enc_len)
        dec_len = decryptor.update(enc_len)
        (length,) = struct.unpack(">I", dec_len)
    else:
        msg = f"unsupported backup header version {header_version}"
        raise UnsupportedVersionError(msg)

    if length < 10:
        msg = f"frame length too small ({length})"
        raise SignalBackupError(msg)
    ciphertext = stream.read(length - 10)
    if len(ciphertext) != length - 10:
        msg = "EOF inside frame ciphertext"
        raise SignalBackupError(msg)
    their_mac = stream.read(10)
    hmac.update(ciphertext)
    our_mac = hmac.finalize()[:10]
    if their_mac != our_mac:
        raise MACMismatchError("Bad MAC — wrong passphrase or corrupted backup")
    plaintext = decryptor.update(ciphertext) + decryptor.finalize()
    return decode_backup_frame(plaintext)


def _stream_payload(
    stream: BinaryIO,
    *,
    keys: Keys,
    iv: bytes,
    length: int,
    chunk: int = 64 * 1024,
) -> Iterator[bytes]:
    hmac = HMAC(keys.hmac_key, SHA256())
    hmac.update(iv)
    cipher = Cipher(AES(keys.cipher_key), CTR(iv))
    decryptor = cipher.decryptor()
    remaining = length
    while remaining > 0:
        take = min(chunk, remaining)
        ct = stream.read(take)
        if len(ct) != take:
            msg = "EOF inside attachment payload"
            raise SignalBackupError(msg)
        remaining -= take
        hmac.update(ct)
        yield decryptor.update(ct)
    their_mac = stream.read(10)
    our_mac = hmac.finalize()[:10]
    if their_mac != our_mac:
        raise MACMismatchError("Bad MAC inside attachment payload")
    yield decryptor.finalize()


def _sql_param_to_native(p: Any) -> Any:
    if p.is_null:
        return None
    if p.string_value is not None:
        return p.string_value
    if p.int_value is not None:
        # Signal stores int64 as unsigned varint — sign-extend.
        i = p.int_value
        if i & (1 << 63):
            i -= 1 << 64
        return i
    if p.double_value is not None:
        return p.double_value
    if p.blob_value is not None:
        return p.blob_value
    return None


_SKIP_PREFIXES = (
    "create table sqlite_",
    "create index sqlite_",
)


def _should_skip_stmt(stmt: str) -> bool:
    low = stmt.lstrip().lower()
    if low.startswith(_SKIP_PREFIXES):
        return True
    # Full-text-search shadow tables — Signal versions have varied names.
    if "sms_fts" in low or "mms_fts" in low or "message_fts" in low:
        return True
    return False


@dataclass(slots=True)
class DecryptedBackup:
    db_path: Path
    attachments_dir: Path
    stickers_dir: Path
    avatars_dir: Path
    preferences_path: Path
    key_values_path: Path
    stats: DecryptStats = field(default_factory=DecryptStats)


def decrypt_backup(
    backup_path: Path,
    passphrase: str,
    out_dir: Path,
    *,
    progress: Any = None,
) -> DecryptedBackup:
    """Decrypt ``backup_path`` (a Signal Android .backup) into ``out_dir``.

    Produces ``database.sqlite``, ``attachments/``, ``stickers/``, ``avatars/``,
    ``preferences.json``, ``key_value.json``. Returns a ``DecryptedBackup``
    describing the result.

    ``progress`` is an optional callable receiving ``(bytes_read, total_bytes)``
    after every control frame so a CLI can render a progress bar.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    attachments_dir = out_dir / "attachments"
    stickers_dir = out_dir / "stickers"
    avatars_dir = out_dir / "avatars"
    for d in (attachments_dir, stickers_dir, avatars_dir):
        d.mkdir(parents=True, exist_ok=True)

    db_path = out_dir / "database.sqlite"
    if db_path.exists():
        db_path.unlink()

    total_bytes = backup_path.stat().st_size
    stats = DecryptStats()
    preferences: dict[str, dict[str, dict[str, Any]]] = {}
    key_values: dict[str, dict[str, Any]] = {}

    # `with conn:` only commits — it never closes. `closing` around it keeps the
    # connection from leaking into GC (which surfaces as a finalizer error).
    with (
        backup_path.open("rb") as stream,
        closing(sqlite3.connect(db_path)) as conn,
        conn,
    ):
        header_frame = _read_initial_header(stream)
        assert header_frame.header is not None
        iv = header_frame.header.iv
        salt = header_frame.header.salt
        header_version = header_frame.header.version

        keys = derive_keys(passphrase, salt)

        cur = conn.cursor()
        while True:
            frame = _decrypt_control_frame(
                stream, keys=keys, iv=iv, header_version=header_version
            )
            iv = _increment_iv(iv)

            if frame.end:
                break
            if frame.db_version is not None:
                stats.db_version = frame.db_version.version
                cur.execute(f"PRAGMA user_version = {frame.db_version.version:d}")
            elif frame.statement is not None:
                if _should_skip_stmt(frame.statement.statement):
                    stats.skipped_statements += 1
                else:
                    params = tuple(_sql_param_to_native(p) for p in frame.statement.parameters)
                    try:
                        cur.execute(frame.statement.statement, params)
                    except sqlite3.OperationalError as exc:
                        # Some Signal schema rows include statements that depend
                        # on extensions or DDL ordering quirks. Log and skip.
                        log.debug("skip sql: %s — %s", frame.statement.statement[:120], exc)
                        stats.skipped_statements += 1
                    else:
                        stats.statements += 1
            elif frame.preference is not None:
                pref = frame.preference
                bucket = preferences.setdefault(pref.file or "", {})
                entry: dict[str, Any] = {}
                if pref.value is not None:
                    entry["value"] = pref.value
                if pref.boolean_value is not None:
                    entry["booleanValue"] = pref.boolean_value
                if pref.is_string_set:
                    entry["stringSetValue"] = list(pref.string_set)
                bucket[pref.key or ""] = entry
                stats.preferences += 1
            elif frame.key_value is not None:
                kv = frame.key_value
                d: dict[str, Any] = {}
                if kv.string_value is not None:
                    d["stringValue"] = kv.string_value
                if kv.integer_value is not None:
                    d["integerValue"] = kv.integer_value
                if kv.long_value is not None:
                    d["longValue"] = kv.long_value
                if kv.boolean_value is not None:
                    d["booleanValue"] = kv.boolean_value
                if kv.blob_value is not None:
                    import base64

                    d["blobBase64"] = base64.b64encode(kv.blob_value).decode("ascii")
                key_values[kv.key or ""] = d
                stats.key_values += 1
            elif frame.attachment is not None:
                _write_payload(
                    stream,
                    keys,
                    iv,
                    frame.attachment.length,
                    attachments_dir / f"{frame.attachment.attachment_id or frame.attachment.row_id}.bin",
                )
                iv = _increment_iv(iv)
                stats.attachments += 1
            elif frame.sticker is not None:
                _write_payload(
                    stream,
                    keys,
                    iv,
                    frame.sticker.length,
                    stickers_dir / f"{frame.sticker.row_id}.bin",
                )
                iv = _increment_iv(iv)
                stats.stickers += 1
            elif frame.avatar is not None:
                name = frame.avatar.recipient_id or frame.avatar.name or "unknown"
                _write_payload(
                    stream,
                    keys,
                    iv,
                    frame.avatar.length,
                    avatars_dir / f"{name}.bin",
                )
                iv = _increment_iv(iv)
                stats.avatars += 1
            # else: empty frame — ignore.

            stats.bytes_read = stream.tell()
            if progress is not None:
                progress(stats.bytes_read, total_bytes)

        conn.commit()

    (out_dir / "preferences.json").write_text(
        json.dumps(preferences, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out_dir / "key_value.json").write_text(
        json.dumps(key_values, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return DecryptedBackup(
        db_path=db_path,
        attachments_dir=attachments_dir,
        stickers_dir=stickers_dir,
        avatars_dir=avatars_dir,
        preferences_path=out_dir / "preferences.json",
        key_values_path=out_dir / "key_value.json",
        stats=stats,
    )


def _write_payload(
    stream: BinaryIO, keys: Keys, iv: bytes, length: int, dst: Path
) -> None:
    with dst.open("wb") as f:
        for chunk in _stream_payload(stream, keys=keys, iv=iv, length=length):
            f.write(chunk)
