"""Round-trip tests for the Signal backup decryptor.

Builds a minimal synthetic backup file using the same crypto primitives in
reverse, then runs ``decrypt_backup`` on it and asserts the rebuilt SQLite
database contains the expected rows.
"""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from chatvault.sources.signal import decrypt as sd
from chatvault.sources.signal.proto import BACKUPS_PROTO_REFERENCE


# ---------------------------------------------------------------------------
# Protobuf encoding helpers (mirror tests/sources/test_signal_proto.py).
# ---------------------------------------------------------------------------


def _enc_varint(v: int) -> bytes:
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _tag(num: int, wire: int) -> bytes:
    return _enc_varint((num << 3) | wire)


def _str(num: int, s: str) -> bytes:
    r = s.encode("utf-8")
    return _tag(num, 2) + _enc_varint(len(r)) + r


def _int(num: int, v: int) -> bytes:
    return _tag(num, 0) + _enc_varint(v)


def _b(num: int, raw: bytes) -> bytes:
    return _tag(num, 2) + _enc_varint(len(raw)) + raw


def _msg(num: int, inner: bytes) -> bytes:
    return _tag(num, 2) + _enc_varint(len(inner)) + inner


# ---------------------------------------------------------------------------
# Synthetic backup builder.
# ---------------------------------------------------------------------------


def _build_header_frame(iv: bytes, salt: bytes) -> bytes:
    """Header frame is plaintext: 4-byte BE length + BackupFrame{Header}."""
    inner = _b(1, iv) + _b(2, salt)  # version omitted → plaintext lengths thereafter
    header = _msg(1, inner)  # field 1 of BackupFrame
    return struct.pack(">I", len(header)) + header


def _build_ctrl_frame(plaintext_proto: bytes, keys: sd.Keys, iv: bytes) -> bytes:
    """Encrypt one control frame: length || ciphertext || mac10."""
    from cryptography.hazmat.primitives.ciphers import Cipher
    from cryptography.hazmat.primitives.ciphers.algorithms import AES
    from cryptography.hazmat.primitives.ciphers.modes import CTR
    from cryptography.hazmat.primitives.hashes import SHA256
    from cryptography.hazmat.primitives.hmac import HMAC

    enc = Cipher(AES(keys.cipher_key), CTR(iv)).encryptor()
    ct = enc.update(plaintext_proto) + enc.finalize()
    mac = HMAC(keys.hmac_key, SHA256())
    mac.update(ct)
    mac10 = mac.finalize()[:10]
    length = len(ct) + 10
    return struct.pack(">I", length) + ct + mac10


def _build_attachment_payload(plaintext: bytes, keys: sd.Keys, iv: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers import Cipher
    from cryptography.hazmat.primitives.ciphers.algorithms import AES
    from cryptography.hazmat.primitives.ciphers.modes import CTR
    from cryptography.hazmat.primitives.hashes import SHA256
    from cryptography.hazmat.primitives.hmac import HMAC

    enc = Cipher(AES(keys.cipher_key), CTR(iv)).encryptor()
    ct = enc.update(plaintext) + enc.finalize()
    mac = HMAC(keys.hmac_key, SHA256())
    mac.update(iv)
    mac.update(ct)
    return ct + mac.finalize()[:10]


def _statement_frame(sql: str) -> bytes:
    return _msg(2, _str(1, sql))


def _statement_frame_with_param(sql: str, value: str) -> bytes:
    inner = _str(1, sql) + _msg(2, _str(1, value))
    return _msg(2, inner)


def _end_frame() -> bytes:
    return _int(6, 1)


def _db_version_frame(v: int) -> bytes:
    return _msg(5, _int(1, v))


def _attachment_frame(row_id: int, attachment_id: int, length: int) -> bytes:
    return _msg(4, _int(1, row_id) + _int(2, attachment_id) + _int(3, length))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def synthetic_backup(tmp_path: Path) -> tuple[Path, str]:
    passphrase = "00000 00000 00000 00000 00000 00000"
    iv = b"\x10" * 16
    salt = b"\x20" * 32
    keys = sd.derive_keys(passphrase, salt)

    buf = bytearray()
    buf += _build_header_frame(iv, salt)

    cur_iv = iv
    buf += _build_ctrl_frame(_db_version_frame(42), keys, cur_iv)
    cur_iv = sd._increment_iv(cur_iv)

    buf += _build_ctrl_frame(
        _statement_frame("CREATE TABLE t (id INTEGER, name TEXT)"), keys, cur_iv
    )
    cur_iv = sd._increment_iv(cur_iv)

    buf += _build_ctrl_frame(
        _statement_frame_with_param("INSERT INTO t VALUES (1, ?)", "alpha"), keys, cur_iv
    )
    cur_iv = sd._increment_iv(cur_iv)

    # Attachment: control frame + raw encrypted payload.
    payload = b"hello world\n"
    buf += _build_ctrl_frame(_attachment_frame(1, 77, len(payload)), keys, cur_iv)
    cur_iv = sd._increment_iv(cur_iv)
    buf += _build_attachment_payload(payload, keys, cur_iv)
    cur_iv = sd._increment_iv(cur_iv)

    buf += _build_ctrl_frame(_end_frame(), keys, cur_iv)

    backup = tmp_path / "fake.backup"
    backup.write_bytes(bytes(buf))
    return backup, passphrase


def test_decrypt_synthetic_backup(synthetic_backup: tuple[Path, str], tmp_path: Path) -> None:
    backup_path, passphrase = synthetic_backup
    out = tmp_path / "out"
    result = sd.decrypt_backup(backup_path, passphrase, out)

    assert result.stats.db_version == 42
    assert result.stats.statements == 2
    assert result.stats.attachments == 1
    assert (out / "attachments" / "77.bin").read_bytes() == b"hello world\n"

    import sqlite3

    conn = sqlite3.connect(result.db_path)
    rows = list(conn.execute("SELECT id, name FROM t").fetchall())
    conn.close()
    assert rows == [(1, "alpha")]


def test_wrong_passphrase_raises_mac(synthetic_backup: tuple[Path, str], tmp_path: Path) -> None:
    backup_path, _ = synthetic_backup
    with pytest.raises(sd.MACMismatchError):
        sd.decrypt_backup(backup_path, "wrong-key", tmp_path / "out")


def test_iv_increment_wraps() -> None:
    iv = b"\xff\xff\xff\xff" + b"\x00" * 12
    out = sd._increment_iv(iv)
    assert out == b"\x00\x00\x00\x00" + b"\x00" * 12


def test_reference_proto_string_carries_known_messages() -> None:
    # Sanity that the bundled proto reference still names all sub-messages.
    for name in (
        "SqlStatement",
        "SharedPreference",
        "Attachment",
        "Sticker",
        "Avatar",
        "DatabaseVersion",
        "Header",
        "KeyValue",
        "BackupFrame",
    ):
        assert name in BACKUPS_PROTO_REFERENCE
