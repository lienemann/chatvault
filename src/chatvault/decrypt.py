"""Wrapper for decrypting WA backup files into a plain msgstore.db.

This calls the `wa-crypt-tools` CLI as a subprocess, so the heavy crypto stays in
that audited project. We provide a clean Python API and consistent error types.

The CLI accepts either a Java-keystore-format key file or the raw 64-character
hex key as a positional argument. chatvault stores the hex key in
`$XDG_CONFIG_HOME/chatvault/wa.key` and passes it as the hex argument.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import read_key

log = logging.getLogger(__name__)


class DecryptError(RuntimeError):
    """Raised when a backup cannot be decrypted."""


@dataclass(frozen=True, slots=True)
class DecryptResult:
    db_path: Path
    source: Path
    bytes_written: int


def _find_decrypt_binary() -> str:
    """Locate the `wadecrypt` (or compatible) CLI."""
    for candidate in ("wadecrypt", "wadecryptgui", "wa-crypt-tools"):
        if shutil.which(candidate):
            return candidate
    msg = (
        "wa-crypt-tools not on PATH. Install with `pip install wa-crypt-tools` or "
        "`uv tool install wa-crypt-tools`."
    )
    raise DecryptError(msg)


def decrypt(
    encrypted: Path,
    *,
    key_path: Path,
    output: Path,
    overwrite: bool = True,
) -> DecryptResult:
    """Decrypt a `.crypt15` backup to plaintext.

    Parameters:
        encrypted: path to the source .crypt15 file
        key_path:  path to the 64-character backup key (hex, in a plain file)
        output:    where to write the decrypted .db
        overwrite: if False, raise when output already exists
    """
    if not encrypted.exists():
        msg = f"encrypted backup not found: {encrypted}"
        raise DecryptError(msg)
    try:
        hex_key = read_key(key_path)
    except ValueError as exc:
        raise DecryptError(str(exc)) from None
    if hex_key is None:
        msg = f"key file not found: {key_path}"
        raise DecryptError(msg)
    if output.exists():
        if not overwrite:
            msg = f"output exists and overwrite=False: {output}"
            raise DecryptError(msg)
        output.unlink()

    output.parent.mkdir(parents=True, exist_ok=True)
    binary = _find_decrypt_binary()

    log.info("Decrypting %s → %s", encrypted, output)
    # Passing the hex key as the positional `keyfile` arg — wa-crypt-tools
    # accepts either a key-file or a hex string in that slot.
    proc = subprocess.run(
        [binary, hex_key, str(encrypted), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        msg = (
            f"decrypt failed (exit {proc.returncode}): "
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )
        raise DecryptError(msg)
    if not output.exists() or output.stat().st_size == 0:
        msg = f"decrypt produced empty output at {output}"
        raise DecryptError(msg)

    return DecryptResult(db_path=output, source=encrypted, bytes_written=output.stat().st_size)
