"""Wrapper for decrypting WA backup files into a plain msgstore.db.

This runs `wa-crypt-tools` as a subprocess, so the heavy crypto stays in that
audited project. We provide a clean Python API and consistent error types.
Preferred form is `python -m wa_crypt_tools.wadecrypt` with our own interpreter,
falling back to the `wadecrypt` console script on PATH.

The CLI accepts either a Java-keystore-format key file or the raw 64-character
hex key as a positional argument. chatvault stores the hex key in
`$XDG_CONFIG_HOME/chatvault/wa.key` and passes it as the hex argument.
"""

from __future__ import annotations

import importlib.util
import logging
import shutil
import subprocess
import sys
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


def _module_available() -> bool:
    """Whether `wa_crypt_tools` is importable by the interpreter we run under."""
    try:
        return importlib.util.find_spec("wa_crypt_tools") is not None
    except (ImportError, ValueError):
        return False


def _decrypt_command() -> list[str]:
    """Build the wa-crypt-tools invocation, preferring `-m` over a console script.

    pip bakes the absolute path of the installing interpreter into a console
    script's shebang, so `wadecrypt` stops working the moment that Python is
    removed — a routine Python minor upgrade breaks it. Running the module with
    our own `sys.executable` has no shebang to rot. The PATH lookup stays as a
    fallback for installs that are not visible to this interpreter (a global
    `uv tool install`, say).
    """
    if _module_available():
        return [sys.executable, "-m", "wa_crypt_tools.wadecrypt"]
    for candidate in ("wadecrypt", "wadecryptgui", "wa-crypt-tools"):
        found = shutil.which(candidate)
        if found:
            return [found]
    msg = (
        "wa-crypt-tools not installed. Install with `pip install wa-crypt-tools` or "
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
    command = _decrypt_command()

    log.info("Decrypting %s → %s", encrypted, output)
    # Passing the hex key as the positional `keyfile` arg — wa-crypt-tools
    # accepts either a key-file or a hex string in that slot.
    try:
        proc = subprocess.run(
            [*command, hex_key, str(encrypted), str(output)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        # `shutil.which` only checks that the file exists and is executable, so a
        # script whose shebang points at a since-removed interpreter gets this far
        # and fails at exec time. Report that instead of a bare FileNotFoundError.
        msg = (
            f"found {command[0]} but could not run it ({exc}). The installed "
            "wa-crypt-tools may target an old Python; reinstall it."
        )
        raise DecryptError(msg) from exc
    if proc.returncode != 0:
        msg = (
            f"decrypt failed (exit {proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}"
        )
        raise DecryptError(msg)
    if not output.exists() or output.stat().st_size == 0:
        msg = f"decrypt produced empty output at {output}"
        raise DecryptError(msg)

    return DecryptResult(db_path=output, source=encrypted, bytes_written=output.stat().st_size)
