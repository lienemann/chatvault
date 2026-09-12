"""Tests for the wa-crypt-tools subprocess wrapper."""

from __future__ import annotations

import sys

import pytest

from chatvault import decrypt as decrypt_mod
from chatvault.decrypt import DecryptError, decrypt


@pytest.fixture
def key_file(tmp_path):
    path = tmp_path / "wa.key"
    path.write_text("0" * 64)
    return path


def test_command_prefers_our_own_interpreter(monkeypatch):
    """With the module importable we run it via -m, not via a console script."""
    monkeypatch.setattr(decrypt_mod, "_module_available", lambda: True)
    assert decrypt_mod._decrypt_command() == [sys.executable, "-m", "wa_crypt_tools.wadecrypt"]


def test_command_falls_back_to_console_script(monkeypatch):
    """A global-only install is still usable through PATH."""
    monkeypatch.setattr(decrypt_mod, "_module_available", lambda: False)
    monkeypatch.setattr(
        decrypt_mod.shutil,
        "which",
        lambda name: "/usr/bin/wadecrypt" if name == "wadecrypt" else None,
    )
    assert decrypt_mod._decrypt_command() == ["/usr/bin/wadecrypt"]


def test_command_missing_everywhere_raises(monkeypatch):
    monkeypatch.setattr(decrypt_mod, "_module_available", lambda: False)
    monkeypatch.setattr(decrypt_mod.shutil, "which", lambda name: None)
    with pytest.raises(DecryptError, match="wa-crypt-tools"):
        decrypt_mod._decrypt_command()


def test_unrunnable_binary_raises_decrypt_error(tmp_path, key_file, monkeypatch):
    """A console script with a dead shebang passes `which` but fails at exec time."""
    binary = tmp_path / "wadecrypt"
    binary.write_text("#!/nonexistent/python3.13\n")
    binary.chmod(0o755)
    monkeypatch.setattr(decrypt_mod, "_decrypt_command", lambda: [str(binary)])

    encrypted = tmp_path / "msgstore.db.crypt15"
    encrypted.write_bytes(b"ciphertext")

    with pytest.raises(DecryptError, match="could not run it"):
        decrypt(encrypted, key_path=key_file, output=tmp_path / "out.db")


def test_missing_backup_raises_decrypt_error(tmp_path, key_file):
    with pytest.raises(DecryptError, match="encrypted backup not found"):
        decrypt(tmp_path / "absent.crypt15", key_path=key_file, output=tmp_path / "out.db")
