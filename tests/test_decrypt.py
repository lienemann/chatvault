"""Tests for the wa-crypt-tools subprocess wrapper."""

from __future__ import annotations

import pytest

from chatvault.decrypt import DecryptError, decrypt


@pytest.fixture
def key_file(tmp_path):
    path = tmp_path / "wa.key"
    path.write_text("0" * 64)
    return path


def test_unrunnable_binary_raises_decrypt_error(tmp_path, key_file, monkeypatch):
    """A binary with a dead shebang passes `which` but fails at exec time."""
    binary = tmp_path / "wadecrypt"
    binary.write_text("#!/nonexistent/python3.13\n")
    binary.chmod(0o755)
    monkeypatch.setattr("chatvault.decrypt._find_decrypt_binary", lambda: str(binary))

    encrypted = tmp_path / "msgstore.db.crypt15"
    encrypted.write_bytes(b"ciphertext")

    with pytest.raises(DecryptError, match="could not run it"):
        decrypt(encrypted, key_path=key_file, output=tmp_path / "out.db")


def test_missing_backup_raises_decrypt_error(tmp_path, key_file):
    with pytest.raises(DecryptError, match="encrypted backup not found"):
        decrypt(tmp_path / "absent.crypt15", key_path=key_file, output=tmp_path / "out.db")
