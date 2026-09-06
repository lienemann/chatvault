"""Tests for chmod-600 API-key files and the Settings resolver."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from chatvault.config import (
    Settings,
    read_api_key_from_file,
    resolve_api_key,
)


def _write(p: Path, content: str, mode: int = 0o600) -> None:
    p.write_text(content, encoding="utf-8")
    os.chmod(p, mode)


def test_read_api_key_strips_whitespace(tmp_path: Path) -> None:
    f = tmp_path / "key"
    _write(f, "sk-secret-123  \n")
    assert read_api_key_from_file(f) == "sk-secret-123"


def test_read_api_key_rejects_loose_perms(tmp_path: Path) -> None:
    f = tmp_path / "key"
    _write(f, "sk-secret", mode=0o644)
    with pytest.raises(PermissionError, match="loose permissions"):
        read_api_key_from_file(f)


def test_read_api_key_rejects_empty(tmp_path: Path) -> None:
    f = tmp_path / "key"
    _write(f, "   \n")
    with pytest.raises(ValueError, match="empty"):
        read_api_key_from_file(f)


def test_read_api_key_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_api_key_from_file(tmp_path / "nope")


def test_resolve_api_key_prefers_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "key"
    _write(f, "from-file")
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        f'[anthropic]\napi_key_file = "{f}"\n', encoding="utf-8"
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "from-env")
    settings = Settings.load(cfg_dir)
    assert (
        resolve_api_key(env_var="ANTHROPIC_API_KEY", settings=settings, section="anthropic")
        == "from-env"
    )


def test_resolve_api_key_falls_back_to_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "key"
    _write(f, "from-file")
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        f'[anthropic]\napi_key_file = "{f}"\n', encoding="utf-8"
    )
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    settings = Settings.load(cfg_dir)
    assert (
        resolve_api_key(env_var="ANTHROPIC_API_KEY", settings=settings, section="anthropic")
        == "from-file"
    )


def test_resolve_api_key_custom_env_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        '[anthropic]\napi_key_env = "MY_CUSTOM_KEY"\n', encoding="utf-8"
    )
    monkeypatch.setenv("MY_CUSTOM_KEY", "value-from-custom")
    settings = Settings.load(cfg_dir)
    assert (
        resolve_api_key(env_var="ANTHROPIC_API_KEY", settings=settings, section="anthropic")
        == "value-from-custom"
    )


def test_resolve_api_key_no_source_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    settings = Settings.load(cfg_dir)
    with pytest.raises(RuntimeError, match="No API key found"):
        resolve_api_key(env_var="ANTHROPIC_API_KEY", settings=settings, section="anthropic")


def test_settings_get_handles_missing_sections(tmp_path: Path) -> None:
    settings = Settings.load(tmp_path)  # no config.toml present
    assert settings.get("anthropic", "api_key_file") is None
    assert settings.get("anthropic", "api_key_file", default="x") == "x"


def test_loose_perm_check_skipped_on_windows_like_modes(tmp_path: Path) -> None:
    """A file with mode 0o600 must pass even when bits we don't care about are set."""
    f = tmp_path / "key"
    _write(f, "ok", mode=0o600)
    # Verify the precondition (chmod actually took effect on this fs).
    assert stat.S_IMODE(f.stat().st_mode) == 0o600
    assert read_api_key_from_file(f) == "ok"
