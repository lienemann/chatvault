"""Configuration: XDG paths, secrets handling, runtime settings."""

from __future__ import annotations

import contextlib
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _xdg(env: str, fallback: str) -> Path:
    raw = os.environ.get(env)
    return Path(raw).expanduser() if raw else Path.home() / fallback


@dataclass(frozen=True, slots=True)
class Paths:
    """Canonical filesystem locations for chatvault state.

    Honours XDG Base Directory: $XDG_CONFIG_HOME, $XDG_DATA_HOME, $XDG_CACHE_HOME,
    $XDG_STATE_HOME. Override the root with $CHATVAULT_HOME for testing.
    """

    config_dir: Path
    data_dir: Path
    cache_dir: Path
    state_dir: Path

    @classmethod
    def default(cls) -> Paths:
        if (override := os.environ.get("CHATVAULT_HOME")) is not None:
            root = Path(override).expanduser()
            return cls(
                config_dir=root / "config",
                data_dir=root / "data",
                cache_dir=root / "cache",
                state_dir=root / "state",
            )
        return cls(
            config_dir=_xdg("XDG_CONFIG_HOME", ".config") / "chatvault",
            data_dir=_xdg("XDG_DATA_HOME", ".local/share") / "chatvault",
            cache_dir=_xdg("XDG_CACHE_HOME", ".cache") / "chatvault",
            state_dir=_xdg("XDG_STATE_HOME", ".local/state") / "chatvault",
        )

    @property
    def db_path(self) -> Path:
        return self.data_dir / "archive.db"

    @property
    def media_dir(self) -> Path:
        return self.data_dir / "media"

    @property
    def staging_dir(self) -> Path:
        return self.cache_dir / "staging"

    @property
    def key_path(self) -> Path:
        return self.config_dir / "wa.key"

    @property
    def config_file(self) -> Path:
        return self.config_dir / "config.toml"

    @property
    def log_file(self) -> Path:
        return self.state_dir / "chatvault.log"

    def ensure(self) -> None:
        """Create all directories with sensible permissions."""
        for d in (
            self.config_dir,
            self.data_dir,
            self.cache_dir,
            self.state_dir,
            self.media_dir,
            self.staging_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)
        # Config dir holds the key — restrict.
        with contextlib.suppress(OSError):
            os.chmod(self.config_dir, 0o700)


def read_key(path: Path) -> str | None:
    """Read the 64-character backup key from disk. Returns None if not set."""
    if not path.exists():
        return None
    raw = path.read_text(encoding="ascii").strip()
    if len(raw) != 64 or not all(c in "0123456789abcdefABCDEF" for c in raw):
        msg = f"key at {path} is not 64 hex characters"
        raise ValueError(msg)
    return raw.lower()


def write_key(path: Path, key: str) -> None:
    """Write the key with chmod 600. Validates format."""
    key = key.strip().lower()
    if len(key) != 64 or not all(c in "0123456789abcdef" for c in key):
        msg = "key must be 64 hex characters"
        raise ValueError(msg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(key, encoding="ascii")
    os.chmod(path, 0o600)


def load_config(path: Path) -> dict[str, Any]:
    """Read config.toml. Empty dict if missing or unreadable."""
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return {}


@dataclass(frozen=True, slots=True)
class Settings:
    """Thin wrapper around the parsed config.toml. Use dotted ``get`` lookups."""

    raw: dict[str, Any]

    @classmethod
    def load(cls, config_dir: Path) -> Settings:
        return cls(raw=load_config(config_dir / "config.toml"))

    def get(self, *keys: str, default: Any = None) -> Any:
        cur: Any = self.raw
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
            if cur is None:
                return default
        return cur


def read_api_key_from_file(path: Path) -> str:
    """Read an API key from a chmod-600 file. Refuses loose permissions.

    A leaked API key is a real cost incident, so we hard-fail rather than warn
    when the file is group- or world-readable.
    """
    if not path.exists():
        msg = f"api_key_file {path} does not exist"
        raise FileNotFoundError(msg)
    st = path.stat()
    if st.st_mode & 0o077:
        msg = (
            f"api_key_file {path} has loose permissions {oct(st.st_mode & 0o777)} — "
            "run `chmod 600` and try again."
        )
        raise PermissionError(msg)
    key = path.read_text(encoding="utf-8").strip()
    if not key:
        msg = f"api_key_file {path} is empty"
        raise ValueError(msg)
    return key


def resolve_api_key(
    *,
    env_var: str,
    settings: Settings,
    section: str,
) -> str:
    """Resolve an API key for ``[section]`` using config.toml + environment.

    Order:
    1. ``$env_var`` (or the env var named in ``[section].api_key_env``) if set.
    2. ``[section].api_key_file`` — read the chmod-600 file at that path.
    3. RuntimeError describing both options.
    """
    env_name = settings.get(section, "api_key_env", default=env_var)
    if isinstance(env_name, str) and env_name:
        v = os.environ.get(env_name)
        if v:
            return v
    file_setting = settings.get(section, "api_key_file")
    if isinstance(file_setting, str) and file_setting.strip():
        return read_api_key_from_file(Path(os.path.expanduser(file_setting)))
    msg = (
        f"No API key found for [{section}]. Either export ${env_name}, or put the key "
        f"into a chmod-600 file and add `api_key_file = \"<path>\"` under [{section}] "
        "in config.toml."
    )
    raise RuntimeError(msg)


def owner_name_from_config(config_dir: Path, default: str = "Me") -> str:
    """Read [owner].name from config.toml, falling back to `default`."""
    cfg = load_config(config_dir / "config.toml")
    owner = cfg.get("owner") if isinstance(cfg.get("owner"), dict) else {}
    name = owner.get("name") if isinstance(owner, dict) else None
    if isinstance(name, str) and name.strip():
        return name.strip()
    return default
