"""SQLite connection management and migration runner."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger(__name__)

MIGRATIONS_PACKAGE = "chatvault.migrations"


def list_migrations() -> list[tuple[int, str]]:
    """Return [(version, filename)] for all migration scripts, sorted ascending."""
    out: list[tuple[int, str]] = []
    pkg = resources.files(MIGRATIONS_PACKAGE)
    for entry in pkg.iterdir():
        name = entry.name
        if not name.endswith(".sql"):
            continue
        head = name.split("_", 1)[0]
        try:
            version = int(head)
        except ValueError:
            continue
        out.append((version, name))
    out.sort()
    return out


def read_migration(name: str) -> str:
    return (resources.files(MIGRATIONS_PACKAGE) / name).read_text(encoding="utf-8")


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Read `_meta.schema_version` (defaults to 0 for an uninitialised db)."""
    try:
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    if row is None:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return 0


def apply_pending_migrations(conn: sqlite3.Connection) -> list[int]:
    """Apply all migrations whose version is greater than the current schema version.

    Returns the list of versions applied (empty if up-to-date).
    """
    current = get_schema_version(conn)
    applied: list[int] = []
    for version, name in list_migrations():
        if version <= current:
            continue
        log.info("Applying migration %s", name)
        sql = read_migration(name)
        with conn:
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO _meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(version),),
            )
        applied.append(version)
    if applied:
        log.info("Migrations applied: %s", applied)
    return applied


def connect(db_path: Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a SQLite connection with sensible pragmas. Creates parent dirs."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if read_only:
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = OFF")  # explicit; we handle ordering
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


def init_db(db_path: Path) -> sqlite3.Connection:
    """Open + apply all pending migrations. Returns an open connection."""
    conn = connect(db_path)
    apply_pending_migrations(conn)
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Context manager that begins+commits/rollbacks a transaction."""
    try:
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def get_state(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute(
        "SELECT value FROM extraction_state WHERE key = ?", (key,)
    ).fetchone()
    return row[0] if row else default


def set_state(conn: sqlite3.Connection, key: str, value: str | None) -> None:
    conn.execute(
        "INSERT INTO extraction_state(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET "
        "value = excluded.value, "
        "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        (key, value),
    )


def get_state_int(conn: sqlite3.Connection, key: str, default: int = 0) -> int:
    raw = get_state(conn, key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def set_state_int(conn: sqlite3.Connection, key: str, value: int) -> None:
    set_state(conn, key, str(value))


def upsert(
    conn: sqlite3.Connection,
    table: str,
    row: dict[str, Any],
    *,
    key_columns: list[str],
) -> None:
    """Generic INSERT … ON CONFLICT(keys) DO UPDATE.

    `row` is a flat dict; keys become column names. `key_columns` is the conflict
    target — typically the primary key. All non-key columns are updated on conflict.
    """
    cols = list(row.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_list = ", ".join(cols)
    update_cols = [c for c in cols if c not in key_columns]
    if update_cols:
        update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(key_columns)}) DO UPDATE SET {update_clause}"
        )
    else:
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(key_columns)}) DO NOTHING"
        )
    conn.execute(sql, [row[c] for c in cols])


def insert_or_ignore(
    conn: sqlite3.Connection, table: str, row: dict[str, Any]
) -> None:
    """INSERT OR IGNORE for tables where re-seeing the same row is a no-op."""
    cols = list(row.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_list = ", ".join(cols)
    sql = f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"
    conn.execute(sql, [row[c] for c in cols])
