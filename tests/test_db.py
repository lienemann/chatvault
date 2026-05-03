"""Migration runner and state helpers."""

from __future__ import annotations

from chatvault.db import (
    apply_pending_migrations,
    get_schema_version,
    get_state,
    get_state_int,
    list_migrations,
    set_state,
    set_state_int,
)


def test_migrations_listed_in_order() -> None:
    versions = [v for v, _ in list_migrations()]
    assert versions == sorted(versions)
    assert versions[0] == 1


def test_apply_migrations_is_idempotent(archive_db) -> None:
    assert get_schema_version(archive_db) == 1
    applied = apply_pending_migrations(archive_db)
    assert applied == []  # already up-to-date


def test_state_round_trip(archive_db) -> None:
    set_state(archive_db, "k", "v")
    assert get_state(archive_db, "k") == "v"
    assert get_state(archive_db, "missing") is None
    assert get_state(archive_db, "missing", "default") == "default"


def test_state_int_round_trip(archive_db) -> None:
    set_state_int(archive_db, "rid", 42)
    assert get_state_int(archive_db, "rid") == 42
    assert get_state_int(archive_db, "missing", default=99) == 99
