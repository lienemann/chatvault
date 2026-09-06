"""Tests for chat-slug routing and per-chat media mirror layout."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from chatvault.db import init_db
from chatvault.media.layout import (
    ORPHANS_DIR,
    chat_slug,
    pin_slug,
    resolve_chat_slug,
)
from chatvault.media_mirror import (
    FlatLayoutBlocked,
    get_layout,
    migrate_flat_to_per_chat,
    snapshot_pass,
)


# ---------------------------------------------------------------------------
# Slug
# ---------------------------------------------------------------------------


def test_chat_slug_sanitises_and_appends_hash() -> None:
    s = chat_slug("12345@s.whatsapp.net", "Birthday Alice 🎉")
    assert s.startswith("birthday-alice")
    # six hex characters at the end after the last dash: sha1(jid)[:6].
    assert s.split("-")[-1] == "9aa9e1"
    assert len(s.split("-")[-1]) == 6


def test_chat_slug_strips_accents() -> None:
    s = chat_slug("g1@g.us", "Café Crème")
    assert "café" not in s
    assert s.startswith("cafe-creme")


def test_chat_slug_falls_back_for_empty_name() -> None:
    s = chat_slug("123@s.whatsapp.net", "")
    assert s.startswith("chat-")


def test_resolve_chat_slug_pins_first_seen_name(tmp_path: Path) -> None:
    """Once pinned, a chat's slug must NOT change even if the chat is renamed."""
    archive = init_db(tmp_path / "a.db")
    try:
        archive.execute(
            "INSERT INTO chats(jid, kind, subject, source) "
            "VALUES('g1@g.us', 'group', 'Original Name', 'whatsapp')"
        )
        first = resolve_chat_slug(archive, "g1@g.us")
        assert first.startswith("original-name-")
        # Rename in the source.
        archive.execute(
            "UPDATE chats SET subject = 'Renamed Now' WHERE jid = 'g1@g.us'"
        )
        second = resolve_chat_slug(archive, "g1@g.us")
        assert second == first  # pinned, not recomputed
    finally:
        archive.close()


def test_resolve_chat_slug_uses_contacts_for_user_chats(tmp_path: Path) -> None:
    archive = init_db(tmp_path / "a.db")
    try:
        archive.execute(
            "INSERT INTO chats(jid, kind, source) "
            "VALUES('uuid@signal', 'user', 'signal')"
        )
        archive.execute(
            "INSERT INTO contacts(phone_jid, name, source, updated_at) "
            "VALUES('uuid@signal', 'Alice Anderson', 'signal', '2026-06-19T00:00:00Z')"
        )
        slug = resolve_chat_slug(archive, "uuid@signal")
        assert slug.startswith("alice-anderson-")
    finally:
        archive.close()


# ---------------------------------------------------------------------------
# Snapshot + migration
# ---------------------------------------------------------------------------


def _seed_message_media(
    conn: sqlite3.Connection,
    *,
    chat_jid: str,
    chat_kind: str,
    chat_subject: str | None,
    basename: str,
    source_file_path: str,
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO chats(jid, kind, subject, source) VALUES (?, ?, ?, 'whatsapp')",
        (chat_jid, chat_kind, chat_subject),
    )
    msg_id = f"{chat_jid}:0:{basename}"
    conn.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, text, key_id, source) "
        "VALUES (?, ?, ?, ?, 0, '2026-06-19T00:00:00Z', 'image', 1, NULL, ?, 'whatsapp')",
        (msg_id, hash(basename) & 0xFFFFFFFF, chat_jid, chat_jid, basename),
    )
    conn.execute(
        "INSERT INTO message_media(message_id, file_path) VALUES (?, ?)",
        (msg_id, source_file_path),
    )


def test_snapshot_pass_uses_per_chat_layout(tmp_path: Path) -> None:
    archive = init_db(tmp_path / "a.db")
    try:
        # Seed: chat exists with a known message_media row pointing at a file
        # we'll then place in the WA source tree.
        _seed_message_media(
            archive,
            chat_jid="g1@g.us",
            chat_kind="group",
            chat_subject="Birthday Alice",
            basename="IMG-001.jpg",
            source_file_path="WhatsApp Images/IMG-001.jpg",
        )
        archive.commit()

        media_root = tmp_path / "wa-media"
        (media_root / "WhatsApp Images").mkdir(parents=True)
        (media_root / "WhatsApp Images" / "IMG-001.jpg").write_bytes(b"jpeg-bytes")
        # Also a stray status file that no message references — should land in _orphans/.
        (media_root / ".Statuses").mkdir()
        (media_root / ".Statuses" / "stray.jpg").write_bytes(b"status")

        archive_root = tmp_path / "archive"
        result = snapshot_pass(archive, media_root=media_root, archive_root=archive_root)
        assert result.new_files == 2

        # IMG-001 went into the per-chat folder.
        slug = chat_slug("g1@g.us", "Birthday Alice")
        assert (archive_root / slug / "IMG-001.jpg").exists()
        # message_media.mirrored_path is set, relative to archive_root.
        mirrored = archive.execute(
            "SELECT mirrored_path FROM message_media WHERE file_path = "
            "'WhatsApp Images/IMG-001.jpg'"
        ).fetchone()
        assert mirrored[0] == f"{slug}/IMG-001.jpg"

        # The unclaimed status file is in _orphans/.
        assert (archive_root / ORPHANS_DIR / "stray.jpg").exists()
    finally:
        archive.close()


def test_snapshot_rehomes_orphans_when_message_arrives(tmp_path: Path) -> None:
    archive = init_db(tmp_path / "a.db")
    try:
        media_root = tmp_path / "wa-media"
        (media_root / ".Statuses").mkdir(parents=True)
        f = media_root / ".Statuses" / "later.jpg"
        f.write_bytes(b"status-content")
        archive_root = tmp_path / "archive"

        # First pass: no message_media row → file goes to _orphans/.
        snapshot_pass(archive, media_root=media_root, archive_root=archive_root)
        assert (archive_root / ORPHANS_DIR / "later.jpg").exists()

        # Now message arrives.
        _seed_message_media(
            archive,
            chat_jid="alice@s.whatsapp.net",
            chat_kind="user",
            chat_subject=None,
            basename="later.jpg",
            source_file_path="WhatsApp Images/later.jpg",
        )
        archive.execute(
            "INSERT INTO contacts(phone_jid, name, source, updated_at) "
            "VALUES('alice@s.whatsapp.net', 'Alice', 'whatsapp', '2026-06-19T00:00:00Z')"
        )
        archive.commit()

        # Second pass: orphan re-homes into Alice's per-chat folder.
        result = snapshot_pass(archive, media_root=media_root, archive_root=archive_root)
        assert result.rehomed_orphans == 1
        slug = chat_slug("alice@s.whatsapp.net", "Alice")
        assert (archive_root / slug / "later.jpg").exists()
        assert not (archive_root / ORPHANS_DIR / "later.jpg").exists()

        # mirrored_path now points at the new home.
        mirrored = archive.execute(
            "SELECT mirrored_path FROM message_media WHERE file_path = "
            "'WhatsApp Images/later.jpg'"
        ).fetchone()
        assert mirrored[0] == f"{slug}/later.jpg"
    finally:
        archive.close()


def test_snapshot_blocked_when_layout_flat(tmp_path: Path) -> None:
    archive = init_db(tmp_path / "a.db")
    try:
        # Simulate an existing archive: media_mirror is non-empty before migration,
        # so the v5 migration tags it 'flat'. We reproduce that state directly.
        archive.execute(
            "INSERT INTO media_mirror(source_path, archive_path, mirrored_at) "
            "VALUES('/x/y.jpg', '/archive/.Statuses/y.jpg', '2026-06-19T00:00:00Z')"
        )
        archive.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES('media_layout', 'flat')"
        )
        archive.commit()

        with pytest.raises(FlatLayoutBlocked, match="mirror migrate"):
            snapshot_pass(archive, media_root=tmp_path / "src", archive_root=tmp_path / "ar")
    finally:
        archive.close()


def test_migrate_flat_to_per_chat_moves_existing_files(tmp_path: Path) -> None:
    archive = init_db(tmp_path / "a.db")
    try:
        archive_root = tmp_path / "archive"
        flat_path = archive_root / ".Statuses" / "old.jpg"
        flat_path.parent.mkdir(parents=True)
        flat_path.write_bytes(b"data")

        # Pretend the snapshot previously recorded this file in flat layout.
        archive.execute(
            "INSERT INTO media_mirror(source_path, archive_path, mirrored_at) "
            "VALUES(?, ?, '2026-06-19T00:00:00Z')",
            ("/sdcard/WhatsApp/Media/.Statuses/old.jpg", str(flat_path)),
        )
        archive.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES('media_layout', 'flat')"
        )
        _seed_message_media(
            archive,
            chat_jid="bob@s.whatsapp.net",
            chat_kind="user",
            chat_subject=None,
            basename="old.jpg",
            source_file_path="WhatsApp Images/old.jpg",
        )
        archive.execute(
            "INSERT INTO contacts(phone_jid, name, source, updated_at) "
            "VALUES('bob@s.whatsapp.net', 'Bob', 'whatsapp', '2026-06-19T00:00:00Z')"
        )
        archive.commit()

        result = migrate_flat_to_per_chat(archive, archive_root)
        assert result.moved == 1
        slug = chat_slug("bob@s.whatsapp.net", "Bob")
        assert (archive_root / slug / "old.jpg").exists()
        assert not flat_path.exists()
        assert get_layout(archive) == "per_chat"

        # Re-running is a no-op.
        again = migrate_flat_to_per_chat(archive, archive_root)
        assert again.moved == 0
    finally:
        archive.close()


def test_pin_slug_does_not_overwrite_existing(tmp_path: Path) -> None:
    archive = init_db(tmp_path / "a.db")
    try:
        pin_slug(archive, "g1@g.us", "original-aaaaaa")
        # Attempting to pin again with a different value is silently ignored.
        pin_slug(archive, "g1@g.us", "other-bbbbbb")
        row = archive.execute(
            "SELECT value FROM _meta WHERE key = 'chat_slug:g1@g.us'"
        ).fetchone()
        assert row[0] == "original-aaaaaa"
    finally:
        archive.close()
