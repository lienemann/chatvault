"""Test the Signal extractor against a hand-built synthetic decrypted DB.

We don't drive `decrypt_backup` here (covered in tests/sources/test_signal_decrypt.py);
we just construct a SQLite that mimics the modern Signal Android schema closely
enough to exercise every code path: 1:1 chat, group chat, outgoing + incoming
messages, an attachment with a matching file on disk, and a quote.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from chatvault.db import init_db
from chatvault.extractors import signal as sig


@pytest.fixture
def fake_signal_bundle(tmp_path: Path) -> sig.SignalExtractInput:
    """Build a minimal decrypted Signal bundle under tmp_path."""
    sig_root = tmp_path / "signal"
    attachments = sig_root / "attachments"
    attachments.mkdir(parents=True)

    db_path = sig_root / "database.sqlite"
    src = sqlite3.connect(db_path)
    src.executescript(
        """
        CREATE TABLE recipient (
            _id                INTEGER PRIMARY KEY,
            aci                TEXT,
            pni                TEXT,
            e164               TEXT,
            group_id           BLOB,
            system_joined_name TEXT,
            profile_joined_name TEXT
        );
        CREATE TABLE thread (
            _id          INTEGER PRIMARY KEY,
            recipient_id INTEGER NOT NULL
        );
        CREATE TABLE message (
            _id                INTEGER PRIMARY KEY,
            thread_id          INTEGER NOT NULL,
            from_recipient_id  INTEGER,
            to_recipient_id    INTEGER,
            date_sent          INTEGER NOT NULL,
            date_received      INTEGER,
            body               TEXT,
            type               INTEGER,
            quote_id           INTEGER,
            quote_author       INTEGER,
            quote_body         TEXT
        );
        CREATE TABLE attachment (
            _id          INTEGER PRIMARY KEY,
            message_id   INTEGER NOT NULL,
            content_type TEXT,
            data_size    INTEGER,
            file_name    TEXT,
            caption      TEXT,
            width        INTEGER,
            height       INTEGER,
            voice_note   INTEGER,
            unique_id    INTEGER
        );
        """
    )

    # Recipients: self (1), 1:1 partner (2), group (3), group member (4).
    src.executemany(
        "INSERT INTO recipient(_id, aci, pni, e164, group_id, "
        "system_joined_name, profile_joined_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "11111111-1111-1111-1111-111111111111", None, "+41790000001", None, None, "Me"),
            (2, "22222222-2222-2222-2222-222222222222", None, "+41790000002", None, "Alice", None),
            (3, None, None, None, b"\xde\xad\xbe\xef\xca\xfe", None, "Group Chat"),
            (4, "44444444-4444-4444-4444-444444444444", None, "+41790000004", None, "Bob", None),
        ],
    )
    src.executemany(
        "INSERT INTO thread(_id, recipient_id) VALUES (?, ?)",
        [(10, 2), (11, 3)],
    )
    src.executemany(
        "INSERT INTO message(_id, thread_id, from_recipient_id, to_recipient_id, "
        "date_sent, date_received, body, type, quote_id, quote_author, quote_body) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (100, 10, 2, 1, 1_700_000_000_000, 1_700_000_001_000, "hi from alice", 20, None, None, None),
            (101, 10, 1, 2, 1_700_000_002_000, None, "hi back", 23, None, None, None),
            (102, 10, 2, 1, 1_700_000_003_000, None, None, 20, None, None, None),  # attachment-only
            (103, 11, 4, 3, 1_700_000_004_000, None, "group hello", 20, None, None, None),
            (104, 10, 1, 2, 1_700_000_005_000, None, "quoting earlier", 23, 100, 2, "hi from alice"),
        ],
    )
    # Attach an image to msg 102; data on disk at attachments/<unique_id>.bin
    src.execute(
        "INSERT INTO attachment(_id, message_id, content_type, data_size, file_name, "
        "caption, width, height, voice_note, unique_id) "
        "VALUES (500, 102, 'image/jpeg', 1234, 'IMG.jpg', 'hello cap', 640, 480, 0, 9001)"
    )
    src.commit()
    src.close()

    (attachments / "9001.bin").write_bytes(b"\xff\xd8\xff fake jpeg")

    kv_path = sig_root / "key_value.json"
    kv_path.write_text(
        json.dumps(
            {
                "account.aci": {"stringValue": "11111111-1111-1111-1111-111111111111"},
                "account.e164": {"stringValue": "+41790000001"},
            }
        ),
        encoding="utf-8",
    )

    return sig.SignalExtractInput(
        db_path=db_path,
        attachments_dir=attachments,
        key_values_path=kv_path,
        media_out_dir=tmp_path / "media",
    )


def test_extract_writes_messages_chats_contacts(
    fake_signal_bundle: sig.SignalExtractInput, tmp_path: Path
) -> None:
    archive = init_db(tmp_path / "archive.db")
    try:
        result = sig.extract(fake_signal_bundle, archive)
        assert result.rows_written == 5
        assert result.rows_skipped == 0

        # Two chats touched: 1:1 with Alice + group.
        chats = list(archive.execute("SELECT jid, kind, subject, source FROM chats ORDER BY kind"))
        assert {c["jid"] for c in chats} == {
            "22222222-2222-2222-2222-222222222222@signal",
            "deadbeefcafe@signal-group",
        }
        assert all(c["source"] == "signal" for c in chats)
        group = next(c for c in chats if c["kind"] == "group")
        assert group["subject"] == "Group Chat"

        # Messages tagged source='signal', from_me set correctly for Me-as-recipient-1.
        msgs = list(
            archive.execute(
                "SELECT id, chat_jid, sender_jid, from_me, type, text, source "
                "FROM messages WHERE source = 'signal' ORDER BY ts"
            )
        )
        assert len(msgs) == 5
        outgoing = [m for m in msgs if m["from_me"] == 1]
        assert len(outgoing) == 2
        assert all(m["sender_jid"] for m in msgs)
        # The attachment-only message gets type='image'.
        with_image = [m for m in msgs if m["type"] == "image"]
        assert len(with_image) == 1
        # Caption falls into body when message body is empty.
        assert with_image[0]["text"] == "hello cap"

        media = list(archive.execute("SELECT message_id, file_path, mime FROM message_media"))
        assert len(media) == 1
        assert media[0]["file_path"] == "signal/attachments/9001.bin"
        assert media[0]["mime"] == "image/jpeg"

        # Quote captured.
        quoted = list(archive.execute("SELECT quoted_text FROM message_quoted"))
        assert quoted and quoted[0]["quoted_text"] == "hi from alice"

        # Contacts: Alice + Bob (both have display name + phone JID).
        contacts = {c["name"]: c for c in archive.execute("SELECT name, source FROM contacts")}
        assert {"Alice", "Bob"}.issubset(contacts.keys())
        assert all(c["source"] == "signal" for c in contacts.values())

        # State key advances so a second run is incremental.
        assert (
            archive.execute(
                "SELECT value FROM extraction_state WHERE key = 'signal_last_message_rowid'"
            ).fetchone()[0]
            == "104"
        )
    finally:
        archive.close()


def test_extract_is_incremental(
    fake_signal_bundle: sig.SignalExtractInput, tmp_path: Path
) -> None:
    archive = init_db(tmp_path / "archive.db")
    try:
        sig.extract(fake_signal_bundle, archive)
        # Second run must not re-write any of the existing rows.
        result = sig.extract(fake_signal_bundle, archive)
        assert result.rows_written == 0
    finally:
        archive.close()


def test_mirror_attachments_copies_and_updates(
    fake_signal_bundle: sig.SignalExtractInput, tmp_path: Path
) -> None:
    from chatvault.media.layout import chat_slug

    archive = init_db(tmp_path / "archive.db")
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    try:
        sig.extract(fake_signal_bundle, archive)
        n = sig.mirror_attachments(fake_signal_bundle.attachments_dir, media_dir, archive)
        assert n == 1
        # Attachment belongs to a 1:1 chat with Alice (recipient 2). slug uses
        # the resolved chat name, here the recipient's display name "Alice".
        alice_jid = "22222222-2222-2222-2222-222222222222@signal"
        slug = chat_slug(alice_jid, "Alice")
        expected = media_dir / slug / "9001.bin"
        assert expected.exists()
        row = archive.execute(
            "SELECT mirrored_path FROM message_media WHERE file_path = "
            "'signal/attachments/9001.bin'"
        ).fetchone()
        assert row["mirrored_path"] == f"{slug}/9001.bin"
        # Running mirror again is a noop.
        assert sig.mirror_attachments(fake_signal_bundle.attachments_dir, media_dir, archive) == 0
    finally:
        archive.close()


def test_merge_old_then_new_full_scan(tmp_path: Path) -> None:
    """Old backup has a message that the newer backup has since deleted.

    Workflow the docstring promises:
      1. extract --full-scan OLD  → archive gets every row including deleted one.
      2. extract --full-scan NEW  → archive keeps the deleted row, gains new rows.
    """
    archive = init_db(tmp_path / "archive.db")

    def _build(db_path: Path, rows: list[tuple]) -> sig.SignalExtractInput:
        c = sqlite3.connect(db_path)
        c.executescript(
            """
            CREATE TABLE recipient (
                _id INTEGER PRIMARY KEY, aci TEXT, pni TEXT, e164 TEXT,
                group_id BLOB, system_joined_name TEXT, profile_joined_name TEXT
            );
            CREATE TABLE thread (_id INTEGER PRIMARY KEY, recipient_id INTEGER NOT NULL);
            CREATE TABLE message (
                _id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL,
                from_recipient_id INTEGER, to_recipient_id INTEGER,
                date_sent INTEGER NOT NULL, date_received INTEGER, body TEXT, type INTEGER,
                quote_id INTEGER, quote_author INTEGER, quote_body TEXT
            );
            CREATE TABLE attachment (
                _id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL, content_type TEXT,
                data_size INTEGER, file_name TEXT, caption TEXT, width INTEGER,
                height INTEGER, voice_note INTEGER, unique_id INTEGER
            );
            """
        )
        c.executemany(
            "INSERT INTO recipient(_id, aci, e164, system_joined_name) VALUES (?, ?, ?, ?)",
            [
                (1, "11111111-1111-1111-1111-111111111111", "+41790000001", "Me"),
                (2, "22222222-2222-2222-2222-222222222222", "+41790000002", "Alice"),
            ],
        )
        c.execute("INSERT INTO thread(_id, recipient_id) VALUES (10, 2)")
        c.executemany(
            "INSERT INTO message(_id, thread_id, from_recipient_id, to_recipient_id, "
            "date_sent, body, type) VALUES (?, 10, ?, ?, ?, ?, ?)",
            rows,
        )
        c.commit()
        c.close()
        return sig.SignalExtractInput(
            db_path=db_path, attachments_dir=tmp_path / "att", key_values_path=None
        )

    (tmp_path / "att").mkdir()
    old_db = _build(
        tmp_path / "old.sqlite",
        [
            (100, 2, 1, 1_700_000_000_000, "kept from old", 20),
            (101, 2, 1, 1_700_000_001_000, "deleted in new", 20),
            (102, 1, 2, 1_700_000_002_000, "also kept", 23),
        ],
    )
    new_db = _build(
        tmp_path / "new.sqlite",
        [
            (100, 2, 1, 1_700_000_000_000, "kept from old", 20),
            # rowid 101 absent — deleted from this snapshot.
            (102, 1, 2, 1_700_000_002_000, "also kept", 23),
            (103, 2, 1, 1_700_000_003_000, "fresh message", 20),
        ],
    )

    try:
        sig.extract(old_db, archive, full_scan=True)
        bodies_after_old = {
            r["text"]
            for r in archive.execute(
                "SELECT text FROM messages WHERE source = 'signal'"
            )
        }
        assert "deleted in new" in bodies_after_old

        sig.extract(new_db, archive, full_scan=True)
        bodies_after_new = {
            r["text"]
            for r in archive.execute(
                "SELECT text FROM messages WHERE source = 'signal'"
            )
        }
        # Deleted-from-new row survives, new row arrives.
        assert "deleted in new" in bodies_after_new
        assert "fresh message" in bodies_after_new
        assert "kept from old" in bodies_after_new
        # No duplicates from running twice.
        rowcount = archive.execute(
            "SELECT COUNT(*) FROM messages WHERE source = 'signal'"
        ).fetchone()[0]
        assert rowcount == 4

        # Cursor advanced to the maximum across both passes.
        cursor = archive.execute(
            "SELECT value FROM extraction_state WHERE key = 'signal_last_message_rowid'"
        ).fetchone()[0]
        assert cursor == "103"
    finally:
        archive.close()


def test_extract_rejects_legacy_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    src = sqlite3.connect(db_path)
    src.executescript("CREATE TABLE sms (id INTEGER); CREATE TABLE recipient (_id INTEGER);")
    src.commit()
    src.close()
    inp = sig.SignalExtractInput(
        db_path=db_path,
        attachments_dir=tmp_path,
        key_values_path=None,
        media_out_dir=None,
    )
    archive = init_db(tmp_path / "archive.db")
    try:
        with pytest.raises(RuntimeError, match="unsupported schema"):
            sig.extract(inp, archive)
    finally:
        archive.close()
