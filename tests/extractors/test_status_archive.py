"""Status-archive extractor tests: own (msgstore) + received (status_backup)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from chatvault.extractors.status_archive import extract_own, extract_received


def _build_status_backup(path: Path) -> sqlite3.Connection:
    """Minimal status_backup.db schema, matching the columns the extractor reads."""
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE status (
            row_id INTEGER PRIMARY KEY,
            sort_id INTEGER, uuid TEXT, sender_user_jid TEXT,
            status_info_row_id INTEGER, type INTEGER, timestamp INTEGER,
            received_timestamp INTEGER, text_data TEXT, audience_type INTEGER,
            is_archived INTEGER
        );
        CREATE TABLE status_info (
            row_id INTEGER PRIMARY KEY, chat_jid TEXT
        );
        CREATE TABLE media_content (
            row_id INTEGER PRIMARY KEY,
            mime_type TEXT, width INTEGER, height INTEGER, media_duration INTEGER,
            file_size INTEGER, file_path TEXT, media_url TEXT, direct_path TEXT,
            media_key BLOB, file_hash BLOB, enc_file_hash BLOB,
            accessibility_label TEXT, media_name TEXT
        );
        CREATE TABLE status_media_link (
            row_id INTEGER PRIMARY KEY,
            status_row_id INTEGER, media_content_row_id INTEGER
        );
        CREATE TABLE status_thumbnail (
            row_id INTEGER PRIMARY KEY,
            status_row_id INTEGER, media_content_row_id INTEGER,
            thumbnail BLOB, thumbnail_path TEXT, highres_thumbnail_path TEXT
        );
        """
    )
    return db


def test_extract_own_writes_only_public_statuses(populated_source, archive_db) -> None:
    """Group messages with status_message_info rows must NOT count as statuses.
    Only chat_jid = 'status@broadcast' qualifies as a public 24h status.
    """
    s = populated_source
    # Add the special status@broadcast jid + chat, plus a public-status message.
    s.execute(
        "INSERT INTO jid(_id, user, server, raw_string, type) "
        "VALUES(99, 'status', 'broadcast', 'status@broadcast', 11)"
    )
    s.execute(
        "INSERT INTO chat(_id, jid_row_id, subject, archived, hidden, sort_timestamp) "
        "VALUES(99, 99, NULL, 0, 0, 1700000400000)"
    )
    s.execute(
        "INSERT INTO message(_id, chat_row_id, from_me, key_id, sender_jid_row_id, "
        "                    timestamp, message_type, text_data, starred, status, origin) "
        "VALUES(99, 99, 1, 'KSTATUS', NULL, 1700000400000, 1, 'public status text', 0, 0, 0)"
    )
    # Public status row.
    s.execute("INSERT INTO status_message_info(message_row_id, audience_type) VALUES(99, 0)")
    # And a group-message status_message_info row that should be IGNORED.
    s.execute("INSERT INTO status_message_info(message_row_id, audience_type) VALUES(12, 0)")
    s.executemany(
        "INSERT INTO receipt_user(_id, message_row_id, receipt_user_jid_row_id, "
        "                         receipt_timestamp, read_timestamp, played_timestamp) "
        "VALUES(?, ?, ?, ?, ?, ?)",
        [
            (1, 99, 4, 1700000500000, 1700000600000, None),
            (2, 99, 2, 1700000501000, None,           None),
            # Group receipts on msg 12 — must NOT become status views.
            (3, 12, 4, 1700000300000, 1700000400000, None),
        ],
    )
    s.commit()

    res = extract_own(s, archive_db)
    assert res.rows_written == 1

    rows = list(archive_db.execute(
        "SELECT id, kind, chat_jid, text FROM status_archive"
    ))
    assert len(rows) == 1
    assert rows[0]["chat_jid"] == "status@broadcast"
    assert rows[0]["text"] == "public status text"

    views = list(archive_db.execute(
        "SELECT status_id, viewer_jid FROM status_archive_views ORDER BY viewer_jid"
    ))
    assert len(views) == 2
    # All views belong to the public status, not the group message.
    assert all(v["status_id"] == rows[0]["id"] for v in views)


def test_extract_own_skips_when_no_status(populated_source, archive_db) -> None:
    res = extract_own(populated_source, archive_db)
    assert res.rows_written == 0
    assert archive_db.execute("SELECT COUNT(*) FROM status_archive").fetchone()[0] == 0


def test_extract_received_writes_status_with_media_and_thumb(archive_db, tmp_path) -> None:
    sb = _build_status_backup(tmp_path / "status_backup.db")
    sb.execute("INSERT INTO status_info(row_id, chat_jid) VALUES(1, '100100100100100@lid')")
    sb.execute(
        "INSERT INTO status(row_id, uuid, sender_user_jid, status_info_row_id, type, "
        "                   timestamp, received_timestamp, text_data, audience_type, is_archived) "
        "VALUES(1, 'UU1', '100100100100100@lid', 1, 1, 1770800000000, 1770800100000, "
        "       'caption', 0, 0)"
    )
    sb.execute(
        "INSERT INTO media_content(row_id, mime_type, width, height, media_duration, "
        "                          file_size, file_path, media_url, direct_path, "
        "                          accessibility_label) "
        "VALUES(1, 'image/jpeg', 1080, 1920, 0, 12345, '/Media/img.jpg', "
        "       'https://x.example/u', '/v/abc', 'photo')"
    )
    sb.execute("INSERT INTO status_media_link(row_id, status_row_id, media_content_row_id) VALUES(1, 1, 1)")
    sb.execute(
        "INSERT INTO status_thumbnail(row_id, status_row_id, thumbnail, thumbnail_path) "
        "VALUES(1, 1, X'AABBCC', '/thumbs/1.jpg')"
    )
    sb.commit()

    res = extract_received(sb, archive_db)
    sb.close()

    assert res.rows_written == 1

    sa = archive_db.execute(
        "SELECT id, kind, sender_jid, chat_jid, type, text, uuid FROM status_archive"
    ).fetchone()
    assert sa["kind"] == "received"
    assert sa["sender_jid"] == "100100100100100@lid"
    assert sa["chat_jid"] == "100100100100100@lid"
    assert sa["uuid"] == "UU1"
    assert sa["text"] == "caption"

    media = archive_db.execute(
        "SELECT mime, width, file_path, media_url, accessibility_label FROM status_archive_media"
    ).fetchone()
    assert media["mime"] == "image/jpeg"
    assert media["width"] == 1080
    assert media["media_url"] == "https://x.example/u"

    thumb = archive_db.execute(
        "SELECT thumbnail, thumbnail_path FROM status_archive_thumbnails"
    ).fetchone()
    assert thumb["thumbnail"] == b"\xaa\xbb\xcc"
    assert thumb["thumbnail_path"] == "/thumbs/1.jpg"


def test_extract_received_idempotent(archive_db, tmp_path) -> None:
    sb = _build_status_backup(tmp_path / "status_backup.db")
    sb.execute(
        "INSERT INTO status(row_id, uuid, sender_user_jid, type, timestamp, "
        "                   received_timestamp, text_data, audience_type, is_archived) "
        "VALUES(1, 'UU1', '100@lid', 5, 1770800000000, 1770800100000, 'hi', 0, 0)"
    )
    sb.commit()
    extract_received(sb, archive_db)
    res2 = extract_received(sb, archive_db)
    sb.close()
    assert res2.rows_written == 1
    assert archive_db.execute("SELECT COUNT(*) FROM status_archive").fetchone()[0] == 1
