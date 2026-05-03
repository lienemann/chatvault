"""Extract 24h status posts: text + media + (for own) viewer receipts.

Two complementary sources:

* Own statuses + view receipts come from `msgstore.db` — `status_message_info`
  joined with `message`, `message_media`, and `receipt_user`. Full history
  back as far as the source DB retains.
* Received statuses (from contacts) come from a separately-decrypted
  `status_backup.db` (only the current 24h backup window). Body, media URL,
  and thumbnail blob live there; nothing in msgstore replicates them.

Both feed the same `status_archive`/`status_archive_media`/...thumbnails
tables; queries can mix or filter on `kind`. Views are own-only.
"""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, label_for_type, ms_to_iso, stable_message_id

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Own statuses + views (source: msgstore.db)
# ---------------------------------------------------------------------------

OWN_QUERY = """
SELECT
    m._id                AS rowid,
    m.key_id             AS key_id,
    m.from_me            AS from_me,
    m.timestamp          AS ts_ms,
    m.received_timestamp AS ts_received_ms,
    m.message_type       AS type_raw,
    m.text_data          AS text,
    s.audience_type      AS audience_type,
    j_chat.raw_string    AS chat_jid,
    j_sender.raw_string  AS sender_jid
FROM status_message_info s
JOIN message m            ON s.message_row_id     = m._id
LEFT JOIN chat c          ON m.chat_row_id        = c._id
LEFT JOIN jid j_chat      ON c.jid_row_id         = j_chat._id
LEFT JOIN jid j_sender    ON m.sender_jid_row_id  = j_sender._id
WHERE m.from_me = 1
  AND m.key_id IS NOT NULL
  AND j_chat.raw_string = 'status@broadcast'
"""

OWN_MEDIA_QUERY = """
SELECT message_row_id, file_path, file_size, mime_type, file_hash, media_caption,
       media_duration, width, height, media_name
FROM message_media WHERE message_row_id IN ({placeholders})
"""

OWN_VIEWS_QUERY = """
SELECT ru.message_row_id, ru.receipt_timestamp, ru.read_timestamp, ru.played_timestamp,
       j.raw_string AS viewer_jid
FROM receipt_user ru
LEFT JOIN jid j ON ru.receipt_user_jid_row_id = j._id
WHERE ru.message_row_id IN ({placeholders})
"""


def extract_own(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    """Own statuses (from msgstore) + view receipts → status_archive[*]."""
    res = ExtractorResult(name="status_archive_own")

    own_rows = list(source.execute(OWN_QUERY))
    if not own_rows:
        return res

    rowid_to_id: dict[int, str] = {}
    with transaction(archive):
        for r in own_rows:
            chat_jid = r["chat_jid"]
            message_id = stable_message_id(chat_jid, True, r["key_id"])
            rowid_to_id[r["rowid"]] = message_id
            archive.execute(
                "INSERT INTO status_archive(id, kind, sender_jid, chat_jid, type, type_raw, "
                "                           ts, received_ts, text, audience_type, "
                "                           is_archived, uuid, message_id) "
                "VALUES(?, 'own', ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "  sender_jid = excluded.sender_jid, "
                "  chat_jid = excluded.chat_jid, "
                "  type = excluded.type, "
                "  type_raw = excluded.type_raw, "
                "  ts = excluded.ts, "
                "  received_ts = excluded.received_ts, "
                "  text = excluded.text, "
                "  audience_type = excluded.audience_type, "
                "  message_id = excluded.message_id",
                (
                    message_id,
                    r["sender_jid"],
                    chat_jid,
                    label_for_type(r["type_raw"]),
                    r["type_raw"],
                    ms_to_iso(r["ts_ms"]),
                    ms_to_iso(r["ts_received_ms"]),
                    r["text"],
                    r["audience_type"],
                    message_id,
                ),
            )
            res.rows_written += 1

        # Media
        rowids = list(rowid_to_id)
        if rowids:
            ph = ", ".join("?" for _ in rowids)
            for r in source.execute(OWN_MEDIA_QUERY.format(placeholders=ph), rowids):
                sid = rowid_to_id.get(r["message_row_id"])
                if not sid:
                    continue
                archive.execute(
                    "INSERT INTO status_archive_media(status_id, mime, width, height, duration_s, "
                    "                                 file_size, file_path, accessibility_label, "
                    "                                 file_hash, media_name) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(status_id) DO UPDATE SET "
                    "  mime = excluded.mime, width = excluded.width, height = excluded.height, "
                    "  duration_s = excluded.duration_s, file_size = excluded.file_size, "
                    "  file_path = excluded.file_path, "
                    "  accessibility_label = excluded.accessibility_label, "
                    "  file_hash = excluded.file_hash, media_name = excluded.media_name",
                    (
                        sid, r["mime_type"], r["width"], r["height"], r["media_duration"],
                        r["file_size"], r["file_path"], r["media_caption"],
                        r["file_hash"], r["media_name"],
                    ),
                )

        # Views
        if rowids:
            ph = ", ".join("?" for _ in rowids)
            view_count = 0
            for r in source.execute(OWN_VIEWS_QUERY.format(placeholders=ph), rowids):
                sid = rowid_to_id.get(r["message_row_id"])
                if not sid or not r["viewer_jid"]:
                    continue
                archive.execute(
                    "INSERT INTO status_archive_views(status_id, viewer_jid, received_ts, "
                    "                                 read_ts, played_ts) "
                    "VALUES(?, ?, ?, ?, ?) "
                    "ON CONFLICT(status_id, viewer_jid) DO UPDATE SET "
                    "  received_ts = excluded.received_ts, "
                    "  read_ts = excluded.read_ts, "
                    "  played_ts = excluded.played_ts",
                    (
                        sid, r["viewer_jid"],
                        ms_to_iso(r["receipt_timestamp"]),
                        ms_to_iso(r["read_timestamp"]),
                        ms_to_iso(r["played_timestamp"]),
                    ),
                )
                view_count += 1
            res.with_note(f"{view_count} view receipts")

    return res


# ---------------------------------------------------------------------------
# Received statuses (source: status_backup.db, separate connection)
# ---------------------------------------------------------------------------

RECEIVED_QUERY = """
SELECT
    s.row_id, s.uuid, s.sender_user_jid, s.type, s.timestamp,
    s.received_timestamp, s.text_data, s.audience_type, s.is_archived,
    si.chat_jid AS info_chat_jid
FROM status s
LEFT JOIN status_info si ON s.status_info_row_id = si.row_id
"""

RECEIVED_MEDIA_QUERY = """
SELECT
    sml.status_row_id, mc.mime_type, mc.width, mc.height, mc.media_duration,
    mc.file_size, mc.file_path, mc.media_url, mc.direct_path, mc.media_key,
    mc.file_hash, mc.enc_file_hash, mc.accessibility_label, mc.media_name
FROM status_media_link sml
JOIN media_content mc ON sml.media_content_row_id = mc.row_id
"""

RECEIVED_THUMBS_QUERY = """
SELECT status_row_id, thumbnail, thumbnail_path, highres_thumbnail_path
FROM status_thumbnail
"""


def _received_id(uuid: str | None, sender_jid: str | None) -> str:
    return f"status_recv:{sender_jid or '?'}:{uuid or '?'}"


def extract_received(
    status_db: sqlite3.Connection, archive: sqlite3.Connection
) -> ExtractorResult:
    """Received 24h statuses (from decrypted status_backup.db) → status_archive[*]."""
    res = ExtractorResult(name="status_archive_received")

    rows = list(status_db.execute(RECEIVED_QUERY))
    if not rows:
        return res

    rowid_to_id: dict[int, str] = {}
    with transaction(archive):
        for r in rows:
            sid = _received_id(r["uuid"], r["sender_user_jid"])
            rowid_to_id[r["row_id"]] = sid
            archive.execute(
                "INSERT INTO status_archive(id, kind, sender_jid, chat_jid, type, type_raw, "
                "                           ts, received_ts, text, audience_type, "
                "                           is_archived, uuid, message_id) "
                "VALUES(?, 'received', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL) "
                "ON CONFLICT(id) DO UPDATE SET "
                "  sender_jid = excluded.sender_jid, "
                "  chat_jid = excluded.chat_jid, "
                "  type = excluded.type, "
                "  type_raw = excluded.type_raw, "
                "  ts = excluded.ts, "
                "  received_ts = excluded.received_ts, "
                "  text = excluded.text, "
                "  audience_type = excluded.audience_type, "
                "  is_archived = excluded.is_archived, "
                "  uuid = excluded.uuid",
                (
                    sid,
                    r["sender_user_jid"],
                    r["info_chat_jid"],
                    label_for_type(r["type"]),
                    r["type"],
                    ms_to_iso(r["timestamp"]),
                    ms_to_iso(r["received_timestamp"]),
                    r["text_data"],
                    r["audience_type"],
                    r["is_archived"],
                    r["uuid"],
                ),
            )
            res.rows_written += 1

        media_count = 0
        for r in status_db.execute(RECEIVED_MEDIA_QUERY):
            sid = rowid_to_id.get(r["status_row_id"])
            if not sid:
                continue
            archive.execute(
                "INSERT INTO status_archive_media(status_id, mime, width, height, duration_s, "
                "                                 file_size, file_path, media_url, direct_path, "
                "                                 media_key, file_hash, enc_file_hash, "
                "                                 accessibility_label, media_name) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(status_id) DO UPDATE SET "
                "  mime = excluded.mime, width = excluded.width, height = excluded.height, "
                "  duration_s = excluded.duration_s, file_size = excluded.file_size, "
                "  file_path = excluded.file_path, media_url = excluded.media_url, "
                "  direct_path = excluded.direct_path, media_key = excluded.media_key, "
                "  file_hash = excluded.file_hash, enc_file_hash = excluded.enc_file_hash, "
                "  accessibility_label = excluded.accessibility_label, "
                "  media_name = excluded.media_name",
                (
                    sid, r["mime_type"], r["width"], r["height"], r["media_duration"],
                    r["file_size"], r["file_path"], r["media_url"], r["direct_path"],
                    r["media_key"], r["file_hash"], r["enc_file_hash"],
                    r["accessibility_label"], r["media_name"],
                ),
            )
            media_count += 1

        thumb_count = 0
        for r in status_db.execute(RECEIVED_THUMBS_QUERY):
            sid = rowid_to_id.get(r["status_row_id"])
            if not sid:
                continue
            archive.execute(
                "INSERT INTO status_archive_thumbnails(status_id, thumbnail, thumbnail_path, "
                "                                       highres_thumbnail_path) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(status_id) DO UPDATE SET "
                "  thumbnail = excluded.thumbnail, "
                "  thumbnail_path = excluded.thumbnail_path, "
                "  highres_thumbnail_path = excluded.highres_thumbnail_path",
                (sid, r["thumbnail"], r["thumbnail_path"], r["highres_thumbnail_path"]),
            )
            thumb_count += 1

        res.with_note(f"{media_count} media, {thumb_count} thumbs")

    return res
