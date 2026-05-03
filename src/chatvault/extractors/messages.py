"""Extract messages and their per-message extensions."""

from __future__ import annotations

import logging
import sqlite3

from ..db import get_state_int, set_state_int, transaction
from . import (
    SKIP_MESSAGE_TYPES,
    ExtractorResult,
    label_for_type,
    ms_to_iso,
    stable_message_id,
)

log = logging.getLogger(__name__)


MESSAGE_QUERY = """
SELECT
    m._id                AS rowid,
    m.key_id             AS key_id,
    m.from_me            AS from_me,
    m.timestamp          AS ts_ms,
    m.received_timestamp AS ts_received_ms,
    m.message_type       AS type_raw,
    m.text_data          AS text,
    m.starred            AS starred,
    m.status             AS status,
    m.origin             AS origin,
    j_chat.raw_string    AS chat_jid,
    j_sender.raw_string  AS sender_jid
FROM message m
LEFT JOIN chat c       ON m.chat_row_id      = c._id
LEFT JOIN jid j_chat   ON c.jid_row_id       = j_chat._id
LEFT JOIN jid j_sender ON m.sender_jid_row_id = j_sender._id
WHERE m._id > ?
ORDER BY m._id
"""

MEDIA_QUERY = """
SELECT message_row_id, file_path, file_size, mime_type, file_hash,
       media_caption, media_duration, width, height, media_name
FROM message_media WHERE message_row_id > ?
"""

QUOTED_QUERY = """
SELECT q.message_row_id, q.key_id, q.text_data, q.message_type,
       j.raw_string AS sender_jid
FROM message_quoted q
LEFT JOIN jid j ON q.sender_jid_row_id = j._id
WHERE q.message_row_id > ?
"""

LINK_QUERY = """
SELECT message_row_id, description, page_title, url
FROM message_text WHERE message_row_id > ?
"""

MENTIONS_QUERY = """
SELECT mm.message_row_id, mm.display_name, j.raw_string AS jid
FROM message_mentions mm
LEFT JOIN jid j ON mm.jid_row_id = j._id
WHERE mm.message_row_id > ?
"""

FORWARDED_QUERY = """
SELECT message_row_id, forward_score, forward_origin
FROM message_forwarded WHERE message_row_id > ?
"""

REVOKED_QUERY = """
SELECT mr.message_row_id, mr.revoked_key_id, mr.revoke_timestamp,
       j.raw_string AS admin_jid
FROM message_revoked mr
LEFT JOIN jid j ON mr.admin_jid_row_id = j._id
WHERE mr.message_row_id > ?
"""

ALBUM_QUERY = """
SELECT message_row_id, image_count, video_count, expected_image_count, expected_video_count
FROM message_album WHERE message_row_id > ?
"""

VIEW_ONCE_QUERY = """
SELECT message_row_id, state FROM message_view_once_media WHERE message_row_id > ?
"""

EPHEMERAL_QUERY = """
SELECT message_row_id, duration, expire_timestamp
FROM message_ephemeral WHERE message_row_id > ?
"""

LOCATION_QUERY = """
SELECT message_row_id, latitude, longitude, place_name, place_address, url,
       live_location_share_duration, live_location_final_latitude,
       live_location_final_longitude, live_location_final_timestamp
FROM message_location WHERE message_row_id > ?
"""


def _build_aux(source: sqlite3.Connection, since_rowid: int) -> dict[str, dict[int, dict]]:
    """Pre-load extension tables for the rowid range we're about to process."""
    media: dict[int, dict] = {}
    for r in source.execute(MEDIA_QUERY, (since_rowid,)):
        media[r["message_row_id"]] = {
            "file_path": r["file_path"],
            "file_size": r["file_size"],
            "mime": r["mime_type"],
            "file_hash": r["file_hash"],
            "caption": r["media_caption"],
            "duration_s": r["media_duration"],
            "width": r["width"],
            "height": r["height"],
            "name": r["media_name"],
        }

    quoted: dict[int, dict] = {}
    for r in source.execute(QUOTED_QUERY, (since_rowid,)):
        quoted[r["message_row_id"]] = {
            "quoted_key_id": r["key_id"],
            "quoted_text": r["text_data"],
            "quoted_message_type": r["message_type"],
            "quoted_sender_jid": r["sender_jid"],
        }

    links: dict[int, dict] = {}
    for r in source.execute(LINK_QUERY, (since_rowid,)):
        if r["url"]:
            links[r["message_row_id"]] = {
                "url": r["url"],
                "title": r["page_title"],
                "description": r["description"],
            }

    mentions: dict[int, list[dict]] = {}
    for r in source.execute(MENTIONS_QUERY, (since_rowid,)):
        mentions.setdefault(r["message_row_id"], []).append({
            "mentioned_jid": r["jid"],
            "display_name": r["display_name"],
        })

    forwarded: dict[int, dict] = {}
    for r in source.execute(FORWARDED_QUERY, (since_rowid,)):
        forwarded[r["message_row_id"]] = {
            "forward_score": r["forward_score"],
            "forward_origin": r["forward_origin"],
        }

    revoked: dict[int, dict] = {}
    for r in source.execute(REVOKED_QUERY, (since_rowid,)):
        revoked[r["message_row_id"]] = {
            "revoked_key_id": r["revoked_key_id"],
            "admin_jid": r["admin_jid"],
            "revoke_ts": ms_to_iso(r["revoke_timestamp"]),
        }

    albums: dict[int, dict] = {}
    for r in source.execute(ALBUM_QUERY, (since_rowid,)):
        albums[r["message_row_id"]] = {
            "image_count": r["image_count"],
            "video_count": r["video_count"],
            "expected_image_count": r["expected_image_count"],
            "expected_video_count": r["expected_video_count"],
        }

    view_once: dict[int, dict] = {}
    for r in source.execute(VIEW_ONCE_QUERY, (since_rowid,)):
        view_once[r["message_row_id"]] = {"state": r["state"]}

    ephemeral: dict[int, dict] = {}
    for r in source.execute(EPHEMERAL_QUERY, (since_rowid,)):
        ephemeral[r["message_row_id"]] = {
            "expire_seconds": r["duration"],
            "ephemeral_setting_ts": ms_to_iso(r["expire_timestamp"]),
        }

    locations: dict[int, dict] = {}
    for r in source.execute(LOCATION_QUERY, (since_rowid,)):
        locations[r["message_row_id"]] = {
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "place_name": r["place_name"],
            "place_address": r["place_address"],
            "url": r["url"],
            "live_share_duration_s": r["live_location_share_duration"],
            "live_final_lat": r["live_location_final_latitude"],
            "live_final_lng": r["live_location_final_longitude"],
            "live_final_ts": ms_to_iso(r["live_location_final_timestamp"]),
        }

    return {
        "media": media,
        "quoted": quoted,
        "links": links,
        "mentions": mentions,
        "forwarded": forwarded,
        "revoked": revoked,
        "albums": albums,
        "view_once": view_once,
        "ephemeral": ephemeral,
        "locations": locations,
    }


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="messages")

    since_rowid = get_state_int(archive, "last_message_rowid", 0)
    log.info("messages: extracting rowid > %d", since_rowid)

    aux = _build_aux(source, since_rowid)
    log.info(
        "messages aux: media=%d quoted=%d links=%d mentions=%d forwarded=%d "
        "revoked=%d albums=%d view_once=%d ephemeral=%d locations=%d",
        len(aux["media"]), len(aux["quoted"]), len(aux["links"]),
        len(aux["mentions"]), len(aux["forwarded"]), len(aux["revoked"]),
        len(aux["albums"]), len(aux["view_once"]), len(aux["ephemeral"]),
        len(aux["locations"]),
    )

    max_rowid = since_rowid

    with transaction(archive):
        for row in source.execute(MESSAGE_QUERY, (since_rowid,)):
            rowid = row["rowid"]
            max_rowid = max(max_rowid, rowid)
            type_raw = row["type_raw"]
            if type_raw is None or type_raw in SKIP_MESSAGE_TYPES:
                res.rows_skipped += 1
                continue

            chat_jid = row["chat_jid"]
            from_me = bool(row["from_me"])
            key_id = row["key_id"]
            if not key_id or not chat_jid:
                # orphaned or malformed source rows — skip silently.
                res.rows_skipped += 1
                continue

            message_id = stable_message_id(chat_jid, from_me, key_id)
            label = label_for_type(type_raw)

            # In 1:1 chats, sender_jid is null when not from_me — the partner is the chat itself.
            sender_jid = row["sender_jid"]
            if sender_jid is None and not from_me and chat_jid and chat_jid.endswith("@s.whatsapp.net"):
                sender_jid = chat_jid

            archive.execute(
                "INSERT OR REPLACE INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, "
                "                                ts, ts_received, type, type_raw, text, key_id, "
                "                                status, origin, starred, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    message_id, rowid, chat_jid, sender_jid, int(from_me),
                    ms_to_iso(row["ts_ms"]), ms_to_iso(row["ts_received_ms"]),
                    label, type_raw, row["text"], key_id,
                    row["status"], row["origin"], int(row["starred"] or 0), None,
                ),
            )
            res.rows_written += 1

            if (m := aux["media"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_media(message_id, file_path, file_size, mime, "
                    "                                     file_hash, caption, duration_s, width, height, name) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (message_id, m["file_path"], m["file_size"], m["mime"], m["file_hash"],
                     m["caption"], m["duration_s"], m["width"], m["height"], m["name"]),
                )

            if (q := aux["quoted"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_quoted(message_id, quoted_key_id, quoted_text, "
                    "                                      quoted_message_type, quoted_sender_jid) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (message_id, q["quoted_key_id"], q["quoted_text"],
                     q["quoted_message_type"], q["quoted_sender_jid"]),
                )

            if (l := aux["links"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_link_previews(message_id, url, title, description) "
                    "VALUES(?, ?, ?, ?)",
                    (message_id, l["url"], l["title"], l["description"]),
                )

            if (mlist := aux["mentions"].get(rowid)) is not None:
                # Idempotency: clear+reinsert this message's mentions.
                archive.execute("DELETE FROM message_mentions WHERE message_id = ?", (message_id,))
                for mn in mlist:
                    archive.execute(
                        "INSERT INTO message_mentions(message_id, mentioned_jid, display_name) "
                        "VALUES(?, ?, ?)",
                        (message_id, mn["mentioned_jid"], mn["display_name"]),
                    )

            if (f := aux["forwarded"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_forwarded(message_id, forward_score, forward_origin) "
                    "VALUES(?, ?, ?)",
                    (message_id, f["forward_score"], f["forward_origin"]),
                )

            if (rv := aux["revoked"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_revoked(message_id, revoked_key_id, admin_jid, revoke_ts) "
                    "VALUES(?, ?, ?, ?)",
                    (message_id, rv["revoked_key_id"], rv["admin_jid"], rv["revoke_ts"]),
                )

            if (al := aux["albums"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_albums(message_id, image_count, video_count, "
                    "                                      expected_image_count, expected_video_count) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (message_id, al["image_count"], al["video_count"],
                     al["expected_image_count"], al["expected_video_count"]),
                )

            if (vo := aux["view_once"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_view_once(message_id, state) VALUES(?, ?)",
                    (message_id, vo["state"]),
                )

            if (ep := aux["ephemeral"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_ephemeral(message_id, expire_seconds, ephemeral_setting_ts) "
                    "VALUES(?, ?, ?)",
                    (message_id, ep["expire_seconds"], ep["ephemeral_setting_ts"]),
                )

            if (loc := aux["locations"].get(rowid)) is not None:
                archive.execute(
                    "INSERT OR REPLACE INTO message_locations(message_id, latitude, longitude, "
                    "                                         place_name, place_address, url, "
                    "                                         live_share_duration_s, live_final_lat, "
                    "                                         live_final_lng, live_final_ts) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (message_id, loc["latitude"], loc["longitude"], loc["place_name"],
                     loc["place_address"], loc["url"], loc["live_share_duration_s"],
                     loc["live_final_lat"], loc["live_final_lng"], loc["live_final_ts"]),
                )

        if max_rowid > since_rowid:
            set_state_int(archive, "last_message_rowid", max_rowid)

    return res
