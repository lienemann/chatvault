"""Extract bot/AI message metadata + AI thread metadata + rich response info."""

from __future__ import annotations

import json
import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, stable_message_id, to_raw_json

log = logging.getLogger(__name__)


BOT_QUERY = """
SELECT
    b.message_row_id      AS rowid,
    b.target_id           AS target_id,
    b.message_state       AS message_state,
    j_inv.raw_string      AS invoker_jid,
    j_bot.raw_string      AS bot_jid,
    b.model_type          AS model_type,
    b.message_disclaimer  AS disclaimer,
    b.bot_response_id     AS bot_response_id,
    m.key_id              AS key_id,
    m.from_me             AS from_me,
    j_chat.raw_string     AS chat_jid
FROM bot_message_info b
JOIN message m         ON b.message_row_id = m._id
LEFT JOIN chat c       ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat   ON c.jid_row_id     = j_chat._id
LEFT JOIN jid j_inv    ON b.invoker_jid_row_id = j_inv._id
LEFT JOIN jid j_bot    ON b.bot_jid_row_id     = j_bot._id
"""

THREAD_QUERY = """
SELECT
    t.thread_id_row_id      AS thread_id,
    t.title                 AS title,
    t.creation_ts           AS creation_ts,
    t.last_message_timestamp AS last_msg_ts,
    t.variant               AS variant,
    t.unseen_message_count  AS unseen,
    t.selected_mode         AS selected_mode
FROM ai_thread_info t
"""

RICH_QUERY = """
SELECT
    r.message_row_id                    AS rowid,
    r.ai_rich_response_message_type     AS type,
    r.ai_rich_response_submessage_types AS submessage_types,
    r.planning_status                   AS planning_status,
    m.key_id                            AS key_id,
    m.from_me                           AS from_me,
    j_chat.raw_string                   AS chat_jid
FROM ai_rich_response_message_core_info r
JOIN message m       ON r.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="bot_messages")

    with transaction(archive):
        # bot_messages
        for r in source.execute(BOT_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            payload = to_raw_json({
                "target_id": r["target_id"],
                "message_state": r["message_state"],
                "invoker_jid": r["invoker_jid"],
                "bot_jid": r["bot_jid"],
                "model_type": r["model_type"],
                "disclaimer": r["disclaimer"],
                "bot_response_id": r["bot_response_id"],
            })
            archive.execute(
                "INSERT OR REPLACE INTO bot_messages(message_id, raw_json) VALUES(?, ?)",
                (mid, payload),
            )
            res.rows_written += 1

        # rich response: fold into bot_messages.raw_json (merge) — separate table
        # would imply two stable rows per message; the consumer can filter on
        # the raw payload's keys.
        for r in source.execute(RICH_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            existing = archive.execute(
                "SELECT raw_json FROM bot_messages WHERE message_id = ?", (mid,)
            ).fetchone()
            base: dict = json.loads(existing[0]) if existing and existing[0] else {}
            base["rich_response"] = {
                "type": r["type"],
                "submessage_types": r["submessage_types"],
                "planning_status": r["planning_status"],
            }
            archive.execute(
                "INSERT INTO bot_messages(message_id, raw_json) VALUES(?, ?) "
                "ON CONFLICT(message_id) DO UPDATE SET raw_json = excluded.raw_json",
                (mid, json.dumps(base, ensure_ascii=False, separators=(",", ":"))),
            )

        # ai threads
        for r in source.execute(THREAD_QUERY):
            payload = to_raw_json({
                "title": r["title"],
                "creation_ts": ms_to_iso(r["creation_ts"]),
                "last_msg_ts": ms_to_iso(r["last_msg_ts"]),
                "variant": r["variant"],
                "unseen": r["unseen"],
                "selected_mode": r["selected_mode"],
            })
            archive.execute(
                "INSERT OR REPLACE INTO ai_threads(thread_id, raw_json) VALUES(?, ?)",
                (r["thread_id"], payload),
            )

    return res
