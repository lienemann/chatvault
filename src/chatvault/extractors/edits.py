"""Extract message edits, deduped by (message_id, edited_ts)."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, now_iso, stable_message_id

log = logging.getLogger(__name__)


QUERY = """
SELECT
    m._id                  AS rowid,
    m.key_id               AS key_id,
    m.from_me              AS from_me,
    m.timestamp            AS original_ts_ms,
    j_chat.raw_string      AS chat_jid,
    e.edited_timestamp     AS edited_ts_ms,
    e.original_key_id      AS original_key_id,
    e.sender_timestamp     AS sender_ts_ms
FROM message_edit_info e
JOIN message m       ON e.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="edits")
    observed_at = now_iso()

    with transaction(archive):
        for row in source.execute(QUERY):
            chat_jid = row["chat_jid"]
            key_id = row["key_id"]
            edited_ts = ms_to_iso(row["edited_ts_ms"])
            if not key_id or not edited_ts:
                res.rows_skipped += 1
                continue
            from_me = bool(row["from_me"])
            message_id = stable_message_id(chat_jid, from_me, key_id)

            archive.execute(
                "INSERT OR IGNORE INTO edits(message_id, edited_ts, sender_ts, original_key_id, "
                "                            original_ts, observed_ts) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (
                    message_id,
                    edited_ts,
                    ms_to_iso(row["sender_ts_ms"]),
                    row["original_key_id"],
                    ms_to_iso(row["original_ts_ms"]),
                    observed_at,
                ),
            )
            res.rows_written += 1

    return res
