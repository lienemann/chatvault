"""Extract status posts metadata."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, stable_message_id

log = logging.getLogger(__name__)


QUERY = """
SELECT
    s.message_row_id              AS rowid,
    s.status_distribution_mode    AS distribution_mode,
    s.audience_type               AS audience_type,
    s.can_be_reshared             AS can_be_reshared,
    s.has_embedded_music          AS has_embedded_music,
    s.is_mentioned                AS is_mentioned,
    s.status_mentions             AS status_mentions,
    s.poster_status_id            AS poster_status_id,
    m.key_id                      AS key_id,
    m.from_me                     AS from_me,
    j_chat.raw_string             AS chat_jid,
    j_sender.raw_string           AS sender_jid
FROM status_message_info s
JOIN message m         ON s.message_row_id = m._id
LEFT JOIN chat c       ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat   ON c.jid_row_id     = j_chat._id
LEFT JOIN jid j_sender ON m.sender_jid_row_id = j_sender._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="status_posts")

    with transaction(archive):
        for row in source.execute(QUERY):
            key_id = row["key_id"]
            if not key_id:
                res.rows_skipped += 1
                continue
            chat_jid = row["chat_jid"]
            from_me = bool(row["from_me"])
            message_id = stable_message_id(chat_jid, from_me, key_id)

            archive.execute(
                "INSERT OR REPLACE INTO status_posts(message_id, poster_jid, distribution_mode, "
                "                                    audience_type, can_be_reshared, has_embedded_music, "
                "                                    is_mentioned, mention_jids, poster_status_id, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    message_id,
                    row["sender_jid"],
                    row["distribution_mode"],
                    row["audience_type"],
                    row["can_be_reshared"],
                    row["has_embedded_music"],
                    row["is_mentioned"],
                    row["status_mentions"],
                    row["poster_status_id"],
                ),
            )
            res.rows_written += 1

    return res
