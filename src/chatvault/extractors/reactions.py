"""Extract reactions, deduplicating cumulatively across runs."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, now_iso, parent_message_id

log = logging.getLogger(__name__)


# Pull every reaction the source remembers. The archive's PRIMARY KEY makes
# re-inserts a no-op via INSERT OR IGNORE — which preserves the original
# observed_ts (when we first saw the reaction).
QUERY = """
SELECT
    a.parent_message_row_id  AS parent_rowid,
    pm.key_id                AS parent_key_id,
    j_pchat.raw_string       AS parent_chat_jid,
    a.key_id                 AS reaction_key_id,
    r.reaction               AS emoji,
    r.sender_timestamp       AS sender_ts,
    a.timestamp              AS observed_ts,
    a.from_me                AS sender_from_me,
    j_sender.raw_string      AS sender_jid
FROM message_add_on a
JOIN message_add_on_reaction r ON a._id = r.message_add_on_row_id
LEFT JOIN message pm           ON a.parent_message_row_id = pm._id
LEFT JOIN chat c               ON pm.chat_row_id          = c._id
LEFT JOIN jid j_pchat          ON c.jid_row_id            = j_pchat._id
LEFT JOIN jid j_sender         ON a.sender_jid_row_id     = j_sender._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="reactions")
    archive_observed = now_iso()

    with transaction(archive):
        for row in source.execute(QUERY):
            parent_chat = row["parent_chat_jid"]
            parent_key = row["parent_key_id"]
            reaction_key = row["reaction_key_id"]
            if not parent_chat or not parent_key or not reaction_key:
                res.rows_skipped += 1
                continue

            archive.execute(
                "INSERT OR IGNORE INTO reactions(parent_message_id, parent_chat_jid, parent_key_id, "
                "                                reaction_key_id, sender_jid, sender_from_me, "
                "                                emoji, sender_ts, observed_ts) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    parent_message_id(parent_chat, parent_key),
                    parent_chat,
                    parent_key,
                    reaction_key,
                    row["sender_jid"],
                    int(row["sender_from_me"]) if row["sender_from_me"] is not None else None,
                    row["emoji"],
                    ms_to_iso(row["sender_ts"]),
                    archive_observed,
                ),
            )
            res.rows_written += 1

    return res
