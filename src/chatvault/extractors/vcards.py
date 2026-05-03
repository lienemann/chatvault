"""Extract shared contact (vcard) attachments."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, stable_message_id

log = logging.getLogger(__name__)


VCARD_QUERY = """
SELECT
    v.message_row_id   AS rowid,
    v.vcard            AS vcard,
    j.raw_string       AS contact_jid,
    m.key_id           AS key_id,
    m.from_me          AS from_me,
    j_chat.raw_string  AS chat_jid
FROM message_vcard v
LEFT JOIN message_vcard_jid vj  ON vj.vcard_row_id = v._id
LEFT JOIN jid j                  ON vj.vcard_jid_row_id = j._id
JOIN message m                   ON v.message_row_id = m._id
LEFT JOIN chat c                 ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat             ON c.jid_row_id     = j_chat._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="vcards")

    with transaction(archive):
        # Idempotent: rebuild per message.
        cleared: set[str] = set()
        for r in source.execute(VCARD_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            if mid not in cleared:
                archive.execute("DELETE FROM message_vcards WHERE message_id = ?", (mid,))
                cleared.add(mid)
            archive.execute(
                "INSERT INTO message_vcards(message_id, contact_jid, vcard) VALUES(?, ?, ?)",
                (mid, r["contact_jid"], r["vcard"]),
            )
            res.rows_written += 1

    return res
