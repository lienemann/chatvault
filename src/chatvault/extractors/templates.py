"""Extract template / interactive-button messages (business chats)."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, stable_message_id, to_raw_json

log = logging.getLogger(__name__)


TEMPLATE_QUERY = """
SELECT
    t.message_row_id        AS rowid,
    t.content_text_data     AS content,
    t.footer_text_data      AS footer,
    t.template_id           AS template_id,
    t.csat_trigger_expiration_ts AS csat_expiry_ts,
    t.category              AS category,
    t.tag                   AS tag,
    m.key_id                AS key_id,
    m.from_me               AS from_me,
    j_chat.raw_string       AS chat_jid
FROM message_template t
JOIN message m       ON t.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
"""

BUTTON_QUERY = """
SELECT
    b.message_row_id     AS rowid,
    b._id                AS button_id,
    b.text_data          AS text_data,
    b.button_type        AS button_type,
    b.used               AS used,
    b.selected_index     AS selected_index,
    b.extra_data         AS extra_data
FROM message_template_button b
ORDER BY b.message_row_id, b._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="templates")

    rowid_to_msg: dict[int, str] = {}
    buttons_by_rowid: dict[int, list[dict]] = {}

    for r in source.execute(BUTTON_QUERY):
        buttons_by_rowid.setdefault(r["rowid"], []).append({
            "id": r["button_id"],
            "text": r["text_data"],
            "type": r["button_type"],
            "used": r["used"],
            "selected_index": r["selected_index"],
            "extra": r["extra_data"],
        })

    with transaction(archive):
        for r in source.execute(TEMPLATE_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            rowid_to_msg[r["rowid"]] = mid
            payload = to_raw_json({
                "content": r["content"],
                "footer": r["footer"],
                "template_id": r["template_id"],
                "csat_expiry_ts": ms_to_iso(r["csat_expiry_ts"]),
                "category": r["category"],
                "tag": r["tag"],
                "buttons": buttons_by_rowid.get(r["rowid"], []),
            })
            archive.execute(
                "INSERT OR REPLACE INTO template_messages(message_id, raw_json) VALUES(?, ?)",
                (mid, payload),
            )
            res.rows_written += 1

    return res
