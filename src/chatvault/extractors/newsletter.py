"""Extract newsletter (channel) metadata, messages, and reactions."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, now_iso, stable_message_id

log = logging.getLogger(__name__)


META_QUERY = """
SELECT
    j.raw_string         AS chat_jid,
    n.name               AS name,
    n.description        AS description,
    n.handle             AS handle,
    n.picture_url        AS picture_url,
    n.preview_url        AS preview_url,
    n.invite_code        AS invite_code,
    n.subscribers_count  AS subscribers_count,
    n.verified           AS verified,
    n.suspended          AS suspended,
    n.deleted            AS deleted,
    n.privacy            AS privacy,
    n.membership         AS membership
FROM newsletter n
JOIN chat c ON n.chat_row_id = c._id
JOIN jid j  ON c.jid_row_id  = j._id
"""

MESSAGES_QUERY = """
SELECT
    n.message_row_id        AS rowid,
    n.server_message_id     AS server_message_id,
    n.view_count            AS view_count,
    n.forwards_count        AS forwards_count,
    n.comments_count        AS comments_count,
    n.is_paid_partnership   AS is_paid_partnership,
    m.key_id                AS key_id,
    m.from_me               AS from_me,
    j_chat.raw_string       AS chat_jid
FROM newsletter_message n
JOIN message m       ON n.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
"""

REACTIONS_QUERY = """
SELECT
    nr.message_row_id   AS rowid,
    nr.reaction         AS emoji,
    nr.reaction_count   AS count,
    m.key_id            AS key_id,
    m.from_me           AS from_me,
    j_chat.raw_string   AS chat_jid
FROM newsletter_message_reaction nr
JOIN message m       ON nr.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id     = c._id
LEFT JOIN jid j_chat ON c.jid_row_id      = j_chat._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="newsletter")
    observed_at = now_iso()

    with transaction(archive):
        # Metadata
        for r in source.execute(META_QUERY):
            archive.execute(
                "INSERT OR REPLACE INTO newsletter_metadata(chat_jid, name, description, handle, "
                "                                           picture_url, preview_url, invite_code, "
                "                                           subscribers_count, verified, suspended, "
                "                                           deleted, privacy, membership, observed_at, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    r["chat_jid"], r["name"], r["description"], r["handle"], r["picture_url"],
                    r["preview_url"], r["invite_code"], r["subscribers_count"], r["verified"],
                    r["suspended"], r["deleted"], r["privacy"], r["membership"], observed_at,
                ),
            )

        # Per-message stats
        for r in source.execute(MESSAGES_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            archive.execute(
                "INSERT OR REPLACE INTO newsletter_messages(message_id, server_message_id, view_count, "
                "                                           forwards_count, comments_count, "
                "                                           is_paid_partnership, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, NULL)",
                (mid, r["server_message_id"], r["view_count"], r["forwards_count"],
                 r["comments_count"], r["is_paid_partnership"]),
            )
            res.rows_written += 1

        # Aggregate reactions: idempotent via UNIQUE.
        for r in source.execute(REACTIONS_QUERY):
            key_id = r["key_id"]
            if not key_id or not r["emoji"]:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            archive.execute(
                "INSERT INTO newsletter_message_reactions(message_id, emoji, count) "
                "VALUES(?, ?, ?) "
                "ON CONFLICT(message_id, emoji) DO UPDATE SET count = excluded.count",
                (mid, r["emoji"], r["count"] or 0),
            )

    return res
