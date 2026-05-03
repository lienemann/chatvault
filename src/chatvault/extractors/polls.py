"""Extract polls, their options, and votes."""

from __future__ import annotations

import json
import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, stable_message_id

log = logging.getLogger(__name__)


POLLS_QUERY = """
SELECT
    p.message_row_id              AS rowid,
    p.selectable_options_count    AS selectable_count,
    p.poll_type                   AS poll_type,
    p.content_type                AS content_type,
    p.end_time                    AS end_time_ms,
    p.allow_add_option            AS allow_add_option,
    p.hide_participant_names      AS hide_participants,
    p.invalid_state               AS invalid,
    m.key_id                      AS key_id,
    m.from_me                     AS from_me,
    j_chat.raw_string             AS chat_jid
FROM message_poll p
JOIN message m       ON p.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
"""

OPTIONS_QUERY = """
SELECT
    o._id                AS option_id,
    o.message_row_id     AS message_row_id,
    o.option_name        AS name,
    o.vote_total         AS vote_total
FROM message_poll_option o
ORDER BY o.message_row_id, o._id
"""

VOTES_QUERY = """
SELECT
    a._id                    AS add_on_id,
    a.parent_message_row_id  AS parent_rowid,
    pm.key_id                AS parent_key_id,
    pm.from_me               AS parent_from_me,
    j_pchat.raw_string       AS parent_chat_jid,
    j_voter.raw_string       AS voter_jid,
    pv.sender_timestamp      AS sender_ts_ms
FROM message_add_on_poll_vote pv
JOIN message_add_on a ON pv.message_add_on_row_id = a._id
LEFT JOIN message pm  ON a.parent_message_row_id  = pm._id
LEFT JOIN chat c      ON pm.chat_row_id           = c._id
LEFT JOIN jid j_pchat ON c.jid_row_id             = j_pchat._id
LEFT JOIN jid j_voter ON a.sender_jid_row_id      = j_voter._id
"""

VOTE_SELECTIONS_QUERY = """
SELECT message_add_on_row_id, message_poll_option_id
FROM message_add_on_poll_vote_selected_option
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="polls")

    # Map source rowid -> stable message_id for option/vote linking.
    rowid_to_msg: dict[int, str] = {}

    with transaction(archive):
        for row in source.execute(POLLS_QUERY):
            chat_jid = row["chat_jid"]
            key_id = row["key_id"]
            if not key_id:
                res.rows_skipped += 1
                continue
            from_me = bool(row["from_me"])
            message_id = stable_message_id(chat_jid, from_me, key_id)
            rowid_to_msg[row["rowid"]] = message_id

            archive.execute(
                "INSERT OR REPLACE INTO polls(message_id, selectable_count, poll_type, "
                "                             content_type, end_time_ts, allow_add_option, "
                "                             hide_participants, invalid, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    message_id, row["selectable_count"], row["poll_type"], row["content_type"],
                    ms_to_iso(row["end_time_ms"]), row["allow_add_option"],
                    row["hide_participants"], row["invalid"],
                ),
            )
            res.rows_written += 1

        # Options — idempotent: wipe per message and reinsert in order.
        for rowid, message_id in rowid_to_msg.items():
            archive.execute("DELETE FROM poll_options WHERE message_id = ?", (message_id,))
        idx_per_msg: dict[str, int] = {}
        option_id_to_index: dict[int, int] = {}  # source option _id → option_index
        for r in source.execute(OPTIONS_QUERY):
            mid = rowid_to_msg.get(r["message_row_id"])
            if not mid:
                continue
            i = idx_per_msg.get(mid, 0)
            option_id_to_index[r["option_id"]] = i
            archive.execute(
                "INSERT INTO poll_options(message_id, option_index, option_name, vote_total) "
                "VALUES(?, ?, ?, ?)",
                (mid, i, r["name"], r["vote_total"]),
            )
            idx_per_msg[mid] = i + 1

        # Pre-aggregate vote selections by add_on row id.
        selections: dict[int, list[int]] = {}
        for r in source.execute(VOTE_SELECTIONS_QUERY):
            opt_idx = option_id_to_index.get(r["message_poll_option_id"])
            if opt_idx is None:
                continue
            selections.setdefault(r["message_add_on_row_id"], []).append(opt_idx)

        # Votes — idempotent: clear all and reinsert.
        archive.execute("DELETE FROM poll_votes")
        for r in source.execute(VOTES_QUERY):
            parent_chat = r["parent_chat_jid"]
            parent_key = r["parent_key_id"]
            if not parent_chat or not parent_key:
                continue
            parent_id = stable_message_id(parent_chat, bool(r["parent_from_me"]), parent_key)
            sel_list = sorted(selections.get(r["add_on_id"], []))
            archive.execute(
                "INSERT INTO poll_votes(parent_message_id, voter_jid, sender_ts, selected_indexes) "
                "VALUES(?, ?, ?, ?)",
                (parent_id, r["voter_jid"], ms_to_iso(r["sender_ts_ms"]),
                 json.dumps(sel_list, ensure_ascii=False)),
            )

    return res
