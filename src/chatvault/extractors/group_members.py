"""Extract group memberships and their history."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, now_iso

log = logging.getLogger(__name__)


CURRENT_QUERY = """
SELECT
    g.raw_string  AS chat_jid,
    j.raw_string  AS member_jid,
    gpu.rank      AS rank,
    gpu.pending   AS pending,
    gpu.add_timestamp AS joined_ts,
    gpu.join_method AS join_method,
    gpu.label     AS label
FROM group_participant_user gpu
JOIN jid g ON g._id = gpu.group_jid_row_id
JOIN jid j ON j._id = gpu.user_jid_row_id
"""

PAST_QUERY = """
SELECT
    g.raw_string  AS chat_jid,
    j.raw_string  AS member_jid,
    gp.is_leave   AS is_leave,
    gp.timestamp  AS event_ts
FROM group_past_participant_user gp
JOIN jid g ON g._id = gp.group_jid_row_id
JOIN jid j ON j._id = gp.user_jid_row_id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="group_members")
    observed_at = now_iso()

    # ---- snapshot of current membership ----
    seen_pairs: set[tuple[str, str]] = set()

    # We compute the diff against existing chat_members rows: any new (chat, member)
    # pair triggers a 'join' history row, any rank change a 'role_change'.
    existing = {
        (row["chat_jid"], row["member_jid"]): row["rank"]
        for row in archive.execute("SELECT chat_jid, member_jid, rank FROM chat_members")
    }

    with transaction(archive):
        for r in source.execute(CURRENT_QUERY):
            chat_jid = r["chat_jid"]
            member_jid = r["member_jid"]
            if not chat_jid or not member_jid:
                res.rows_skipped += 1
                continue
            seen_pairs.add((chat_jid, member_jid))
            joined_ts = ms_to_iso(r["joined_ts"])
            new_rank = r["rank"]

            old_rank = existing.get((chat_jid, member_jid))
            archive.execute(
                "INSERT INTO chat_members(chat_jid, member_jid, rank, pending, joined_ts, "
                "                         join_method, label) "
                "VALUES(?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(chat_jid, member_jid) DO UPDATE SET "
                "  rank        = excluded.rank, "
                "  pending     = excluded.pending, "
                "  joined_ts   = COALESCE(chat_members.joined_ts, excluded.joined_ts), "
                "  join_method = excluded.join_method, "
                "  label       = excluded.label",
                (chat_jid, member_jid, new_rank, int(r["pending"] or 0), joined_ts,
                 r["join_method"], r["label"]),
            )

            if old_rank is None:
                archive.execute(
                    "INSERT INTO chat_members_history(chat_jid, member_jid, op, rank, "
                    "                                 observed_at, source) "
                    "VALUES(?, ?, 'join', ?, ?, 'snapshot')",
                    (chat_jid, member_jid, new_rank, joined_ts or observed_at),
                )
            elif old_rank != new_rank:
                archive.execute(
                    "INSERT INTO chat_members_history(chat_jid, member_jid, op, rank, "
                    "                                 old_rank, observed_at, source) "
                    "VALUES(?, ?, 'role_change', ?, ?, ?, 'snapshot')",
                    (chat_jid, member_jid, new_rank, old_rank, observed_at),
                )

            res.rows_written += 1

        # Detect leaves: rows present last time but not this time.
        for (chat_jid, member_jid) in set(existing.keys()) - seen_pairs:
            archive.execute(
                "DELETE FROM chat_members WHERE chat_jid = ? AND member_jid = ?",
                (chat_jid, member_jid),
            )
            archive.execute(
                "INSERT INTO chat_members_history(chat_jid, member_jid, op, observed_at, source) "
                "VALUES(?, ?, 'leave', ?, 'snapshot')",
                (chat_jid, member_jid, observed_at),
            )

        # ---- past_participants: known historical events ----
        for r in source.execute(PAST_QUERY):
            chat_jid = r["chat_jid"]
            member_jid = r["member_jid"]
            if not chat_jid or not member_jid:
                continue
            event_ts = ms_to_iso(r["event_ts"]) or observed_at
            op = "leave" if r["is_leave"] else "join"
            archive.execute(
                "INSERT INTO chat_members_history(chat_jid, member_jid, op, observed_at, source) "
                "VALUES(?, ?, ?, ?, 'past_participant')",
                (chat_jid, member_jid, op, event_ts),
            )

    return res
