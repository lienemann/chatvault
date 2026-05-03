"""Extract community structure (parent groups + child group memberships)."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso, to_raw_json

log = logging.getLogger(__name__)


COMMUNITY_QUERY = """
SELECT
    j.raw_string             AS parent_jid,
    cc.last_activity_ts      AS last_activity_ts,
    cc.join_ts               AS join_ts,
    cc.closed                AS closed,
    cc.nesting_state         AS nesting_state
FROM community_chat cc
JOIN chat c ON cc.chat_row_id = c._id
JOIN jid j  ON c.jid_row_id   = j._id
"""

PARENT_PARTICIPANTS_QUERY = """
SELECT
    j_parent.raw_string AS parent_jid,
    j_user.raw_string   AS user_jid
FROM parent_group_participants pp
JOIN jid j_parent ON pp.parent_group_jid_row_id = j_parent._id
JOIN jid j_user   ON pp.user_jid_row_id         = j_user._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="communities")

    # Aggregate participants per parent so the raw_json carries them.
    participants: dict[str, list[str]] = {}
    for r in source.execute(PARENT_PARTICIPANTS_QUERY):
        participants.setdefault(r["parent_jid"], []).append(r["user_jid"])

    with transaction(archive):
        for r in source.execute(COMMUNITY_QUERY):
            parent_jid = r["parent_jid"]
            if not parent_jid:
                continue
            payload = to_raw_json({
                "last_activity_ts": ms_to_iso(r["last_activity_ts"]),
                "join_ts": ms_to_iso(r["join_ts"]),
                "closed": r["closed"],
                "nesting_state": r["nesting_state"],
                "participants": participants.get(parent_jid, []),
            })
            archive.execute(
                "INSERT OR REPLACE INTO communities(parent_jid, raw_json) VALUES(?, ?)",
                (parent_jid, payload),
            )
            res.rows_written += 1

    return res
