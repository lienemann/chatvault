"""Extract call logs and per-call participants."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, ms_to_iso

log = logging.getLogger(__name__)


CALL_QUERY = """
SELECT
    c._id                          AS rowid,
    c.call_id                      AS call_id,
    j_peer.raw_string              AS peer_jid,
    j_group.raw_string             AS group_jid,
    j_creator.raw_string           AS creator_jid,
    c.from_me                      AS from_me,
    c.timestamp                    AS ts_ms,
    c.video_call                   AS video,
    c.duration                     AS duration_s,
    c.call_result                  AS result,
    c.bytes_transferred            AS bytes_transferred,
    c.is_dnd_mode_on               AS is_dnd_mode_on,
    c.call_type                    AS call_type,
    c.scheduled_id                 AS scheduled_id
FROM call_log c
LEFT JOIN jid j_peer    ON c.jid_row_id = j_peer._id
LEFT JOIN jid j_group   ON c.group_jid_row_id = j_group._id
LEFT JOIN jid j_creator ON c.call_creator_device_jid_row_id = j_creator._id
"""

PARTICIPANTS_QUERY = """
SELECT
    cl.call_id          AS call_id,
    j.raw_string        AS participant_jid,
    p.call_result       AS result
FROM call_log_participant_v2 p
JOIN call_log cl ON p.call_log_row_id = cl._id
LEFT JOIN jid j  ON p.jid_row_id      = j._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="calls")

    with transaction(archive):
        for row in source.execute(CALL_QUERY):
            call_id = row["call_id"]
            if not call_id:
                res.rows_skipped += 1
                continue
            archive.execute(
                "INSERT OR REPLACE INTO calls(call_id, source_rowid, peer_jid, group_jid, "
                "                             creator_jid, from_me, ts, video, duration_s, "
                "                             result, bytes_transferred, is_dnd_mode_on, "
                "                             call_type, scheduled_id, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    call_id, row["rowid"], row["peer_jid"], row["group_jid"], row["creator_jid"],
                    int(row["from_me"]) if row["from_me"] is not None else None,
                    ms_to_iso(row["ts_ms"]),
                    int(row["video"]) if row["video"] is not None else None,
                    row["duration_s"], row["result"], row["bytes_transferred"],
                    row["is_dnd_mode_on"], row["call_type"], row["scheduled_id"],
                ),
            )
            res.rows_written += 1

        # Idempotent participants: clear+insert.
        archive.execute("DELETE FROM call_participants")
        for r in source.execute(PARTICIPANTS_QUERY):
            if not r["call_id"]:
                continue
            archive.execute(
                "INSERT INTO call_participants(call_id, participant_jid, result) "
                "VALUES(?, ?, ?)",
                (r["call_id"], r["participant_jid"], r["result"]),
            )

    return res
