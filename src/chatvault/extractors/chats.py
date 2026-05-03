"""Extract chat metadata, with snapshot history."""

from __future__ import annotations

import json
import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, jid_kind, ms_to_iso, now_iso

log = logging.getLogger(__name__)


CHAT_QUERY = """
SELECT
    j.raw_string                              AS jid,
    c.subject                                 AS subject,
    c.created_timestamp                       AS created_ts,
    c.archived                                AS archived,
    c.hidden                                  AS hidden,
    c.chat_lock                               AS locked,
    c.ephemeral_expiration                    AS ephemeral_seconds,
    c.group_type                              AS group_type,
    c.group_member_count                      AS group_member_count,
    c.is_contact                              AS is_contact,
    c.sort_timestamp                          AS sort_ts
FROM chat c
LEFT JOIN jid j ON c.jid_row_id = j._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="chats")
    observed_at = now_iso()

    # Pre-load existing snapshots so we can compare and only write history on change.
    prev = {
        r["jid"]: dict(r)
        for r in archive.execute(
            "SELECT jid, subject, archived, hidden, locked, ephemeral_seconds, "
            "       group_type, group_member_count, is_contact "
            "FROM chats"
        )
    }

    with transaction(archive):
        for r in source.execute(CHAT_QUERY):
            jid = r["jid"]
            if not jid:
                res.rows_skipped += 1
                continue

            kind = jid_kind(jid)
            row = {
                "jid": jid,
                "kind": kind,
                "subject": r["subject"],
                "created_ts": ms_to_iso(r["created_ts"]),
                "archived": int(r["archived"] or 0),
                "hidden": int(r["hidden"] or 0),
                "locked": int(r["locked"] or 0),
                "pinned": 0,                                # not tracked separately in chat table
                "muted_until_ts": None,                     # mute lives in a separate table; revisit
                "ephemeral_seconds": r["ephemeral_seconds"],
                "group_type": r["group_type"],
                "group_member_count": r["group_member_count"],
                "is_contact": r["is_contact"],
                "last_message_ts": ms_to_iso(r["sort_ts"]),
                "last_seen_ts": observed_at,
                "raw_json": None,
            }

            archive.execute(
                "INSERT INTO chats(jid, kind, subject, created_ts, archived, hidden, locked, "
                "                  pinned, muted_until_ts, ephemeral_seconds, group_type, "
                "                  group_member_count, is_contact, last_message_ts, last_seen_ts, raw_json) "
                "VALUES(:jid, :kind, :subject, :created_ts, :archived, :hidden, :locked, "
                "       :pinned, :muted_until_ts, :ephemeral_seconds, :group_type, "
                "       :group_member_count, :is_contact, :last_message_ts, :last_seen_ts, :raw_json) "
                "ON CONFLICT(jid) DO UPDATE SET "
                "  kind               = excluded.kind, "
                "  subject            = excluded.subject, "
                "  archived           = excluded.archived, "
                "  hidden             = excluded.hidden, "
                "  locked             = excluded.locked, "
                "  ephemeral_seconds  = excluded.ephemeral_seconds, "
                "  group_type         = excluded.group_type, "
                "  group_member_count = excluded.group_member_count, "
                "  is_contact         = excluded.is_contact, "
                "  last_message_ts    = excluded.last_message_ts, "
                "  last_seen_ts       = excluded.last_seen_ts",
                row,
            )

            # Only write to chats_history when an interesting field changes —
            # avoids unbounded growth on idle re-runs.
            interesting = {
                "subject": row["subject"], "archived": row["archived"],
                "hidden": row["hidden"], "locked": row["locked"],
                "ephemeral_seconds": row["ephemeral_seconds"],
                "group_type": row["group_type"],
                "group_member_count": row["group_member_count"],
                "is_contact": row["is_contact"],
            }
            prev_row = prev.get(jid)
            changed = prev_row is None or any(
                prev_row.get(k) != v for k, v in interesting.items()
            )
            if changed:
                archive.execute(
                    "INSERT INTO chats_history(jid, observed_at, snapshot_json) VALUES(?, ?, ?)",
                    (jid, observed_at, json.dumps(row, ensure_ascii=False, default=str)),
                )

            res.rows_written += 1

    return res
