"""Ad-hoc receipt lookup against the original source DB.

Receipt data lives in the source's `receipt_user` / `receipt_device` /
`receipts` tables, which are huge (~1M rows for a personal archive). Rather
than copying it all into chatvault, we resolve receipts on demand by joining
on the source DB's `key_id`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from ..extractors import ms_to_iso
from ..identities import NameResolver

# We read the source DB read-only and treat it as a peer SQLite file.
QUERY = """
SELECT
    r.receipt_timestamp        AS ts_ms,
    r.read_timestamp           AS read_ts_ms,
    r.played_timestamp         AS played_ts_ms,
    j.raw_string               AS recipient_jid
FROM receipt_user r
JOIN message m ON r.message_row_id = m._id
LEFT JOIN jid j ON r.receipt_user_jid_row_id = j._id
WHERE m.key_id = ?
ORDER BY r.receipt_timestamp
"""


def receipts_for(
    archive: sqlite3.Connection, source_db: Path, message_id: str
) -> list[dict[str, Any]]:
    """Return receipt records for a single chatvault message_id."""
    row = archive.execute(
        "SELECT key_id FROM messages WHERE id = ?", (message_id,)
    ).fetchone()
    if not row:
        msg = f"unknown message_id: {message_id}"
        raise LookupError(msg)
    key_id = row[0]

    resolver = NameResolver(archive)
    src = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row

    out: list[dict[str, Any]] = []
    try:
        for r in src.execute(QUERY, (key_id,)):
            status = "delivered"
            if r["read_ts_ms"]:
                status = "read"
            elif r["played_ts_ms"]:
                status = "played"
            out.append({
                "ts": (
                    ms_to_iso(r["read_ts_ms"]) or ms_to_iso(r["played_ts_ms"]) or
                    ms_to_iso(r["ts_ms"]) or ""
                ),
                "recipient": resolver.resolve(r["recipient_jid"]),
                "status": status,
            })
    finally:
        src.close()
    return out
