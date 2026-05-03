"""Extract voice-note metadata and transcription segments."""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, stable_message_id

log = logging.getLogger(__name__)


AUDIO_QUERY = """
SELECT
    a.message_row_id          AS rowid,
    a.transcription_status    AS status,
    a.transcription_locale    AS locale,
    a.transcription_id        AS transcription_id,
    a.waveform IS NOT NULL    AS has_waveform,
    m.key_id                  AS key_id,
    m.from_me                 AS from_me,
    j_chat.raw_string         AS chat_jid
FROM audio_data a
JOIN message m       ON a.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
"""

SEGMENTS_QUERY = """
SELECT
    s._id                AS seg_id,
    s.message_row_id     AS rowid,
    s.substring_start    AS substring_start,
    s.substring_length   AS substring_length,
    s.timestamp          AS seg_ts,
    s.duration           AS duration_ms,
    s.confidence         AS confidence,
    m.key_id             AS key_id,
    m.from_me            AS from_me,
    m.text_data          AS message_text,
    j_chat.raw_string    AS chat_jid
FROM transcription_segment s
JOIN message m       ON s.message_row_id = m._id
LEFT JOIN chat c     ON m.chat_row_id    = c._id
LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id
ORDER BY s.message_row_id, s._id
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="transcriptions")

    with transaction(archive):
        for r in source.execute(AUDIO_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            archive.execute(
                "INSERT OR REPLACE INTO message_audio(message_id, transcription_status, "
                "                                     transcription_locale, transcription_id, has_waveform) "
                "VALUES(?, ?, ?, ?, ?)",
                (mid, r["status"], r["locale"], r["transcription_id"], int(bool(r["has_waveform"]))),
            )
            res.rows_written += 1

        # Segments — clear and reinsert per-message.
        seq_per_msg: dict[str, int] = {}
        cleared: set[str] = set()
        for r in source.execute(SEGMENTS_QUERY):
            key_id = r["key_id"]
            if not key_id:
                continue
            mid = stable_message_id(r["chat_jid"], bool(r["from_me"]), key_id)
            if mid not in cleared:
                archive.execute(
                    "DELETE FROM message_transcription_segments WHERE message_id = ?",
                    (mid,),
                )
                cleared.add(mid)
                seq_per_msg[mid] = 0
            seq = seq_per_msg[mid]
            seq_per_msg[mid] = seq + 1

            text = None
            full = r["message_text"] or ""
            start = r["substring_start"]
            length = r["substring_length"]
            if isinstance(start, int) and isinstance(length, int) and start >= 0 and length > 0:
                text = full[start : start + length] or None

            archive.execute(
                "INSERT INTO message_transcription_segments(message_id, seq, text, "
                "                                           substring_start, substring_length, "
                "                                           seg_ts, duration_ms, confidence) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (mid, seq, text, start, length, r["seg_ts"], r["duration_ms"], r["confidence"]),
            )

    return res
