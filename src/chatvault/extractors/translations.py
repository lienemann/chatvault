"""Back-fill fields the source app writes to *existing* messages after the fact.

Two cases, both invisible to the `messages` extractor (which only reads source
rowids above its watermark, so a row it has already seen is never re-read):

  * WhatsApp's own translation of a message (`message.translated_text`) — asked
    for by the user days or months after the message arrived;
  * the voice-note transcript WhatsApp writes back into `message.text_data`
    once on-device transcription finishes.

Hence the full-table sweep. It is cheap: the WHERE clause keeps only rows that
actually carry one of the two payloads.
"""

from __future__ import annotations

import logging
import sqlite3

from chatvault.db import transaction

from . import ExtractorResult, stable_message_id

log = logging.getLogger(__name__)


AUDIO_TYPE = 2  # message.message_type for voice notes / audio


def _has_translation_column(source: sqlite3.Connection) -> bool:
    """`message.translated_text` only exists on msgstore versions that ship the
    in-app translation feature — older backups simply lack the column."""
    try:
        rows = source.execute("PRAGMA table_info(message)").fetchall()
    except sqlite3.OperationalError:
        return False
    return any(r[1] == "translated_text" for r in rows)


def _query(has_translation: bool) -> str:
    translated = "m.translated_text" if has_translation else "NULL"
    return f"""
SELECT
    m.key_id           AS key_id,
    m.from_me          AS from_me,
    m.message_type     AS type_raw,
    m.text_data        AS text,
    {translated}       AS translated_text,
    j_chat.raw_string  AS chat_jid
FROM message m
LEFT JOIN chat c     ON m.chat_row_id = c._id
LEFT JOIN jid j_chat ON c.jid_row_id  = j_chat._id
WHERE ({translated} IS NOT NULL AND {translated} <> '')
   OR (m.message_type = {AUDIO_TYPE} AND m.text_data IS NOT NULL AND m.text_data <> '')
"""


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="translations")

    has_translation = _has_translation_column(source)
    if not has_translation:
        res.with_note("source has no message.translated_text column")

    with transaction(archive):
        for r in source.execute(_query(has_translation)):
            key_id = r["key_id"]
            chat_jid = r["chat_jid"]
            if not key_id or not chat_jid:
                res.rows_skipped += 1
                continue
            mid = stable_message_id(chat_jid, bool(r["from_me"]), key_id)
            touched = 0

            translated = r["translated_text"]
            if translated:
                # Only write on change — keeps re-runs from churning rows.
                cur = archive.execute(
                    "UPDATE messages SET translated_text = ? "
                    "WHERE id = ? AND (translated_text IS NULL OR translated_text <> ?)",
                    (translated, mid, translated),
                )
                touched += cur.rowcount

            # Late transcript: fill `text` only while it is still empty, so we
            # never clobber a caption or an edited body we already captured.
            text = r["text"]
            if text and r["type_raw"] == AUDIO_TYPE:
                cur = archive.execute(
                    "UPDATE messages SET text = ? WHERE id = ? AND (text IS NULL OR text = '')",
                    (text, mid),
                )
                touched += cur.rowcount

            if touched:
                res.rows_written += touched
            else:
                res.rows_skipped += 1

    return res
