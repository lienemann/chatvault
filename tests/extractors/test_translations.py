"""The translations extractor back-fills late-arriving source fields.

WhatsApp writes a translation (and a voice-note transcript) into a message row
long after that row first appeared, so the incremental `messages` extractor can
never see it. These tests pin the full-sweep behaviour that covers the gap.
"""

from __future__ import annotations

import sqlite3

import pytest

from chatvault.extractors import identities as ex_identities
from chatvault.extractors import messages as ex_messages
from chatvault.extractors import translations as ex_translations

MID_K1 = "120363000000000001@g.us:0:K1"


def _extract_messages(source: sqlite3.Connection, archive: sqlite3.Connection) -> None:
    ex_identities.extract(source, archive)
    ex_messages.extract(source, archive)


def test_translation_added_after_message_was_archived(populated_source, archive_db) -> None:
    _extract_messages(populated_source, archive_db)
    assert archive_db.execute(
        "SELECT translated_text FROM messages WHERE id = ?", (MID_K1,)
    ).fetchone()[0] is None

    # The user asks WhatsApp to translate an *old* message: rowid 10 is far
    # below the messages watermark, so only the sweep can pick this up.
    populated_source.execute("UPDATE message SET translated_text = 'Hallo' WHERE _id = 10")
    populated_source.commit()

    res = ex_translations.extract(populated_source, archive_db)

    assert res.rows_written == 1
    assert archive_db.execute(
        "SELECT translated_text FROM messages WHERE id = ?", (MID_K1,)
    ).fetchone()[0] == "Hallo"


def test_messages_rerun_alone_would_miss_it(populated_source, archive_db) -> None:
    _extract_messages(populated_source, archive_db)
    populated_source.execute("UPDATE message SET translated_text = 'Hallo' WHERE _id = 10")
    populated_source.commit()

    ex_messages.extract(populated_source, archive_db)  # watermark blocks the re-read

    assert archive_db.execute(
        "SELECT translated_text FROM messages WHERE id = ?", (MID_K1,)
    ).fetchone()[0] is None


def test_rerun_is_idempotent(populated_source, archive_db) -> None:
    _extract_messages(populated_source, archive_db)
    populated_source.execute("UPDATE message SET translated_text = 'Hallo' WHERE _id = 10")
    populated_source.commit()

    ex_translations.extract(populated_source, archive_db)
    second = ex_translations.extract(populated_source, archive_db)

    assert second.rows_written == 0
    assert second.rows_skipped == 1


def test_retranslation_overwrites(populated_source, archive_db) -> None:
    _extract_messages(populated_source, archive_db)
    populated_source.execute("UPDATE message SET translated_text = 'Hallo' WHERE _id = 10")
    populated_source.commit()
    ex_translations.extract(populated_source, archive_db)

    populated_source.execute("UPDATE message SET translated_text = 'Guten Tag' WHERE _id = 10")
    populated_source.commit()
    res = ex_translations.extract(populated_source, archive_db)

    assert res.rows_written == 1
    assert archive_db.execute(
        "SELECT translated_text FROM messages WHERE id = ?", (MID_K1,)
    ).fetchone()[0] == "Guten Tag"


def test_late_voice_note_transcript_fills_empty_text(populated_source, archive_db) -> None:
    populated_source.execute(
        "INSERT INTO message(_id, chat_row_id, from_me, key_id, sender_jid_row_id, "
        "                    timestamp, message_type, text_data, starred, status, origin) "
        "VALUES(20, 1, 0, 'K9', 2, 1700000300000, 2, NULL, 0, 0, 0)"
    )
    populated_source.commit()
    _extract_messages(populated_source, archive_db)
    mid = "120363000000000001@g.us:0:K9"
    assert archive_db.execute("SELECT text FROM messages WHERE id = ?", (mid,)).fetchone()[0] is None

    # Transcription finishes later and WhatsApp writes the transcript back.
    populated_source.execute("UPDATE message SET text_data = 'guten morgen' WHERE _id = 20")
    populated_source.commit()

    res = ex_translations.extract(populated_source, archive_db)

    assert res.rows_written == 1
    assert (
        archive_db.execute("SELECT text FROM messages WHERE id = ?", (mid,)).fetchone()[0]
        == "guten morgen"
    )


def test_existing_text_is_never_clobbered(populated_source, archive_db) -> None:
    populated_source.execute(
        "INSERT INTO message(_id, chat_row_id, from_me, key_id, sender_jid_row_id, "
        "                    timestamp, message_type, text_data, starred, status, origin) "
        "VALUES(20, 1, 0, 'K9', 2, 1700000300000, 2, 'original caption', 0, 0, 0)"
    )
    populated_source.commit()
    _extract_messages(populated_source, archive_db)

    populated_source.execute("UPDATE message SET text_data = 'something else' WHERE _id = 20")
    populated_source.commit()
    ex_translations.extract(populated_source, archive_db)

    mid = "120363000000000001@g.us:0:K9"
    assert (
        archive_db.execute("SELECT text FROM messages WHERE id = ?", (mid,)).fetchone()[0]
        == "original caption"
    )


def test_source_without_translation_column(populated_source, archive_db) -> None:
    """Older msgstore backups predate the feature — extract must degrade, not raise."""
    _extract_messages(populated_source, archive_db)
    populated_source.execute("ALTER TABLE message DROP COLUMN translated_text")
    populated_source.commit()

    res = ex_translations.extract(populated_source, archive_db)

    assert res.rows_written == 0
    assert res.notes == ["source has no message.translated_text column"]


def test_unknown_message_is_skipped_not_inserted(populated_source, archive_db) -> None:
    """A translated message we never archived must not conjure a `messages` row."""
    _extract_messages(populated_source, archive_db)
    before = archive_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    populated_source.execute(
        "INSERT INTO message(_id, chat_row_id, from_me, key_id, sender_jid_row_id, "
        "                    timestamp, message_type, text_data, starred, status, origin, "
        "                    translated_text) "
        "VALUES(30, 1, 0, 'K90', 2, 1700000400000, 90, 'x', 0, 0, 0, 'übersetzt')"
    )
    populated_source.commit()

    res = ex_translations.extract(populated_source, archive_db)

    assert archive_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == before
    assert res.rows_skipped == 1


@pytest.mark.parametrize("value", ["", None])
def test_empty_translation_ignored(populated_source, archive_db, value) -> None:
    _extract_messages(populated_source, archive_db)
    populated_source.execute("UPDATE message SET translated_text = ? WHERE _id = 10", (value,))
    populated_source.commit()

    res = ex_translations.extract(populated_source, archive_db)

    assert res.rows_written == 0
