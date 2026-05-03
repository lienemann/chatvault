"""Smoke-test the messages extractor end-to-end against a synthetic source."""

from __future__ import annotations

from chatvault.extractors import identities as ex_identities
from chatvault.extractors import messages as ex_messages
from chatvault.extractors import reactions as ex_reactions


def test_messages_extracted(populated_source, archive_db) -> None:
    ex_identities.extract(populated_source, archive_db)
    ex_messages.extract(populated_source, archive_db)

    rows = list(archive_db.execute("SELECT id, chat_jid, sender_jid, text, type, from_me FROM messages ORDER BY ts"))
    assert len(rows) == 3
    assert rows[0]["text"] == "Hello"
    assert rows[0]["from_me"] == 0
    assert rows[2]["text"] == "From me"
    assert rows[2]["from_me"] == 1
    assert all(r["chat_jid"] == "120363000000000001@g.us" for r in rows)


def test_messages_idempotent_on_rerun(populated_source, archive_db) -> None:
    ex_identities.extract(populated_source, archive_db)
    ex_messages.extract(populated_source, archive_db)
    first = archive_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    # second run should pick up nothing new
    res = ex_messages.extract(populated_source, archive_db)
    second = archive_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    assert first == second
    assert res.rows_written == 0


def test_reactions_dedup(populated_source, archive_db) -> None:
    ex_identities.extract(populated_source, archive_db)
    ex_messages.extract(populated_source, archive_db)
    ex_reactions.extract(populated_source, archive_db)
    ex_reactions.extract(populated_source, archive_db)  # idempotent

    rows = list(archive_db.execute("SELECT emoji, sender_jid FROM reactions"))
    assert len(rows) == 1
    assert rows[0]["emoji"] == "👍"
