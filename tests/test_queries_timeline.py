"""Tests for timeline / forgotten / chat-member-explain queries."""

from __future__ import annotations

from chatvault.queries.timeline import (
    chat_member_explain,
    forgotten_contacts,
    timeline_for_member,
)


def _seed(archive_db) -> None:
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('4111@s.whatsapp.net', 'Anna', 'address_book', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('4222@s.whatsapp.net', 'Anna', 'address_book', '2026-01-01T00:00:00Z')"
    )  # second number for the same person
    archive_db.execute(
        "INSERT INTO identity_links(lid_jid, phone_jid, first_observed_ts, last_observed_ts) "
        "VALUES('500@lid', '4111@s.whatsapp.net', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) VALUES('CG@g.us', 'group', 'Café')"
    )
    archive_db.executemany(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES(?, ?, 'CG@g.us', ?, 0, ?, 'text', 0, ?)",
        [
            ("CG@g.us:0:K1", 1, "500@lid", "2026-04-01T10:00:00Z", "K1"),
            ("CG@g.us:0:K2", 2, "4222@s.whatsapp.net", "2026-04-02T10:00:00Z", "K2"),
            ("CG@g.us:0:K3", 3, "4111@s.whatsapp.net", "2026-04-03T10:00:00Z", "K3"),
        ],
    )
    archive_db.commit()


def test_timeline_expands_jids_via_contacts_and_identity_links(archive_db) -> None:
    _seed(archive_db)
    rows = timeline_for_member(archive_db, "Anna", limit=10)
    assert len(rows) == 3
    # Most recent first
    assert rows[0]["ts"] == "2026-04-03T10:00:00Z"


def test_timeline_returns_empty_for_unknown_name(archive_db) -> None:
    _seed(archive_db)
    assert timeline_for_member(archive_db, "Carla") == []


def test_forgotten_contacts_returns_no_outgoing(archive_db) -> None:
    _seed(archive_db)
    rows = forgotten_contacts(archive_db, days=1, limit=10)
    # Both contacts have no outgoing messages, so both are 'forgotten'.
    names = {r["name"] for r in rows}
    assert names == {"Anna"}  # de-duplicated by chat_jid not by name; both are Annas


def test_chat_member_explain_traces_chain(archive_db) -> None:
    _seed(archive_db)
    archive_db.execute(
        "INSERT INTO chat_members(chat_jid, member_jid, rank) VALUES('CG@g.us', '500@lid', 0)"
    )
    archive_db.commit()
    result = chat_member_explain(archive_db, "CG@g.us", "500@lid")
    assert result["resolved_name"] == "Anna"
    chain_text = "\n".join(result["chain"])
    assert "identity_links" in chain_text
    assert "Anna" in chain_text


def test_resolve_chat_finds_person_by_contact_name(archive_db) -> None:
    """A 1:1 chat (NULL subject) is reachable via the contact name."""
    from chatvault.queries.chats import resolve_chat
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('41799372433@s.whatsapp.net', 'Andrea Wirz', 'address_book', "
        "       '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41799372433@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES('m1', 1, '41799372433@s.whatsapp.net', '41799372433@s.whatsapp.net', "
        "       0, '2026-04-01', 'text', 0, 'k1')"
    )
    archive_db.commit()
    assert resolve_chat(archive_db, "Andrea") == "41799372433@s.whatsapp.net"
    assert resolve_chat(archive_db, "Andrea Wirz") == "41799372433@s.whatsapp.net"


def test_resolve_chat_finds_person_by_phone_number(archive_db) -> None:
    from chatvault.queries.chats import resolve_chat
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41799372433@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES('m1', 1, '41799372433@s.whatsapp.net', '41799372433@s.whatsapp.net', "
        "       0, '2026-04-01', 'text', 0, 'k1')"
    )
    archive_db.commit()
    assert resolve_chat(archive_db, "+41 79 937 24 33") == "41799372433@s.whatsapp.net"


def test_list_chats_backfills_display_name_for_1on1(archive_db) -> None:
    from chatvault.queries.chats import list_chats
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('41799372433@s.whatsapp.net', 'Andrea Wirz', 'address_book', "
        "       '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41799372433@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES('m1', 1, '41799372433@s.whatsapp.net', '41799372433@s.whatsapp.net', "
        "       0, '2026-04-01', 'text', 0, 'k1')"
    )
    archive_db.commit()
    rows = list_chats(archive_db)
    by_jid = {r["jid"]: r for r in rows}
    assert by_jid["41799372433@s.whatsapp.net"]["display_name"] == "Andrea Wirz"


def test_members_for_returns_peer_and_owner_for_1on1(archive_db) -> None:
    from chatvault.queries.chats import members_for
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41799372433@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.commit()
    members = members_for(archive_db, "41799372433@s.whatsapp.net")
    roles = {m["role"] for m in members}
    # 'owner' present only if the resolver discovers an owner JID; in tests
    # without seeded owner messages, only the peer is returned.
    assert "peer" in roles
    assert any(m["jid"] == "41799372433@s.whatsapp.net" for m in members)


def test_chat_message_at_python_indexing(archive_db) -> None:
    from chatvault.queries.chats import chat_message_at
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) VALUES('C@g.us', 'group', 'C')"
    )
    archive_db.executemany(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES(?, ?, 'C@g.us', '4111@s.whatsapp.net', 0, ?, 'text', 0, ?)",
        [
            (f"C@g.us:0:K{i}", i, f"2026-04-{i:02d}", f"K{i}")
            for i in range(1, 6)  # 5 messages, 2026-04-01 .. 2026-04-05
        ],
    )
    archive_db.commit()
    assert chat_message_at(archive_db, "C@g.us", 1) == "C@g.us:0:K1"   # oldest
    assert chat_message_at(archive_db, "C@g.us", 5) == "C@g.us:0:K5"   # newest by 1-based
    assert chat_message_at(archive_db, "C@g.us", -1) == "C@g.us:0:K5"  # newest
    assert chat_message_at(archive_db, "C@g.us", -5) == "C@g.us:0:K1"  # oldest from end


def test_chat_message_at_rejects_zero_and_oor(archive_db) -> None:
    import pytest
    from chatvault.queries.chats import chat_message_at
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) VALUES('C@g.us', 'group', 'C')"
    )
    archive_db.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES('C@g.us:0:K1', 1, 'C@g.us', '4111@s.whatsapp.net', 0, '2026-04-01', "
        "       'text', 0, 'K1')"
    )
    archive_db.commit()
    with pytest.raises(ValueError):
        chat_message_at(archive_db, "C@g.us", 0)
    with pytest.raises(LookupError):
        chat_message_at(archive_db, "C@g.us", 5)
    with pytest.raises(LookupError):
        chat_message_at(archive_db, "C@g.us", -5)


def test_resolve_message_full_id_passthrough(archive_db) -> None:
    from chatvault.queries.chats import resolve_message
    full = "41799372433@s.whatsapp.net:0:ABCDEF1234567890"
    assert resolve_message(archive_db, full) == full


def test_resolve_message_chat_colon_int(archive_db) -> None:
    from chatvault.queries.chats import resolve_message
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('41791234567@s.whatsapp.net', 'Anna', 'manual', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41791234567@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.executemany(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES(?, ?, '41791234567@s.whatsapp.net', '41791234567@s.whatsapp.net', "
        "       0, ?, 'text', 0, ?)",
        [
            ("41791234567@s.whatsapp.net:0:K1", 1, "2026-04-01", "K1"),
            ("41791234567@s.whatsapp.net:0:K2", 2, "2026-04-02", "K2"),
            ("41791234567@s.whatsapp.net:0:K3", 3, "2026-04-03", "K3"),
        ],
    )
    archive_db.commit()
    # Plain chat ref → newest
    assert resolve_message(archive_db, "Anna") == "41791234567@s.whatsapp.net:0:K3"
    # Chat:int positive
    assert resolve_message(archive_db, "Anna:1") == "41791234567@s.whatsapp.net:0:K1"
    # Chat:int negative (Python-style)
    assert resolve_message(archive_db, "Anna:-1") == "41791234567@s.whatsapp.net:0:K3"
    assert resolve_message(archive_db, "Anna:-2") == "41791234567@s.whatsapp.net:0:K2"


def test_resolve_chat_via_alias(archive_db, tmp_path, monkeypatch) -> None:
    """A user-defined alias resolves before names/numbers."""
    from chatvault.queries.chats import (
        resolve_chat,
        save_chat_aliases,
    )
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41799372433@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES('m1', 1, '41799372433@s.whatsapp.net', '41799372433@s.whatsapp.net', "
        "       0, '2026-04-01', 'text', 0, 'k1')"
    )
    archive_db.commit()
    monkeypatch.setenv("CHATVAULT_HOME", str(tmp_path))
    from chatvault.config import Paths
    cfg = Paths.default().config_dir
    save_chat_aliases(cfg, {"andi": "41799372433@s.whatsapp.net"})
    assert resolve_chat(archive_db, "andi") == "41799372433@s.whatsapp.net"


def test_resolve_chat_long_int_is_phone_not_cache_index(archive_db, tmp_path, monkeypatch) -> None:
    """A 11+ digit bare integer must be parsed as a phone, not a cache index."""
    from chatvault.queries.chats import resolve_chat
    archive_db.execute(
        "INSERT INTO chats(jid, kind, subject) "
        "VALUES('41799372433@s.whatsapp.net', 'user', NULL)"
    )
    archive_db.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES('m1', 1, '41799372433@s.whatsapp.net', '41799372433@s.whatsapp.net', "
        "       0, '2026-04-01', 'text', 0, 'k1')"
    )
    archive_db.commit()
    monkeypatch.setenv("CHATVAULT_HOME", str(tmp_path))
    # No chat-list cache exists, but the long int falls through to phone path.
    assert resolve_chat(archive_db, "41799372433") == "41799372433@s.whatsapp.net"
