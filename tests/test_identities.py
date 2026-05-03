"""Resolver tests: name lookup chain and edge cases."""

from __future__ import annotations

from chatvault.identities import NameResolver, jid_kind, jid_user_part


def test_jid_kind_classifies() -> None:
    assert jid_kind("123@s.whatsapp.net") == "user"
    assert jid_kind("123@lid") == "lid"
    assert jid_kind("123@g.us") == "group"
    assert jid_kind("123@newsletter") == "newsletter"
    assert jid_kind(None) == "unknown"
    assert jid_kind("garbage") == "other"


def test_jid_user_part() -> None:
    assert jid_user_part("123@s.whatsapp.net") == "123"
    assert jid_user_part(None) is None


def test_resolver_falls_back_to_phone(archive_db) -> None:
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('41791234567@s.whatsapp.net', 'Anna', 'address_book', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO identity_links(lid_jid, phone_jid, first_observed_ts, last_observed_ts) "
        "VALUES('100@lid', '41791234567@s.whatsapp.net', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()

    r = NameResolver(archive_db)
    assert r.resolve("41791234567@s.whatsapp.net") == "Anna"
    assert r.resolve("100@lid") == "Anna"
    assert r.resolve("unknown@s.whatsapp.net") == "+unknown"
    assert r.resolve("rogue@lid") == "lid:rogue"


def test_resolver_uses_real_display_name_for_lid(archive_db) -> None:
    archive_db.execute(
        "INSERT INTO identity_display_names(jid, display_name, observed_at) "
        "VALUES('300@lid', 'Berit', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    r = NameResolver(archive_db)
    assert r.resolve("300@lid") == "Berit"


def test_resolver_skips_masked_phone_display_name(archive_db) -> None:
    archive_db.execute(
        "INSERT INTO identity_display_names(jid, display_name, observed_at) "
        "VALUES('400@lid', '+41∙∙∙∙∙∙∙99', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    r = NameResolver(archive_db)
    assert r.resolve("400@lid") == "lid:400"


def test_resolver_uses_display_name_for_phone_jid(archive_db) -> None:
    """Push-name on a phone JID (not in address book) wins over '+phone'."""
    archive_db.execute(
        "INSERT INTO identity_display_names(jid, display_name, observed_at) "
        "VALUES('491723105522@s.whatsapp.net', 'Raphael', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    r = NameResolver(archive_db)
    assert r.resolve("491723105522@s.whatsapp.net") == "Raphael"


def test_resolver_uses_lid_display_name_via_link(archive_db) -> None:
    """LID with a phone link but no contact: prefer LID push-name over '+phone'."""
    archive_db.execute(
        "INSERT INTO identity_links(lid_jid, phone_jid, first_observed_ts, last_observed_ts) "
        "VALUES('170@lid', '491723105522@s.whatsapp.net', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO identity_display_names(jid, display_name, observed_at) "
        "VALUES('170@lid', 'Raphael', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    r = NameResolver(archive_db)
    assert r.resolve("170@lid") == "Raphael"
    assert r.resolve("491723105522@s.whatsapp.net") == "+49 172 3105522"


def test_resolver_contact_beats_push_name(archive_db) -> None:
    """Address-book name still wins over a push-name."""
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('491723105522@s.whatsapp.net', 'Raphael Lienemann', 'address_book', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO identity_display_names(jid, display_name, observed_at) "
        "VALUES('491723105522@s.whatsapp.net', 'Raphi', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    r = NameResolver(archive_db)
    assert r.resolve("491723105522@s.whatsapp.net") == "Raphael Lienemann"
