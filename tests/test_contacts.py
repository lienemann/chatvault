"""Phone-number normalisation and contacts sync."""

from __future__ import annotations

import io
import json

import pytest

from chatvault.contacts import (
    import_pins_csv,
    normalize_number,
    number_to_jid,
    pin_contact,
    sync_contacts,
    unpin_contact,
)


def test_normalize_e164_plus() -> None:
    assert normalize_number("+41 79 799 10 26") == "41797991026"


def test_normalize_national_zero() -> None:
    assert normalize_number("0792621008", default_country="41") == "41792621008"


def test_normalize_double_zero() -> None:
    assert normalize_number("004915777785558") == "4915777785558"


def test_normalize_rejects_special() -> None:
    assert normalize_number("*100#") is None
    assert normalize_number("") is None
    assert normalize_number(None) is None
    assert normalize_number("abc") is None


def test_number_to_jid() -> None:
    assert number_to_jid("+41 79 799 10 26") == "41797991026@s.whatsapp.net"
    assert number_to_jid(None) is None


def test_sync_contacts_round_trip(archive_db, monkeypatch) -> None:
    payload = json.dumps([
        {"name": "Anna", "number": "+41 79 123 45 67"},
        {"name": "Berit", "number": "0792224444"},
        {"name": "junk", "number": "*100#"},
    ])
    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    res = sync_contacts(archive_db, default_country="41", from_stdin=True)
    assert res.total == 2
    assert res.set_count == 2
    assert res.remove_count == 0

    rows = {r["phone_jid"]: r["name"]
            for r in archive_db.execute("SELECT phone_jid, name FROM contacts")}
    assert rows == {
        "41791234567@s.whatsapp.net": "Anna",
        "41792224444@s.whatsapp.net": "Berit",
    }
    history_count = archive_db.execute(
        "SELECT COUNT(*) FROM contacts_history WHERE op='set'"
    ).fetchone()[0]
    assert history_count == 2


def test_sync_contacts_idempotent(archive_db, monkeypatch) -> None:
    payload = json.dumps([{"name": "Anna", "number": "+41 79 123 45 67"}])
    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    sync_contacts(archive_db, from_stdin=True)
    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    res = sync_contacts(archive_db, from_stdin=True)
    assert res.set_count == 0
    assert res.remove_count == 0


def test_sync_contacts_detects_removals(archive_db, monkeypatch) -> None:
    initial = json.dumps([
        {"name": "Anna", "number": "+41 79 123 45 67"},
        {"name": "Berit", "number": "0792224444"},
    ])
    monkeypatch.setattr("sys.stdin", io.StringIO(initial))
    sync_contacts(archive_db, from_stdin=True)

    second = json.dumps([{"name": "Anna", "number": "+41 79 123 45 67"}])
    monkeypatch.setattr("sys.stdin", io.StringIO(second))
    res = sync_contacts(archive_db, from_stdin=True)
    assert res.remove_count == 1
    remaining = archive_db.execute("SELECT phone_jid FROM contacts").fetchall()
    assert len(remaining) == 1


def test_pin_contact_accepts_phone_number(archive_db) -> None:
    jid = pin_contact(archive_db, "+49 172 3105522", "Raphael")
    assert jid == "491723105522@s.whatsapp.net"
    row = archive_db.execute(
        "SELECT name, source FROM contacts WHERE phone_jid=?", (jid,)
    ).fetchone()
    assert row["name"] == "Raphael"
    assert row["source"] == "manual"


def test_pin_contact_accepts_jid(archive_db) -> None:
    jid = pin_contact(archive_db, "491634376543@s.whatsapp.net", "STT")
    assert jid == "491634376543@s.whatsapp.net"


def test_pin_contact_rejects_lid(archive_db) -> None:
    with pytest.raises(ValueError, match="LID"):
        pin_contact(archive_db, "170196702634193@lid", "Raphael")


def test_pin_contact_overwrites_address_book(archive_db) -> None:
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('491723105522@s.whatsapp.net', 'Raphael (auto)', 'address_book', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    pin_contact(archive_db, "491723105522@s.whatsapp.net", "Raphael Lienemann")
    row = archive_db.execute(
        "SELECT name, source FROM contacts WHERE phone_jid='491723105522@s.whatsapp.net'"
    ).fetchone()
    assert row["name"] == "Raphael Lienemann"
    assert row["source"] == "manual"


def test_pin_survives_address_book_sync(archive_db, monkeypatch) -> None:
    """A manual pin must not be removed by an address_book sync that lacks the JID."""
    pin_contact(archive_db, "+49 172 3105522", "Raphael")
    payload = json.dumps([{"name": "Anna", "number": "+41 79 123 45 67"}])
    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    sync_contacts(archive_db, from_stdin=True)
    row = archive_db.execute(
        "SELECT name, source FROM contacts WHERE phone_jid='491723105522@s.whatsapp.net'"
    ).fetchone()
    assert row is not None
    assert row["name"] == "Raphael"
    assert row["source"] == "manual"


def test_unpin_contact(archive_db) -> None:
    pin_contact(archive_db, "491634376543@s.whatsapp.net", "STT")
    assert unpin_contact(archive_db, "491634376543@s.whatsapp.net") is True
    assert archive_db.execute(
        "SELECT 1 FROM contacts WHERE phone_jid='491634376543@s.whatsapp.net'"
    ).fetchone() is None
    assert unpin_contact(archive_db, "491634376543@s.whatsapp.net") is False


def test_unpin_does_not_remove_address_book(archive_db) -> None:
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('491634376543@s.whatsapp.net', 'auto', 'address_book', '2026-01-01T00:00:00Z')"
    )
    archive_db.commit()
    assert unpin_contact(archive_db, "491634376543@s.whatsapp.net") is False
    assert archive_db.execute(
        "SELECT name FROM contacts WHERE phone_jid='491634376543@s.whatsapp.net'"
    ).fetchone()["name"] == "auto"


def test_import_pins_csv_basic(archive_db, tmp_path) -> None:
    f = tmp_path / "pins.csv"
    f.write_text(
        "phone,name\n"
        "+49 172 3105522,Raphael\n"
        "491575 7340981,Chris\n"
        "+49 163 4376543,STT\n",
        encoding="utf-8",
    )
    res = import_pins_csv(archive_db, f)
    assert res.total == 3
    assert res.set_count == 3
    assert res.skipped == []
    rows = {r["phone_jid"]: r["name"]
            for r in archive_db.execute("SELECT phone_jid, name FROM contacts WHERE source='manual'")}
    assert rows == {
        "491723105522@s.whatsapp.net": "Raphael",
        "4915757340981@s.whatsapp.net": "Chris",
        "491634376543@s.whatsapp.net": "STT",
    }


def test_import_pins_csv_collects_invalid_rows(archive_db, tmp_path) -> None:
    f = tmp_path / "pins.csv"
    f.write_text(
        "phone,name\n"
        "+49 172 3105522,Raphael\n"
        ",NoPhone\n"
        "*100#,Junk\n"
        "170196702634193@lid,LidNotAllowed\n",
        encoding="utf-8",
    )
    res = import_pins_csv(archive_db, f)
    assert res.total == 4
    assert res.set_count == 1
    assert len(res.skipped) == 3
    reasons = [reason for _, _, reason in res.skipped]
    assert any("empty" in r for r in reasons)
    assert any("parse" in r for r in reasons)
    assert any("LID" in r for r in reasons)


def test_import_pins_csv_rejects_missing_columns(archive_db, tmp_path) -> None:
    f = tmp_path / "pins.csv"
    f.write_text("foo,bar\nx,y\n", encoding="utf-8")
    with pytest.raises(ValueError, match="phone.*name"):
        import_pins_csv(archive_db, f)


def test_import_pins_csv_accepts_alternate_headers(archive_db, tmp_path) -> None:
    f = tmp_path / "pins.csv"
    f.write_text(
        "Number,Display_Name\n+49 172 3105522,Raphael\n",
        encoding="utf-8",
    )
    res = import_pins_csv(archive_db, f)
    assert res.set_count == 1


def test_pretty_phone_swiss() -> None:
    from chatvault.contacts import pretty_phone
    assert pretty_phone("41797991026@s.whatsapp.net") == "+41 79 799 10 26"


def test_pretty_phone_german() -> None:
    from chatvault.contacts import pretty_phone
    assert pretty_phone("491723105522") == "+49 172 3105522"


def test_pretty_phone_unknown_falls_back() -> None:
    from chatvault.contacts import pretty_phone
    assert pretty_phone("garbage") == "+garbage"
    assert pretty_phone(None) == "?"


def test_manual_pin_persists_to_json(archive_db, tmp_path) -> None:
    from chatvault.contacts import load_manual_pins, pin_contact
    pin_contact(
        archive_db, "+49 172 3105522", "Raphael",
        config_dir=tmp_path,
    )
    pins = load_manual_pins(tmp_path)
    assert pins == {"491723105522@s.whatsapp.net": "Raphael"}


def test_unpin_removes_from_json(archive_db, tmp_path) -> None:
    from chatvault.contacts import load_manual_pins, pin_contact, unpin_contact
    pin_contact(archive_db, "+49 172 3105522", "Raphael", config_dir=tmp_path)
    unpin_contact(archive_db, "+49 172 3105522", config_dir=tmp_path)
    assert load_manual_pins(tmp_path) == {}


def test_restore_manual_pins_after_re_init(archive_db, tmp_path) -> None:
    from chatvault.contacts import (
        load_manual_pins, pin_contact, restore_manual_pins,
    )
    pin_contact(archive_db, "+49 172 3105522", "Raphael", config_dir=tmp_path)
    # Simulate re-init: drop the row from the DB but keep JSON.
    archive_db.execute("DELETE FROM contacts")
    archive_db.commit()
    n = restore_manual_pins(archive_db, tmp_path)
    assert n == 1
    row = archive_db.execute(
        "SELECT name, source FROM contacts WHERE phone_jid='491723105522@s.whatsapp.net'"
    ).fetchone()
    assert row["name"] == "Raphael"
    assert row["source"] == "manual"


def _seed_msg(conn, mid, sender_jid, ts="2026-02-01"):
    conn.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, key_id) "
        "VALUES(?, ?, 'g@g.us', ?, 0, ?, 'text', 0, ?)",
        (mid, abs(hash(mid)) % 1_000_000, sender_jid, ts, mid),
    )


def test_unresolved_senders_excludes_known(archive_db) -> None:
    from chatvault.contacts import unresolved_senders
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('41791234567@s.whatsapp.net', 'Anna', 'manual', '2026-01-01T00:00:00Z')"
    )
    archive_db.executemany(
        "INSERT INTO identity_links(lid_jid, phone_jid, first_observed_ts, last_observed_ts) "
        "VALUES(?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        [
            ("100@lid", "41791234567@s.whatsapp.net"),
            ("200@lid", "41799999999@s.whatsapp.net"),
        ],
    )
    _seed_msg(archive_db, "m1", "100@lid")
    _seed_msg(archive_db, "m2", "200@lid", "2026-02-02")
    _seed_msg(archive_db, "m3", "200@lid", "2026-02-03")
    archive_db.commit()
    rows = unresolved_senders(archive_db, min_messages=1, limit=10)
    phones = [r["phone_jid"] for r in rows]
    assert "41799999999@s.whatsapp.net" in phones
    assert "41791234567@s.whatsapp.net" not in phones  # Anna already known


def test_sync_does_not_overwrite_manual_pin(archive_db, monkeypatch) -> None:
    """A manual pin must survive an address_book sync that includes the same JID."""
    pin_contact(archive_db, "+41 77 4380506", "Monika 7vibes")
    payload = json.dumps([{"name": "MONIKA 7vibes", "number": "+41 77 4380506"}])
    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    sync_contacts(archive_db, from_stdin=True)
    row = archive_db.execute(
        "SELECT name, source FROM contacts WHERE phone_jid='41774380506@s.whatsapp.net'"
    ).fetchone()
    assert row["name"] == "Monika 7vibes"
    assert row["source"] == "manual"


def test_resolution_stats(archive_db) -> None:
    from chatvault.contacts import resolution_stats
    archive_db.execute(
        "INSERT INTO contacts(phone_jid, name, source, updated_at) "
        "VALUES('41791234567@s.whatsapp.net', 'Anna', 'manual', '2026-01-01T00:00:00Z')"
    )
    archive_db.execute(
        "INSERT INTO identity_links(lid_jid, phone_jid, first_observed_ts, last_observed_ts) "
        "VALUES('100@lid', '41791234567@s.whatsapp.net', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
    )
    _seed_msg(archive_db, "m1", "100@lid")
    _seed_msg(archive_db, "m2", "99999999999@s.whatsapp.net", "2026-02-02")
    archive_db.commit()
    stats = resolution_stats(archive_db)
    assert stats["total"] == 2
    assert stats["resolved"] == 1
