"""vCard parser tests."""

from __future__ import annotations

from pathlib import Path

from chatvault.vcard import parse_vcard_text


def test_single_record_with_multiple_tels() -> None:
    blob = """BEGIN:VCARD
VERSION:2.1
N:Krumpal;Robin;;;
FN:Robin Krumpal
TEL;CELL:+41774208847
TEL;CELL:+4915730211930
TEL;CELL:076-447-5119
END:VCARD
"""
    entries = parse_vcard_text(blob)
    assert len(entries) == 1
    e = entries[0]
    assert e.name == "Robin Krumpal"
    assert "+41774208847" in e.numbers
    assert "+4915730211930" in e.numbers
    assert "076-447-5119" in e.numbers


def test_multiple_records_in_one_blob() -> None:
    blob = """BEGIN:VCARD
VERSION:3.0
FN:Anna
TEL:+41791111111
END:VCARD
BEGIN:VCARD
VERSION:3.0
FN:Berit
TEL:+41792222222
TEL:+41793333333
END:VCARD
"""
    entries = parse_vcard_text(blob)
    assert len(entries) == 2
    assert entries[0].name == "Anna"
    assert entries[1].numbers == ["+41792222222", "+41793333333"]


def test_falls_back_to_n_when_fn_missing() -> None:
    blob = "BEGIN:VCARD\nN:Last;First;;;\nTEL:+41700\nEND:VCARD\n"
    entries = parse_vcard_text(blob)
    assert entries[0].name == "First Last"


def test_skips_records_without_tel_or_name() -> None:
    blob = (
        "BEGIN:VCARD\nFN:NoNumber\nEND:VCARD\n"
        "BEGIN:VCARD\nTEL:+41700\nEND:VCARD\n"
    )
    assert parse_vcard_text(blob) == []


def test_handles_line_folding() -> None:
    # Per RFC: lines starting with whitespace are continuations.
    blob = "BEGIN:VCARD\nFN:Long Na\n me\nTEL:+41700\nEND:VCARD\n"
    entries = parse_vcard_text(blob)
    assert entries[0].name == "Long Name"
