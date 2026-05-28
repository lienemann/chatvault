"""Minimal vCard parser for importing contacts with multiple numbers.

Supports vCard 2.1, 3.0, and 4.0 well enough for typical Android exports.
Extracts FN/N for the display name and all TEL fields (regardless of CELL/HOME/
WORK type) as numbers. We deliberately do not pull a full vCard library — the
subset we need is small and stable.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(slots=True)
class VCardEntry:
    name: str
    numbers: list[str] = field(default_factory=list)


_LINE_FOLD_RE = re.compile(r"\r?\n[ \t]")  # RFC: continuation lines start with whitespace


def _unfold(text: str) -> str:
    return _LINE_FOLD_RE.sub("", text)


def _split_records(text: str) -> Iterable[str]:
    """Yield each `BEGIN:VCARD … END:VCARD` block."""
    blocks: list[list[str]] = []
    current: list[str] | None = None
    for raw_line in _unfold(text).splitlines():
        line = raw_line.strip()
        if line.upper() == "BEGIN:VCARD":
            current = []
        elif line.upper() == "END:VCARD":
            if current is not None:
                blocks.append(current)
                current = None
        elif current is not None:
            current.append(line)
    yield from ("\n".join(b) for b in blocks)


def _parse_record(record: str) -> VCardEntry | None:
    """Pull FN + all TEL values from a single vCard block."""
    fn: str | None = None
    n_parts: tuple[str, str] | None = None  # (last, first)
    numbers: list[str] = []

    for line in record.splitlines():
        if not line:
            continue
        # 'KEY[;PARAMS]:VALUE'
        head, sep, value = line.partition(":")
        if not sep:
            continue
        key = head.split(";", 1)[0].upper()
        if key == "FN":
            fn = value.strip()
        elif key == "N":
            parts = value.split(";")
            last = parts[0].strip() if parts else ""
            first = parts[1].strip() if len(parts) > 1 else ""
            n_parts = (last, first)
        elif key == "TEL":
            num = value.strip()
            if num:
                numbers.append(num)

    name = fn
    if not name and n_parts:
        first, last = n_parts[1], n_parts[0]
        name = " ".join(p for p in (first, last) if p) or None
    if not name:
        return None
    if not numbers:
        return None
    return VCardEntry(name=name.strip(), numbers=numbers)


def parse_vcard_text(text: str) -> list[VCardEntry]:
    """Parse one or more vCard records from a text blob."""
    out: list[VCardEntry] = []
    for record in _split_records(text):
        entry = _parse_record(record)
        if entry:
            out.append(entry)
    return out


def parse_vcard_file(path: Path) -> list[VCardEntry]:
    return parse_vcard_text(path.read_text(encoding="utf-8", errors="replace"))


def iter_vcards(path: Path) -> Iterable[VCardEntry]:
    """Yield entries from a single .vcf file or every .vcf under a directory."""
    if path.is_dir():
        for child in sorted(path.rglob("*.vcf")):
            yield from parse_vcard_file(child)
    else:
        yield from parse_vcard_file(path)
