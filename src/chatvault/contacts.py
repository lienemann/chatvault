"""Address-book sync: termux-contact-list / stdin JSON / vCard files → archive."""

from __future__ import annotations

import csv
import json
import logging
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import transaction
from .extractors import now_iso
from .vcard import iter_vcards

log = logging.getLogger(__name__)

JID_SUFFIX = "@s.whatsapp.net"
DEFAULT_COUNTRY_CODE = "41"  # CH; override per call
MANUAL_PINS_FILENAME = "manual_contacts.json"


# ---------------------------------------------------------------------------
# Phone display helpers
# ---------------------------------------------------------------------------


def pretty_phone(phone_or_jid: str | None) -> str:
    """Render an E.164 number or phone JID with country code spaced off.

    Special-cases DE (3-digit operator code + subscriber), CH (2-3-2-2),
    and US/CA (3-3-4). Anything else gets a generic groups-of-3-from-right.
    """
    if not phone_or_jid:
        return "?"
    s = phone_or_jid
    if "@" in s:
        s = s.split("@", 1)[0]
    if not s.isdigit():
        return f"+{s}"
    cc, rest = _split_country_code(s)
    if not cc or not rest:
        return f"+{s}"
    if cc == "49" and len(rest) >= 4:
        # German mobile: 3-digit operator code + subscriber.
        return f"+49 {rest[:3]} {rest[3:]}"
    if cc == "41" and len(rest) == 9:
        # Swiss mobile: 79 799 10 26 — 2-3-2-2.
        return f"+41 {rest[:2]} {rest[2:5]} {rest[5:7]} {rest[7:]}"
    if cc == "1" and len(rest) == 10:
        # NANP: 3-3-4.
        return f"+1 {rest[:3]} {rest[3:6]} {rest[6:]}"
    # Generic: groups of 3 from the right.
    chunks: list[str] = []
    tail = rest
    while len(tail) > 3:
        chunks.append(tail[-3:])
        tail = tail[:-3]
    if tail:
        chunks.append(tail)
    chunks.reverse()
    return f"+{cc} " + " ".join(chunks)


_KNOWN_CC_PREFIXES_2 = frozenset({
    "20", "27", "30", "31", "32", "33", "34", "36", "39", "40", "41", "43",
    "44", "45", "46", "47", "48", "49", "51", "52", "53", "54", "55", "56",
    "57", "58", "60", "61", "62", "63", "64", "65", "66", "81", "82", "84",
    "86", "90", "91", "92", "93", "94", "95", "98",
})
_KNOWN_CC_PREFIXES_3 = frozenset({
    "212", "213", "216", "218", "220", "221", "222", "223", "224", "225",
    "226", "227", "228", "229", "230", "231", "232", "233", "234", "235",
    "236", "237", "238", "239", "240", "241", "242", "243", "244", "245",
    "248", "249", "250", "251", "252", "253", "254", "255", "256", "257",
    "258", "260", "261", "262", "263", "264", "265", "266", "267", "268",
    "269", "350", "351", "352", "353", "354", "355", "356", "357", "358",
    "359", "370", "371", "372", "373", "374", "375", "376", "377", "378",
    "380", "381", "382", "383", "385", "386", "387", "389", "420", "421",
    "423", "591", "592", "593", "594", "595", "597", "598", "670", "672",
    "673", "674", "675", "676", "677", "678", "679", "680", "681", "682",
    "683", "685", "686", "687", "688", "689", "690", "691", "692", "850",
    "852", "853", "855", "856", "880", "886", "960", "961", "962", "963",
    "964", "965", "966", "967", "968", "970", "971", "972", "973", "974",
    "975", "976", "977", "992", "993", "994", "995", "996", "998",
})


def _split_country_code(digits: str) -> tuple[str, str]:
    if digits.startswith("1") and len(digits) >= 11:
        return "1", digits[1:]
    if len(digits) >= 4 and digits[:3] in _KNOWN_CC_PREFIXES_3:
        return digits[:3], digits[3:]
    if len(digits) >= 4 and digits[:2] in _KNOWN_CC_PREFIXES_2:
        return digits[:2], digits[2:]
    if digits[:1] == "7":
        return "7", digits[1:]
    return "", ""


# ---------------------------------------------------------------------------
# Manual pins persistence (JSON sidecar)
# ---------------------------------------------------------------------------


def manual_pins_path(config_dir: Path) -> Path:
    return config_dir / MANUAL_PINS_FILENAME


def load_manual_pins(config_dir: Path) -> dict[str, str]:
    p = manual_pins_path(config_dir)
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("could not read %s: %s", p, exc)
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def _save_manual_pins(config_dir: Path, data: dict[str, str]) -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    p = manual_pins_path(config_dir)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(p)
    try:
        p.chmod(0o600)
    except OSError:
        pass


def restore_manual_pins(conn, config_dir: Path) -> int:
    """Re-apply JSON-stored manual pins to the contacts table. Returns count restored."""
    pins = load_manual_pins(config_dir)
    if not pins:
        return 0
    observed_at = now_iso()
    n = 0
    with transaction(conn):
        for jid, name in pins.items():
            existing = conn.execute(
                "SELECT name, source FROM contacts WHERE phone_jid = ?", (jid,)
            ).fetchone()
            if existing and existing["source"] == "manual" and existing["name"] == name:
                continue
            conn.execute(
                "INSERT INTO contacts(phone_jid, name, source, updated_at) "
                "VALUES(?, ?, 'manual', ?) "
                "ON CONFLICT(phone_jid) DO UPDATE SET "
                "  name = excluded.name, source = excluded.source, "
                "  updated_at = excluded.updated_at",
                (jid, name, observed_at),
            )
            n += 1
    return n


def normalize_number(raw: str | None, default_country: str = DEFAULT_COUNTRY_CODE) -> str | None:
    """E.164-without-plus, or None if unparseable."""
    if not raw:
        return None
    cleaned = re.sub(r"[\s()\-./]", "", raw)
    if not cleaned or not re.fullmatch(r"\+?\d+", cleaned):
        return None
    if cleaned.startswith("+"):
        e164 = cleaned[1:]
    elif cleaned.startswith("00"):
        e164 = cleaned[2:]
    elif cleaned.startswith("0"):
        e164 = default_country + cleaned[1:]
    else:
        e164 = cleaned
    if not (7 <= len(e164) <= 15):
        return None
    return e164


def number_to_jid(raw: str | None, default_country: str = DEFAULT_COUNTRY_CODE) -> str | None:
    e164 = normalize_number(raw, default_country)
    return e164 + JID_SUFFIX if e164 else None


def fetch_termux_contacts() -> list[dict]:
    proc = subprocess.run(
        ["termux-contact-list"], capture_output=True, text=True, timeout=60, check=False
    )
    if proc.returncode != 0:
        msg = f"termux-contact-list failed (exit {proc.returncode}): {proc.stderr.strip()}"
        raise RuntimeError(msg)
    return json.loads(proc.stdout)


@dataclass(slots=True)
class SyncResult:
    total: int
    set_count: int
    remove_count: int


def sync_contacts(conn, *, default_country: str = DEFAULT_COUNTRY_CODE,
                  from_stdin: bool = False) -> SyncResult:
    """Sync the device address book and reconcile against `contacts`.

    Removes contacts that have disappeared from the source. Use `import_vcards`
    if you want additive import (no removals) from a vCard backup.
    """
    raw_contacts = json.load(sys.stdin) if from_stdin else fetch_termux_contacts()

    new_map: dict[str, str] = {}
    for entry in raw_contacts:
        name = (entry.get("name") or "").strip()
        if not name:
            continue
        jid = number_to_jid(entry.get("number"), default_country)
        if jid is None:
            continue
        new_map.setdefault(jid, name)  # first one wins on conflict

    existing = {
        row["phone_jid"]: row["name"]
        for row in conn.execute(
            "SELECT phone_jid, name FROM contacts WHERE source = 'address_book'"
        )
    }
    manual_jids = {
        row["phone_jid"]
        for row in conn.execute("SELECT phone_jid FROM contacts WHERE source = 'manual'")
    }
    observed_at = now_iso()
    set_count = remove_count = 0

    with transaction(conn):
        for jid, name in new_map.items():
            if jid in manual_jids:
                # Manual pin wins — never overwrite from address-book sync.
                continue
            if existing.get(jid) != name:
                conn.execute(
                    "INSERT INTO contacts(phone_jid, name, source, updated_at) "
                    "VALUES(?, ?, 'address_book', ?) "
                    "ON CONFLICT(phone_jid) DO UPDATE SET "
                    "  name = excluded.name, "
                    "  source = excluded.source, "
                    "  updated_at = excluded.updated_at",
                    (jid, name, observed_at),
                )
                conn.execute(
                    "INSERT INTO contacts_history(phone_jid, name, source, op, observed_at) "
                    "VALUES(?, ?, 'address_book', 'set', ?)",
                    (jid, name, observed_at),
                )
                set_count += 1

        for jid in set(existing) - set(new_map):
            conn.execute(
                "DELETE FROM contacts WHERE phone_jid = ? AND source = 'address_book'",
                (jid,),
            )
            conn.execute(
                "INSERT INTO contacts_history(phone_jid, name, source, op, observed_at) "
                "VALUES(?, NULL, 'address_book', 'remove', ?)",
                (jid, observed_at),
            )
            remove_count += 1

    return SyncResult(total=len(new_map), set_count=set_count, remove_count=remove_count)


def import_vcards(
    conn, vcard_path: Path, *, default_country: str = DEFAULT_COUNTRY_CODE,
    source_label: str = "vcard",
) -> SyncResult:
    """Additive import from a vCard file or directory.

    Each TEL line in a vCard becomes its own contact row keyed on the resulting
    phone JID. Existing rows from `address_book` syncs are not deleted; rows
    from prior vCard imports are upserted. Rows with source='manual' are
    preserved untouched.
    """
    set_count = 0
    seen: set[str] = set()
    observed_at = now_iso()

    with transaction(conn):
        for entry in iter_vcards(vcard_path):
            for raw in entry.numbers:
                jid = number_to_jid(raw, default_country)
                if not jid:
                    continue
                seen.add(jid)
                existing = conn.execute(
                    "SELECT name, source FROM contacts WHERE phone_jid = ?", (jid,)
                ).fetchone()
                # Don't override an address_book entry with the same name from
                # a vCard — but do override if the name disagrees, since the
                # vCard is usually the more complete source.
                if existing and existing["name"] == entry.name:
                    continue
                if existing and existing["source"] == "manual":
                    continue
                conn.execute(
                    "INSERT INTO contacts(phone_jid, name, source, updated_at) "
                    "VALUES(?, ?, ?, ?) "
                    "ON CONFLICT(phone_jid) DO UPDATE SET "
                    "  name = excluded.name, "
                    "  source = excluded.source, "
                    "  updated_at = excluded.updated_at",
                    (jid, entry.name, source_label, observed_at),
                )
                conn.execute(
                    "INSERT INTO contacts_history(phone_jid, name, source, op, observed_at) "
                    "VALUES(?, ?, ?, 'set', ?)",
                    (jid, entry.name, source_label, observed_at),
                )
                set_count += 1

    return SyncResult(total=len(seen), set_count=set_count, remove_count=0)


def _resolve_phone_arg(value: str, default_country: str = DEFAULT_COUNTRY_CODE) -> str:
    """Accept a phone number or a phone JID. Reject LIDs and groups."""
    v = value.strip()
    if "@lid" in v:
        msg = (
            f"{v!r} is a LID, not a phone JID. Pin requires a phone number "
            "(LIDs are linked via identity_links automatically)."
        )
        raise ValueError(msg)
    if v.endswith(JID_SUFFIX):
        return v
    if "@" in v:
        msg = f"unsupported JID type: {v!r}"
        raise ValueError(msg)
    jid = number_to_jid(v, default_country)
    if not jid:
        msg = f"could not parse phone number: {value!r}"
        raise ValueError(msg)
    return jid


def pin_contact(
    conn, phone_or_jid: str, name: str,
    *, default_country: str = DEFAULT_COUNTRY_CODE,
    config_dir: Path | None = None,
) -> str:
    """Set a manual name override for a phone JID. Returns the resolved JID.

    If `config_dir` is given, mirrors the pin to a JSON sidecar so it survives
    a full archive re-init (re-applied via `restore_manual_pins`).
    """
    jid = _resolve_phone_arg(phone_or_jid, default_country)
    name = name.strip()
    if not name:
        msg = "name must not be empty"
        raise ValueError(msg)
    observed_at = now_iso()
    with transaction(conn):
        conn.execute(
            "INSERT INTO contacts(phone_jid, name, source, updated_at) "
            "VALUES(?, ?, 'manual', ?) "
            "ON CONFLICT(phone_jid) DO UPDATE SET "
            "  name = excluded.name, "
            "  source = excluded.source, "
            "  updated_at = excluded.updated_at",
            (jid, name, observed_at),
        )
        conn.execute(
            "INSERT INTO contacts_history(phone_jid, name, source, op, observed_at) "
            "VALUES(?, ?, 'manual', 'set', ?)",
            (jid, name, observed_at),
        )
    if config_dir is not None:
        pins = load_manual_pins(config_dir)
        pins[jid] = name
        _save_manual_pins(config_dir, pins)
    return jid


@dataclass(slots=True)
class CsvImportResult:
    total: int
    set_count: int
    skipped: list[tuple[int, str, str]]  # (line_no, raw_value, reason)


def import_pins_csv(
    conn, csv_path: Path,
    *, default_country: str = DEFAULT_COUNTRY_CODE,
) -> CsvImportResult:
    """Bulk-pin from a CSV with columns 'phone' and 'name'.

    Header is required. Other columns are ignored. Each row becomes a manual
    contact override (source='manual'). Invalid rows are collected and reported,
    not raised, so a single bad row doesn't abort the whole batch.
    """
    skipped: list[tuple[int, str, str]] = []
    set_count = 0
    total = 0
    observed_at = now_iso()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            msg = f"empty CSV: {csv_path}"
            raise ValueError(msg)
        # Allow common header variants.
        headers = {h.strip().lower(): h for h in reader.fieldnames}
        phone_col = next((headers[k] for k in ("phone", "number", "tel") if k in headers), None)
        name_col = next((headers[k] for k in ("name", "display_name") if k in headers), None)
        if not phone_col or not name_col:
            msg = (
                f"CSV must have 'phone' and 'name' columns "
                f"(got: {list(reader.fieldnames)})"
            )
            raise ValueError(msg)

        with transaction(conn):
            for row in reader:
                total += 1
                line_no = reader.line_num
                phone_raw = (row.get(phone_col) or "").strip()
                name = (row.get(name_col) or "").strip()
                if not phone_raw or not name:
                    skipped.append((line_no, phone_raw, "empty phone or name"))
                    continue
                try:
                    jid = _resolve_phone_arg(phone_raw, default_country)
                except ValueError as exc:
                    skipped.append((line_no, phone_raw, str(exc)))
                    continue
                conn.execute(
                    "INSERT INTO contacts(phone_jid, name, source, updated_at) "
                    "VALUES(?, ?, 'manual', ?) "
                    "ON CONFLICT(phone_jid) DO UPDATE SET "
                    "  name = excluded.name, "
                    "  source = excluded.source, "
                    "  updated_at = excluded.updated_at",
                    (jid, name, observed_at),
                )
                conn.execute(
                    "INSERT INTO contacts_history(phone_jid, name, source, op, observed_at) "
                    "VALUES(?, ?, 'manual', 'set', ?)",
                    (jid, name, observed_at),
                )
                set_count += 1

    return CsvImportResult(total=total, set_count=set_count, skipped=skipped)


def unpin_contact(
    conn, phone_or_jid: str,
    *, default_country: str = DEFAULT_COUNTRY_CODE,
    config_dir: Path | None = None,
) -> bool:
    """Remove a manual override. Returns True if a row was removed."""
    jid = _resolve_phone_arg(phone_or_jid, default_country)
    observed_at = now_iso()
    removed = False
    with transaction(conn):
        cur = conn.execute(
            "DELETE FROM contacts WHERE phone_jid = ? AND source = 'manual'",
            (jid,),
        )
        if cur.rowcount:
            conn.execute(
                "INSERT INTO contacts_history(phone_jid, name, source, op, observed_at) "
                "VALUES(?, NULL, 'manual', 'remove', ?)",
                (jid, observed_at),
            )
            removed = True
    if config_dir is not None:
        pins = load_manual_pins(config_dir)
        if jid in pins:
            del pins[jid]
            _save_manual_pins(config_dir, pins)
            removed = True
    return removed


# ---------------------------------------------------------------------------
# Visibility queries
# ---------------------------------------------------------------------------


def list_contacts(
    conn, *, source: str | None = None, limit: int = 1000,
) -> list[dict[str, Any]]:
    sql = "SELECT phone_jid, name, source, updated_at FROM contacts"
    params: list[Any] = []
    if source:
        sql += " WHERE source = ?"
        params.append(source)
    sql += " ORDER BY name COLLATE NOCASE LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params)]


def unresolved_senders(
    conn, *, min_messages: int = 1, limit: int = 200,
) -> list[dict[str, Any]]:
    """Phone JIDs (resolved from LIDs where possible) that have messages but no contact entry.

    Includes message count + a sample LID and last-seen timestamp, sorted by activity.
    """
    sql = """
        WITH sender_phone AS (
            SELECT
                COALESCE(il.phone_jid, m.sender_jid) AS phone_jid,
                m.sender_jid AS raw_jid,
                m.ts AS ts
            FROM messages m
            LEFT JOIN identity_links il ON il.lid_jid = m.sender_jid
            WHERE m.sender_jid IS NOT NULL AND m.from_me = 0
        )
        SELECT phone_jid,
               COUNT(*)               AS message_count,
               MAX(ts)                AS last_seen,
               (SELECT raw_jid FROM sender_phone sp2
                WHERE sp2.phone_jid = sender_phone.phone_jid LIMIT 1) AS sample_jid
        FROM sender_phone
        WHERE phone_jid LIKE '%@s.whatsapp.net'
          AND phone_jid NOT IN (SELECT phone_jid FROM contacts)
        GROUP BY phone_jid
        HAVING message_count >= ?
        ORDER BY message_count DESC
        LIMIT ?
    """
    return [dict(r) for r in conn.execute(sql, (min_messages, limit))]


def resolution_stats(conn) -> dict[str, int]:
    """How many distinct sender phone JIDs are resolved by contacts vs unresolved."""
    row = conn.execute("""
        WITH sender_phone AS (
            SELECT DISTINCT COALESCE(il.phone_jid, m.sender_jid) AS phone_jid
            FROM messages m
            LEFT JOIN identity_links il ON il.lid_jid = m.sender_jid
            WHERE m.sender_jid IS NOT NULL AND m.from_me = 0
              AND COALESCE(il.phone_jid, m.sender_jid) LIKE '%@s.whatsapp.net'
        )
        SELECT
            (SELECT COUNT(*) FROM sender_phone) AS total,
            (SELECT COUNT(*) FROM sender_phone WHERE phone_jid IN (SELECT phone_jid FROM contacts)) AS resolved
    """).fetchone()
    return {"total": row["total"] or 0, "resolved": row["resolved"] or 0}
