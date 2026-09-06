"""Extract chats, messages, contacts and media from a decrypted Signal Android DB.

The decrypted SQLite that ``chatvault.sources.signal.decrypt.decrypt_backup``
produces is the input. We map Signal's recipient / thread / message / attachment
tables into chatvault's own schema, tagging every row with ``source='signal'``.

Signal's schema is a moving target across app versions. To stay tolerant we
detect columns at runtime (``PRAGMA table_info``) and pick the first synonym
that exists from a candidate list. Anything we can't map is dropped silently,
not crashed on — full source row is preserved in ``raw_json`` for forward fix.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from chatvault.db import get_state, get_state_int, set_state, set_state_int, transaction

from . import ExtractorResult, now_iso, to_raw_json

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema helpers — Signal's columns shift across versions; detect at runtime.
# ---------------------------------------------------------------------------


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {r[1] for r in rows}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _pick(cols: set[str], *candidates: str) -> str | None:
    """Return the first candidate present in `cols`, or None."""
    for c in candidates:
        if c in cols:
            return c
    return None


def _detect_message_table(conn: sqlite3.Connection) -> str:
    """Modern Signal uses unified `message`. Pre-2023 used `sms` + `mms`.

    We only support the unified table. If only legacy tables are present the
    caller will see zero messages — fine for an MVP, the legacy path can be
    added later.
    """
    for name in ("message", "mms"):  # mms is the closer match in legacy schemas
        if _table_exists(conn, name):
            return name
    msg = "decrypted Signal DB has neither `message` nor `mms` — unsupported schema"
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# JID synthesis.
# ---------------------------------------------------------------------------


def user_jid(identifier: str) -> str:
    """E.164 phone or ACI/UUID → chatvault JID with @signal suffix."""
    s = identifier.strip().lstrip("+")
    return f"{s.lower()}@signal"


def group_jid(group_id: bytes | str) -> str:
    if isinstance(group_id, (bytes, bytearray, memoryview)):
        return f"{bytes(group_id).hex()}@signal-group"
    s = str(group_id).strip()
    return f"{s.lower()}@signal-group"


def _to_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=UTC).isoformat()
    except (ValueError, OSError, OverflowError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Self account — needed to compute `from_me`.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SelfAccount:
    aci: str | None = None
    pni: str | None = None
    e164: str | None = None
    recipient_id: int | None = None

    def jids(self) -> set[str]:
        out: set[str] = set()
        for v in (self.aci, self.pni, self.e164):
            if v:
                out.add(user_jid(v))
        return out


def _read_self_account(key_values_path: Path | None) -> SelfAccount:
    """Read self ACI/PNI/E164 from key_value.json that decrypt_backup writes."""
    self_ = SelfAccount()
    if key_values_path is None or not key_values_path.exists():
        return self_
    try:
        kv = json.loads(key_values_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return self_
    for key in ("account.aci", "account.uuid"):
        v = (kv.get(key) or {}).get("stringValue")
        if isinstance(v, str) and v:
            self_.aci = v
            break
    v = (kv.get("account.pni") or {}).get("stringValue")
    if isinstance(v, str) and v:
        self_.pni = v
    v = (kv.get("account.e164") or {}).get("stringValue")
    if isinstance(v, str) and v:
        self_.e164 = v
    return self_


# ---------------------------------------------------------------------------
# Recipients — id → (jid, kind, display_name).
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Recipient:
    rid: int
    jid: str | None
    kind: str  # 'user' | 'group'
    display_name: str | None
    raw: dict[str, Any]


def _load_recipients(conn: sqlite3.Connection) -> dict[int, Recipient]:
    cols = _columns(conn, "recipient")
    if not cols:
        return {}

    aci_col = _pick(cols, "aci", "uuid", "service_id")
    pni_col = _pick(cols, "pni")
    phone_col = _pick(cols, "e164", "phone")
    group_col = _pick(cols, "group_id", "groupId")
    sys_name_col = _pick(
        cols, "system_joined_name", "system_display_name", "system_contact_name"
    )
    profile_name_col = _pick(cols, "profile_joined_name", "signal_profile_name")
    given_col = _pick(cols, "system_given_name", "profile_given_name")
    family_col = _pick(cols, "system_family_name", "profile_family_name")

    sel = ["_id"]
    for c in (aci_col, pni_col, phone_col, group_col, sys_name_col,
              profile_name_col, given_col, family_col):
        if c:
            sel.append(c)
    q = f"SELECT {', '.join(sel)} FROM recipient"
    out: dict[int, Recipient] = {}
    for r in conn.execute(q):
        raw = {k: r[k] for k in r.keys()}
        rid = int(r["_id"])
        jid: str | None
        kind: str
        group_val = raw.get(group_col) if group_col else None
        aci_val = raw.get(aci_col) if aci_col else None
        phone_val = raw.get(phone_col) if phone_col else None
        if group_val:
            jid = group_jid(group_val)
            kind = "group"
        elif aci_val:
            jid = user_jid(str(aci_val))
            kind = "user"
        elif phone_val:
            jid = user_jid(str(phone_val))
            kind = "user"
        else:
            jid = None
            kind = "user"

        display_name = None
        for c in (sys_name_col, profile_name_col):
            if c and raw.get(c):
                display_name = str(raw[c]).strip()
                if display_name:
                    break
        if not display_name and (given_col or family_col):
            parts = []
            if given_col and raw.get(given_col):
                parts.append(str(raw[given_col]).strip())
            if family_col and raw.get(family_col):
                parts.append(str(raw[family_col]).strip())
            display_name = " ".join(p for p in parts if p) or None

        out[rid] = Recipient(rid=rid, jid=jid, kind=kind, display_name=display_name, raw=raw)
    return out


# ---------------------------------------------------------------------------
# Threads → chats.
# ---------------------------------------------------------------------------


def _load_threads(
    conn: sqlite3.Connection, recipients: dict[int, Recipient]
) -> dict[int, tuple[str, Recipient]]:
    """thread._id → (chat_jid, recipient row).

    Modern Signal threads carry `recipient_id`. Older variants used
    `thread_recipient_id`.
    """
    cols = _columns(conn, "thread")
    if not cols:
        return {}
    rec_col = _pick(cols, "recipient_id", "thread_recipient_id")
    if not rec_col:
        return {}
    out: dict[int, tuple[str, Recipient]] = {}
    for r in conn.execute(f"SELECT _id, {rec_col} AS rid FROM thread"):
        tid = int(r["_id"])
        rid = r["rid"]
        if rid is None:
            continue
        rec = recipients.get(int(rid))
        if not rec or not rec.jid:
            continue
        out[tid] = (rec.jid, rec)
    return out


# ---------------------------------------------------------------------------
# Attachments — keyed by message rowid.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AttachmentRow:
    rowid: int
    msg_rowid: int
    file_basename: str  # name of the decrypted file on disk (relative to attachments_dir)
    mime: str | None
    file_size: int | None
    file_name: str | None
    caption: str | None
    width: int | None
    height: int | None
    voice_note: bool
    raw: dict[str, Any]


def _load_attachments(
    conn: sqlite3.Connection, attachments_dir: Path
) -> dict[int, list[AttachmentRow]]:
    table = None
    for cand in ("attachment", "part"):
        if _table_exists(conn, cand):
            table = cand
            break
    if not table:
        return {}
    cols = _columns(conn, table)
    msg_col = _pick(cols, "message_id", "mid")
    if not msg_col:
        return {}
    mime_col = _pick(cols, "content_type", "ct")
    size_col = _pick(cols, "data_size", "sz")
    fn_col = _pick(cols, "file_name", "fn")
    caption_col = _pick(cols, "caption")
    width_col = _pick(cols, "width", "w")
    height_col = _pick(cols, "height", "h")
    voice_col = _pick(cols, "voice_note", "voice_message")
    unique_col = _pick(cols, "unique_id")
    sel = ["_id", f"{msg_col} AS msg_rowid"]
    for c, alias in (
        (mime_col, "mime"),
        (size_col, "size"),
        (fn_col, "fn"),
        (caption_col, "caption"),
        (width_col, "width"),
        (height_col, "height"),
        (voice_col, "voice_note"),
        (unique_col, "unique_id"),
    ):
        if c:
            sel.append(f"{c} AS {alias}")
    q = f"SELECT {', '.join(sel)} FROM {table} ORDER BY _id"
    out: dict[int, list[AttachmentRow]] = {}
    for r in conn.execute(q):
        rowid = int(r["_id"])
        msg_rowid_raw = r["msg_rowid"]
        if msg_rowid_raw is None:
            continue
        msg_rowid = int(msg_rowid_raw)
        keys = set(r.keys())
        # Backup file naming: attachments/{attachment_id or row_id}.bin where
        # attachment_id == unique_id in modern Signal.
        candidates: list[str] = []
        if "unique_id" in keys and r["unique_id"] is not None:
            candidates.append(f"{r['unique_id']}.bin")
        candidates.append(f"{rowid}.bin")
        on_disk: str | None = None
        for cand in candidates:
            if (attachments_dir / cand).exists():
                on_disk = cand
                break
        if on_disk is None:
            # Still record the row — file may be missing but the metadata is useful.
            on_disk = candidates[0]
        raw = {k: r[k] for k in keys}
        out.setdefault(msg_rowid, []).append(
            AttachmentRow(
                rowid=rowid,
                msg_rowid=msg_rowid,
                file_basename=on_disk,
                mime=raw.get("mime") if isinstance(raw.get("mime"), str) else None,
                file_size=int(raw["size"]) if isinstance(raw.get("size"), int) else None,
                file_name=raw.get("fn") if isinstance(raw.get("fn"), str) else None,
                caption=raw.get("caption") if isinstance(raw.get("caption"), str) else None,
                width=int(raw["width"]) if isinstance(raw.get("width"), int) else None,
                height=int(raw["height"]) if isinstance(raw.get("height"), int) else None,
                voice_note=bool(raw.get("voice_note") or 0),
                raw=raw,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Message type labelling. Signal's `type` is a bitmask; we keep the raw int
# and apply a coarse mime-based label when an attachment exists.
# ---------------------------------------------------------------------------


def _label_for(text: str | None, attachments: Sequence[AttachmentRow]) -> str:
    if attachments:
        mimes = [a.mime or "" for a in attachments]
        first = mimes[0].lower()
        if any(a.voice_note for a in attachments):
            return "audio"
        if first.startswith("image/"):
            return "image"
        if first.startswith("video/"):
            return "video"
        if first.startswith("audio/"):
            return "audio"
        if first:
            return "document"
        return "media"
    if text:
        return "text"
    return "empty"


# ---------------------------------------------------------------------------
# Main entry.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SignalExtractInput:
    """Where to find decrypted Signal artefacts on disk."""

    db_path: Path
    attachments_dir: Path
    key_values_path: Path | None = None
    media_out_dir: Path | None = None  # where to mirror attachments into

    def opened(self) -> sqlite3.Connection:
        c = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        c.row_factory = sqlite3.Row
        return c


SIGNAL_LAST_ROWID_KEY = "signal_last_message_rowid"


def _resolve_from_me(
    from_rid: int | None, to_rid: int | None, self_rid: int | None
) -> bool:
    if self_rid is None:
        return False
    if from_rid is not None and int(from_rid) == int(self_rid):
        return True
    # Some legacy variants only carry to_recipient_id with a sent-flag bitmask.
    # Without decoding the bitmask we approximate: if from_rid is null and
    # to_rid is set, treat as outgoing.
    return from_rid is None and to_rid is not None


def _find_self_recipient_id(
    recipients: dict[int, Recipient], self_account: SelfAccount
) -> int | None:
    self_jids = self_account.jids()
    for r in recipients.values():
        if r.jid and r.jid in self_jids:
            return r.rid
    return None


def extract(  # noqa: PLR0915 — single coherent extractor, splitting hides flow
    inp: SignalExtractInput,
    archive: sqlite3.Connection,
    *,
    full_scan: bool = False,
) -> ExtractorResult:
    res = ExtractorResult(name="signal")
    source = inp.opened()
    try:
        recipients = _load_recipients(source)
        threads = _load_threads(source, recipients)
        attachments = _load_attachments(source, inp.attachments_dir)
        self_account = _read_self_account(inp.key_values_path)
        self_account.recipient_id = _find_self_recipient_id(recipients, self_account)
        log.info(
            "signal: recipients=%d threads=%d attachments=%d self_rid=%s",
            len(recipients),
            len(threads),
            sum(len(v) for v in attachments.values()),
            self_account.recipient_id,
        )

        msg_table = _detect_message_table(source)
        mcols = _columns(source, msg_table)
        thread_col = _pick(mcols, "thread_id")
        from_col = _pick(mcols, "from_recipient_id", "address")
        to_col = _pick(mcols, "to_recipient_id")
        ts_sent_col = _pick(mcols, "date_sent", "date", "date_sent_received")
        ts_recv_col = _pick(mcols, "date_received", "date_server")
        body_col = _pick(mcols, "body")
        type_col = _pick(mcols, "type", "msg_box")
        read_col = _pick(mcols, "read")
        quote_id_col = _pick(mcols, "quote_id")
        quote_author_col = _pick(mcols, "quote_author")
        quote_body_col = _pick(mcols, "quote_body")
        if not (thread_col and ts_sent_col and body_col is not None):
            msg = (
                f"unsupported Signal `{msg_table}` schema — missing required columns "
                "(thread_id / date_sent / body)"
            )
            raise RuntimeError(msg)

        sel_parts = ["_id AS rowid", f"{thread_col} AS thread_id", f"{ts_sent_col} AS ts_sent"]
        for c, alias in (
            (from_col, "from_rid"),
            (to_col, "to_rid"),
            (ts_recv_col, "ts_recv"),
            (body_col, "body"),
            (type_col, "type_raw"),
            (read_col, "read"),
            (quote_id_col, "quote_id"),
            (quote_author_col, "quote_author"),
            (quote_body_col, "quote_body"),
        ):
            if c:
                sel_parts.append(f"{c} AS {alias}")

        if full_scan:
            since_rowid = 0
            log.info("signal: full-scan over %s (cursor ignored)", msg_table)
        else:
            since_rowid = get_state_int(archive, SIGNAL_LAST_ROWID_KEY, 0)
            log.info("signal: extracting %s rowid > %d", msg_table, since_rowid)

        # Track which chats we touch to (a) upsert the chat row and (b) refresh
        # last_message_ts after the pass.
        chats_seen: dict[str, Recipient] = {}
        max_rowid = since_rowid

        q = (
            f"SELECT {', '.join(sel_parts)} FROM {msg_table} "
            f"WHERE _id > ? ORDER BY _id"
        )
        with transaction(archive):
            for row in source.execute(q, (since_rowid,)):
                rowid = int(row["rowid"])
                max_rowid = max(max_rowid, rowid)
                tid = row["thread_id"]
                if tid is None or int(tid) not in threads:
                    res.rows_skipped += 1
                    continue
                chat_jid, chat_rec = threads[int(tid)]
                chats_seen[chat_jid] = chat_rec

                from_rid = row["from_rid"] if from_col else None
                to_rid = row["to_rid"] if to_col else None
                from_me = _resolve_from_me(from_rid, to_rid, self_account.recipient_id)

                sender_rec = recipients.get(int(from_rid)) if from_rid is not None else None
                sender_jid = sender_rec.jid if sender_rec else None
                if sender_jid is None and from_me:
                    self_jids = self_account.jids()
                    sender_jid = next(iter(self_jids)) if self_jids else None
                # 1:1 fallback: incoming message in a user chat → sender is the chat partner.
                if sender_jid is None and not from_me and chat_rec.kind == "user":
                    sender_jid = chat_rec.jid

                ts_iso = _to_iso(row["ts_sent"])
                ts_recv_iso = _to_iso(row["ts_recv"]) if ts_recv_col else None
                if ts_iso is None:
                    res.rows_skipped += 1
                    continue

                key_id = f"signal-{rowid}"
                message_id = f"{chat_jid}:{int(from_me)}:{key_id}"
                atts = attachments.get(rowid, [])
                body = row["body"] if body_col else None
                # Pull caption from first attachment if message body is empty.
                if not body and atts and atts[0].caption:
                    body = atts[0].caption
                label = _label_for(body, atts)
                type_raw = int(row["type_raw"]) if type_col and row["type_raw"] is not None else 0

                raw_msg = {k: row[k] for k in row.keys()}
                archive.execute(
                    "INSERT OR REPLACE INTO messages(id, source_rowid, chat_jid, sender_jid, "
                    "from_me, ts, ts_received, type, type_raw, text, key_id, status, "
                    "origin, starred, raw_json, source) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'signal')",
                    (
                        message_id,
                        rowid,
                        chat_jid,
                        sender_jid,
                        int(from_me),
                        ts_iso,
                        ts_recv_iso,
                        label,
                        type_raw,
                        body,
                        key_id,
                        None,
                        None,
                        0,
                        to_raw_json(raw_msg),
                    ),
                )
                res.rows_written += 1

                if atts:
                    a = atts[0]
                    media_rel = f"signal/attachments/{a.file_basename}"
                    archive.execute(
                        "INSERT OR REPLACE INTO message_media(message_id, file_path, file_size, "
                        "mime, file_hash, caption, duration_s, width, height, name) "
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            message_id,
                            media_rel,
                            a.file_size,
                            a.mime,
                            None,
                            a.caption,
                            None,
                            a.width,
                            a.height,
                            a.file_name,
                        ),
                    )

                if quote_id_col and row["quote_id"]:
                    qa_rid = row["quote_author"] if quote_author_col else None
                    qa_jid = None
                    if qa_rid is not None:
                        qa_rec = recipients.get(int(qa_rid))
                        qa_jid = qa_rec.jid if qa_rec else None
                    archive.execute(
                        "INSERT OR REPLACE INTO message_quoted(message_id, quoted_key_id, "
                        "quoted_text, quoted_message_type, quoted_sender_jid) "
                        "VALUES(?, ?, ?, ?, ?)",
                        (
                            message_id,
                            f"signal-{row['quote_id']}",
                            row["quote_body"] if quote_body_col else None,
                            None,
                            qa_jid,
                        ),
                    )

            # Chats + identities + contacts for everything we touched.
            seen_ts = now_iso()
            for jid, rec in chats_seen.items():
                last_ts = archive.execute(
                    "SELECT MAX(ts) FROM messages WHERE chat_jid = ? AND source = 'signal'",
                    (jid,),
                ).fetchone()[0]
                subject = rec.display_name if rec.kind == "group" else None
                archive.execute(
                    "INSERT INTO chats(jid, kind, subject, last_message_ts, last_seen_ts, "
                    "raw_json, source) "
                    "VALUES(?, ?, ?, ?, ?, ?, 'signal') "
                    "ON CONFLICT(jid) DO UPDATE SET "
                    "  kind = excluded.kind, "
                    "  subject = COALESCE(excluded.subject, chats.subject), "
                    "  last_message_ts = excluded.last_message_ts, "
                    "  last_seen_ts = excluded.last_seen_ts, "
                    "  raw_json = excluded.raw_json, "
                    "  source = 'signal'",
                    (jid, rec.kind, subject, last_ts, seen_ts, to_raw_json(rec.raw)),
                )

            # Identities: every recipient we have a JID for, plus self.
            for rec in recipients.values():
                if not rec.jid:
                    continue
                archive.execute(
                    "INSERT INTO identities(jid, kind, user_part, server_part, "
                    "                       first_seen_ts, last_seen_ts, raw_json) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(jid) DO UPDATE SET "
                    "  last_seen_ts = excluded.last_seen_ts, "
                    "  raw_json = excluded.raw_json",
                    (
                        rec.jid,
                        rec.kind,
                        rec.jid.split("@", 1)[0],
                        rec.jid.split("@", 1)[1] if "@" in rec.jid else None,
                        seen_ts,
                        seen_ts,
                        to_raw_json(rec.raw),
                    ),
                )

            # Contacts: user recipients with a display name and a phone-like JID.
            for rec in recipients.values():
                if rec.kind != "user" or not rec.jid or not rec.display_name:
                    continue
                archive.execute(
                    "INSERT INTO contacts(phone_jid, name, source, updated_at) "
                    "VALUES(?, ?, 'signal', ?) "
                    "ON CONFLICT(phone_jid) DO UPDATE SET "
                    "  name = excluded.name, "
                    "  source = 'signal', "
                    "  updated_at = excluded.updated_at",
                    (rec.jid, rec.display_name, seen_ts),
                )

            prior_cursor = get_state_int(archive, SIGNAL_LAST_ROWID_KEY, 0)
            set_state_int(archive, SIGNAL_LAST_ROWID_KEY, max(prior_cursor, max_rowid))
            set_state(archive, "signal_last_run_ts", seen_ts)

        log.info(
            "signal: wrote %d messages, skipped %d, touched %d chats",
            res.rows_written,
            res.rows_skipped,
            len(chats_seen),
        )
        res.with_note(f"chats touched: {len(chats_seen)}")
        return res
    finally:
        source.close()


# ---------------------------------------------------------------------------
# Attachment mirroring — copy decrypted files into the chatvault media dir.
# ---------------------------------------------------------------------------


def mirror_attachments(
    decrypted_attachments_dir: Path,
    media_dir: Path,
    archive: sqlite3.Connection,
) -> int:
    """Hardlink/copy decrypted Signal attachments into per-chat folders.

    Layout: ``<media_dir>/<chat_slug>/<basename>``. Attachments whose parent
    message hasn't been extracted go to ``<media_dir>/_orphans/<basename>``.
    Returns the number of newly mirrored files.
    """
    from chatvault.fs import link_or_copy
    from chatvault.media.layout import ORPHANS_DIR, resolve_chat_slug

    mirrored = 0
    rows = archive.execute(
        "SELECT mm.message_id, mm.file_path, m.chat_jid "
        "FROM message_media mm JOIN messages m ON mm.message_id = m.id "
        "WHERE m.source = 'signal' AND mm.file_path LIKE 'signal/attachments/%' "
        "  AND (mm.mirrored_path IS NULL OR mm.mirrored_path = '')"
    ).fetchall()
    for r in rows:
        rel = r["file_path"]
        basename = Path(rel).name
        src = decrypted_attachments_dir / basename
        if not src.exists():
            continue
        chat_jid = r["chat_jid"]
        folder = resolve_chat_slug(archive, chat_jid) if chat_jid else ORPHANS_DIR
        dst = media_dir / folder / basename
        link_or_copy(src, dst)
        archive.execute(
            "UPDATE message_media SET mirrored_path = ? WHERE message_id = ?",
            (str(dst.relative_to(media_dir)), r["message_id"]),
        )
        mirrored += 1
    archive.commit()
    return mirrored
