"""Chat-level queries: list, resolve by name, members, info."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ..identities import NameResolver

CHAT_LIST_CACHE_FILE = "last_chat_list.json"
CHAT_ALIAS_FILE = "chat_aliases.json"
# Bare integers up to this many digits are interpreted as a chat-list-cache
# index. Longer integers are treated as phone numbers (E.164 minimum is 7).
CHAT_LIST_CACHE_INDEX_MAX_DIGITS = 5


def chat_alias_path(config_dir: Path) -> Path:
    return config_dir / CHAT_ALIAS_FILE


def load_chat_aliases(config_dir: Path) -> dict[str, str]:
    p = chat_alias_path(config_dir)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items() if isinstance(v, str)}


def save_chat_aliases(config_dir: Path, data: dict[str, str]) -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    p = chat_alias_path(config_dir)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(p)
    try:
        p.chmod(0o600)
    except OSError:
        pass


def chat_message_at(
    conn: sqlite3.Connection, chat_jid: str, idx: int
) -> str:
    """Resolve a per-chat message index (Python-style) → stable message id.

    `idx`: 1-based position when positive (1 = oldest), Python-style when
    negative (-1 = newest, -2 = next-to-newest). 0 is rejected.

    Stable as long as messages aren't deleted from the chat — that's the
    archive's normal mode. The same number always points to the same message.
    """
    if idx == 0:
        msg = "index 0 is invalid; use 1 for oldest or -1 for newest."
        raise ValueError(msg)
    total = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE chat_jid = ?", (chat_jid,)
    ).fetchone()[0]
    if not total:
        msg = f"chat has no messages: {chat_jid}"
        raise LookupError(msg)
    pos = total + idx + 1 if idx < 0 else idx
    if not (1 <= pos <= total):
        msg = f"index {idx} out of range; chat has {total} messages."
        raise LookupError(msg)
    row = conn.execute(
        "SELECT id FROM messages WHERE chat_jid = ? "
        "ORDER BY ts ASC, source_rowid ASC LIMIT 1 OFFSET ?",
        (chat_jid, pos - 1),
    ).fetchone()
    return row["id"]


def resolve_message(conn: sqlite3.Connection, query: str) -> str:
    """Resolve a message reference to a stable message id.

    Accepts:
      • full message id (`<chat_jid>:<from_me>:<key_id>`)
      • `<chat-ref>:<int>` — chat-ref handled by `resolve_chat`, idx by
        `chat_message_at` (negatives = from-end, Python-style).
      • `<chat-ref>` alone — defaults to `-1` (newest in that chat).
    """
    parts = query.split(":")
    if (
        len(parts) >= 3
        and "@" in parts[0]
        and parts[1] in ("0", "1", "?")
        and parts[-1]
    ):
        # Looks like a literal stable message id; trust it.
        return query
    if ":" in query:
        prefix, _, tail = query.rpartition(":")
        try:
            idx = int(tail)
        except ValueError:
            msg = f"unrecognised message reference: {query!r}"
            raise LookupError(msg) from None
        chat_jid = resolve_chat(conn, prefix)
        return chat_message_at(conn, chat_jid, idx)
    chat_jid = resolve_chat(conn, query)
    return chat_message_at(conn, chat_jid, -1)


def chat_list_cache_path(cache_dir: Path) -> Path:
    return cache_dir / CHAT_LIST_CACHE_FILE


def save_chat_list_cache(cache_dir: Path, rows: list[dict[str, Any]]) -> None:
    """Persist `chat list` index → jid mapping for later integer-arg lookup."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "idx": i + 1,
            "jid": r["jid"],
            "name": r.get("display_name") or r.get("subject") or "",
            "kind": r.get("kind"),
        }
        for i, r in enumerate(rows)
    ]
    p = chat_list_cache_path(cache_dir)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(p)


def _load_chat_list_cache(cache_dir: Path) -> list[dict[str, Any]] | None:
    p = chat_list_cache_path(cache_dir)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, list):
        return None
    return data


def list_chats(
    conn: sqlite3.Connection,
    *,
    kind: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Chats with last activity, sorted descending.

    For 1:1 chats (kind=user/lid), `subject` is empty in the source schema —
    we backfill `display_name` via contacts (and identity_links for LIDs)
    plus a pretty-phone fallback so person-chats are first-class in the list.
    """
    from ..contacts import pretty_phone
    from ..identities import NameResolver

    sql = """
        SELECT c.jid, c.kind, c.subject,
               (SELECT MAX(ts) FROM messages m WHERE m.chat_jid = c.jid) AS last_ts,
               (SELECT COUNT(*) FROM messages m WHERE m.chat_jid = c.jid) AS message_count
        FROM chats c
    """
    params: list[Any] = []
    where: list[str] = []
    if kind:
        where.append("c.kind = ?")
        params.append(kind)
    if since:
        where.append(
            "(SELECT MAX(ts) FROM messages m WHERE m.chat_jid = c.jid) >= ?"
        )
        params.append(since)
    if until:
        where.append(
            "(SELECT MAX(ts) FROM messages m WHERE m.chat_jid = c.jid) <= ?"
        )
        params.append(until)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY last_ts DESC NULLS LAST LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in conn.execute(sql, params)]
    if not rows:
        return rows

    resolver = NameResolver(conn)
    for r in rows:
        if r["subject"]:
            r["display_name"] = r["subject"]
            continue
        k = r["kind"]
        jid = r["jid"]
        if k == "user":
            r["display_name"] = resolver.resolve(jid)
        elif k == "lid":
            r["display_name"] = resolver.resolve(jid)
        elif k == "broadcast":
            r["display_name"] = "<broadcast>"
        elif k == "newsletter":
            r["display_name"] = resolver.resolve(jid)
        elif k == "status":
            r["display_name"] = "<status>"
        else:
            r["display_name"] = pretty_phone(jid) if jid and "@" in jid else (jid or "?")
    return rows


def resolve_chat(conn: sqlite3.Connection, query: str) -> str:
    """Find a chat JID from a flexible query.

    Lookup order (first non-empty match wins; ambiguity raises):
      1. user-defined alias (from `chat alias`)
      2. short bare integer (≤5 digits) → index into the last `chat list` cache
      3. literal JID (any with '@')
      4. group subject substring
      5. contact name substring → phone_jid (1:1 chat); LIDs linked to that
         phone are tried too. Picks the most active matching chat.
      6. phone number (normalized to E.164) → phone_jid (longer integers
         resolve here, not via the chat-list cache)
    """
    from ..config import Paths
    from ..contacts import normalize_number

    paths = Paths.default()
    sel = query.strip()

    # 1. Alias.
    aliases = load_chat_aliases(paths.config_dir)
    if sel in aliases:
        return aliases[sel]

    # 2. Short integer → chat-list cache. Longer ints fall through to the
    # phone-number path so things like `41799372433` work.
    if sel.isdigit() and len(sel) <= CHAT_LIST_CACHE_INDEX_MAX_DIGITS:
        idx = int(sel)
        cached = _load_chat_list_cache(paths.cache_dir)
        if cached is None:
            msg = (
                f"chat #{idx}: no `chat list` cache yet. "
                "Run `chatvault chat list` first."
            )
            raise LookupError(msg)
        if not (1 <= idx <= len(cached)):
            msg = (
                f"chat #{idx} out of range: cache has {len(cached)} entries. "
                "Run `chatvault chat list` to refresh."
            )
            raise LookupError(msg)
        return cached[idx - 1]["jid"]

    if "@" in query:
        return query

    # 2. Group subject substring.
    rows = list(conn.execute(
        "SELECT jid, subject FROM chats WHERE subject IS NOT NULL "
        "AND LOWER(subject) LIKE ? "
        "ORDER BY (SELECT MAX(ts) FROM messages m WHERE m.chat_jid = chats.jid) DESC",
        (f"%{query.lower()}%",),
    ))
    if rows:
        if len(rows) == 1:
            return rows[0]["jid"]
        candidates = "\n  ".join(f"{r['subject']}  [{r['jid']}]" for r in rows[:10])
        msg = f"multiple chats match subject {query!r}:\n  {candidates}"
        raise LookupError(msg)

    # 3. Contact name → phone JID (and any linked LID-keyed chat).
    contact_rows = list(conn.execute(
        "SELECT phone_jid, name FROM contacts "
        "WHERE LOWER(name) LIKE ? ORDER BY name",
        (f"%{query.lower()}%",),
    ))
    if contact_rows:
        candidates_jids: list[str] = []
        for cr in contact_rows:
            candidates_jids.append(cr["phone_jid"])
            for lid_row in conn.execute(
                "SELECT lid_jid FROM identity_links WHERE phone_jid = ?",
                (cr["phone_jid"],),
            ):
                candidates_jids.append(lid_row["lid_jid"])
        # Filter to JIDs that actually have a chats row, sort by activity.
        ph = ",".join("?" for _ in candidates_jids)
        active = list(conn.execute(
            f"SELECT c.jid, c.kind, "
            f"       (SELECT name FROM contacts WHERE phone_jid = c.jid) AS pname, "
            f"       (SELECT MAX(ts) FROM messages m WHERE m.chat_jid = c.jid) AS last_ts, "
            f"       (SELECT COUNT(*) FROM messages m WHERE m.chat_jid = c.jid) AS n "
            f"FROM chats c WHERE c.jid IN ({ph}) "
            f"ORDER BY n DESC NULLS LAST, last_ts DESC NULLS LAST",
            candidates_jids,
        ))
        active = [r for r in active if r["n"] and r["n"] > 0]
        if not active:
            # Fall back to phone JID even if no `chats` row (rare).
            return contact_rows[0]["phone_jid"]
        if len(contact_rows) > 1 and len({r["pname"] for r in active}) > 1:
            names = "\n  ".join(
                f"{cr['name']}  [{cr['phone_jid']}]" for cr in contact_rows[:10]
            )
            msg = f"multiple contacts match {query!r}:\n  {names}"
            raise LookupError(msg)
        return active[0]["jid"]

    # 4. Maybe it's a phone number.
    e164 = normalize_number(query)
    if e164:
        candidate = f"{e164}@s.whatsapp.net"
        if conn.execute(
            "SELECT 1 FROM chats WHERE jid = ? UNION "
            "SELECT 1 FROM messages WHERE chat_jid = ? LIMIT 1",
            (candidate, candidate),
        ).fetchone():
            return candidate

    msg = f"no chat matches {query!r}"
    raise LookupError(msg)


def members_for(
    conn: sqlite3.Connection, chat_jid: str, *, include_history: bool = False
) -> list[dict[str, Any]]:
    """Members of a chat. For 1:1 chats, returns owner + the other party."""
    resolver = NameResolver(conn)
    out: list[dict[str, Any]] = []
    role_label = {0: "member", None: "member", 1: "admin", 2: "superadmin"}

    chat_kind_row = conn.execute(
        "SELECT kind FROM chats WHERE jid = ?", (chat_jid,),
    ).fetchone()
    chat_kind = chat_kind_row["kind"] if chat_kind_row else None

    if chat_kind in ("user", "lid") and not include_history:
        out.append({
            "jid": chat_jid,
            "name": resolver.resolve(chat_jid),
            "role": "peer",
            "joined": None,
        })
        if resolver.owner_jid:
            out.append({
                "jid": resolver.owner_jid,
                "name": resolver.owner_name,
                "role": "owner",
                "joined": None,
            })
        return out

    for r in conn.execute(
        "SELECT member_jid, rank, joined_ts FROM chat_members WHERE chat_jid = ?",
        (chat_jid,),
    ):
        out.append({
            "jid": r["member_jid"],
            "name": resolver.resolve(r["member_jid"]),
            "role": role_label.get(r["rank"], f"rank_{r['rank']}"),
            "joined": r["joined_ts"],
        })
    out.sort(key=lambda x: (x["role"] != "admin", x["name"].lower()))

    if include_history:
        for r in conn.execute(
            "SELECT member_jid, op, observed_at FROM chat_members_history "
            "WHERE chat_jid = ? AND op = 'leave' ORDER BY observed_at DESC LIMIT 200",
            (chat_jid,),
        ):
            out.append({
                "jid": r["member_jid"],
                "name": f"(left) {resolver.resolve(r['member_jid'])}",
                "role": "former",
                "joined": r["observed_at"],
            })
    return out


def chat_info(conn: sqlite3.Connection, chat_jid: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM chats WHERE jid = ?", (chat_jid,),
    ).fetchone()
    if not row:
        msg = f"chat not found: {chat_jid}"
        raise LookupError(msg)
    info = dict(row)
    # Resolved display name — for 1:1 chats `subject` is NULL upstream.
    if not info.get("subject") and info.get("kind") in ("user", "lid"):
        info["display_name"] = NameResolver(conn).resolve(chat_jid)
    info["message_count"] = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE chat_jid = ?", (chat_jid,),
    ).fetchone()[0]
    info["first_message_ts"] = conn.execute(
        "SELECT MIN(ts) FROM messages WHERE chat_jid = ?", (chat_jid,),
    ).fetchone()[0]
    info["last_message_ts"] = conn.execute(
        "SELECT MAX(ts) FROM messages WHERE chat_jid = ?", (chat_jid,),
    ).fetchone()[0]
    info["reaction_count"] = conn.execute(
        "SELECT COUNT(*) FROM reactions WHERE parent_chat_jid = ?", (chat_jid,),
    ).fetchone()[0]
    return info
