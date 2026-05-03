"""Per-member activity timeline and 'forgotten contacts' query."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..identities import NameResolver, jid_kind


def _expand_jids_for_name(conn: sqlite3.Connection, name: str) -> list[str]:
    """Find all JIDs that resolve to a given contact name.

    A name can map to multiple phone JIDs (multiple numbers per contact),
    each with multiple LIDs via identity_links.
    """
    phones = [
        r["phone_jid"]
        for r in conn.execute(
            "SELECT phone_jid FROM contacts WHERE name = ? OR LOWER(name) LIKE ?",
            (name, f"%{name.lower()}%"),
        )
    ]
    if not phones:
        return []
    out: set[str] = set(phones)
    placeholders = ", ".join("?" for _ in phones)
    for r in conn.execute(
        f"SELECT lid_jid FROM identity_links WHERE phone_jid IN ({placeholders})",
        phones,
    ):
        out.add(r["lid_jid"])
    return list(out)


def timeline_for_member(
    conn: sqlite3.Connection,
    name: str,
    *,
    limit: int = 50,
    since: str | None = None,
    until: str | None = None,
) -> list[dict[str, Any]]:
    """Last messages from any of the JIDs that map to the given contact name."""
    jids = _expand_jids_for_name(conn, name)
    if not jids:
        return []

    placeholders = ", ".join("?" for _ in jids)
    sql = (
        "SELECT m.ts, m.text, m.type, c.subject, c.kind, m.chat_jid, m.sender_jid, "
        "       mm.file_path AS media_file_path, "
        "       mm.mirrored_path AS media_mirrored_path, "
        "       mm.mime AS media_mime "
        "FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid "
        "LEFT JOIN message_media mm ON mm.message_id = m.id "
        f"WHERE m.sender_jid IN ({placeholders}) "
    )
    params: list[Any] = list(jids)
    if since:
        sql += " AND m.ts >= ?"
        params.append(since)
    if until:
        sql += " AND m.ts <= ?"
        params.append(until)
    sql += " ORDER BY m.ts DESC LIMIT ?"
    params.append(limit)

    return [dict(r) for r in conn.execute(sql, params)]


def forgotten_contacts(
    conn: sqlite3.Connection, *, days: int = 365, limit: int = 50
) -> list[dict[str, Any]]:
    """Contacts the owner has not initiated a message with in `days` days."""
    sql = """
    WITH last_outgoing AS (
        SELECT chat_jid, MAX(ts) AS last_ts
        FROM messages WHERE from_me = 1 GROUP BY chat_jid
    )
    SELECT c.phone_jid, c.name, lo.last_ts
    FROM contacts c
    LEFT JOIN last_outgoing lo ON lo.chat_jid = c.phone_jid
    WHERE lo.last_ts IS NULL
       OR lo.last_ts < datetime('now', ? || ' days')
    ORDER BY lo.last_ts ASC NULLS FIRST
    LIMIT ?
    """
    return [
        dict(r)
        for r in conn.execute(sql, (f"-{days}", limit))
    ]


def chat_member_explain(
    conn: sqlite3.Connection, chat_jid: str, member_jid: str
) -> dict[str, Any]:
    """Trace the resolution chain for one member of a chat — useful for debugging
    surprising names ('why is this LID showing up as X?')."""
    resolver = NameResolver(conn)
    chain: list[str] = []
    chain.append(f"jid: {member_jid} (kind={jid_kind(member_jid)})")

    in_members = conn.execute(
        "SELECT rank, joined_ts FROM chat_members WHERE chat_jid = ? AND member_jid = ?",
        (chat_jid, member_jid),
    ).fetchone()
    chain.append(f"in chat_members: {dict(in_members) if in_members else 'NO'}")

    if member_jid.endswith("@lid"):
        link = conn.execute(
            "SELECT phone_jid, first_observed_ts FROM identity_links WHERE lid_jid = ?",
            (member_jid,),
        ).fetchone()
        chain.append(f"identity_links → phone: {dict(link) if link else 'NONE'}")
        if link:
            phone = link["phone_jid"]
            contact = conn.execute(
                "SELECT name, source FROM contacts WHERE phone_jid = ?", (phone,)
            ).fetchone()
            chain.append(f"contacts[{phone}]: {dict(contact) if contact else 'NONE'}")
        dn = conn.execute(
            "SELECT display_name FROM identity_display_names WHERE jid = ?",
            (member_jid,),
        ).fetchone()
        chain.append(f"identity_display_names: {dn['display_name'] if dn else 'NONE'}")
    elif member_jid.endswith("@s.whatsapp.net"):
        contact = conn.execute(
            "SELECT name FROM contacts WHERE phone_jid = ?", (member_jid,)
        ).fetchone()
        chain.append(f"contacts: {contact['name'] if contact else 'NONE'}")

    msg_count = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE chat_jid = ? AND sender_jid = ?",
        (chat_jid, member_jid),
    ).fetchone()[0]
    last_ts = conn.execute(
        "SELECT MAX(ts) FROM messages WHERE chat_jid = ? AND sender_jid = ?",
        (chat_jid, member_jid),
    ).fetchone()[0]
    chain.append(f"messages in chat: {msg_count} (last: {last_ts or 'never'})")

    return {
        "resolved_name": resolver.resolve(member_jid),
        "chain": chain,
    }
