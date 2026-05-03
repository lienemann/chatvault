"""Substring search across messages."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..identities import NameResolver
from .chats import resolve_chat


def search_messages(
    conn: sqlite3.Connection,
    query: str,
    *,
    chat: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Case-insensitive substring search on `messages.text`."""
    resolver = NameResolver(conn)
    sql = (
        "SELECT m.id, m.ts, m.chat_jid, m.sender_jid, m.from_me, m.text, "
        "       c.subject AS chat_subject, "
        "       mm.file_path AS media_file_path, "
        "       mm.mirrored_path AS media_mirrored_path, "
        "       mm.mime AS media_mime "
        "FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid "
        "LEFT JOIN message_media mm ON mm.message_id = m.id "
        "WHERE m.text IS NOT NULL AND LOWER(m.text) LIKE ?"
    )
    params: list[Any] = [f"%{query.lower()}%"]
    if chat:
        sql += " AND m.chat_jid = ?"
        params.append(resolve_chat(conn, chat))
    if since:
        sql += " AND m.ts >= ?"
        params.append(since)
    if until:
        sql += " AND m.ts <= ?"
        params.append(until)
    sql += " ORDER BY m.ts DESC LIMIT ?"
    params.append(limit)

    out: list[dict[str, Any]] = []
    for r in conn.execute(sql, params):
        d = dict(r)
        d["sender_name"] = resolver.resolve(d["sender_jid"], from_me=bool(d["from_me"]))
        out.append(d)
    return out
