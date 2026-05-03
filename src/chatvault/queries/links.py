"""Aggregate links shared in chats."""

from __future__ import annotations

import re
import sqlite3
from typing import Any

from .chats import resolve_chat

URL_RE = re.compile(r'https?://[^\s<>"\')]+')


def list_links(
    conn: sqlite3.Connection,
    *,
    chat: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Combine inline-text URLs and link_preview URLs."""
    chat_jid = resolve_chat(conn, chat) if chat else None
    out: list[dict[str, Any]] = []

    # link previews
    sql = (
        "SELECT lp.url, lp.title, m.ts, m.chat_jid, m.sender_jid "
        "FROM message_link_previews lp JOIN messages m ON m.id = lp.message_id"
    )
    params: list[Any] = []
    where: list[str] = []
    if chat_jid:
        where.append("m.chat_jid = ?")
        params.append(chat_jid)
    if since:
        where.append("m.ts >= ?")
        params.append(since)
    if until:
        where.append("m.ts <= ?")
        params.append(until)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY m.ts DESC LIMIT ?"
    params.append(limit)

    for r in conn.execute(sql, params):
        out.append({
            "url": r["url"],
            "title": r["title"],
            "ts": r["ts"] or "",
            "chat_jid": r["chat_jid"],
            "sender_jid": r["sender_jid"],
        })

    # inline urls in text — use a separate pass to avoid duplicating preview rows.
    sql2 = "SELECT m.text, m.ts, m.chat_jid, m.sender_jid FROM messages m WHERE m.text LIKE '%http%'"
    params2: list[Any] = []
    if chat_jid:
        sql2 += " AND m.chat_jid = ?"
        params2.append(chat_jid)
    if since:
        sql2 += " AND m.ts >= ?"
        params2.append(since)
    if until:
        sql2 += " AND m.ts <= ?"
        params2.append(until)
    sql2 += " ORDER BY m.ts DESC LIMIT ?"
    params2.append(limit)

    seen = {row["url"] for row in out}
    for r in conn.execute(sql2, params2):
        text = r["text"] or ""
        for url in URL_RE.findall(text):
            url = url.rstrip(",.;:!?")
            if url in seen:
                continue
            seen.add(url)
            out.append({
                "url": url,
                "title": None,
                "ts": r["ts"] or "",
                "chat_jid": r["chat_jid"],
                "sender_jid": r["sender_jid"],
            })

    out.sort(key=lambda x: x["ts"], reverse=True)
    return out[:limit]
