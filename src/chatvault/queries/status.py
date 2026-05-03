"""Status-archive queries: list posts, show views on own posts."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..identities import NameResolver


def list_status(
    conn: sqlite3.Connection,
    *,
    kind: str | None = None,
    sender: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Status posts with sender resolution. Newest first."""
    where: list[str] = []
    params: list[Any] = []
    if kind:
        where.append("kind = ?")
        params.append(kind)
    if since:
        where.append("ts >= ?")
        params.append(since)
    if until:
        where.append("ts <= ?")
        params.append(until)
    sql = (
        "SELECT s.id, s.kind, s.sender_jid, s.type, s.ts, s.text, "
        "       m.mime, m.duration_s, m.file_path, m.media_url "
        "FROM status_archive s "
        "LEFT JOIN status_archive_media m ON s.id = m.status_id"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY s.ts DESC NULLS LAST LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in conn.execute(sql, params)]
    if not rows:
        return rows

    resolver = NameResolver(conn)
    sender_lower = sender.lower() if sender else None
    out: list[dict[str, Any]] = []
    for r in rows:
        name = resolver.resolve(r["sender_jid"]) if r["sender_jid"] else "?"
        if sender_lower and sender_lower not in name.lower():
            continue
        r["sender_name"] = name
        out.append(r)
    return out


def resolve_own_status_id(conn: sqlite3.Connection, selector: str) -> str | None:
    """Resolve a friendly selector to a concrete own-status id.

    Selectors:
        'latest'     — most recent own status by ts
        'top'        — own status with most views
        '<int>'      — 1-based index into newest-first list
        anything else is returned as-is (treated as a literal id).
    """
    sel = selector.strip()
    if sel == "latest":
        row = conn.execute(
            "SELECT id FROM status_archive WHERE kind='own' "
            "ORDER BY ts DESC NULLS LAST LIMIT 1"
        ).fetchone()
        return row["id"] if row else None
    if sel == "top":
        row = conn.execute(
            "SELECT s.id FROM status_archive s WHERE s.kind='own' "
            "ORDER BY (SELECT COUNT(*) FROM status_archive_views v WHERE v.status_id=s.id) DESC, "
            "         s.ts DESC LIMIT 1"
        ).fetchone()
        return row["id"] if row else None
    if sel.isdigit():
        idx = int(sel)
        rows = list(conn.execute(
            "SELECT id FROM status_archive WHERE kind='own' "
            "ORDER BY ts DESC NULLS LAST LIMIT 1 OFFSET ?",
            (max(0, idx - 1),),
        ))
        return rows[0]["id"] if rows else None
    return sel


def views_for_status(conn: sqlite3.Connection, status_id: str) -> list[dict[str, Any]]:
    """Viewers of a single status post (own only — receipts don't exist for received)."""
    resolver = NameResolver(conn)
    out: list[dict[str, Any]] = []
    for r in conn.execute(
        "SELECT viewer_jid, received_ts, read_ts, played_ts "
        "FROM status_archive_views WHERE status_id = ? "
        "ORDER BY COALESCE(read_ts, received_ts) DESC",
        (status_id,),
    ):
        out.append({
            "viewer_jid": r["viewer_jid"],
            "viewer_name": resolver.resolve(r["viewer_jid"]),
            "received_ts": r["received_ts"],
            "read_ts": r["read_ts"],
            "played_ts": r["played_ts"],
        })
    return out


def reactions_for_status(conn: sqlite3.Connection, status_id: str) -> list[dict[str, Any]]:
    """Reactions on a status post. Joins reactions via messages.{chat_jid,key_id}."""
    resolver = NameResolver(conn)
    out: list[dict[str, Any]] = []
    for r in conn.execute(
        "SELECT r.emoji, r.sender_jid, r.sender_from_me, r.sender_ts "
        "FROM status_archive s "
        "JOIN messages m ON s.message_id = m.id "
        "JOIN reactions r ON r.parent_chat_jid = m.chat_jid "
        "                AND r.parent_key_id = m.key_id "
        "WHERE s.id = ? "
        "ORDER BY r.sender_ts DESC",
        (status_id,),
    ):
        out.append({
            "emoji": r["emoji"],
            "sender_jid": r["sender_jid"],
            "sender_name": resolver.resolve(r["sender_jid"], from_me=bool(r["sender_from_me"])),
            "sender_ts": r["sender_ts"],
        })
    return out


def own_status_view_summary(
    conn: sqlite3.Connection,
    *,
    since: str | None = None,
    until: str | None = None,
    limit: int = 50,
    sort: str = "ts",
) -> list[dict[str, Any]]:
    """Per-status view + reaction counts for own posts.

    sort: 'ts' (newest first) or 'views' (most-viewed first).
    """
    where = "WHERE s.kind = 'own'"
    params: list[Any] = []
    if since:
        where += " AND s.ts >= ?"
        params.append(since)
    if until:
        where += " AND s.ts <= ?"
        params.append(until)
    order = (
        "view_count DESC, s.ts DESC"
        if sort == "views"
        else "s.ts DESC NULLS LAST"
    )
    sql = (
        "SELECT s.id, s.ts, s.type, s.text, "
        "       (SELECT COUNT(*) FROM status_archive_views v WHERE v.status_id = s.id) "
        "         AS view_count, "
        "       (SELECT COUNT(*) FROM reactions r "
        "          JOIN messages m ON s.message_id = m.id "
        "         WHERE r.parent_chat_jid = m.chat_jid AND r.parent_key_id = m.key_id) "
        "         AS reaction_count "
        f"FROM status_archive s {where} "
        f"ORDER BY {order} LIMIT ?"
    )
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params)]
