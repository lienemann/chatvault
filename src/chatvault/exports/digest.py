"""Render the last N messages of a chat as Markdown or JSONL."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from typing import Any

from ..identities import NameResolver


def render_digest(
    conn: sqlite3.Connection,
    chat_jid: str,
    *,
    last: int = 200,
    pseudonymise: bool = False,
) -> str:
    """Build a Markdown digest of the last N messages of a single chat."""
    chat_row = conn.execute(
        "SELECT subject, kind FROM chats WHERE jid = ?", (chat_jid,)
    ).fetchone()
    if not chat_row:
        msg = f"chat not found: {chat_jid}"
        raise LookupError(msg)
    resolver = NameResolver(conn)
    chat_title = (
        chat_row["subject"]
        or (resolver.resolve(chat_jid) if chat_row["kind"] in ("user", "lid") else chat_jid)
    )

    msgs = list(
        conn.execute(
            "SELECT * FROM messages WHERE chat_jid = ? "
            "ORDER BY ts DESC LIMIT ?",
            (chat_jid, last),
        )
    )
    msgs.reverse()  # chronological

    if not msgs:
        return f"# Digest: {chat_title}\n\nNo messages.\n"

    # Stable per-chat index: ts ASC, source_rowid ASC. 1 = oldest message
    # ever in the chat, regardless of slice. The slice we just pulled is the
    # last `last` messages, so their indices are total-len+1 ... total.
    chat_total = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE chat_jid = ?", (chat_jid,)
    ).fetchone()[0]
    start_idx = chat_total - len(msgs) + 1

    # Pre-fetch extension data in batches.
    msg_ids = [m["id"] for m in msgs]
    placeholders = ", ".join("?" for _ in msg_ids)
    media = {
        r["message_id"]: dict(r)
        for r in conn.execute(
            f"SELECT * FROM message_media WHERE message_id IN ({placeholders})",
            msg_ids,
        )
    }
    quoted = {
        r["message_id"]: dict(r)
        for r in conn.execute(
            f"SELECT * FROM message_quoted WHERE message_id IN ({placeholders})",
            msg_ids,
        )
    }
    link_previews = {
        r["message_id"]: dict(r)
        for r in conn.execute(
            f"SELECT * FROM message_link_previews WHERE message_id IN ({placeholders})",
            msg_ids,
        )
    }
    reactions_by_msg: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in conn.execute(
        f"SELECT * FROM reactions WHERE parent_message_id IN ({placeholders})",
        msg_ids,
    ):
        reactions_by_msg[r["parent_message_id"]].append(dict(r))
    edits_by_msg: dict[str, int] = defaultdict(int)
    for r in conn.execute(
        f"SELECT message_id, COUNT(*) AS c FROM edits "
        f"WHERE message_id IN ({placeholders}) GROUP BY message_id",
        msg_ids,
    ):
        edits_by_msg[r["message_id"]] = r["c"]

    pseudo_map: dict[str, str] = {}
    pseudo_counter = [0]

    def display(jid: str | None, *, from_me: bool = False) -> str:
        if from_me:
            return resolver.owner_name if not pseudonymise else "ME"
        if not jid:
            return "?"
        real = resolver.resolve(jid)
        if not pseudonymise:
            return real
        if jid not in pseudo_map:
            pseudo_counter[0] += 1
            pseudo_map[jid] = f"P{pseudo_counter[0]}"
        return pseudo_map[jid]

    # Pre-pass to get stable pseudo-IDs in order of appearance.
    if pseudonymise:
        for m in msgs:
            display(m["sender_jid"], from_me=bool(m["from_me"]))

    out: list[str] = []
    out.append(f"# Digest: {chat_title}")
    out.append("")
    out.append(f"- chat kind: {chat_row['kind']}")
    out.append(f"- messages: {len(msgs)}")
    out.append(f"- range: {msgs[0]['ts']} … {msgs[-1]['ts']} (UTC)")
    if pseudonymise:
        out.append("- senders: pseudonymised (ME = owner, P1..PN = others)")
    else:
        out.append(f"- owner: {resolver.owner_name}")
    out.append("")
    out.append("---")
    out.append("")

    for offset, m in enumerate(msgs):
        sender = display(m["sender_jid"], from_me=bool(m["from_me"]))
        ts = m["ts"][:19] if m["ts"] else "?"
        idx = start_idx + offset
        out.append(f"**[#{idx}] [{ts}] {sender}** _({m['type']})_  ")

        if (q := quoted.get(m["id"])):
            q_sender = display(q["quoted_sender_jid"])
            q_text = (q["quoted_text"] or "").replace("\n", " ").strip()
            if len(q_text) > 140:
                q_text = q_text[:140] + "…"
            out.append(f"  ↪ replying to {q_sender}: \"{q_text}\"  ")

        if m["text"]:
            for line in m["text"].splitlines():
                out.append(f"  {line}  ")

        if (md := media.get(m["id"])):
            mime = md["mime"] or "?"
            extra = f' caption="{md["caption"]}"' if md["caption"] else ""
            dur = f" dur={md['duration_s']}s" if md["duration_s"] else ""
            fname = md.get("mirrored_path") or md.get("file_path")
            fpart = f" file={fname}" if fname else ""
            out.append(f"  _[media: {mime}{extra}{dur}{fpart}]_  ")

        if (lp := link_previews.get(m["id"])):
            out.append(f"  _[link: {lp['url']}]_  ")
            if lp["title"]:
                out.append(f"  _title: {lp['title']}_  ")

        if (rxs := reactions_by_msg.get(m["id"])):
            by_emoji: dict[str, list[str]] = defaultdict(list)
            for r in rxs:
                by_emoji[r["emoji"]].append(
                    display(r["sender_jid"], from_me=bool(r["sender_from_me"]))
                )
            parts = [
                f"{e}×{len(ns)} ({', '.join(sorted(set(ns)))})"
                for e, ns in by_emoji.items()
            ]
            out.append(f"  _reactions: {'  '.join(parts)}_  ")

        if edits_by_msg.get(m["id"]):
            out.append(f"  _[edited {edits_by_msg[m['id']]}× — current text above]_  ")

        out.append("")

    return "\n".join(out) + "\n"


def render_digest_jsonl(
    conn: sqlite3.Connection,
    chat_jid: str,
    *,
    last: int = 200,
) -> str:
    """One JSON line per message, with sender resolution + media filename."""
    chat_row = conn.execute(
        "SELECT subject, kind FROM chats WHERE jid = ?", (chat_jid,)
    ).fetchone()
    if not chat_row:
        msg = f"chat not found: {chat_jid}"
        raise LookupError(msg)

    msgs = list(conn.execute(
        "SELECT * FROM messages WHERE chat_jid = ? ORDER BY ts DESC LIMIT ?",
        (chat_jid, last),
    ))
    msgs.reverse()
    if not msgs:
        return ""

    chat_total = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE chat_jid = ?", (chat_jid,)
    ).fetchone()[0]
    start_idx = chat_total - len(msgs) + 1

    msg_ids = [m["id"] for m in msgs]
    placeholders = ",".join("?" for _ in msg_ids)
    media = {r["message_id"]: dict(r) for r in conn.execute(
        f"SELECT * FROM message_media WHERE message_id IN ({placeholders})", msg_ids)}
    quoted = {r["message_id"]: dict(r) for r in conn.execute(
        f"SELECT * FROM message_quoted WHERE message_id IN ({placeholders})", msg_ids)}
    reactions_by_msg: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in conn.execute(
        f"SELECT * FROM reactions WHERE parent_message_id IN ({placeholders})", msg_ids
    ):
        reactions_by_msg[r["parent_message_id"]].append(dict(r))

    resolver = NameResolver(conn)
    lines: list[str] = []
    for offset, m in enumerate(msgs):
        d = dict(m)
        rec: dict[str, Any] = {
            "idx": start_idx + offset,
            "id": d["id"],
            "ts": d["ts"],
            "from_me": bool(d["from_me"]),
            "sender_jid": d["sender_jid"],
            "sender_name": (
                resolver.owner_name if d["from_me"]
                else (resolver.resolve(d["sender_jid"]) if d["sender_jid"] else None)
            ),
            "type": d["type"],
            "text": d["text"],
        }
        if (md := media.get(d["id"])):
            rec["media"] = {
                "mime": md.get("mime"),
                "caption": md.get("caption"),
                "duration_s": md.get("duration_s"),
                "file_path": md.get("file_path"),
                "mirrored_path": md.get("mirrored_path"),
                "file_size": md.get("file_size"),
            }
        if (q := quoted.get(d["id"])):
            rec["quoted"] = {
                "sender_jid": q.get("quoted_sender_jid"),
                "text": q.get("quoted_text"),
            }
        if (rxs := reactions_by_msg.get(d["id"])):
            rec["reactions"] = [
                {
                    "emoji": rx["emoji"],
                    "sender_name": resolver.resolve(
                        rx["sender_jid"], from_me=bool(rx["sender_from_me"])
                    ),
                }
                for rx in rxs
            ]
        lines.append(json.dumps(rec, ensure_ascii=False))
    return "\n".join(lines) + "\n"
