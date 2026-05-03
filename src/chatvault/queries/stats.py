"""Quick row-count overview."""

from __future__ import annotations

import sqlite3


TABLES = [
    "messages", "message_media", "message_quoted", "message_link_previews",
    "message_mentions", "message_locations", "message_vcards",
    "message_audio", "message_transcription_segments",
    "system_events", "reactions", "edits",
    "chats", "chat_members", "chat_members_history",
    "contacts", "identity_links", "identities",
    "calls", "call_participants", "polls", "poll_options", "poll_votes",
    "status_posts", "newsletter_messages", "newsletter_metadata",
    "media_mirror",
]


def quick_stats(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for t in TABLES:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            n = 0
        out.append((t, n))
    return out
