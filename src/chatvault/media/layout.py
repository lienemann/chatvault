"""Per-chat media folder naming.

Slug shape: ``<sanitised-name>-<6 hex>``.

``<sanitised-name>`` is a filesystem-safe transform of the chat's display name
at first-seen time (lowercase, ASCII alphanumeric, dashes for separators,
truncated). ``<6 hex>`` is the first 6 hex digits of ``sha1(jid)``; it makes
the slug identity-stable across name collisions and survives renames without
needing to rewrite paths in the DB.

Once a slug is recorded in ``_meta.chat_slug.<jid>``, we never change it.
Renaming the chat in the source app does NOT rename the folder.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import unicodedata

ORPHANS_DIR = "_orphans"

# Worst-case path lengths on FAT/ext4 vary, but 60 chars keeps slugs comfortably
# inside any sane limit while leaving room for the hash suffix and basename.
_MAX_NAME_LEN = 60
_SLUG_HASH_LEN = 6


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


def _sanitise(name: str) -> str:
    """Return a filesystem-safe lowercase ASCII representation of ``name``."""
    s = _strip_accents(name).lower()
    # Collapse any run of non-alnum into a single dash.
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    if len(s) > _MAX_NAME_LEN:
        s = s[:_MAX_NAME_LEN].rstrip("-")
    return s or "chat"


def chat_slug(jid: str, name: str | None) -> str:
    """Compose ``<sanitised-name>-<6hex>`` for a chat JID + display name."""
    safe = _sanitise(name or "")
    h = hashlib.sha1(jid.encode("utf-8")).hexdigest()[:_SLUG_HASH_LEN]
    return f"{safe}-{h}"


# ---------------------------------------------------------------------------
# Persistent slug pins. Once a chat's slug is recorded we never recompute it
# — even if the display name changes — so paths stay stable forever.
# ---------------------------------------------------------------------------


def _slug_key(jid: str) -> str:
    return f"chat_slug:{jid}"


def get_pinned_slug(conn: sqlite3.Connection, jid: str) -> str | None:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_slug_key(jid),)).fetchone()
    return row[0] if row else None


def pin_slug(conn: sqlite3.Connection, jid: str, slug: str) -> None:
    conn.execute(
        "INSERT INTO _meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO NOTHING",
        (_slug_key(jid), slug),
    )


def resolve_chat_slug(conn: sqlite3.Connection, jid: str) -> str:
    """Return the pinned slug for ``jid``; compute + pin on first use.

    The display name is looked up via ``chats.subject`` (groups) or the
    address-book / identity resolver (users). Falls back to the JID's user
    part if no name is known yet.
    """
    pinned = get_pinned_slug(conn, jid)
    if pinned:
        return pinned

    row = conn.execute("SELECT subject, kind FROM chats WHERE jid = ?", (jid,)).fetchone()
    name: str | None = None
    if row is not None:
        if row[1] == "group" and row[0]:
            name = row[0]
        elif row[1] in ("user", "lid"):
            # Direct contacts lookup first — works regardless of source app.
            crow = conn.execute(
                "SELECT name FROM contacts WHERE phone_jid = ?", (jid,)
            ).fetchone()
            if crow and crow[0]:
                name = crow[0]
            else:
                from chatvault.identities import NameResolver  # local import: cycle-safe

                try:
                    name = NameResolver(conn).resolve(jid)
                except Exception:  # noqa: BLE001 — resolver may fail on partial archives
                    name = None
    if not name:
        name = jid.split("@", 1)[0]

    slug = chat_slug(jid, name)
    pin_slug(conn, jid, slug)
    return slug
