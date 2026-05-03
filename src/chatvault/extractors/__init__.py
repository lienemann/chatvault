"""Per-domain extractors: read from a decrypted msgstore.db, write to chatvault tables."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source message-type ints → our string labels.
# ---------------------------------------------------------------------------

MESSAGE_TYPES: dict[int, str] = {
    0: "text",
    1: "image",
    2: "audio",
    3: "video",
    4: "contact",
    5: "location",
    7: "system",
    9: "document",
    10: "call_log",
    11: "empty",
    13: "gif",
    15: "deleted",
    16: "template",
    20: "sticker",
    25: "product",
    27: "group_invite",
    28: "list",
    36: "poll",
    42: "text",       # variant — verified identical to type 0
    90: "reaction_stub",  # parallel reaction row, FILTERED out of message stream
}

# Filtered out of message stream (these belong to other extractors).
SKIP_MESSAGE_TYPES: frozenset[int] = frozenset({90})


def label_for_type(raw: int | None) -> str:
    if raw is None:
        return "unknown"
    return MESSAGE_TYPES.get(raw, f"unknown_{raw}")


# ---------------------------------------------------------------------------
# Time helpers.
# ---------------------------------------------------------------------------


def ms_to_iso(ms: int | None) -> str | None:
    """Unix-ms → ISO-8601 UTC. None and 0 return None."""
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# JID kind (mirrors identities.jid_kind, duplicated here to avoid import cycle).
# ---------------------------------------------------------------------------


def jid_kind(jid: str | None) -> str:
    if not jid:
        return "unknown"
    if jid.endswith("@g.us"):
        return "group"
    if jid.endswith("@s.whatsapp.net"):
        return "user"
    if jid.endswith("@lid"):
        return "lid"
    if jid.endswith("@broadcast"):
        return "broadcast"
    if jid.endswith("@newsletter"):
        return "newsletter"
    if jid.endswith("@status"):
        return "status"
    return "other"


def stable_message_id(chat_jid: str | None, from_me: bool, key_id: str) -> str:
    return f"{chat_jid or 'unknown'}:{int(bool(from_me))}:{key_id}"


def parent_message_id(parent_chat_jid: str | None, parent_key_id: str) -> str:
    """Same shape as messages.id but with '?' for from_me, since reactions/edits don't carry it."""
    return f"{parent_chat_jid or 'unknown'}:?:{parent_key_id}"


# ---------------------------------------------------------------------------
# Stat container returned from each extractor.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ExtractorResult:
    name: str
    rows_written: int = 0
    rows_skipped: int = 0
    notes: list[str] | None = None

    def with_note(self, note: str) -> ExtractorResult:
        if self.notes is None:
            self.notes = []
        self.notes.append(note)
        return self


# ---------------------------------------------------------------------------
# JSON helper
# ---------------------------------------------------------------------------


def to_raw_json(d: dict[str, object]) -> str:
    """Serialise to compact JSON for raw_json columns."""
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), default=str)


# ---------------------------------------------------------------------------
# Iter helper that ensures Row objects work as dicts.
# ---------------------------------------------------------------------------


def rows(cur: sqlite3.Cursor) -> Iterable[sqlite3.Row]:
    cur.row_factory = sqlite3.Row
    yield from cur
