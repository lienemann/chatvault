"""Extract system messages with reconstructed human-readable bodies.

Source: `message_system` (action_type) joined with the various `message_system_*`
sub-tables that carry the structured payload (participants, old/new values, etc.).
We render a short body sentence so consumers don't have to parse action types.
"""

from __future__ import annotations

import json
import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, jid_kind, ms_to_iso, stable_message_id

log = logging.getLogger(__name__)


# Mapping of source action_type → kind label + body template.
# action_type ints are stable on Android WA 2024+; verified against schema.
ACTION_KIND: dict[int, str] = {
    1: "group_subject_change",
    4: "group_chat_participant_added",
    5: "group_chat_participant_left",
    6: "group_subject_change",          # subject change variant
    7: "group_chat_participant_removed",
    11: "group_created",
    12: "group_chat_participant_added",
    14: "group_chat_participant_removed",
    15: "group_chat_participant_left",
    18: "encryption_change",
    19: "ephemeral_change",
    20: "group_chat_participant_promoted",
    21: "group_chat_participant_demoted",
    25: "group_icon_change",
    27: "phone_number_change",
    46: "video_call_missed",
    50: "blocked_contact",
    56: "voice_call_missed",
    58: "business_state",
    61: "group_description_change",
    67: "ephemeral_setting_off",
    68: "ephemeral_setting_on",
    101: "lid_change",
}


def _kind_for(action_type: int | None) -> str:
    if action_type is None:
        return "unknown_system"
    return ACTION_KIND.get(action_type, f"unknown_system_{action_type}")


def _participants_for(source: sqlite3.Connection, message_row_id: int) -> list[str]:
    out: list[str] = []
    for r in source.execute(
        "SELECT j.raw_string FROM message_system_chat_participant p "
        "LEFT JOIN jid j ON p.user_jid_row_id = j._id "
        "WHERE p.message_row_id = ?",
        (message_row_id,),
    ):
        if r[0]:
            out.append(r[0])
    return out


def _value_change(source: sqlite3.Connection, message_row_id: int) -> tuple[str | None, str | None]:
    row = source.execute(
        "SELECT old_data FROM message_system_value_change WHERE message_row_id = ?",
        (message_row_id,),
    ).fetchone()
    return (row[0], None) if row else (None, None)


def _number_change(
    source: sqlite3.Connection, message_row_id: int
) -> tuple[str | None, str | None]:
    row = source.execute(
        "SELECT j_old.raw_string, j_new.raw_string "
        "FROM message_system_number_change c "
        "LEFT JOIN jid j_old ON c.old_jid_row_id = j_old._id "
        "LEFT JOIN jid j_new ON c.new_jid_row_id = j_new._id "
        "WHERE c.message_row_id = ?",
        (message_row_id,),
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def _render_body(kind: str, participants: list[str], old: str | None, new: str | None) -> str:
    """Short human-readable summary. Names are JIDs at this stage; the resolver runs at query time."""
    if kind == "group_created":
        return "Group created"
    if kind == "group_subject_change":
        if old or new:
            return f"Subject changed: {old or '?'} → {new or '?'}"
        return "Subject changed"
    if kind == "group_icon_change":
        return "Group icon changed"
    if kind == "group_description_change":
        return "Group description changed"
    if kind in ("group_chat_participant_added",):
        return "Added " + ", ".join(participants) if participants else "Participant added"
    if kind == "group_chat_participant_removed":
        return "Removed " + ", ".join(participants) if participants else "Participant removed"
    if kind == "group_chat_participant_left":
        return "Left: " + ", ".join(participants) if participants else "Participant left"
    if kind == "group_chat_participant_promoted":
        return "Promoted " + ", ".join(participants) if participants else "Participant promoted"
    if kind == "group_chat_participant_demoted":
        return "Demoted " + ", ".join(participants) if participants else "Participant demoted"
    if kind == "phone_number_change":
        return f"Phone number changed: {old or '?'} → {new or '?'}"
    if kind == "encryption_change":
        return "Encryption details changed"
    if kind == "ephemeral_setting_off":
        return "Disappearing messages: off"
    if kind == "ephemeral_setting_on":
        return "Disappearing messages: on"
    if kind == "blocked_contact":
        return "Block status changed"
    if kind == "video_call_missed":
        return "Missed video call"
    if kind == "voice_call_missed":
        return "Missed voice call"
    if kind == "lid_change":
        return f"LID changed: {old or '?'} → {new or '?'}"
    return f"System event: {kind}"


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="system_events")

    # Find all messages with a system row, joined to chat+key_id for the stable id.
    cur = source.execute(
        "SELECT s.message_row_id AS rowid, s.action_type AS action_type, "
        "       m.key_id AS key_id, m.from_me AS from_me, m.timestamp AS ts_ms, "
        "       j_chat.raw_string AS chat_jid "
        "FROM message_system s "
        "JOIN message m       ON s.message_row_id = m._id "
        "LEFT JOIN chat c     ON m.chat_row_id    = c._id "
        "LEFT JOIN jid j_chat ON c.jid_row_id     = j_chat._id"
    )

    with transaction(archive):
        for row in cur:
            rowid = row["rowid"]
            chat_jid = row["chat_jid"]
            key_id = row["key_id"]
            if not key_id:
                res.rows_skipped += 1
                continue
            from_me = bool(row["from_me"])
            message_id = stable_message_id(chat_jid, from_me, key_id)

            action_type = row["action_type"]
            kind = _kind_for(action_type)
            participants = _participants_for(source, rowid)
            old, new = (None, None)
            if "phone_number" in kind or "lid_change" in kind:
                old, new = _number_change(source, rowid)
            elif "subject" in kind or "description" in kind:
                old, _ = _value_change(source, rowid)

            body = _render_body(kind, participants, old, new)

            # Ensure a messages row exists (the messages extractor would have skipped
            # action_type=7, so we insert a stub here).
            archive.execute(
                "INSERT OR IGNORE INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, "
                "                               ts, type, type_raw, text, key_id, starred) "
                "VALUES(?, ?, ?, NULL, ?, ?, 'system', 7, NULL, ?, 0)",
                (message_id, rowid, chat_jid, int(from_me), ms_to_iso(row["ts_ms"]), key_id),
            )

            actor_jid = participants[0] if participants and jid_kind(participants[0]) in ("user", "lid") else None
            archive.execute(
                "INSERT OR REPLACE INTO system_events(message_id, action_type, kind, body, "
                "                                     actor_jid, affected_jids, old_value, new_value, raw_json) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    message_id, action_type, kind, body,
                    actor_jid,
                    json.dumps(participants, ensure_ascii=False) if participants else None,
                    old, new, None,
                ),
            )
            res.rows_written += 1

    return res
