"""JID resolution: turn a raw JID into a human-readable name.

The chain (most preferred first):
    1. address-book name              (contacts table)
    2. source-app display_name        (identity_display_names, if it has letters)
    3. '+' + raw phone number         (when only the phone JID is known)
    4. 'lid:{user_part}'              (we know an identity but no name)
    5. raw JID                        (last resort)
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass


def jid_kind(jid: str | None) -> str:
    """Classify a JID by suffix. Mirrors source-app categories."""
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


def jid_user_part(jid: str | None) -> str | None:
    if not jid or "@" not in jid:
        return None
    return jid.split("@", 1)[0]


def jid_server_part(jid: str | None) -> str | None:
    if not jid or "@" not in jid:
        return None
    return jid.split("@", 1)[1]


# Display names from the source app are often masked phones like '+41∙∙∙∙∙∙∙04'.
# We accept those as a fallback, but a "real" looking name (letters) ranks higher.
_MASKED_PHONE_RE = re.compile(r"^\+?[\d∙\s]+$")


def _looks_like_real_name(s: str) -> bool:
    return bool(re.search(r"[A-Za-zÀ-ÿ]", s))


@dataclass(frozen=True, slots=True)
class _ResolverState:
    contacts: dict[str, str]                 # phone_jid -> name
    lid_to_phone: dict[str, str]             # lid_jid -> phone_jid
    lid_display_names: dict[str, str]        # jid -> display_name
    group_subjects: dict[str, str]           # group_jid -> subject
    newsletter_names: dict[str, str]         # chat_jid -> name
    owner_jid: str | None                    # the archive owner's phone JID, if discoverable
    owner_name: str                          # what to call them


class NameResolver:
    """Resolves JIDs to display names, with in-memory caching of lookup tables.

    Construct once per query/run, not per message — it preloads tables.
    """

    def __init__(self, conn: sqlite3.Connection, *, owner_label_fallback: str | None = None) -> None:
        contacts = {
            r[0]: r[1] for r in conn.execute("SELECT phone_jid, name FROM contacts")
        }
        lid_to_phone = {
            r[0]: r[1]
            for r in conn.execute("SELECT lid_jid, phone_jid FROM identity_links")
        }
        lid_display_names = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT jid, display_name FROM identity_display_names "
                "WHERE display_name IS NOT NULL"
            )
        }
        group_subjects = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT jid, subject FROM chats WHERE kind = 'group' AND subject IS NOT NULL"
            )
        }
        newsletter_names = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT chat_jid, name FROM newsletter_metadata WHERE name IS NOT NULL"
            )
        }

        owner_jid = self._discover_owner(conn)
        owner_name = self._owner_name(
            owner_jid, contacts, owner_label_fallback or self._owner_from_config()
        )

        self._s = _ResolverState(
            contacts=contacts,
            lid_to_phone=lid_to_phone,
            lid_display_names=lid_display_names,
            group_subjects=group_subjects,
            newsletter_names=newsletter_names,
            owner_jid=owner_jid,
            owner_name=owner_name,
        )

    @staticmethod
    def _discover_owner(conn: sqlite3.Connection) -> str | None:
        row = conn.execute(
            "SELECT sender_jid FROM messages "
            "WHERE from_me = 1 AND sender_jid IS NOT NULL "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _owner_from_config() -> str:
        from .config import Paths, owner_name_from_config

        return owner_name_from_config(Paths.default().config_dir, default="Me")

    @staticmethod
    def _owner_name(
        owner_jid: str | None,
        contacts: dict[str, str],
        fallback: str,
    ) -> str:
        if owner_jid and owner_jid in contacts:
            return contacts[owner_jid]
        return fallback

    @property
    def owner_name(self) -> str:
        return self._s.owner_name

    @property
    def owner_jid(self) -> str | None:
        return self._s.owner_jid

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, jid: str | None, *, from_me: bool = False) -> str:
        """Best-effort display name for a JID."""
        if from_me:
            return self._s.owner_name
        if not jid:
            return "?"
        # Source app uses 'lid_me' / 'status_me' pseudo-JIDs for the owner.
        if jid in ("lid_me", "status_me"):
            return self._s.owner_name
        kind = jid_kind(jid)
        if kind == "user":
            return self._resolve_phone(jid)
        if kind == "lid":
            return self._resolve_lid(jid)
        if kind == "group":
            return self._s.group_subjects.get(jid) or f"<group {jid_user_part(jid)}>"
        if kind == "newsletter":
            return self._s.newsletter_names.get(jid) or "<newsletter>"
        if kind == "broadcast":
            return "<broadcast>"
        if kind == "status":
            return "<status>"
        return jid

    def _resolve_phone(self, phone_jid: str) -> str:
        name = self._s.contacts.get(phone_jid)
        if name:
            return name
        dn = self._s.lid_display_names.get(phone_jid)
        if dn and _looks_like_real_name(dn):
            return dn
        from .contacts import pretty_phone
        return pretty_phone(phone_jid)

    def _resolve_lid(self, lid_jid: str) -> str:
        # Step 1: do we have a phone JID for this LID?
        phone = self._s.lid_to_phone.get(lid_jid)
        if phone:
            resolved = self._resolve_phone(phone)
            # If phone resolution only got us a "+phone" fallback, prefer the
            # LID's own display_name if it has letters (push-name).
            if not resolved.startswith("+"):
                return resolved
            dn_lid = self._s.lid_display_names.get(lid_jid)
            if dn_lid and _looks_like_real_name(dn_lid):
                return dn_lid
            return resolved
        # Step 2: did the source app give us a real-looking display name?
        dn = self._s.lid_display_names.get(lid_jid)
        if dn and _looks_like_real_name(dn):
            return dn
        # Step 3: fall back to a stable lid identifier
        return f"lid:{jid_user_part(lid_jid) or lid_jid}"

    def is_known(self, jid: str | None) -> bool:
        """True if we have a real-looking name (not just a phone or lid: ref)."""
        if not jid:
            return False
        kind = jid_kind(jid)
        if kind == "user":
            return jid in self._s.contacts
        if kind == "lid":
            phone = self._s.lid_to_phone.get(jid)
            if phone and phone in self._s.contacts:
                return True
            dn = self._s.lid_display_names.get(jid)
            return bool(dn and _looks_like_real_name(dn))
        return False
