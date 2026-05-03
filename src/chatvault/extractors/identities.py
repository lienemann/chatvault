"""Extract JIDs, LID↔phone mappings, and source-app display names.

This is the foundation for all later extractors: every JID a message references
needs to exist in `identities` first (we can't enforce FKs but we want consistent
referenceability for queries and joins).
"""

from __future__ import annotations

import logging
import sqlite3

from ..db import transaction
from . import ExtractorResult, jid_kind, now_iso

log = logging.getLogger(__name__)


def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult:
    res = ExtractorResult(name="identities")
    observed_at = now_iso()

    # ---- 1. identities table: every JID seen ----
    with transaction(archive):
        for r in source.execute(
            "SELECT _id, raw_string, user, server, type FROM jid WHERE raw_string IS NOT NULL"
        ):
            jid = r["raw_string"]
            kind = jid_kind(jid)
            archive.execute(
                "INSERT INTO identities(jid, kind, user_part, server_part, first_seen_ts, last_seen_ts) "
                "VALUES(?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(jid) DO UPDATE SET "
                "  kind        = excluded.kind, "
                "  user_part   = excluded.user_part, "
                "  server_part = excluded.server_part, "
                "  last_seen_ts = excluded.last_seen_ts",
                (jid, kind, r["user"], r["server"], observed_at, observed_at),
            )
            res.rows_written += 1

        # Country-code metadata, if present.
        for r in source.execute(
            "SELECT j.raw_string AS jid, m.country_code "
            "FROM jid_user_metadata m JOIN jid j ON m.jid_row_id = j._id"
        ):
            archive.execute(
                "UPDATE identities SET country_code = ? WHERE jid = ?",
                (r["country_code"], r["jid"]),
            )

    # ---- 2. identity_links: cumulative LID -> phone JID ----
    new_mappings: dict[str, str] = {}
    for r in source.execute(
        "SELECT j_lid.raw_string AS lid, j_pn.raw_string AS phone "
        "FROM jid_map jm "
        "JOIN jid j_lid ON jm.lid_row_id = j_lid._id "
        "JOIN jid j_pn  ON jm.jid_row_id = j_pn._id "
        "WHERE j_lid.raw_string LIKE '%@lid' "
        "  AND j_pn.raw_string  LIKE '%@s.whatsapp.net'"
    ):
        new_mappings[r["lid"]] = r["phone"]

    existing = {
        row[0]: row[1]
        for row in archive.execute("SELECT lid_jid, phone_jid FROM identity_links")
    }

    set_count = 0
    with transaction(archive):
        # Cumulative: once seen, always remembered. Update phone_jid if changed,
        # keep first_observed_ts on the original sighting.
        for lid, phone in new_mappings.items():
            prev = existing.get(lid)
            if prev is None:
                archive.execute(
                    "INSERT INTO identity_links(lid_jid, phone_jid, first_observed_ts, last_observed_ts) "
                    "VALUES(?, ?, ?, ?)",
                    (lid, phone, observed_at, observed_at),
                )
                archive.execute(
                    "INSERT INTO identity_links_history(lid_jid, phone_jid, op, observed_at) "
                    "VALUES(?, ?, 'set', ?)",
                    (lid, phone, observed_at),
                )
                set_count += 1
            elif prev != phone:
                # phone changed (rare). Update + history record.
                archive.execute(
                    "UPDATE identity_links "
                    "SET phone_jid = ?, last_observed_ts = ? WHERE lid_jid = ?",
                    (phone, observed_at, lid),
                )
                archive.execute(
                    "INSERT INTO identity_links_history(lid_jid, phone_jid, op, observed_at) "
                    "VALUES(?, ?, 'set', ?)",
                    (lid, phone, observed_at),
                )
                set_count += 1
            else:
                # Same mapping → just bump last_observed.
                archive.execute(
                    "UPDATE identity_links SET last_observed_ts = ? WHERE lid_jid = ?",
                    (observed_at, lid),
                )

        # Mappings that disappeared from the source: keep them (cumulative model).
        # WhatsApp can drop a mapping but we don't want to lose a known LID→phone
        # link. Future extension: emit op='remove' here if the user wants strict
        # parity with the source.

    res.with_note(f"identity_links: {set_count} new/changed, {len(new_mappings)} total")

    # ---- 3. identity_display_names: snapshot lid_display_name ----
    dn_written = 0
    with transaction(archive):
        for r in source.execute(
            "SELECT j.raw_string AS jid, ldn.display_name, ldn.username "
            "FROM lid_display_name ldn JOIN jid j ON ldn.lid_row_id = j._id"
        ):
            archive.execute(
                "INSERT INTO identity_display_names(jid, display_name, username, observed_at) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(jid) DO UPDATE SET "
                "  display_name = excluded.display_name, "
                "  username     = excluded.username, "
                "  observed_at  = excluded.observed_at",
                (r["jid"], r["display_name"], r["username"], observed_at),
            )
            dn_written += 1
    res.with_note(f"identity_display_names: {dn_written} snapshot rows")

    return res
