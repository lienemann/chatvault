"""Shared fixtures: an in-memory archive DB and a synthetic source DB."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from chatvault.db import apply_pending_migrations, connect


@pytest.fixture()
def archive_db(tmp_path: Path) -> sqlite3.Connection:
    """A freshly-migrated chatvault archive DB on disk (so migrations exercise file mode)."""
    conn = connect(tmp_path / "archive.db")
    apply_pending_migrations(conn)
    yield conn
    conn.close()


@pytest.fixture()
def synthetic_source(tmp_path: Path) -> sqlite3.Connection:
    """A minimal source DB resembling the relevant subset of msgstore.db.

    Only the columns the extractors actually read are populated.
    """
    src = sqlite3.connect(tmp_path / "msgstore.db")
    src.row_factory = sqlite3.Row
    src.executescript(
        """
        CREATE TABLE jid (
            _id INTEGER PRIMARY KEY,
            user TEXT, server TEXT, agent INTEGER, type INTEGER, raw_string TEXT, device INTEGER
        );
        CREATE TABLE chat (
            _id INTEGER PRIMARY KEY,
            jid_row_id INTEGER, hidden INTEGER, subject TEXT,
            created_timestamp INTEGER, archived INTEGER, sort_timestamp INTEGER,
            chat_lock INTEGER, ephemeral_expiration INTEGER, group_type INTEGER,
            group_member_count INTEGER, is_contact INTEGER
        );
        CREATE TABLE message (
            _id INTEGER PRIMARY KEY,
            chat_row_id INTEGER, from_me INTEGER, key_id TEXT,
            sender_jid_row_id INTEGER, timestamp INTEGER, received_timestamp INTEGER,
            message_type INTEGER, text_data TEXT, starred INTEGER, status INTEGER, origin INTEGER
        );
        CREATE TABLE message_media (
            message_row_id INTEGER PRIMARY KEY,
            file_path TEXT, file_size INTEGER, mime_type TEXT, file_hash TEXT,
            media_caption TEXT, media_duration INTEGER, width INTEGER, height INTEGER, media_name TEXT
        );
        CREATE TABLE message_quoted (
            message_row_id INTEGER PRIMARY KEY,
            key_id TEXT, text_data TEXT, message_type INTEGER, sender_jid_row_id INTEGER
        );
        CREATE TABLE message_text (
            message_row_id INTEGER PRIMARY KEY, description TEXT, page_title TEXT, url TEXT
        );
        CREATE TABLE message_mentions (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, jid_row_id INTEGER, display_name TEXT
        );
        CREATE TABLE message_forwarded (
            message_row_id INTEGER PRIMARY KEY, forward_score INTEGER, forward_origin INTEGER
        );
        CREATE TABLE message_revoked (
            message_row_id INTEGER PRIMARY KEY,
            revoked_key_id TEXT, admin_jid_row_id INTEGER, revoke_timestamp INTEGER
        );
        CREATE TABLE message_album (
            message_row_id INTEGER PRIMARY KEY,
            image_count INTEGER, video_count INTEGER,
            expected_image_count INTEGER, expected_video_count INTEGER
        );
        CREATE TABLE message_view_once_media (
            message_row_id INTEGER PRIMARY KEY, state INTEGER
        );
        CREATE TABLE message_ephemeral (
            message_row_id INTEGER PRIMARY KEY,
            duration INTEGER, expire_timestamp INTEGER
        );
        CREATE TABLE message_location (
            message_row_id INTEGER PRIMARY KEY,
            latitude REAL, longitude REAL, place_name TEXT, place_address TEXT, url TEXT,
            live_location_share_duration INTEGER,
            live_location_final_latitude REAL,
            live_location_final_longitude REAL,
            live_location_final_timestamp INTEGER
        );
        CREATE TABLE message_add_on (
            _id INTEGER PRIMARY KEY,
            parent_message_row_id INTEGER, sender_jid_row_id INTEGER, key_id TEXT,
            timestamp INTEGER, from_me INTEGER, message_add_on_type INTEGER
        );
        CREATE TABLE message_add_on_reaction (
            message_add_on_row_id INTEGER PRIMARY KEY,
            reaction TEXT, sender_timestamp INTEGER
        );
        CREATE TABLE message_edit_info (
            message_row_id INTEGER PRIMARY KEY,
            edited_timestamp INTEGER, sender_timestamp INTEGER, original_key_id TEXT
        );
        CREATE TABLE jid_map (
            lid_row_id INTEGER PRIMARY KEY, jid_row_id INTEGER, sort_id INTEGER
        );
        CREATE TABLE jid_user_metadata (
            jid_row_id INTEGER PRIMARY KEY, country_code TEXT
        );
        CREATE TABLE lid_display_name (
            lid_row_id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, username TEXT
        );
        CREATE TABLE group_participant_user (
            _id INTEGER PRIMARY KEY,
            group_jid_row_id INTEGER, user_jid_row_id INTEGER,
            rank INTEGER, pending INTEGER, add_timestamp INTEGER, label TEXT, join_method INTEGER
        );
        CREATE TABLE group_past_participant_user (
            _id INTEGER PRIMARY KEY,
            group_jid_row_id INTEGER, user_jid_row_id INTEGER, is_leave INTEGER, timestamp INTEGER
        );
        CREATE TABLE message_system (
            message_row_id INTEGER PRIMARY KEY, action_type INTEGER
        );
        CREATE TABLE message_system_chat_participant (
            message_row_id INTEGER, user_jid_row_id INTEGER
        );
        CREATE TABLE message_system_value_change (
            message_row_id INTEGER PRIMARY KEY, old_data TEXT
        );
        CREATE TABLE message_system_number_change (
            message_row_id INTEGER PRIMARY KEY, old_jid_row_id INTEGER, new_jid_row_id INTEGER
        );

        -- Tables that exist in any real source DB but may be empty in tests:
        CREATE TABLE call_log (
            _id INTEGER PRIMARY KEY,
            jid_row_id INTEGER, from_me INTEGER, call_id TEXT, timestamp INTEGER,
            video_call INTEGER, duration INTEGER, call_result INTEGER,
            bytes_transferred INTEGER, group_jid_row_id INTEGER,
            call_creator_device_jid_row_id INTEGER, is_dnd_mode_on INTEGER,
            call_type INTEGER, scheduled_id TEXT
        );
        CREATE TABLE call_log_participant_v2 (
            _id INTEGER PRIMARY KEY, call_log_row_id INTEGER, jid_row_id INTEGER, call_result INTEGER
        );
        CREATE TABLE message_poll (
            message_row_id INTEGER PRIMARY KEY,
            selectable_options_count INTEGER, poll_type INTEGER, content_type INTEGER,
            end_time INTEGER, allow_add_option INTEGER, hide_participant_names INTEGER,
            invalid_state INTEGER
        );
        CREATE TABLE message_poll_option (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, option_name TEXT, vote_total INTEGER
        );
        CREATE TABLE message_add_on_poll_vote (
            message_add_on_row_id INTEGER PRIMARY KEY, sender_timestamp INTEGER
        );
        CREATE TABLE message_add_on_poll_vote_selected_option (
            _id INTEGER PRIMARY KEY,
            message_add_on_row_id INTEGER, message_poll_option_id INTEGER
        );
        CREATE TABLE status_message_info (
            message_row_id INTEGER PRIMARY KEY,
            status_distribution_mode INTEGER, audience_type INTEGER,
            can_be_reshared INTEGER, has_embedded_music INTEGER,
            is_mentioned INTEGER, status_mentions TEXT, poster_status_id TEXT
        );
        CREATE TABLE receipt_user (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, receipt_user_jid_row_id INTEGER,
            receipt_timestamp INTEGER, read_timestamp INTEGER, played_timestamp INTEGER
        );
        CREATE TABLE newsletter (
            chat_row_id INTEGER PRIMARY KEY,
            name TEXT, description TEXT, handle TEXT, picture_url TEXT, preview_url TEXT,
            invite_code TEXT, subscribers_count INTEGER, verified INTEGER, suspended INTEGER,
            deleted INTEGER, privacy INTEGER, membership INTEGER,
            name_id INTEGER NOT NULL DEFAULT 0, description_id INTEGER NOT NULL DEFAULT 0,
            picture_id INTEGER NOT NULL DEFAULT 0, preview_id INTEGER NOT NULL DEFAULT 0,
            oldest_message_retrieved INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE newsletter_message (
            message_row_id INTEGER PRIMARY KEY,
            chat_row_id INTEGER, server_message_id INTEGER, view_count INTEGER,
            forwards_count INTEGER, comments_count INTEGER, is_paid_partnership INTEGER
        );
        CREATE TABLE newsletter_message_reaction (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, reaction TEXT, reaction_count INTEGER
        );
        CREATE TABLE audio_data (
            message_row_id INTEGER PRIMARY KEY,
            waveform BLOB, transcription_status INTEGER, transcription_locale INTEGER,
            transcription_id TEXT
        );
        CREATE TABLE transcription_segment (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, substring_start INTEGER, substring_length INTEGER,
            timestamp INTEGER, duration INTEGER, confidence INTEGER
        );
        CREATE TABLE message_vcard (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, vcard TEXT
        );
        CREATE TABLE message_vcard_jid (
            _id INTEGER PRIMARY KEY,
            vcard_jid_row_id INTEGER, vcard_row_id INTEGER, message_row_id INTEGER
        );
        CREATE TABLE bot_message_info (
            message_row_id INTEGER PRIMARY KEY,
            target_id TEXT, message_state INTEGER, invoker_jid_row_id INTEGER,
            bot_jid_row_id INTEGER, model_type INTEGER, message_disclaimer TEXT,
            bot_response_id TEXT
        );
        CREATE TABLE ai_thread_info (
            thread_id_row_id INTEGER PRIMARY KEY,
            title TEXT, creation_ts INTEGER NOT NULL, variant INTEGER,
            last_message_timestamp INTEGER, unseen_message_count INTEGER, selected_mode INTEGER
        );
        CREATE TABLE ai_rich_response_message_core_info (
            message_row_id INTEGER PRIMARY KEY,
            ai_rich_response_message_type INTEGER NOT NULL DEFAULT 0,
            ai_rich_response_submessage_types TEXT NOT NULL DEFAULT '',
            planning_status INTEGER
        );
        CREATE TABLE community_chat (
            chat_row_id INTEGER PRIMARY KEY,
            last_activity_ts INTEGER, join_ts INTEGER, closed INTEGER, nesting_state INTEGER
        );
        CREATE TABLE parent_group_participants (
            parent_group_jid_row_id INTEGER NOT NULL, user_jid_row_id INTEGER NOT NULL
        );
        CREATE TABLE message_template (
            message_row_id INTEGER PRIMARY KEY,
            content_text_data TEXT, footer_text_data TEXT, template_id TEXT,
            csat_trigger_expiration_ts INTEGER, category TEXT, tag TEXT
        );
        CREATE TABLE message_template_button (
            _id INTEGER PRIMARY KEY,
            message_row_id INTEGER, text_data TEXT, button_type INTEGER, used INTEGER,
            selected_index INTEGER, extra_data TEXT
        );
        """
    )
    yield src
    src.close()


@pytest.fixture()
def populated_source(synthetic_source: sqlite3.Connection) -> sqlite3.Connection:
    """Fill the synthetic source with a small but representative dataset."""
    s = synthetic_source
    # JIDs
    s.executemany(
        "INSERT INTO jid(_id, user, server, raw_string, type) VALUES(?, ?, ?, ?, ?)",
        [
            (1, "120363000000000001", "g.us", "120363000000000001@g.us", 6),
            (2, "100100100100100", "lid", "100100100100100@lid", 18),
            (3, "200200200200200", "lid", "200200200200200@lid", 18),
            (4, "41791234567", "s.whatsapp.net", "41791234567@s.whatsapp.net", 0),
            (5, "41799999999", "s.whatsapp.net", "41799999999@s.whatsapp.net", 0),  # owner
        ],
    )
    s.execute(
        "INSERT INTO chat(_id, jid_row_id, subject, archived, hidden, sort_timestamp) "
        "VALUES(1, 1, 'Test Group', 0, 0, 1700000200000)"
    )
    # jid_map: lid 100... → phone 41791234567
    s.execute("INSERT INTO jid_map(lid_row_id, jid_row_id) VALUES(2, 4)")
    # one display name (masked)
    s.execute(
        "INSERT INTO lid_display_name(lid_row_id, display_name, username) "
        "VALUES(3, '+41∙∙∙∙∙∙∙99', NULL)"
    )
    # messages
    s.executemany(
        "INSERT INTO message(_id, chat_row_id, from_me, key_id, sender_jid_row_id, "
        "                    timestamp, message_type, text_data, starred, status, origin) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)",
        [
            (10, 1, 0, "K1", 2, 1700000100000, 0, "Hello"),
            (11, 1, 0, "K2", 3, 1700000150000, 0, "Reply target"),
            (12, 1, 1, "K3", None, 1700000200000, 0, "From me"),
        ],
    )
    # group membership
    s.executemany(
        "INSERT INTO group_participant_user(group_jid_row_id, user_jid_row_id, rank, pending) "
        "VALUES(?, ?, ?, 0)",
        [(1, 2, 0), (1, 3, 1), (1, 5, 2)],
    )
    # one reaction by lid 200 on K2
    s.execute(
        "INSERT INTO message_add_on(_id, parent_message_row_id, sender_jid_row_id, "
        "                           key_id, timestamp, from_me, message_add_on_type) "
        "VALUES(900, 11, 3, 'R1', 1700000180000, 0, 1)"
    )
    s.execute(
        "INSERT INTO message_add_on_reaction(message_add_on_row_id, reaction, sender_timestamp) "
        "VALUES(900, '👍', 1700000180000)"
    )
    s.commit()
    return s
