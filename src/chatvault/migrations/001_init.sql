-- chatvault schema v1
-- This is OUR schema, intentionally decoupled from the source app's tables.
-- Every domain has stable primary keys, optional raw_json escape hatches for
-- fields we don't yet model, and append-on-change history tables for slow-
-- changing state. Migrations are forward-only; never edit a released file.

PRAGMA application_id = 0x63767401;  -- 'cvt' + version-byte 1
PRAGMA user_version = 1;

-- ---------------------------------------------------------------------------
-- _meta: simple key/value for schema-level facts (version, init time, ...)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- ---------------------------------------------------------------------------
-- extraction_state: last_message_rowid, last_run_ts, etc. One key per fact.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS extraction_state (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ===========================================================================
-- Identity layer
-- ===========================================================================

-- Every JID we have ever seen. JID is the stable identifier across all data.
-- kind is one of: 'user', 'lid', 'group', 'newsletter', 'broadcast', 'status',
-- 'community', 'other'.
CREATE TABLE IF NOT EXISTS identities (
    jid           TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    user_part     TEXT,         -- the part before '@', for searching
    server_part   TEXT,         -- the part after '@'
    country_code  TEXT,         -- from jid_user_metadata, when known
    first_seen_ts TEXT,
    last_seen_ts  TEXT,
    raw_json      TEXT
);
CREATE INDEX IF NOT EXISTS ix_identities_kind ON identities(kind);

-- Cumulative LID -> phone JID mapping. Once seen, never forgotten.
-- The current state. History in identity_links_history.
CREATE TABLE IF NOT EXISTS identity_links (
    lid_jid           TEXT PRIMARY KEY,
    phone_jid         TEXT NOT NULL,
    first_observed_ts TEXT NOT NULL,
    last_observed_ts  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_identity_links_phone ON identity_links(phone_jid);

CREATE TABLE IF NOT EXISTS identity_links_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lid_jid     TEXT NOT NULL,
    phone_jid   TEXT,                              -- null on op='remove'
    op          TEXT NOT NULL CHECK (op IN ('set', 'remove')),
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_identity_links_history_lid ON identity_links_history(lid_jid);

-- Display names that the source app derived for LIDs (often masked phone like
-- '+41∙∙∙∙∙∙∙04', sometimes a real push-name). Useful as a fallback hint.
CREATE TABLE IF NOT EXISTS identity_display_names (
    jid          TEXT PRIMARY KEY,
    display_name TEXT,
    username     TEXT,
    observed_at  TEXT NOT NULL
);

-- Address-book contacts (synced from termux-contact-list or similar).
-- Phone-JID format only.
CREATE TABLE IF NOT EXISTS contacts (
    phone_jid  TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    source     TEXT NOT NULL DEFAULT 'address_book',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contacts_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    phone_jid   TEXT NOT NULL,
    name        TEXT,                              -- null on op='remove'
    source      TEXT NOT NULL DEFAULT 'address_book',
    op          TEXT NOT NULL CHECK (op IN ('set', 'remove')),
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_contacts_history_jid ON contacts_history(phone_jid);

-- ===========================================================================
-- Chats
-- ===========================================================================

-- Current chat metadata. One row per chat JID.
CREATE TABLE IF NOT EXISTS chats (
    jid                TEXT PRIMARY KEY,
    kind               TEXT NOT NULL,         -- group / user / newsletter / broadcast / status / community
    subject            TEXT,
    created_ts         TEXT,
    archived           INTEGER NOT NULL DEFAULT 0,
    hidden             INTEGER NOT NULL DEFAULT 0,
    locked             INTEGER NOT NULL DEFAULT 0,
    pinned             INTEGER NOT NULL DEFAULT 0,
    muted_until_ts     TEXT,
    ephemeral_seconds  INTEGER,                -- disappearing-message duration
    group_type         INTEGER,                -- group/community/parent
    group_member_count INTEGER,
    is_contact         INTEGER,
    last_message_ts    TEXT,
    last_seen_ts       TEXT,
    raw_json           TEXT
);

-- Append-only history of chat metadata snapshots taken at each extraction run.
-- One row per (jid, observed_at). Snapshot stored as full JSON.
CREATE TABLE IF NOT EXISTS chats_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    jid           TEXT NOT NULL,
    observed_at   TEXT NOT NULL,
    snapshot_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_chats_history_jid ON chats_history(jid);

-- Newsletter metadata when the chat is a newsletter/channel.
CREATE TABLE IF NOT EXISTS newsletter_metadata (
    chat_jid          TEXT PRIMARY KEY,
    name              TEXT,
    description       TEXT,
    handle            TEXT,
    picture_url       TEXT,
    preview_url       TEXT,
    invite_code       TEXT,
    subscribers_count INTEGER,
    verified          INTEGER,
    suspended         INTEGER,
    deleted           INTEGER,
    privacy           INTEGER,
    membership        INTEGER,
    observed_at       TEXT NOT NULL,
    raw_json          TEXT
);

-- ===========================================================================
-- Group memberships
-- ===========================================================================

-- Current state of who is in which group.
CREATE TABLE IF NOT EXISTS chat_members (
    chat_jid       TEXT NOT NULL,
    member_jid     TEXT NOT NULL,
    rank           INTEGER,                       -- 0=member, 1=admin, 2=superadmin
    pending        INTEGER NOT NULL DEFAULT 0,
    joined_ts      TEXT,
    join_method    INTEGER,
    label          TEXT,
    PRIMARY KEY (chat_jid, member_jid)
);
CREATE INDEX IF NOT EXISTS ix_chat_members_member ON chat_members(member_jid);

-- Append-only history of memberships.
-- op: 'join' | 'leave' | 'role_change' (role_change carries old_rank/new_rank)
CREATE TABLE IF NOT EXISTS chat_members_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_jid    TEXT NOT NULL,
    member_jid  TEXT NOT NULL,
    op          TEXT NOT NULL CHECK (op IN ('join', 'leave', 'role_change')),
    rank        INTEGER,
    old_rank    INTEGER,
    observed_at TEXT NOT NULL,
    source      TEXT NOT NULL                    -- 'snapshot' | 'past_participant' | 'system_event'
);
CREATE INDEX IF NOT EXISTS ix_chat_members_history_chat ON chat_members_history(chat_jid);
CREATE INDEX IF NOT EXISTS ix_chat_members_history_member ON chat_members_history(member_jid);

-- ===========================================================================
-- Messages — the core domain.
-- ===========================================================================

-- One row per content message. System messages live in system_events.
-- id format: '{chat_jid}:{from_me}:{key_id}'  — stable across re-extracts.
CREATE TABLE IF NOT EXISTS messages (
    id            TEXT PRIMARY KEY,
    source_rowid  INTEGER NOT NULL,
    chat_jid      TEXT NOT NULL,
    sender_jid    TEXT,                            -- null for self-from-1on1 oddities; resolve via from_me
    from_me       INTEGER NOT NULL,
    ts            TEXT NOT NULL,
    ts_received   TEXT,
    type          TEXT NOT NULL,                   -- 'text', 'image', 'audio', ... (our label, not source-int)
    type_raw      INTEGER NOT NULL,
    text          TEXT,                            -- main text body (or caption)
    key_id        TEXT NOT NULL,
    status        INTEGER,
    origin        INTEGER,
    starred       INTEGER NOT NULL DEFAULT 0,
    raw_json      TEXT
);
CREATE INDEX IF NOT EXISTS ix_messages_chat_ts     ON messages(chat_jid, ts);
CREATE INDEX IF NOT EXISTS ix_messages_sender_ts   ON messages(sender_jid, ts);
CREATE INDEX IF NOT EXISTS ix_messages_key_id      ON messages(key_id);
CREATE INDEX IF NOT EXISTS ix_messages_type        ON messages(type);
CREATE INDEX IF NOT EXISTS ix_messages_source_rid  ON messages(source_rowid);

-- Optional 1:1 extensions on a message.
CREATE TABLE IF NOT EXISTS message_media (
    message_id    TEXT PRIMARY KEY REFERENCES messages(id),
    file_path     TEXT,                            -- relative path under WA Media root
    file_size     INTEGER,
    mime          TEXT,
    file_hash     TEXT,                            -- WA-internal base64 hash, NOT sha256
    caption       TEXT,
    duration_s    INTEGER,
    width         INTEGER,
    height        INTEGER,
    name          TEXT,
    mirrored_path TEXT                             -- set by media_mirror once captured
);

CREATE TABLE IF NOT EXISTS message_quoted (
    message_id          TEXT PRIMARY KEY REFERENCES messages(id),
    quoted_key_id       TEXT,
    quoted_text         TEXT,
    quoted_message_type INTEGER,
    quoted_sender_jid   TEXT,
    quoted_message_id   TEXT                       -- if we can resolve to a known messages.id, fill it
);

CREATE TABLE IF NOT EXISTS message_link_previews (
    message_id  TEXT PRIMARY KEY REFERENCES messages(id),
    url         TEXT NOT NULL,
    title       TEXT,
    description TEXT
);

CREATE TABLE IF NOT EXISTS message_mentions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      TEXT NOT NULL REFERENCES messages(id),
    mentioned_jid   TEXT NOT NULL,
    display_name    TEXT
);
CREATE INDEX IF NOT EXISTS ix_mentions_message ON message_mentions(message_id);
CREATE INDEX IF NOT EXISTS ix_mentions_jid     ON message_mentions(mentioned_jid);

CREATE TABLE IF NOT EXISTS message_forwarded (
    message_id     TEXT PRIMARY KEY REFERENCES messages(id),
    forward_score  INTEGER,
    forward_origin INTEGER
);

CREATE TABLE IF NOT EXISTS message_revoked (
    message_id      TEXT PRIMARY KEY REFERENCES messages(id),
    revoked_key_id  TEXT NOT NULL,
    admin_jid       TEXT,
    revoke_ts       TEXT
);

CREATE TABLE IF NOT EXISTS message_albums (
    message_id              TEXT PRIMARY KEY REFERENCES messages(id),
    image_count             INTEGER,
    video_count             INTEGER,
    expected_image_count    INTEGER,
    expected_video_count    INTEGER
);

CREATE TABLE IF NOT EXISTS message_view_once (
    message_id TEXT PRIMARY KEY REFERENCES messages(id),
    state      INTEGER NOT NULL                    -- 0=intact, 1=opened, 2=expired (source-defined)
);

CREATE TABLE IF NOT EXISTS message_ephemeral (
    message_id              TEXT PRIMARY KEY REFERENCES messages(id),
    expire_seconds          INTEGER,
    ephemeral_setting_ts    TEXT
);

CREATE TABLE IF NOT EXISTS message_locations (
    message_id            TEXT PRIMARY KEY REFERENCES messages(id),
    latitude              REAL,
    longitude             REAL,
    place_name            TEXT,
    place_address         TEXT,
    url                   TEXT,
    live_share_duration_s INTEGER,
    live_final_lat        REAL,
    live_final_lng        REAL,
    live_final_ts         TEXT
);

CREATE TABLE IF NOT EXISTS message_vcards (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id   TEXT NOT NULL REFERENCES messages(id),
    contact_jid  TEXT,                             -- the JID of the person whose vcard was shared
    vcard        TEXT
);
CREATE INDEX IF NOT EXISTS ix_vcards_message ON message_vcards(message_id);

CREATE TABLE IF NOT EXISTS message_audio (
    message_id              TEXT PRIMARY KEY REFERENCES messages(id),
    transcription_status    INTEGER,
    transcription_locale    INTEGER,
    transcription_id        TEXT,
    has_waveform            INTEGER NOT NULL DEFAULT 0
);

-- Voice-note transcription segments. The substring is into messages.text;
-- we materialise the slice for convenience but keep the raw offsets too.
CREATE TABLE IF NOT EXISTS message_transcription_segments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      TEXT NOT NULL REFERENCES messages(id),
    seq             INTEGER NOT NULL,              -- 0-based ordering
    text            TEXT,                          -- materialised slice for convenience
    substring_start INTEGER,
    substring_length INTEGER,
    seg_ts          INTEGER,
    duration_ms     INTEGER,
    confidence      INTEGER
);
CREATE INDEX IF NOT EXISTS ix_transcr_seg_message ON message_transcription_segments(message_id);

-- ===========================================================================
-- System events — reconstructed from message_system_* sub-tables.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS system_events (
    message_id        TEXT PRIMARY KEY REFERENCES messages(id),
    action_type       INTEGER NOT NULL,
    kind              TEXT NOT NULL,           -- 'group_chat_participant', 'group_subject_change', 'phone_change', etc.
    body              TEXT,                    -- human-readable reconstructed sentence
    actor_jid         TEXT,                    -- typical "Anna added Bob" → actor=Anna
    affected_jids     TEXT,                    -- JSON array of affected JIDs
    old_value         TEXT,
    new_value         TEXT,
    raw_json          TEXT
);
CREATE INDEX IF NOT EXISTS ix_system_events_kind ON system_events(kind);

-- ===========================================================================
-- Reactions and edits (deduplicated, cumulative).
-- ===========================================================================

CREATE TABLE IF NOT EXISTS reactions (
    parent_message_id TEXT NOT NULL,                -- '{parent_chat_jid}:?:{parent_key_id}'
    parent_chat_jid   TEXT NOT NULL,
    parent_key_id     TEXT NOT NULL,
    reaction_key_id   TEXT NOT NULL,
    sender_jid        TEXT,
    sender_from_me    INTEGER,
    emoji             TEXT,
    sender_ts         TEXT,
    observed_ts       TEXT NOT NULL,                -- first time we saw this reaction
    PRIMARY KEY (parent_chat_jid, parent_key_id, reaction_key_id)
);
CREATE INDEX IF NOT EXISTS ix_reactions_parent ON reactions(parent_chat_jid, parent_key_id);
CREATE INDEX IF NOT EXISTS ix_reactions_sender ON reactions(sender_jid);

CREATE TABLE IF NOT EXISTS edits (
    message_id        TEXT NOT NULL REFERENCES messages(id),
    edited_ts         TEXT NOT NULL,
    sender_ts         TEXT,
    original_key_id   TEXT,
    original_ts       TEXT,
    observed_ts       TEXT NOT NULL,
    PRIMARY KEY (message_id, edited_ts)
);
CREATE INDEX IF NOT EXISTS ix_edits_message ON edits(message_id);

-- ===========================================================================
-- Polls
-- ===========================================================================

CREATE TABLE IF NOT EXISTS polls (
    message_id          TEXT PRIMARY KEY REFERENCES messages(id),
    selectable_count    INTEGER,
    poll_type           INTEGER,
    content_type        INTEGER,
    end_time_ts         TEXT,
    allow_add_option    INTEGER,
    hide_participants   INTEGER,
    invalid             INTEGER,
    raw_json            TEXT
);

CREATE TABLE IF NOT EXISTS poll_options (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id   TEXT NOT NULL REFERENCES messages(id),
    option_index INTEGER NOT NULL,
    option_name  TEXT,
    vote_total   INTEGER
);
CREATE INDEX IF NOT EXISTS ix_poll_options_msg ON poll_options(message_id);

CREATE TABLE IF NOT EXISTS poll_votes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_message_id   TEXT NOT NULL,
    voter_jid           TEXT,
    sender_ts           TEXT,
    selected_indexes    TEXT NOT NULL                   -- JSON array of option_index
);
CREATE INDEX IF NOT EXISTS ix_poll_votes_parent ON poll_votes(parent_message_id);

-- ===========================================================================
-- Calls
-- ===========================================================================

CREATE TABLE IF NOT EXISTS calls (
    call_id           TEXT PRIMARY KEY,
    source_rowid      INTEGER NOT NULL,
    peer_jid          TEXT,
    group_jid         TEXT,
    creator_jid       TEXT,
    from_me           INTEGER,
    ts                TEXT,
    video             INTEGER,
    duration_s        INTEGER,
    result            INTEGER,                          -- raw call_result int from source
    bytes_transferred INTEGER,
    is_dnd_mode_on    INTEGER,
    call_type         INTEGER,
    scheduled_id      TEXT,
    raw_json          TEXT
);
CREATE INDEX IF NOT EXISTS ix_calls_peer ON calls(peer_jid);
CREATE INDEX IF NOT EXISTS ix_calls_ts   ON calls(ts);

CREATE TABLE IF NOT EXISTS call_participants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id         TEXT NOT NULL REFERENCES calls(call_id),
    participant_jid TEXT,
    result          INTEGER
);
CREATE INDEX IF NOT EXISTS ix_call_participants_call ON call_participants(call_id);

-- ===========================================================================
-- Status posts metadata
-- ===========================================================================

CREATE TABLE IF NOT EXISTS status_posts (
    message_id              TEXT PRIMARY KEY REFERENCES messages(id),
    poster_jid              TEXT,
    distribution_mode       INTEGER,
    audience_type           INTEGER,
    can_be_reshared         INTEGER,
    has_embedded_music      INTEGER,
    is_mentioned            INTEGER,
    mention_jids            TEXT,                  -- JSON array
    poster_status_id        TEXT,
    raw_json                TEXT
);

-- 24h status archive: full text + media + (for own posts) viewer receipts.
-- Mixed source: own from msgstore.message, received from status_backup.db.
-- 'received' rows are inherently ephemeral — only the current backup window.

CREATE TABLE IF NOT EXISTS status_archive (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('own', 'received')),
    sender_jid      TEXT,
    chat_jid        TEXT,
    type            TEXT,
    type_raw        INTEGER,
    ts              TEXT,
    received_ts     TEXT,
    text            TEXT,
    audience_type   INTEGER,
    is_archived     INTEGER,
    uuid            TEXT,
    message_id      TEXT REFERENCES messages(id)
);
CREATE INDEX IF NOT EXISTS ix_status_archive_sender ON status_archive(sender_jid);
CREATE INDEX IF NOT EXISTS ix_status_archive_ts     ON status_archive(ts);
CREATE INDEX IF NOT EXISTS ix_status_archive_kind   ON status_archive(kind);

CREATE TABLE IF NOT EXISTS status_archive_media (
    status_id           TEXT PRIMARY KEY REFERENCES status_archive(id),
    mime                TEXT,
    width               INTEGER,
    height              INTEGER,
    duration_s          INTEGER,
    file_size           INTEGER,
    file_path           TEXT,
    media_url           TEXT,
    direct_path         TEXT,
    media_key           BLOB,
    file_hash           BLOB,
    enc_file_hash       BLOB,
    accessibility_label TEXT,
    media_name          TEXT
);

CREATE TABLE IF NOT EXISTS status_archive_thumbnails (
    status_id              TEXT PRIMARY KEY REFERENCES status_archive(id),
    thumbnail              BLOB,
    thumbnail_path         TEXT,
    highres_thumbnail_path TEXT
);

CREATE TABLE IF NOT EXISTS status_archive_views (
    status_id    TEXT NOT NULL REFERENCES status_archive(id),
    viewer_jid   TEXT NOT NULL,
    received_ts  TEXT,
    read_ts      TEXT,
    played_ts    TEXT,
    PRIMARY KEY (status_id, viewer_jid)
);
CREATE INDEX IF NOT EXISTS ix_status_archive_views_viewer ON status_archive_views(viewer_jid);

-- ===========================================================================
-- Newsletter messages
-- ===========================================================================

CREATE TABLE IF NOT EXISTS newsletter_messages (
    message_id          TEXT PRIMARY KEY REFERENCES messages(id),
    server_message_id   INTEGER,
    view_count          INTEGER,
    forwards_count      INTEGER,
    comments_count      INTEGER,
    is_paid_partnership INTEGER,
    raw_json            TEXT
);

CREATE TABLE IF NOT EXISTS newsletter_message_reactions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id   TEXT NOT NULL REFERENCES messages(id),
    emoji        TEXT NOT NULL,
    count        INTEGER NOT NULL DEFAULT 0,
    UNIQUE (message_id, emoji)
);

-- ===========================================================================
-- Bot / AI / template / interactive
-- ===========================================================================

CREATE TABLE IF NOT EXISTS bot_messages (
    message_id TEXT PRIMARY KEY REFERENCES messages(id),
    raw_json   TEXT
);

CREATE TABLE IF NOT EXISTS ai_threads (
    thread_id INTEGER PRIMARY KEY,
    raw_json  TEXT
);

CREATE TABLE IF NOT EXISTS template_messages (
    message_id TEXT PRIMARY KEY REFERENCES messages(id),
    raw_json   TEXT
);

-- ===========================================================================
-- Communities
-- ===========================================================================

CREATE TABLE IF NOT EXISTS communities (
    parent_jid  TEXT PRIMARY KEY,
    raw_json    TEXT
);

-- ===========================================================================
-- Media mirror — local copies of files referenced by messages, hardlinked
-- from the source app's media directory into the archive.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS media_mirror (
    source_path        TEXT PRIMARY KEY,         -- absolute path on the host
    archive_path       TEXT NOT NULL,            -- absolute path under our archive
    file_size          INTEGER,
    file_hash          TEXT,                     -- chatvault sha256 (may differ from source's WA hash)
    mirrored_at        TEXT NOT NULL,
    source_modified_ts TEXT,
    is_status          INTEGER NOT NULL DEFAULT 0,
    is_view_once       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_media_mirror_archive ON media_mirror(archive_path);
CREATE INDEX IF NOT EXISTS ix_media_mirror_hash    ON media_mirror(file_hash);

-- ---------------------------------------------------------------------------
-- Mark schema as initialised.
-- ---------------------------------------------------------------------------
INSERT OR IGNORE INTO _meta(key, value) VALUES
    ('schema_version', '1'),
    ('initialised_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
