# Schema

This document is a hand-curated companion to the migration files
(`src/chatvault/migrations/`). When you add a new column or table, update both.

## Conventions

- **JIDs are TEXT and `@`-suffixed.** Phone JIDs end `@s.whatsapp.net`,
  pseudonymous IDs end `@lid`, groups `@g.us`, channels `@newsletter`,
  status feeds `@status`, broadcasts `@broadcast`.
- **Timestamps are ISO-8601 in UTC** (`2026-04-30T12:34:56.789+00:00`). Any
  column ending `_ts` is ISO-8601, never Unix-ms — we convert at the boundary.
- **Stable message ids** are `'{chat_jid}:{from_me}:{key_id}'`. They survive
  re-extraction.
- **Append-on-change history.** Slowly-changing state (contacts, group
  memberships, identity mappings, chat metadata) is mirrored into a `*_history`
  table where every row is a single observation. Current state is the projection.
- **`raw_json`** columns are escape hatches for fields we don't yet model. We
  promote fields out of `raw_json` into proper columns in later migrations.

## Domains

### Identity layer (`identities`, `identity_links`, `identity_display_names`, `contacts`)

Every JID we have ever seen lives in `identities`. LID↔phone mappings live in
`identity_links` (cumulative — once seen, always remembered). Real names come
from `contacts` (synced from the device address book) and, as a fallback, from
`identity_display_names` (the source app's own per-LID display strings).

### Chats (`chats`, `chats_history`, `newsletter_metadata`)

`chats` is the current snapshot per JID. `chats_history` is one snapshot row
per extraction run. Newsletter (channel) metadata lives in its own table
because newsletters have many attributes that don't apply to other chat kinds.

### Group memberships (`chat_members`, `chat_members_history`)

`chat_members` is the current member set per group (rows for non-group chats
just don't exist). `chat_members_history` records joins, leaves, and role
changes — derived from snapshots, from `group_past_participant_user`, and
(future) from `system_events`.

### Messages (`messages` + per-feature side tables)

`messages` is the central table. Messages of type `'system'` exist as stub
rows in `messages` and have their detail in `system_events`.

Side tables are 1:1 (or 1:N for mentions/vcards/transcription_segments) and
key on `message_id`:

- `message_media` — file path, mime, hash, dimensions
- `message_quoted` — the message this one replies to
- `message_link_previews` — URL + title + description
- `message_mentions` — `@`-mentions (1:N)
- `message_forwarded` — forward score / origin
- `message_revoked` — tombstone for deleted messages
- `message_albums` — multi-photo group metadata
- `message_view_once` — view-once state
- `message_ephemeral` — disappearing-message attributes
- `message_locations` — lat/lng + place + live-share
- `message_vcards` — shared contacts (1:N)
- `message_audio` — voice-note metadata
- `message_transcription_segments` — voice-note transcript slices

### Reactions and edits

Both are deduplicated cumulatively. `reactions` is keyed on
`(parent_chat_jid, parent_key_id, reaction_key_id)`; `edits` on
`(message_id, edited_ts)`. `INSERT OR IGNORE` preserves the original
`observed_ts` (when we first saw the row).

### Polls (`polls`, `poll_options`, `poll_votes`)

One poll per message. Options are ordered by `option_index`. Votes link
back to the parent message id and carry a JSON array of selected indexes.

### Calls (`calls`, `call_participants`)

Group-call participants live in `call_participants`. The source's
`call_log_participant_v2` is the only authoritative list — we mirror it.

### Status posts (`status_posts`)

The metadata side. Media bytes are captured by the `media_mirror` daemon
(see architecture).

### Newsletter messages (`newsletter_messages`, `newsletter_message_reactions`)

Per-message stats (view count, forwards, comments) and per-emoji aggregate
reactions.

### System events (`system_events`)

Reconstructed from `message_system_*` sub-tables. `kind` is our own label
(e.g. `'group_chat_participant_added'`), `body` is a short human-readable
sentence with JIDs (the resolver inflates them at query time).

### Media mirror (`media_mirror`)

Ledger of files we've copied. `source_path` is the original location on the
host; `archive_path` is our copy.

### Extraction state (`extraction_state`)

Simple key/value. The most important key is `last_message_rowid` — the
checkpoint that makes message extraction incremental.

## Future migrations

Add a new file `src/chatvault/migrations/002_*.sql` with the new DDL. The
`db.py` runner picks up the new version, applies it, and updates
`_meta.schema_version`. Never edit a released migration.
