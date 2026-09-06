-- chatvault schema v4
-- Multi-source support: tag every chat/message with the source app it came from.
-- Existing rows are backfilled to 'whatsapp' (the only source until now).
-- Downstream tables (message_media, reactions, edits, whisper_transcriptions,
-- image_descriptions, group_members, …) inherit `source` via their FK to
-- messages/chats — no need to duplicate the column there.

PRAGMA user_version = 4;

ALTER TABLE chats    ADD COLUMN source TEXT NOT NULL DEFAULT 'whatsapp';
ALTER TABLE messages ADD COLUMN source TEXT NOT NULL DEFAULT 'whatsapp';

CREATE INDEX IF NOT EXISTS ix_chats_source    ON chats(source);
CREATE INDEX IF NOT EXISTS ix_messages_source ON messages(source);
