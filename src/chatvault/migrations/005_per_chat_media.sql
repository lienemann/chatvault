-- chatvault schema v5
-- Per-chat media layout. Files mirrored under <chat_slug>/<basename>; files
-- without a known parent message live in <archive_root>/_orphans/ until a
-- matching message_media row lands. All bookkeeping reuses the v1
-- media_mirror table and message_media.mirrored_path — no new tables.
--
-- Layout state lives in `_meta.media_layout`:
--   - existing archives (any rows already in media_mirror) inherit 'flat'
--     and must run `chatvault mirror migrate` before the snapshot pass will
--     touch their media tree;
--   - fresh archives start at 'per_chat' so the first snapshot writes the
--     new layout directly.

PRAGMA user_version = 5;

INSERT OR IGNORE INTO _meta(key, value)
SELECT 'media_layout',
       CASE WHEN EXISTS (SELECT 1 FROM media_mirror LIMIT 1)
            THEN 'flat'
            ELSE 'per_chat'
       END;
