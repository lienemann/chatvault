-- chatvault schema v3
-- Vision-derived descriptions of image media (chat screenshots forwarded into
-- the chat, social/profile screenshots, plain photos/memes).
-- Populated by `chatvault describe-images` via the Anthropic vision API.

PRAGMA user_version = 3;

CREATE TABLE IF NOT EXISTS image_descriptions (
    message_id   TEXT PRIMARY KEY REFERENCES messages(id),
    image_path   TEXT NOT NULL,            -- absolute path of the image we read
    kind         TEXT NOT NULL,            -- 'chat_screenshot' | 'social_profile' | 'image'
    summary      TEXT NOT NULL,            -- 2-4 line digest-friendly summary
    raw_json     TEXT NOT NULL,            -- full structured response from the model
    model        TEXT,                     -- model alias used (sonnet|opus|…)
    described_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_image_descriptions_kind ON image_descriptions(kind);
