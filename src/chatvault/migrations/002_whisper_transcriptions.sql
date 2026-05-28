-- chatvault schema v2
-- Local whisper.cpp transcriptions of snapshotted voice notes / audio messages.
-- Distinct from `message_transcription_segments`, which mirrors the source
-- app's own on-device transcriptions.

PRAGMA user_version = 2;

CREATE TABLE IF NOT EXISTS whisper_transcriptions (
    message_id     TEXT PRIMARY KEY REFERENCES messages(id),
    source_path    TEXT NOT NULL,            -- absolute path of the audio file we read
    text           TEXT NOT NULL,
    language       TEXT,                     -- ISO 639-1 code reported by whisper-cli, or 'auto'
    model          TEXT,                     -- basename of the ggml model used
    duration_s     REAL,                     -- audio duration as probed by ffmpeg
    transcribed_at TEXT NOT NULL,
    tool_version   TEXT                      -- whisper-cli version string, if available
);
CREATE INDEX IF NOT EXISTS ix_whisper_transcriptions_lang ON whisper_transcriptions(language);
