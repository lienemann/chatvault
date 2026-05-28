"""Unit tests for the local whisper.cpp transcription pipeline.

External tools (ffmpeg, whisper-cli) are not invoked — every code path that
shells out is replaced with a fake. The point is to verify the SQL, the
digest join, and the candidate-discovery filter.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from chatvault import whisper as wh
from chatvault.config import Paths
from chatvault.db import apply_pending_migrations, connect
from chatvault.exports.digest import render_digest, render_digest_jsonl


@pytest.fixture
def paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Paths:
    monkeypatch.setenv("CHATVAULT_HOME", str(tmp_path))
    p = Paths.default()
    p.ensure()
    return p


@pytest.fixture
def archive(paths: Paths) -> sqlite3.Connection:
    conn = connect(paths.db_path)
    apply_pending_migrations(conn)
    yield conn
    conn.close()


def _seed_audio_message(
    conn: sqlite3.Connection,
    *,
    mid: str,
    chat_jid: str,
    ts: str,
    file_rel: str,
    mirrored: str | None,
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO chats(jid, kind, subject) VALUES(?, 'user', ?)",
        (chat_jid, "John"),
    )
    conn.execute(
        "INSERT INTO messages(id, source_rowid, chat_jid, sender_jid, from_me, ts, "
        "                     type, type_raw, text, key_id) "
        "VALUES(?, 1, ?, ?, 0, ?, 'audio', 2, NULL, 'K')",
        (mid, chat_jid, chat_jid, ts),
    )
    conn.execute(
        "INSERT INTO message_media(message_id, file_path, mime, duration_s, mirrored_path) "
        "VALUES(?, ?, 'audio/ogg; codecs=opus', 7, ?)",
        (mid, file_rel, mirrored),
    )
    conn.commit()


def test_iter_audio_candidates_skips_already_transcribed(archive: sqlite3.Connection) -> None:
    _seed_audio_message(
        archive,
        mid="m1",
        chat_jid="a@s.whatsapp.net",
        ts="2026-01-01T00:00:00Z",
        file_rel="Media/WhatsApp Voice Notes/x.opus",
        mirrored=None,
    )
    _seed_audio_message(
        archive,
        mid="m2",
        chat_jid="a@s.whatsapp.net",
        ts="2026-01-02T00:00:00Z",
        file_rel="Media/WhatsApp Voice Notes/y.opus",
        mirrored=None,
    )
    archive.execute(
        "INSERT INTO whisper_transcriptions(message_id, source_path, text, language, "
        "                                   model, duration_s, transcribed_at) "
        "VALUES('m1', '/tmp/x.wav', 'old text', 'de', 'ggml-small.bin', 7, "
        "'2026-01-03T00:00:00Z')"
    )

    ids = [r.message_id for r in wh.iter_audio_candidates(archive)]
    assert ids == ["m2"]

    ids_force = [r.message_id for r in wh.iter_audio_candidates(archive, force=True)]
    assert set(ids_force) == {"m1", "m2"}


def test_resolve_audio_path_prefers_mirror(tmp_path: Path, paths: Paths) -> None:
    mirror_file = paths.media_dir / "WhatsApp Voice Notes" / "x.opus"
    mirror_file.parent.mkdir(parents=True, exist_ok=True)
    mirror_file.write_bytes(b"\x00")
    row = wh.AudioRow(
        message_id="m1",
        chat_jid="c",
        ts="2026-01-01T00:00:00Z",
        file_path="Media/WhatsApp Voice Notes/x.opus",
        mirrored_path=None,
        mime="audio/ogg",
        duration_s=1,
    )
    found = wh.resolve_audio_path(row, paths, live_root=None)
    assert found == mirror_file


def test_strip_timestamps_removes_brackets() -> None:
    raw = "[00:00:00.000 --> 00:00:02.500]  Hallo,\n[00:00:02.500 --> 00:00:04.000]  wie geht's?\n"
    assert wh._strip_timestamps(raw) == "Hallo,\nwie geht's?"


def test_transcribe_all_writes_rows_and_joins_into_digest(
    monkeypatch: pytest.MonkeyPatch,
    archive: sqlite3.Connection,
    paths: Paths,
) -> None:
    # Snapshot a fake audio file into the mirror so resolve_audio_path() finds it.
    mirror_file = paths.media_dir / "WhatsApp Voice Notes" / "abc.opus"
    mirror_file.parent.mkdir(parents=True, exist_ok=True)
    mirror_file.write_bytes(b"\x00")
    _seed_audio_message(
        archive,
        mid="abc",
        chat_jid="a@s.whatsapp.net",
        ts="2026-01-01T00:00:00Z",
        file_rel="Media/WhatsApp Voice Notes/abc.opus",
        mirrored=None,
    )

    # Pretend both tools are present and stub out the real subprocess work.
    monkeypatch.setattr(
        wh,
        "detect_tools",
        lambda: wh.ToolStatus(ffmpeg="/bin/true", whisper_cli="/bin/true"),
    )
    monkeypatch.setattr(
        wh,
        "whisper_cli_version",
        lambda _p: "fake-1.0",
    )

    def fake_decode(ffmpeg: str, src: Path, dst: Path) -> None:
        dst.write_bytes(b"RIFF")

    def fake_batch(whisper_cli: str, wavs, **kw):
        return ["Hallo, das ist ein Test." for _ in wavs]

    monkeypatch.setattr(wh, "_decode_to_wav", fake_decode)
    monkeypatch.setattr(wh, "_run_whisper_batch", fake_batch)

    model = paths.state_dir / "whisper" / "ggml-small.bin"
    model.parent.mkdir(parents=True, exist_ok=True)
    model.write_bytes(b"\x00")

    result = wh.transcribe_all(archive, paths, model=model, backend="cli")
    assert result.candidates == 1
    assert result.transcribed == 1

    row = archive.execute(
        "SELECT message_id, text, model, tool_version FROM whisper_transcriptions"
    ).fetchone()
    assert row["message_id"] == "abc"
    assert row["text"] == "Hallo, das ist ein Test."
    assert row["model"] == "ggml-small.bin"
    assert row["tool_version"] == "fake-1.0"

    md = render_digest(archive, "a@s.whatsapp.net", last=10)
    assert "Hallo, das ist ein Test." in md
    assert "transcript" in md

    jsonl = render_digest_jsonl(archive, "a@s.whatsapp.net", last=10)
    assert "Hallo, das ist ein Test." in jsonl
    assert "transcription" in jsonl
