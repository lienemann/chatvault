"""Local transcription of snapshotted audio messages with whisper.cpp.

Uses two external tools, both expected on PATH:

    ffmpeg       — decodes the source file (typically .opus) into the 16 kHz
                   mono PCM WAV that whisper.cpp wants.
    whisper-cli  — the whisper.cpp inference binary. Reads the WAV and writes
                   a plain-text transcript to ``<basename>.txt``.

The module never reaches the network and keeps every artefact under the
chatvault state dir.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Paths
from .db import transaction

ProgressCb = Callable[["AudioRow", "Path | None", str], None]

log = logging.getLogger(__name__)


_TIMESTAMP_RE = re.compile(
    r"^\s*\[\s*\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?\s*-->\s*"
    r"\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?\s*\]\s*"
)


def _strip_timestamps(text: str) -> str:
    """Remove ``[00:00:00.000 --> 00:00:02.500]`` style prefixes from each line."""
    out: list[str] = []
    for line in text.splitlines():
        cleaned = _TIMESTAMP_RE.sub("", line).rstrip()
        if cleaned:
            out.append(cleaned)
    return "\n".join(out)


# The source app stores media under a "Media/<subdir>/<file>" relative path.
# We strip the leading "Media/" when mapping into the chatvault mirror, which
# is rooted at <data_dir>/media/.
_MEDIA_PREFIX = "Media/"


@dataclass(slots=True)
class ToolStatus:
    ffmpeg: str | None
    whisper_cli: str | None
    pywhispercpp: bool = False

    def ok(self, backend: str) -> bool:
        if backend == "cli":
            return bool(self.ffmpeg and self.whisper_cli)
        if backend == "pywhispercpp":
            return bool(self.ffmpeg and self.pywhispercpp)
        return False

    def missing(self, backend: str) -> list[str]:
        out: list[str] = []
        if not self.ffmpeg:
            out.append("ffmpeg")
        if backend == "cli" and not self.whisper_cli:
            out.append("whisper-cli")
        if backend == "pywhispercpp" and not self.pywhispercpp:
            out.append("pywhispercpp (pip install pywhispercpp)")
        return out


@dataclass(slots=True)
class TranscribeResult:
    candidates: int = 0
    transcribed: int = 0
    skipped_missing: int = 0
    failed: int = 0


@dataclass(slots=True)
class AudioRow:
    message_id: str
    chat_jid: str
    ts: str
    file_path: str | None
    mirrored_path: str | None
    mime: str | None
    duration_s: int | None


# User-prefix bin dirs that we probe in addition to PATH, so a whisper-cli
# dropped into ~/.local/bin (or ~/bin) is found even when the user hasn't
# extended their shell PATH.
_FALLBACK_BIN_DIRS: tuple[Path, ...] = (
    Path.home() / ".local" / "bin",
    Path.home() / "bin",
)


def _find_on_path_or_fallback(name: str) -> str | None:
    """``shutil.which`` plus a couple of well-known user-prefix bin dirs."""
    found = shutil.which(name)
    if found:
        return found
    for d in _FALLBACK_BIN_DIRS:
        candidate = d / name
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def detect_tools() -> ToolStatus:
    """Probe PATH (+ user-prefix bin dirs) for ffmpeg/whisper-cli and pywhispercpp."""
    try:
        import importlib.util as _il

        has_pwc = _il.find_spec("pywhispercpp") is not None
    except ImportError:  # pragma: no cover - importlib.util is stdlib
        has_pwc = False
    return ToolStatus(
        ffmpeg=_find_on_path_or_fallback("ffmpeg"),
        whisper_cli=_find_on_path_or_fallback("whisper-cli"),
        pywhispercpp=has_pwc,
    )


def _subprocess_env_for(tool: str) -> dict[str, str] | None:
    """Return an env dict that adds ``<prefix>/lib`` to ``LD_LIBRARY_PATH``.

    whisper.cpp builds usually ship as ``<prefix>/bin/whisper-cli`` +
    ``<prefix>/lib/libwhisper.so*``. If the user's prefix isn't on the system
    linker path (e.g. ``~/.local``), the binary refuses to start with
    ``CANNOT LINK EXECUTABLE … libwhisper.so.1 not found``. We patch the env
    for child processes so the user doesn't have to fiddle with shellrc.
    """
    tool_path = Path(tool)
    if tool_path.parent.name != "bin":
        return None
    lib_dir = tool_path.parent.parent / "lib"
    if not lib_dir.exists():
        return None
    if not any(lib_dir.glob("libwhisper.so*")):
        return None
    env = os.environ.copy()
    existing = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = f"{lib_dir}{os.pathsep}{existing}" if existing else str(lib_dir)
    return env


def resolve_audio_path(row: AudioRow, paths: Paths, live_root: Path | None) -> Path | None:
    """Find the on-disk audio file. Mirror copy wins; falls back to live source."""
    if row.mirrored_path:
        p = Path(row.mirrored_path)
        if p.exists():
            return p
    rel = row.file_path
    if rel:
        # Mirror layout strips the leading "Media/" segment.
        mirror_rel = rel[len(_MEDIA_PREFIX) :] if rel.startswith(_MEDIA_PREFIX) else rel
        mirror = paths.media_dir / mirror_rel
        if mirror.exists():
            return mirror
        if live_root is not None:
            live = live_root / mirror_rel
            if live.exists():
                return live
    return None


def _build_query(
    chat_jid: str | None, *, force: bool, limit: int | None
) -> tuple[str, list[object]]:
    where = [
        "(m.type IN ('audio', 'ptt') OR mm.mime LIKE 'audio/%')",
        "(mm.file_path IS NOT NULL OR mm.mirrored_path IS NOT NULL)",
    ]
    params: list[object] = []
    if chat_jid is not None:
        where.append("m.chat_jid = ?")
        params.append(chat_jid)
    if not force:
        where.append(
            "NOT EXISTS (SELECT 1 FROM whisper_transcriptions w WHERE w.message_id = m.id)"
        )
    sql = (
        "SELECT m.id AS message_id, m.chat_jid, m.ts, "
        "       mm.file_path, mm.mirrored_path, mm.mime, mm.duration_s "
        "FROM messages m JOIN message_media mm ON mm.message_id = m.id "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY m.ts DESC"
    )
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return sql, params


def iter_audio_candidates(
    conn: sqlite3.Connection,
    *,
    chat_jid: str | None = None,
    force: bool = False,
    limit: int | None = None,
) -> Iterator[AudioRow]:
    sql, params = _build_query(chat_jid, force=force, limit=limit)
    for r in conn.execute(sql, params):
        yield AudioRow(
            message_id=r["message_id"],
            chat_jid=r["chat_jid"],
            ts=r["ts"],
            file_path=r["file_path"],
            mirrored_path=r["mirrored_path"],
            mime=r["mime"],
            duration_s=r["duration_s"],
        )


def whisper_cli_version(whisper_cli: str) -> str | None:
    """Best-effort `--version` probe. Returns None when unsupported."""
    try:
        out = subprocess.run(
            [whisper_cli, "--version"],
            check=False,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=5,
            env=_subprocess_env_for(whisper_cli),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    text = (out.stdout or out.stderr or "").strip()
    return text.splitlines()[0] if text else None


def _decode_to_wav(ffmpeg: str, src: Path, dst: Path) -> None:
    """16 kHz mono PCM WAV — the only format whisper.cpp accepts directly."""
    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(src),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-f",
        "wav",
        str(dst),
    ]
    res = subprocess.run(cmd, check=False, capture_output=True, text=True, errors="replace")
    if res.returncode != 0:
        msg = f"ffmpeg failed for {src}: {res.stderr.strip()[:200]}"
        raise RuntimeError(msg)


def _run_whisper_batch(
    whisper_cli: str,
    wavs: list[Path],
    *,
    model: Path,
    language: str,
    threads: int | None,
    extra_args: list[str] | None = None,
) -> list[str]:
    """One whisper-cli invocation, many ``-f`` inputs. Returns transcripts in input order.

    whisper.cpp loads the model once per process — batching keeps that cost
    amortised across the whole run instead of paying it per voice note.
    Each input wav at ``<dir>/<name>.wav`` produces ``<dir>/<name>.wav.txt``.
    """
    if not wavs:
        return []
    cmd = [whisper_cli, "-m", str(model), "-otxt", "-nt", "-l", language]
    if threads is not None:
        cmd.extend(["-t", str(threads)])
    if extra_args:
        cmd.extend(extra_args)
    for wav in wavs:
        cmd.extend(["-f", str(wav)])
    res = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        errors="replace",
        env=_subprocess_env_for(whisper_cli),
    )
    if res.returncode != 0:
        msg = f"whisper-cli failed: {res.stderr.strip()[:200]}"
        raise RuntimeError(msg)

    out: list[str] = []
    for wav in wavs:
        # whisper-cli writes "<wav>.txt" next to the input (not stripping .wav).
        candidates = [Path(str(wav) + ".txt"), wav.with_suffix(".txt")]
        txt_path = next((c for c in candidates if c.exists()), None)
        if txt_path is None:
            msg = f"whisper-cli produced no output for {wav}"
            raise RuntimeError(msg)
        out.append(
            _strip_timestamps(txt_path.read_text(encoding="utf-8", errors="replace")).strip()
        )
    return out


def _run_whisper(
    whisper_cli: str,
    wav: Path,
    *,
    model: Path,
    language: str,
    threads: int | None,
    extra_args: list[str] | None = None,
) -> str:
    """Single-file convenience wrapper around :func:`_run_whisper_batch`."""
    return _run_whisper_batch(
        whisper_cli,
        [wav],
        model=model,
        language=language,
        threads=threads,
        extra_args=extra_args,
    )[0]


def _load_pywhispercpp_model(model: Path, *, language: str, threads: int | None) -> Any:
    """Construct a pywhispercpp ``Model`` once so the weights are loaded one time only."""
    from pywhispercpp.model import Model

    kwargs: dict[str, object] = {"print_realtime": False, "print_progress": False}
    if language and language != "auto":
        kwargs["language"] = language
    if threads is not None:
        kwargs["n_threads"] = threads
    return Model(str(model), **kwargs)


def _transcribe_with_pwc_model(model_obj: Any, wav: Path) -> str:
    segments = model_obj.transcribe(str(wav))
    text = "\n".join(getattr(s, "text", "").strip() for s in segments)
    return _strip_timestamps(text).strip()


def transcribe_file(
    src: Path,
    *,
    model: Path,
    ffmpeg: str,
    whisper_cli: str | None = None,
    backend: str = "cli",
    language: str = "auto",
    threads: int | None = None,
    extra_args: list[str] | None = None,
    workdir: Path | None = None,
) -> str:
    """Transcribe a single audio file. Returns the transcript text.

    For batch use prefer :func:`transcribe_all`, which avoids reloading the
    model for every voice note.
    """
    if backend not in ("cli", "pywhispercpp"):
        msg = f"unknown backend: {backend!r}"
        raise ValueError(msg)
    if not src.exists():
        msg = f"audio file not found: {src}"
        raise FileNotFoundError(msg)
    if not model.exists():
        msg = f"whisper model not found: {model}"
        raise FileNotFoundError(msg)
    if backend == "cli" and not whisper_cli:
        msg = "backend='cli' requires whisper_cli path"
        raise ValueError(msg)

    ctx: tempfile.TemporaryDirectory[str] | None = None
    if workdir is None:
        ctx = tempfile.TemporaryDirectory(prefix="chatvault-whisper-")
        workdir = Path(ctx.name)
    try:
        wav = workdir / "audio.wav"
        _decode_to_wav(ffmpeg, src, wav)
        if backend == "pywhispercpp":
            m = _load_pywhispercpp_model(model, language=language, threads=threads)
            return _transcribe_with_pwc_model(m, wav)
        assert whisper_cli is not None
        return _run_whisper(
            whisper_cli,
            wav,
            model=model,
            language=language,
            threads=threads,
            extra_args=extra_args,
        )
    finally:
        if ctx is not None:
            ctx.cleanup()


DEFAULT_MODEL_FILENAME = "ggml-small.bin"


def default_model_dir() -> Path:
    """Shared whisper model directory under ``$XDG_DATA_HOME/whisper``."""
    raw = os.environ.get("XDG_DATA_HOME")
    base = Path(raw).expanduser() if raw else Path.home() / ".local" / "share"
    return base / "whisper"


def default_model_path(paths: Paths) -> Path:
    """Resolve the whisper model location.

    Order: ``$CHATVAULT_WHISPER_MODEL`` → ``$XDG_DATA_HOME/whisper/ggml-small.bin``
    (i.e. ``~/.local/share/whisper/ggml-small.bin``).
    Small (multilingual) is a sensible default for German/English voice notes
    on phone-class hardware; bump to ``medium`` for tougher audio.
    """
    env = os.environ.get("CHATVAULT_WHISPER_MODEL")
    if env:
        return Path(env).expanduser()
    return default_model_dir() / DEFAULT_MODEL_FILENAME


def _store(
    conn: sqlite3.Connection,
    *,
    row: AudioRow,
    src: Path,
    text: str,
    language: str,
    model: Path,
    tool_version: str | None,
) -> None:
    conn.execute(
        "INSERT INTO whisper_transcriptions(message_id, source_path, text, language, "
        "                                   model, duration_s, transcribed_at, tool_version) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(message_id) DO UPDATE SET "
        "  source_path    = excluded.source_path, "
        "  text           = excluded.text, "
        "  language       = excluded.language, "
        "  model          = excluded.model, "
        "  duration_s     = excluded.duration_s, "
        "  transcribed_at = excluded.transcribed_at, "
        "  tool_version   = excluded.tool_version",
        (
            row.message_id,
            str(src),
            text,
            language,
            model.name,
            float(row.duration_s) if row.duration_s is not None else None,
            datetime.now(tz=UTC).isoformat(),
            tool_version,
        ),
    )


def _chunked(items: list[Any], size: int) -> Iterator[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def transcribe_all(
    conn: sqlite3.Connection,
    paths: Paths,
    *,
    model: Path,
    backend: str = "cli",
    language: str = "auto",
    chat_jid: str | None = None,
    force: bool = False,
    limit: int | None = None,
    threads: int | None = None,
    batch_size: int = 16,
    live_media_root: Path | None = None,
    extra_args: list[str] | None = None,
    progress: ProgressCb | None = None,
) -> TranscribeResult:
    """Transcribe every snapshotted audio message that doesn't yet have a transcript.

    The model is loaded once per backend invocation:

      • ``cli``         — files are batched ``batch_size`` per ``whisper-cli`` run.
      • ``pywhispercpp`` — a single in-process ``Model`` is reused for every file.

    Existing rows are kept untouched unless ``force=True``.
    """
    if not model.exists():
        msg = f"whisper model not found: {model}"
        raise FileNotFoundError(msg)

    tools = detect_tools()
    if not tools.ok(backend):
        msg = f"missing tools for backend={backend!r}: {', '.join(tools.missing(backend))}"
        raise RuntimeError(msg)
    assert tools.ffmpeg  # narrow for type-checkers

    tool_version = (
        whisper_cli_version(tools.whisper_cli)
        if backend == "cli" and tools.whisper_cli
        else f"pywhispercpp ({backend})"
    )
    result = TranscribeResult()

    candidates = list(iter_audio_candidates(conn, chat_jid=chat_jid, force=force, limit=limit))
    result.candidates = len(candidates)

    # Resolve filesystem paths up front so missing-file rows don't break batching.
    resolved: list[tuple[AudioRow, Path]] = []
    for row in candidates:
        src = resolve_audio_path(row, paths, live_media_root)
        if src is None:
            result.skipped_missing += 1
            if progress:
                progress(row, None, "missing")
            continue
        resolved.append((row, src))

    if not resolved:
        return result

    if backend == "pywhispercpp":
        pwc_model = _load_pywhispercpp_model(model, language=language, threads=threads)
        for row, src in resolved:
            try:
                with tempfile.TemporaryDirectory(prefix="chatvault-whisper-") as tmp:
                    wav = Path(tmp) / "audio.wav"
                    _decode_to_wav(tools.ffmpeg, src, wav)
                    text = _transcribe_with_pwc_model(pwc_model, wav)
            except (RuntimeError, FileNotFoundError) as exc:
                log.warning("transcribe failed for %s: %s", row.message_id, exc)
                result.failed += 1
                if progress:
                    progress(row, src, f"failed: {exc}")
                continue
            with transaction(conn):
                _store(
                    conn,
                    row=row,
                    src=src,
                    text=text,
                    language=language,
                    model=model,
                    tool_version=tool_version,
                )
            result.transcribed += 1
            if progress:
                progress(row, src, "ok")
        return result

    # backend == "cli": batch whisper-cli invocations to amortise model load.
    assert tools.whisper_cli is not None
    for chunk in _chunked(resolved, max(1, batch_size)):
        with tempfile.TemporaryDirectory(prefix="chatvault-whisper-") as tmp:
            tmpdir = Path(tmp)
            wavs: list[Path] = []
            decode_failed: set[str] = set()
            for i, (row, src) in enumerate(chunk):
                wav = tmpdir / f"{i:04d}.wav"
                try:
                    _decode_to_wav(tools.ffmpeg, src, wav)
                except RuntimeError as exc:
                    log.warning("ffmpeg failed for %s: %s", row.message_id, exc)
                    result.failed += 1
                    decode_failed.add(row.message_id)
                    if progress:
                        progress(row, src, f"failed: {exc}")
                    continue
                wavs.append(wav)
            if not wavs:
                continue
            try:
                texts = _run_whisper_batch(
                    tools.whisper_cli,
                    wavs,
                    model=model,
                    language=language,
                    threads=threads,
                    extra_args=extra_args,
                )
            except RuntimeError as exc:
                # Whole batch failed: count every still-pending row.
                for row, src in chunk:
                    if row.message_id in decode_failed:
                        continue
                    log.warning("transcribe failed for %s: %s", row.message_id, exc)
                    result.failed += 1
                    if progress:
                        progress(row, src, f"failed: {exc}")
                continue

            pending = [(r, s) for r, s in chunk if r.message_id not in decode_failed]
            with transaction(conn):
                for (row, src), text in zip(pending, texts, strict=False):
                    _store(
                        conn,
                        row=row,
                        src=src,
                        text=text,
                        language=language,
                        model=model,
                        tool_version=tool_version,
                    )
                    result.transcribed += 1
                    if progress:
                        progress(row, src, "ok")

    return result
