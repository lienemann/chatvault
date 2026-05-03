"""Mirror WA media files into the archive.

Two modes:
    snapshot_pass — one-off rsync-like pass (cron-friendly).
    run_daemon    — long-running, inotify-based watcher (only fires on real changes).

Hardlinks where possible (no extra storage) and copies otherwise. The archive
copy is decoupled from the source path's lifetime.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from . import db as dbmod
from .extractors import now_iso

log = logging.getLogger(__name__)


# Subdirectories under the source media root that we mirror. Order matters
# only insofar as `.Statuses` is the most time-sensitive.
SUBDIRS = [
    ".Statuses",
    "WhatsApp Images",
    "WhatsApp Video",
    "WhatsApp Audio",
    "WhatsApp Voice Notes",
    "WhatsApp Animated Gifs",
    "WhatsApp Stickers",
    "WhatsApp Documents",
    "WhatsApp Profile Photos",
    "WhatsApp Video Notes",
]

EXCLUDE_SUFFIXES = {".nomedia", ".tmp"}


@dataclass(slots=True)
class SnapshotResult:
    new_files: int
    bytes: int
    skipped: int


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _archive_path(archive_root: Path, source: Path, source_root: Path) -> Path:
    rel = source.relative_to(source_root)
    return archive_root / rel


def _link_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _record(
    conn: sqlite3.Connection, src: Path, dst: Path, *, is_status: bool, is_view_once: bool
) -> None:
    stat = src.stat()
    conn.execute(
        "INSERT INTO media_mirror(source_path, archive_path, file_size, file_hash, "
        "                         mirrored_at, source_modified_ts, is_status, is_view_once) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(source_path) DO UPDATE SET "
        "  archive_path = excluded.archive_path, "
        "  file_size    = excluded.file_size, "
        "  file_hash    = excluded.file_hash, "
        "  mirrored_at  = excluded.mirrored_at, "
        "  source_modified_ts = excluded.source_modified_ts",
        (
            str(src), str(dst), stat.st_size, _file_sha256(dst),
            now_iso(),
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)),
            int(is_status), int(is_view_once),
        ),
    )


def _is_skippable(p: Path) -> bool:
    if p.is_dir():
        return True
    if p.suffix.lower() in EXCLUDE_SUFFIXES:
        return True
    return p.name.startswith(".") and p.name not in {".Statuses"}


def snapshot_pass(
    conn: sqlite3.Connection, *, media_root: Path, archive_root: Path
) -> SnapshotResult:
    """Walk the media root and mirror anything new into the archive."""
    archive_root.mkdir(parents=True, exist_ok=True)
    res = SnapshotResult(new_files=0, bytes=0, skipped=0)

    known = {row[0] for row in conn.execute("SELECT source_path FROM media_mirror")}

    for sub in SUBDIRS:
        sub_root = media_root / sub
        if not sub_root.exists():
            continue
        is_status = sub == ".Statuses"
        for path in sub_root.rglob("*"):
            if _is_skippable(path):
                res.skipped += 1
                continue
            src_str = str(path)
            if src_str in known:
                continue
            dst = _archive_path(archive_root, path, media_root)
            try:
                _link_or_copy(path, dst)
                _record(conn, path, dst, is_status=is_status, is_view_once=False)
                res.new_files += 1
                res.bytes += path.stat().st_size
            except OSError as exc:
                log.warning("mirror failed for %s: %s", path, exc)
                res.skipped += 1
        conn.commit()

    return res


# ---------------------------------------------------------------------------
# Daemon mode
# ---------------------------------------------------------------------------


def run_daemon(*, db_path: Path, media_root: Path, archive_root: Path) -> None:
    """Foreground inotify-based mirror. Requires `inotifywait` on PATH (inotify-tools)."""
    if shutil.which("inotifywait") is None:
        log.error("inotifywait not on PATH. Install with `pkg install inotify-tools`.")
        sys.exit(2)

    archive_root.mkdir(parents=True, exist_ok=True)

    cmd = [
        "inotifywait", "-m", "-r",
        "--format", "%w%f|%e",
        "-e", "close_write",
        "-e", "moved_to",
        "-e", "create",
        str(media_root),
    ]
    log.info("media mirror watching %s", media_root)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    assert proc.stdout is not None

    conn = dbmod.connect(db_path)
    try:
        # Initial pass to catch up on anything missed.
        snapshot_pass(conn, media_root=media_root, archive_root=archive_root)

        for line in proc.stdout:
            line = line.strip()
            if "|" not in line:
                continue
            path_str, _events = line.split("|", 1)
            path = Path(path_str)
            if not path.exists():
                continue
            if _is_skippable(path):
                continue
            try:
                rel_parts = path.relative_to(media_root).parts
            except ValueError:
                continue
            if not rel_parts or rel_parts[0] not in SUBDIRS:
                continue
            is_status = rel_parts[0] == ".Statuses"
            dst = _archive_path(archive_root, path, media_root)
            try:
                _link_or_copy(path, dst)
                _record(conn, path, dst, is_status=is_status, is_view_once=False)
                conn.commit()
                log.debug("mirrored %s", path)
            except OSError as exc:
                log.warning("mirror failed for %s: %s", path, exc)
    finally:
        proc.terminate()
        conn.close()
