"""Mirror WA media files into the archive, per-chat layout.

Two modes:
    snapshot_pass — one-off rsync-like pass (cron-friendly).
    run_daemon    — long-running, inotify-based watcher (only fires on real changes).

Hardlinks where possible (no extra storage) and copies otherwise. The archive
copy is decoupled from the source path's lifetime.

Layout
------

Every mirrored file lives under one of:

    <archive_root>/<chat_slug>/<basename>      — file's parent message is known
    <archive_root>/_orphans/<basename>         — message has not yet landed

Re-home: every snapshot pass walks `_orphans/` and re-homes any file whose
basename now matches a `message_media.file_path`.

Layout state lives in `_meta.media_layout`. Existing archives are tagged
'flat' on first encounter (their snapshot table already has rows). They must
run `chatvault mirror migrate` before the new code touches them. Fresh
archives are 'per_chat' from the first migration.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import sqlite3
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from . import db as dbmod
from .extractors import now_iso
from .fs import link_or_copy as _link_or_copy
from .media.layout import ORPHANS_DIR, resolve_chat_slug

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
    rehomed_orphans: int = 0


class FlatLayoutBlocked(RuntimeError):
    """Raised when snapshot_pass is called on an archive still tagged 'flat'.

    Callers should surface this with a hint to run `chatvault mirror migrate`.
    """


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


_MODE_TAG = {"link": "ln", "copy": "cp"}


def _log_mirrored(mode: str, size: int, rel: Path) -> None:
    log.info("mirrored %s %dB %s", _MODE_TAG[mode], size, rel)


def _is_skippable(p: Path) -> bool:
    if p.is_dir():
        return True
    if p.suffix.lower() in EXCLUDE_SUFFIXES:
        return True
    return p.name.startswith(".") and p.name not in {".Statuses"}


# ---------------------------------------------------------------------------
# Layout state.
# ---------------------------------------------------------------------------


def get_layout(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = 'media_layout'"
    ).fetchone()
    return row[0] if row else "per_chat"


def set_layout(conn: sqlite3.Connection, layout: str) -> None:
    conn.execute(
        "INSERT INTO _meta(key, value) VALUES('media_layout', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (layout,),
    )


def _require_per_chat(conn: sqlite3.Connection) -> None:
    layout = get_layout(conn)
    if layout != "per_chat":
        msg = (
            f"media_layout = {layout!r}. Run `chatvault mirror migrate` once to "
            "move existing flat-layout files into per-chat folders before the "
            "next snapshot pass."
        )
        raise FlatLayoutBlocked(msg)


# ---------------------------------------------------------------------------
# Routing — look up the destination path for a source file.
# ---------------------------------------------------------------------------


def _lookup_chat_for_basename(
    conn: sqlite3.Connection, basename: str
) -> str | None:
    """Find a chat_jid whose message_media.file_path ends in this basename."""
    row = conn.execute(
        "SELECT m.chat_jid FROM message_media mm "
        "JOIN messages m ON mm.message_id = m.id "
        "WHERE mm.file_path LIKE ? "
        "ORDER BY m.ts DESC LIMIT 1",
        (f"%/{basename}",),
    ).fetchone()
    if row is None:
        # Try exact match too (in case file_path stored without a folder prefix).
        row = conn.execute(
            "SELECT m.chat_jid FROM message_media mm "
            "JOIN messages m ON mm.message_id = m.id "
            "WHERE mm.file_path = ? "
            "ORDER BY m.ts DESC LIMIT 1",
            (basename,),
        ).fetchone()
    return row[0] if row else None


def _destination(
    conn: sqlite3.Connection, archive_root: Path, source_basename: str
) -> tuple[Path, str | None]:
    """Return (absolute_destination_path, chat_jid_or_None).

    Files whose owning message is known land at ``<archive_root>/<slug>/<base>``.
    Unclaimed files go to ``<archive_root>/_orphans/<base>``.
    """
    chat_jid = _lookup_chat_for_basename(conn, source_basename)
    if chat_jid:
        slug = resolve_chat_slug(conn, chat_jid)
        return archive_root / slug / source_basename, chat_jid
    return archive_root / ORPHANS_DIR / source_basename, None


def _record_media_mirror(
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
            str(src),
            str(dst),
            stat.st_size,
            _file_sha256(dst),
            now_iso(),
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)),
            int(is_status),
            int(is_view_once),
        ),
    )


def _set_message_media_mirrored(
    conn: sqlite3.Connection,
    basename: str,
    chat_jid: str | None,
    relative_archive_path: str,
) -> None:
    """Update ``message_media.mirrored_path`` for every row whose basename matches."""
    if chat_jid is None:
        # An orphan: nothing to link yet.
        return
    conn.execute(
        "UPDATE message_media SET mirrored_path = ? "
        "WHERE (file_path LIKE ? OR file_path = ?) "
        "  AND EXISTS (SELECT 1 FROM messages m "
        "              WHERE m.id = message_media.message_id AND m.chat_jid = ?)",
        (relative_archive_path, f"%/{basename}", basename, chat_jid),
    )


# ---------------------------------------------------------------------------
# Snapshot pass.
# ---------------------------------------------------------------------------


def _iter_source_files(media_root: Path) -> Iterable[tuple[Path, bool]]:
    """Yield (source_path, is_status) for every mirrorable file."""
    for sub in SUBDIRS:
        sub_root = media_root / sub
        if not sub_root.exists():
            continue
        is_status = sub == ".Statuses"
        for path in sub_root.rglob("*"):
            if _is_skippable(path):
                continue
            yield path, is_status


def _mirror_one(
    conn: sqlite3.Connection,
    *,
    src: Path,
    archive_root: Path,
    is_status: bool,
) -> tuple[bool, int]:
    """Mirror a single source file. Returns (was_new, bytes_written)."""
    dst, chat_jid = _destination(conn, archive_root, src.name)
    mode = _link_or_copy(src, dst)
    if mode == "noop":
        return False, 0
    _record_media_mirror(conn, src, dst, is_status=is_status, is_view_once=False)
    _set_message_media_mirrored(
        conn,
        src.name,
        chat_jid,
        str(dst.relative_to(archive_root)),
    )
    size = src.stat().st_size
    _log_mirrored(mode, size, src.relative_to(src.parent.parent))
    return True, size


def _rehome_orphans(conn: sqlite3.Connection, archive_root: Path) -> int:
    """Re-home any orphan file whose owning message has since arrived.

    Walks every row in ``media_mirror`` whose ``archive_path`` is under
    ``_orphans/``; if a matching ``message_media`` row now exists for that
    basename, the file is renamed into the chat's slug folder and both
    ``media_mirror.archive_path`` and ``message_media.mirrored_path`` are
    updated.
    """
    rehomed = 0
    orphan_prefix = str(archive_root / ORPHANS_DIR) + "/"
    rows = conn.execute(
        "SELECT source_path, archive_path FROM media_mirror WHERE archive_path LIKE ?",
        (orphan_prefix + "%",),
    ).fetchall()
    for r in rows:
        archive_p = Path(r[1])
        basename = archive_p.name
        chat_jid = _lookup_chat_for_basename(conn, basename)
        if not chat_jid:
            continue
        slug = resolve_chat_slug(conn, chat_jid)
        new_dst = archive_root / slug / basename
        new_dst.parent.mkdir(parents=True, exist_ok=True)
        if not archive_p.exists():
            # Orphan file vanished — update bookkeeping but skip rename.
            log.warning("orphan file gone before re-home: %s", archive_p)
            continue
        try:
            archive_p.rename(new_dst)
        except OSError as exc:
            log.warning("re-home failed for %s -> %s: %s", archive_p, new_dst, exc)
            continue
        conn.execute(
            "UPDATE media_mirror SET archive_path = ? WHERE source_path = ?",
            (str(new_dst), r[0]),
        )
        _set_message_media_mirrored(
            conn, basename, chat_jid, str(new_dst.relative_to(archive_root))
        )
        rehomed += 1
    if rehomed:
        log.info("re-homed %d orphans into per-chat folders", rehomed)
    return rehomed


def snapshot_pass(
    conn: sqlite3.Connection, *, media_root: Path, archive_root: Path
) -> SnapshotResult:
    """Walk the media root and mirror anything new into the archive."""
    _require_per_chat(conn)
    archive_root.mkdir(parents=True, exist_ok=True)
    res = SnapshotResult(new_files=0, bytes=0, skipped=0)

    known = {row[0] for row in conn.execute("SELECT source_path FROM media_mirror")}

    for src, is_status in _iter_source_files(media_root):
        src_str = str(src)
        if src_str in known:
            continue
        try:
            was_new, size = _mirror_one(
                conn, src=src, archive_root=archive_root, is_status=is_status
            )
            if was_new:
                res.new_files += 1
                res.bytes += size
        except OSError as exc:
            log.warning("mirror failed for %s: %s", src, exc)
            res.skipped += 1
    conn.commit()

    res.rehomed_orphans = _rehome_orphans(conn, archive_root)
    conn.commit()
    return res


# ---------------------------------------------------------------------------
# Migration: flat → per_chat for existing archives.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class MigrationResult:
    moved: int
    orphaned: int
    skipped_missing: int


def migrate_flat_to_per_chat(
    conn: sqlite3.Connection, archive_root: Path
) -> MigrationResult:
    """Move every file recorded in ``media_mirror`` from its flat-layout path
    into the per-chat folder layout. Idempotent and crash-safe — each file's
    DB row is updated in the same transaction as its rename.
    """
    result = MigrationResult(moved=0, orphaned=0, skipped_missing=0)
    rows = conn.execute(
        "SELECT source_path, archive_path FROM media_mirror"
    ).fetchall()
    for r in rows:
        src_str, archive_str = r[0], r[1]
        old_path = Path(archive_str)
        basename = old_path.name
        new_path, chat_jid = _destination(conn, archive_root, basename)
        try:
            new_path.relative_to(archive_root)
        except ValueError:  # pragma: no cover — defensive
            continue
        if new_path == old_path:
            continue
        if not old_path.exists():
            result.skipped_missing += 1
            continue
        new_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            old_path.rename(new_path)
        except OSError as exc:
            log.warning("migrate rename failed %s -> %s: %s", old_path, new_path, exc)
            result.skipped_missing += 1
            continue
        conn.execute(
            "UPDATE media_mirror SET archive_path = ? WHERE source_path = ?",
            (str(new_path), src_str),
        )
        if chat_jid:
            _set_message_media_mirrored(
                conn, basename, chat_jid, str(new_path.relative_to(archive_root))
            )
            result.moved += 1
        else:
            result.orphaned += 1
    set_layout(conn, "per_chat")
    conn.commit()
    return result


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
        "inotifywait",
        "-m",
        "-r",
        "--format",
        "%w%f|%e",
        "-e",
        "close_write",
        "-e",
        "moved_to",
        "-e",
        "create",
        str(media_root),
    ]
    log.info("media mirror watching %s", media_root)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    assert proc.stdout is not None

    conn = dbmod.connect(db_path)
    try:
        # Initial pass to catch up on anything missed.
        snapshot_pass(conn, media_root=media_root, archive_root=archive_root)

        for raw_line in proc.stdout:
            line = raw_line.strip()
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
            try:
                _mirror_one(
                    conn, src=path, archive_root=archive_root, is_status=is_status
                )
                conn.commit()
            except OSError as exc:
                log.warning("mirror failed for %s: %s", path, exc)
    finally:
        proc.terminate()
        conn.close()
