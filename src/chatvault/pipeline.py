"""End-to-end extraction pipeline: decrypt → run extractors → record state."""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from . import db as dbmod
from .config import Paths
from .decrypt import decrypt
from .extractors import (
    ExtractorResult,
    bot_messages,
    calls,
    chats,
    communities,
    edits,
    group_members,
    identities,
    messages,
    newsletter,
    polls,
    reactions,
    status_archive,
    status_posts,
    system_events,
    templates,
    transcriptions,
    vcards,
)

log = logging.getLogger(__name__)


# Order matters: identities + chats first (later extractors reference them),
# then messages (inserts the message rows the others extend), then everything else.
ExtractorFn = Callable[[sqlite3.Connection, sqlite3.Connection], ExtractorResult]

DEFAULT_EXTRACTORS: list[tuple[str, ExtractorFn]] = [
    ("identities", identities.extract),
    ("chats", chats.extract),
    ("group_members", group_members.extract),
    ("messages", messages.extract),
    ("system_events", system_events.extract),
    ("reactions", reactions.extract),
    ("edits", edits.extract),
    ("calls", calls.extract),
    ("polls", polls.extract),
    ("status_posts", status_posts.extract),
    ("status_archive_own", status_archive.extract_own),
    ("newsletter", newsletter.extract),
    ("transcriptions", transcriptions.extract),
    ("vcards", vcards.extract),
    ("bot_messages", bot_messages.extract),
    ("templates", templates.extract),
    ("communities", communities.extract),
]


@dataclass(slots=True)
class PipelineSummary:
    duration_s: float
    results: list[ExtractorResult] = field(default_factory=list)
    sender_total: int = 0
    sender_resolved: int = 0
    media_snapshot_files: int | None = None

    @property
    def lines(self) -> list[str]:
        out: list[str] = []
        for r in self.results:
            extras = ""
            if r.notes:
                extras = " — " + "; ".join(r.notes)
            out.append(f"{r.name:<20} written={r.rows_written:<8} skipped={r.rows_skipped}{extras}")
        if self.sender_total:
            pct = 100 * self.sender_resolved / self.sender_total
            out.append(
                f"{'sender resolution':<20} {self.sender_resolved}/{self.sender_total} "
                f"({pct:.1f}%) by name; rest fall back to +phone"
            )
        if self.media_snapshot_files is not None:
            out.append(f"{'media snapshot':<20} +{self.media_snapshot_files} new files mirrored")
        return out


def run_pipeline(
    *,
    paths: Paths,
    encrypted_backup: Path | None = None,
    encrypted_status_backup: Path | None = None,
    skip_decrypt: bool = False,
    keep_decrypted: bool = False,
    extractors: list[tuple[str, ExtractorFn]] | None = None,
    snapshot_media_root: Path | None = None,
) -> PipelineSummary:
    """Run the full extraction pipeline.

    `encrypted_status_backup` is optional. If omitted, looks for
    `status_backup.db.crypt15` next to the encrypted msgstore. If absent,
    the received-status step is skipped silently.
    """
    extractors = extractors or DEFAULT_EXTRACTORS
    paths.ensure()
    plain = paths.staging_dir / "msgstore.db"
    status_plain = paths.staging_dir / "status_backup.db"

    if not skip_decrypt:
        if encrypted_backup is None:
            msg = (
                "No --backup given and --skip-decrypt not set. Specify a .crypt15 file "
                "or use --skip-decrypt with a pre-decrypted DB at staging/msgstore.db."
            )
            raise RuntimeError(msg)
        decrypt(encrypted_backup, key_path=paths.key_path, output=plain, overwrite=True)

        # Locate status_backup.db.crypt15 — explicit arg wins, else look adjacent.
        status_src = encrypted_status_backup
        if status_src is None:
            candidate = encrypted_backup.parent / "status_backup.db.crypt15"
            if candidate.exists():
                status_src = candidate
        if status_src is not None and status_src.exists():
            try:
                decrypt(status_src, key_path=paths.key_path, output=status_plain, overwrite=True)
            except Exception as exc:  # noqa: BLE001
                log.warning("status_backup.db decrypt failed: %s", exc)
                status_plain = None  # type: ignore[assignment]
        else:
            status_plain = None  # type: ignore[assignment]
    elif not plain.exists():
        msg = f"--skip-decrypt set but no plaintext DB at {plain}"
        raise RuntimeError(msg)
    else:
        # skip_decrypt: use whatever plaintext is already in staging/.
        if not status_plain.exists():
            status_plain = None  # type: ignore[assignment]

    started = time.monotonic()

    archive = dbmod.init_db(paths.db_path)
    # Re-apply manual pins from the JSON sidecar (re-init-safe).
    from .contacts import resolution_stats, restore_manual_pins
    restored = restore_manual_pins(archive, paths.config_dir)
    if restored:
        log.info("[pipeline] restored %d manual pins from JSON sidecar", restored)

    source = sqlite3.connect(f"file:{plain}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row

    summary = PipelineSummary(duration_s=0.0)
    try:
        for name, fn in extractors:
            log.info("[pipeline] running %s", name)
            result = fn(source, archive)
            summary.results.append(result)

        if status_plain is not None:
            log.info("[pipeline] running status_archive_received")
            status_conn = sqlite3.connect(f"file:{status_plain}?mode=ro", uri=True)
            status_conn.row_factory = sqlite3.Row
            try:
                from .extractors import status_archive
                summary.results.append(status_archive.extract_received(status_conn, archive))
            finally:
                status_conn.close()

        dbmod.set_state(archive, "last_run_ts", _iso_now())

        stats = resolution_stats(archive)
        summary.sender_total = stats["total"]
        summary.sender_resolved = stats["resolved"]

        if snapshot_media_root is not None and snapshot_media_root.exists():
            log.info("[pipeline] running media snapshot from %s", snapshot_media_root)
            from . import media_mirror
            snap = media_mirror.snapshot_pass(
                archive, media_root=snapshot_media_root, archive_root=paths.media_dir
            )
            summary.media_snapshot_files = snap.new_files
    finally:
        source.close()
        archive.close()
        if not keep_decrypted:
            for p in (plain, status_plain):
                if p is None:
                    continue
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass

    summary.duration_s = time.monotonic() - started
    return summary


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()
