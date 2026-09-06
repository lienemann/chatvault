"""End-to-end extraction pipeline: decrypt → run extractors → record state."""

from __future__ import annotations

import contextlib
import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC
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
    translations,
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
    ("translations", translations.extract),
    ("vcards", vcards.extract),
    ("bot_messages", bot_messages.extract),
    ("templates", templates.extract),
    ("communities", communities.extract),
]


# Tables whose row-count delta we surface as "new items". These grow between
# extract runs in normal use (chats/group_members rarely change, so we omit
# them to keep the summary focused).
TRACKED_TABLES: list[str] = [
    "messages",
    "reactions",
    "edits",
    "calls",
    "status_posts",
    "status_archive",
    "transcriptions",
]


_PER_CHAT_TOP_N = 8


@dataclass(slots=True)
class PipelineSummary:
    duration_s: float
    results: list[ExtractorResult] = field(default_factory=list)
    sender_total: int = 0
    sender_resolved: int = 0
    media_snapshot_files: int | None = None
    new_rows: dict[str, int] = field(default_factory=dict)
    new_messages_per_chat: list[tuple[str, int]] = field(default_factory=list)

    @property
    def lines(self) -> list[str]:
        out: list[str] = []
        if self.new_rows:
            non_zero = {t: n for t, n in self.new_rows.items() if n > 0}
            if non_zero:
                parts = ", ".join(f"+{n} {t}" for t, n in non_zero.items())
                out.append(f"{'new items':<20} {parts}")
            else:
                out.append(f"{'new items':<20} (none — archive already up to date)")
        if self.new_messages_per_chat:
            shown = self.new_messages_per_chat[:_PER_CHAT_TOP_N]
            extra = len(self.new_messages_per_chat) - len(shown)
            parts = ", ".join(f"+{n} {label}" for label, n in shown)
            tail = f" (and {extra} more chats)" if extra > 0 else ""
            out.append(f"{'by chat':<20} {parts}{tail}")
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


def _count_tables(conn: sqlite3.Connection, tables: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for t in tables:
        try:
            out[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            # Table not present in this schema version — skip silently.
            continue
    return out


def _messages_per_chat(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        r[0]: r[1]
        for r in conn.execute("SELECT chat_jid, COUNT(*) FROM messages GROUP BY chat_jid")
    }


def _resolved_chat_deltas(
    conn: sqlite3.Connection, before: dict[str, int], after: dict[str, int]
) -> list[tuple[str, int]]:
    """Return [(label, delta)] for chats that gained messages, sorted desc by delta."""
    from .identities import NameResolver

    resolver = NameResolver(conn)
    deltas: list[tuple[str, int]] = []
    for jid, post in after.items():
        delta = post - before.get(jid, 0)
        if delta <= 0:
            continue
        row = conn.execute("SELECT subject, kind FROM chats WHERE jid = ?", (jid,)).fetchone()
        if row is None:
            label = jid
        else:
            label = row["subject"] or (
                resolver.resolve(jid) if row["kind"] in ("user", "lid") else jid
            )
        deltas.append((label, delta))
    deltas.sort(key=lambda x: -x[1])
    return deltas


@dataclass(slots=True)
class SignalPipelineSummary:
    duration_s: float
    decrypt_stats: dict[str, int] = field(default_factory=dict)
    extractor: ExtractorResult | None = None
    attachments_mirrored: int = 0
    new_rows: dict[str, int] = field(default_factory=dict)

    @property
    def lines(self) -> list[str]:
        out: list[str] = []
        if self.decrypt_stats:
            parts = ", ".join(f"{k}={v}" for k, v in self.decrypt_stats.items())
            out.append(f"{'decrypt':<20} {parts}")
        if self.extractor is not None:
            r = self.extractor
            extras = " — " + "; ".join(r.notes) if r.notes else ""
            out.append(
                f"{r.name:<20} written={r.rows_written:<8} skipped={r.rows_skipped}{extras}"
            )
        if self.attachments_mirrored:
            out.append(f"{'attachments mirrored':<20} +{self.attachments_mirrored}")
        if self.new_rows:
            non_zero = {t: n for t, n in self.new_rows.items() if n > 0}
            if non_zero:
                parts = ", ".join(f"+{n} {t}" for t, n in non_zero.items())
                out.append(f"{'new items':<20} {parts}")
        return out


def run_signal_pipeline(
    *,
    paths: Paths,
    encrypted_backup: Path | None,
    passphrase: str | None,
    skip_decrypt: bool = False,
    keep_decrypted: bool = False,
    mirror_attachments: bool = True,
    full_scan: bool = False,
) -> SignalPipelineSummary:
    """Decrypt a Signal Android backup and extract into the chatvault archive.

    With ``skip_decrypt`` we look for an already-decrypted bundle at
    ``cache/staging/signal/`` produced by a prior run.
    """
    from .extractors import signal as signal_extractor
    from .sources.signal.decrypt import decrypt_backup

    paths.ensure()
    sig_stage = paths.staging_dir / "signal"
    db_path = sig_stage / "database.sqlite"
    attachments_dir = sig_stage / "attachments"
    key_values_path = sig_stage / "key_value.json"

    decrypt_stats: dict[str, int] = {}
    if not skip_decrypt:
        if encrypted_backup is None or passphrase is None:
            msg = (
                "Signal pipeline needs --backup PATH and --passphrase (or --skip-decrypt "
                "to reuse cache/staging/signal/)."
            )
            raise RuntimeError(msg)
        sig_stage.mkdir(parents=True, exist_ok=True)
        result = decrypt_backup(encrypted_backup, passphrase, sig_stage)
        decrypt_stats = {
            "statements": result.stats.statements,
            "attachments": result.stats.attachments,
            "stickers": result.stats.stickers,
            "avatars": result.stats.avatars,
            "skipped_statements": result.stats.skipped_statements,
        }
    elif not db_path.exists():
        msg = f"--skip-decrypt set but no decrypted Signal DB at {db_path}"
        raise RuntimeError(msg)

    started = time.monotonic()
    archive = dbmod.init_db(paths.db_path)
    before = _count_tables(archive, TRACKED_TABLES)
    try:
        inp = signal_extractor.SignalExtractInput(
            db_path=db_path,
            attachments_dir=attachments_dir,
            key_values_path=key_values_path,
            media_out_dir=paths.media_dir,
        )
        extractor_result = signal_extractor.extract(inp, archive, full_scan=full_scan)

        mirrored = 0
        if mirror_attachments:
            mirrored = signal_extractor.mirror_attachments(
                attachments_dir, paths.media_dir, archive
            )

        dbmod.set_state(archive, "signal_last_run_ts", _iso_now())
        after = _count_tables(archive, TRACKED_TABLES)
        deltas = {t: after.get(t, 0) - before.get(t, 0) for t in after}
    finally:
        archive.close()
        if not keep_decrypted:
            with contextlib.suppress(OSError):
                if db_path.exists():
                    db_path.unlink()

    return SignalPipelineSummary(
        duration_s=time.monotonic() - started,
        decrypt_stats=decrypt_stats,
        extractor=extractor_result,
        attachments_mirrored=mirrored,
        new_rows=deltas,
    )


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
    status_target = paths.staging_dir / "status_backup.db"
    status_plain: Path | None = status_target

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
                decrypt(status_src, key_path=paths.key_path, output=status_target, overwrite=True)
            except Exception as exc:
                log.warning("status_backup.db decrypt failed: %s", exc)
                status_plain = None
        else:
            status_plain = None
    elif not plain.exists():
        msg = f"--skip-decrypt set but no plaintext DB at {plain}"
        raise RuntimeError(msg)
    # skip_decrypt: use whatever plaintext is already in staging/.
    elif not status_target.exists():
        status_plain = None

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
    before_counts = _count_tables(archive, TRACKED_TABLES)
    before_msgs_per_chat = _messages_per_chat(archive)
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

        after_counts = _count_tables(archive, TRACKED_TABLES)
        summary.new_rows = {
            t: after_counts.get(t, 0) - before_counts.get(t, 0) for t in after_counts
        }
        if summary.new_rows.get("messages", 0) > 0:
            after_msgs_per_chat = _messages_per_chat(archive)
            summary.new_messages_per_chat = _resolved_chat_deltas(
                archive, before_msgs_per_chat, after_msgs_per_chat
            )
    finally:
        source.close()
        archive.close()
        if not keep_decrypted:
            for p in (plain, status_plain):
                if p is None:
                    continue
                with contextlib.suppress(FileNotFoundError):
                    p.unlink()

    summary.duration_s = time.monotonic() - started
    return summary


def _iso_now() -> str:
    from datetime import datetime

    return datetime.now(tz=UTC).isoformat()
