"""chatvault command-line interface, built on Typer."""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import Paths, read_key, write_key
from .db import init_db

app = typer.Typer(
    name="chatvault",
    help="Local archive of personal chat history.",
    no_args_is_help=True,
    add_completion=False,
)

# Sub-apps — all singular for consistency: `chat`, `contact`, `link`,
# `key`, `mirror`, `status`. Operations on collections live under their
# entity (e.g. `chat list`).
key_app = typer.Typer(name="key", help="Manage the backup decryption key.", no_args_is_help=True)
contact_app = typer.Typer(name="contact", help="Address-book entries and pins.", no_args_is_help=True)
chat_app = typer.Typer(name="chat", help="Browse and operate on chats (groups + 1:1).", no_args_is_help=True)
mirror_app = typer.Typer(name="mirror", help="Media mirror daemon.", no_args_is_help=True)
link_app = typer.Typer(name="link", help="Link extraction.", no_args_is_help=True)
status_app = typer.Typer(name="status", help="24h status archive (own + received).", no_args_is_help=True)

app.add_typer(key_app)
app.add_typer(contact_app)
app.add_typer(chat_app)
app.add_typer(mirror_app)
app.add_typer(link_app)
app.add_typer(status_app)

console = Console()
err_console = Console(stderr=True)


# ---------------------------------------------------------------------------
# Output helpers — every list-style command supports --format json
# ---------------------------------------------------------------------------


def _emit_json(rows: list[dict]) -> None:
    """Stream rows as JSON Lines to stdout."""
    import json as _json
    for r in rows:
        sys.stdout.write(_json.dumps(r, default=str, ensure_ascii=False) + "\n")


def _check_format(fmt: str) -> None:
    if fmt not in ("table", "json"):
        err_console.print(f"[red]--format must be 'table' or 'json' (got {fmt!r}).[/]")
        raise typer.Exit(code=2)


def _resolve_chat_or_exit(conn: sqlite3.Connection, query: str) -> str:
    """Wrap resolve_chat with a friendly error path (LookupError → Exit 2)."""
    from .queries.chats import resolve_chat
    try:
        return resolve_chat(conn, query)
    except LookupError as exc:
        err_console.print(f"[red]{exc}[/]")
        raise typer.Exit(code=2) from None


# ---------------------------------------------------------------------------
# Globals (very thin — one resolution per invocation)
# ---------------------------------------------------------------------------


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _open_db(paths: Paths, *, read_only: bool = False) -> sqlite3.Connection:
    if read_only:
        if not paths.db_path.exists():
            err_console.print(
                f"[red]No archive at {paths.db_path}. Run `chatvault init` first.[/]"
            )
            raise typer.Exit(code=2)
        from .db import connect

        return connect(paths.db_path, read_only=True)
    # Write paths auto-init if missing — useful for `pin` before first `extract`.
    return init_db(paths.db_path)


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------


@app.callback()
def _main(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Verbose logging")] = False,
) -> None:
    _setup_logging(verbose)


@app.command()
def version() -> None:
    """Print chatvault version."""
    console.print(__version__)


@app.command()
def init() -> None:
    """Create the archive directories, key location, and run migrations."""
    paths = Paths.default()
    paths.ensure()
    conn = init_db(paths.db_path)
    conn.close()
    console.print(f"[green]✓[/] data dir:   {paths.data_dir}")
    console.print(f"[green]✓[/] config dir: {paths.config_dir}")
    console.print(f"[green]✓[/] archive db: {paths.db_path}")
    console.print(f"[green]✓[/] media dir:  {paths.media_dir}")
    if not paths.key_path.exists():
        console.print(
            f"\n[yellow]Next:[/] put your 64-char backup key into [bold]{paths.key_path}[/] "
            "(chmod 600), or use [bold]chatvault key set[/]."
        )


DEFAULT_MEDIA_ROOT = Path(
    "/storage/emulated/0/Android/media/com.whatsapp/WhatsApp/Media"
)


@app.command()
def extract(
    backup: Annotated[
        Optional[Path],
        typer.Option("--backup", help="Encrypted msgstore.db.crypt15. If omitted, uses staging/msgstore.db if present."),
    ] = None,
    status_backup: Annotated[
        Optional[Path],
        typer.Option("--status-backup", help="Encrypted status_backup.db.crypt15. Defaults to a sibling of --backup."),
    ] = None,
    skip_decrypt: Annotated[
        bool, typer.Option("--skip-decrypt", help="Use already-decrypted DBs at staging/")
    ] = False,
    keep_decrypted: Annotated[
        bool, typer.Option("--keep-decrypted", help="Don't delete the staging plaintext DBs after extract")
    ] = False,
    snapshot_media: Annotated[
        bool,
        typer.Option(
            "--snapshot-media/--no-snapshot-media",
            help="After extraction, hardlink-mirror new media files (incl. .Statuses) so they survive WA cleanup.",
        ),
    ] = True,
    media_root: Annotated[
        Path,
        typer.Option("--media-root", help="Source app's Media root (for the snapshot pass)."),
    ] = DEFAULT_MEDIA_ROOT,
) -> None:
    """Decrypt + sync the archive from a source backup."""
    from .pipeline import run_pipeline

    paths = Paths.default()
    paths.ensure()
    snap_root = media_root if snapshot_media else None
    summary = run_pipeline(
        paths=paths,
        encrypted_backup=backup,
        encrypted_status_backup=status_backup,
        skip_decrypt=skip_decrypt,
        keep_decrypted=keep_decrypted,
        snapshot_media_root=snap_root,
    )
    console.print(f"[green]✓[/] extract complete in {summary.duration_s:.1f}s")
    for line in summary.lines:
        console.print(f"  {line}")
    if summary.sender_total and summary.sender_resolved == 0:
        console.print(
            "\n[yellow]Hinweis:[/] Keiner deiner Sender ist namentlich aufgelöst. "
            "Dein Adressbuch ist nicht synchronisiert. Optionen:\n"
            "  • [bold]chatvault contact sync[/]                   (termux-contact-list)\n"
            "  • [bold]chatvault contact import-vcard <vcf>[/]     (vollständig, multi-Nummer)\n"
            "  • [bold]chatvault contact unresolved --csv pins.csv[/]  +  [bold]contact import-csv pins.csv[/]"
        )
    elif summary.sender_total and summary.sender_resolved < summary.sender_total * 0.5:
        miss = summary.sender_total - summary.sender_resolved
        console.print(
            f"\n[yellow]Hinweis:[/] {miss} Sender ohne Namen. "
            "Dump als CSV-Vorlage: [bold]chatvault contact unresolved --csv pins.csv[/]"
        )


# ---------------------------------------------------------------------------
# key
# ---------------------------------------------------------------------------


@key_app.command("set")
def key_set(
    value: Annotated[Optional[str], typer.Argument(help="64-char hex key. If omitted, read from stdin.")] = None,
) -> None:
    """Store the backup decryption key (chmod 600)."""
    paths = Paths.default()
    paths.ensure()
    if value is None:
        if sys.stdin.isatty():
            value = typer.prompt("Backup key (64 hex chars)", hide_input=True)
        else:
            value = sys.stdin.read().strip()
    write_key(paths.key_path, value or "")
    console.print(f"[green]✓[/] key stored at {paths.key_path}")


@key_app.command("path")
def key_path() -> None:
    """Print the path where chatvault expects the key."""
    console.print(str(Paths.default().key_path))


@key_app.command("check")
def key_check() -> None:
    """Verify a key is present and correctly formatted."""
    paths = Paths.default()
    try:
        present = read_key(paths.key_path) is not None
    except ValueError as exc:
        err_console.print(f"[red]Invalid key:[/] {exc}")
        raise typer.Exit(code=1) from None
    if present:
        console.print(f"[green]✓[/] key OK at {paths.key_path}")
    else:
        err_console.print(f"[red]No key at {paths.key_path}[/]")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# contacts
# ---------------------------------------------------------------------------


@contact_app.command("sync")
def contacts_sync(
    default_country: Annotated[
        str, typer.Option("--default-country", help="E.164 country code prefix without '+', for unprefixed numbers.")
    ] = "41",
    from_stdin: Annotated[
        bool, typer.Option("--stdin", help="Read termux-contact-list-style JSON from stdin instead of running it.")
    ] = False,
) -> None:
    """Sync the phone address book into the archive (one-number-per-contact)."""
    from .contacts import sync_contacts

    paths = Paths.default()
    conn = _open_db(paths)
    try:
        result = sync_contacts(conn, default_country=default_country, from_stdin=from_stdin)
    finally:
        conn.close()
    console.print(
        f"[green]✓[/] {result.total} contacts | "
        f"+{result.set_count} updated, -{result.remove_count} removed"
    )


@contact_app.command("pin")
def contacts_pin(
    phone: Annotated[str, typer.Argument(help="Phone number or phone-JID (e.g. '+49 172 3105522' or '491723105522@s.whatsapp.net').")],
    name: Annotated[str, typer.Argument(help="Display name to use for this number.")],
    default_country: Annotated[
        str, typer.Option("--default-country")
    ] = "41",
) -> None:
    """Set a manual name override for a phone JID (wins over sync/vcard imports)."""
    from .contacts import pin_contact

    paths = Paths.default()
    paths.ensure()
    conn = _open_db(paths)
    try:
        try:
            jid = pin_contact(
                conn, phone, name,
                default_country=default_country,
                config_dir=paths.config_dir,
            )
        except ValueError as exc:
            err_console.print(f"[red]{exc}[/]")
            raise typer.Exit(code=2) from None
    finally:
        conn.close()
    console.print(f"[green]✓[/] pinned {jid} → {name}")


@contact_app.command("list")
def contact_list(
    source: Annotated[Optional[str], typer.Option("--source", help="Filter: address_book / vcard / manual.")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 1000,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """List contacts in the archive (optionally filtered by source)."""
    _check_format(fmt)
    from .contacts import list_contacts

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = list_contacts(conn, source=source, limit=limit)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    if not rows:
        err_console.print("[yellow]no contacts.[/]")
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("name")
    table.add_column("source")
    table.add_column("updated", justify="right")
    table.add_column("phone-jid", style="dim", overflow="fold")
    for r in rows:
        table.add_row(
            r["name"],
            r["source"],
            (r["updated_at"] or "")[:19],
            r["phone_jid"],
        )
    console.print(table)
    console.print(f"\n[dim]{len(rows)} entries[/]")


@contact_app.command("unresolved")
def contact_unresolved(
    min_messages: Annotated[int, typer.Option("--min-messages", help="Only show senders with at least N messages.")] = 1,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 200,
    csv_out: Annotated[
        Optional[Path],
        typer.Option("--csv", help="Write a phone,name CSV starter file (importable via `contact import-csv`)."),
    ] = None,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Active sender phones that have NO contact entry — pin candidates."""
    _check_format(fmt)
    from .contacts import pretty_phone, unresolved_senders

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = unresolved_senders(conn, min_messages=min_messages, limit=limit)
    finally:
        conn.close()

    if csv_out:
        import csv as _csv
        with csv_out.open("w", encoding="utf-8", newline="") as f:
            w = _csv.writer(f)
            w.writerow(["phone", "name"])
            for r in rows:
                w.writerow([pretty_phone(r["phone_jid"]), ""])
        console.print(f"[green]✓[/] {csv_out} ({len(rows)} rows). Edit, then `chatvault contact import-csv {csv_out}`.")
        return

    if fmt == "json":
        for r in rows:
            r["phone_pretty"] = pretty_phone(r["phone_jid"])
        _emit_json(rows)
        return
    if not rows:
        console.print("[green]✓[/] no unresolved senders.")
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("phone")
    table.add_column("messages", justify="right")
    table.add_column("last_seen", justify="right")
    table.add_column("phone-jid", style="dim", overflow="fold")
    for r in rows:
        table.add_row(
            pretty_phone(r["phone_jid"]),
            str(r["message_count"]),
            (r["last_seen"] or "")[:19],
            r["phone_jid"],
        )
    console.print(table)
    console.print(f"\n[dim]{len(rows)} unresolved. Use --csv pins.csv to dump for bulk-pinning.[/]")


@contact_app.command("restore-pins")
def contacts_restore_pins() -> None:
    """Re-apply manual pins from the JSON sidecar (auto-runs during `extract`)."""
    from .contacts import restore_manual_pins

    paths = Paths.default()
    paths.ensure()
    conn = _open_db(paths)
    try:
        n = restore_manual_pins(conn, paths.config_dir)
    finally:
        conn.close()
    console.print(f"[green]✓[/] restored {n} manual pins")


@contact_app.command("import-csv")
def contacts_import_csv(
    path: Annotated[Path, typer.Argument(help="CSV file with 'phone' and 'name' columns.")],
    default_country: Annotated[
        str, typer.Option("--default-country")
    ] = "41",
) -> None:
    """Bulk pin contacts from CSV (each row → source='manual')."""
    from .contacts import import_pins_csv

    paths = Paths.default()
    conn = _open_db(paths)
    try:
        try:
            result = import_pins_csv(conn, path, default_country=default_country)
        except (ValueError, FileNotFoundError) as exc:
            err_console.print(f"[red]{exc}[/]")
            raise typer.Exit(code=2) from None
    finally:
        conn.close()
    console.print(
        f"[green]✓[/] {result.total} rows | +{result.set_count} pinned, "
        f"-{len(result.skipped)} skipped"
    )
    for line_no, raw, reason in result.skipped[:20]:
        err_console.print(f"  [yellow]line {line_no}[/] {raw!r}: {reason}")
    if len(result.skipped) > 20:
        err_console.print(f"  … and {len(result.skipped) - 20} more")


@contact_app.command("unpin")
def contacts_unpin(
    phone: Annotated[str, typer.Argument(help="Phone number or phone-JID.")],
    default_country: Annotated[
        str, typer.Option("--default-country")
    ] = "41",
) -> None:
    """Remove a manual override (does not affect address-book / vcard rows)."""
    from .contacts import unpin_contact

    paths = Paths.default()
    paths.ensure()
    conn = _open_db(paths)
    try:
        try:
            removed = unpin_contact(
                conn, phone,
                default_country=default_country,
                config_dir=paths.config_dir,
            )
        except ValueError as exc:
            err_console.print(f"[red]{exc}[/]")
            raise typer.Exit(code=2) from None
    finally:
        conn.close()
    if removed:
        console.print(f"[green]✓[/] unpinned {phone}")
    else:
        console.print(f"[yellow]no manual override for {phone}[/]")


@contact_app.command("import-vcard")
def contacts_import_vcard(
    path: Annotated[Path, typer.Argument(help="A .vcf file or a directory of .vcf files.")],
    default_country: Annotated[
        str, typer.Option("--default-country")
    ] = "41",
    source_label: Annotated[
        str,
        typer.Option(
            "--source",
            help="Label stored in contacts.source for these entries.",
        ),
    ] = "vcard",
) -> None:
    """Additive import from vCards (preserves multi-number contacts)."""
    from .contacts import import_vcards

    paths = Paths.default()
    conn = _open_db(paths)
    try:
        result = import_vcards(
            conn, path, default_country=default_country, source_label=source_label
        )
    finally:
        conn.close()
    console.print(
        f"[green]✓[/] {result.total} numbers seen, +{result.set_count} new/updated"
    )


# ---------------------------------------------------------------------------
# chats
# ---------------------------------------------------------------------------


@chat_app.command("list")
def chat_list(
    kind: Annotated[Optional[str], typer.Option("--kind", help="Filter by kind: group/user/lid/newsletter/...")] = None,
    since: Annotated[Optional[str], typer.Option("--since", help="Only chats with activity at/after this ISO date.")] = None,
    until: Annotated[Optional[str], typer.Option("--until", help="Only chats with activity at/before this ISO date.")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 100,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """List chats with activity (groups + 1:1, mixed)."""
    _check_format(fmt)
    from .queries.chats import list_chats, load_chat_aliases, save_chat_list_cache

    paths = Paths.default()
    conn = _open_db(paths, read_only=True)
    try:
        rows = list_chats(conn, kind=kind, since=since, until=until, limit=limit)
    finally:
        conn.close()

    # Cache the displayed list so other `chat <op>` calls can take a bare
    # integer (e.g. `chat digest 3`).
    save_chat_list_cache(paths.cache_dir, rows)

    aliases_by_jid: dict[str, str] = {}
    for k, v in load_chat_aliases(paths.config_dir).items():
        aliases_by_jid.setdefault(v, k)

    if fmt == "json":
        for i, r in enumerate(rows, 1):
            r["idx"] = i
            r["alias"] = aliases_by_jid.get(r["jid"])
        _emit_json(rows)
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("#", justify="right")
    table.add_column("alias")
    table.add_column("kind")
    table.add_column("name")
    table.add_column("last", justify="right")
    table.add_column("messages", justify="right")
    table.add_column("jid", style="dim", overflow="fold")
    for i, r in enumerate(rows, 1):
        table.add_row(
            str(i),
            aliases_by_jid.get(r["jid"]) or "",
            r["kind"] or "",
            r.get("display_name") or r.get("subject") or "",
            (r["last_ts"] or "")[:19],
            str(r["message_count"] or 0),
            r["jid"],
        )
    console.print(table)
    console.print(
        "\n[dim]The [bold]#[/] column is reordered every time you run [bold]chat list[/] — "
        "use it for ad-hoc drill-down (`chat digest 3`).\n"
        "Stable handles (don't change between runs):\n"
        "  • the [bold]jid[/] column (canonical, but verbose)\n"
        "  • the [bold]alias[/] column — set with [bold]chat alias <name> <chat>[/]\n"
        "  • the chat's own subject/contact-name (substring match always works).[/]"
    )


# ---------------------------------------------------------------------------
# chat (singular: operations on one chat)
# ---------------------------------------------------------------------------


@chat_app.command("alias")
def chat_alias(
    alias: Annotated[
        Optional[str],
        typer.Argument(help="Short alpha handle. Leave empty to list all aliases."),
    ] = None,
    chat: Annotated[
        Optional[str],
        typer.Argument(help="Chat reference (group subject, contact name, phone, JID, or # index)."),
    ] = None,
    remove: Annotated[
        bool, typer.Option("--remove", help="Remove the alias instead of setting it.")
    ] = False,
) -> None:
    """Set / remove / list stable chat aliases — handier than full JIDs.

    Examples:
        chatvault chat alias mom "Manuel Liemann"
        chatvault chat alias work "Geld & Finanzen"
        chatvault chat alias --remove mom
        chatvault chat digest mom
    """
    from .queries.chats import (
        chat_alias_path,
        load_chat_aliases,
        save_chat_aliases,
    )

    paths = Paths.default()
    paths.ensure()
    aliases = load_chat_aliases(paths.config_dir)

    if alias is None:
        if not aliases:
            console.print("[yellow]no aliases set.[/]")
            return
        table = Table(show_header=True, header_style="bold")
        table.add_column("alias")
        table.add_column("jid", style="dim", overflow="fold")
        for k in sorted(aliases):
            table.add_row(k, aliases[k])
        console.print(table)
        console.print(f"\n[dim]stored at {chat_alias_path(paths.config_dir)}[/]")
        return

    # Validate alias shape: alpha-only-leading to avoid colliding with phones,
    # JIDs, and chat-list indices.
    if not alias[:1].isalpha():
        err_console.print(
            f"[red]alias must start with a letter (got {alias!r}). "
            "Numbers/JIDs/phones are reserved for the resolver.[/]"
        )
        raise typer.Exit(code=2)
    if "@" in alias or ":" in alias:
        err_console.print(f"[red]alias must not contain '@' or ':' (got {alias!r}).[/]")
        raise typer.Exit(code=2)

    if remove:
        if alias in aliases:
            del aliases[alias]
            save_chat_aliases(paths.config_dir, aliases)
            console.print(f"[green]✓[/] removed alias {alias!r}")
        else:
            console.print(f"[yellow]no such alias: {alias!r}[/]")
        return

    if chat is None:
        err_console.print("[red]missing chat reference. Pass `chat alias <name> <chat-ref>`.[/]")
        raise typer.Exit(code=2)
    conn = _open_db(paths, read_only=True)
    try:
        chat_jid = _resolve_chat_or_exit(conn, chat)
    finally:
        conn.close()
    aliases[alias] = chat_jid
    save_chat_aliases(paths.config_dir, aliases)
    console.print(f"[green]✓[/] alias {alias!r} → {chat_jid}")


@chat_app.command("members")
def chat_members(
    chat: Annotated[str, typer.Argument(help="Chat identifier (group subject, contact name, phone, or JID).")],
    history: Annotated[bool, typer.Option("--history", help="Include join/leave history.")] = False,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Members of a chat. For groups, current (and optionally past) members. For 1:1, owner + peer."""
    _check_format(fmt)
    from .queries.chats import members_for, resolve_chat

    paths = Paths.default()
    conn = _open_db(paths, read_only=True)
    try:
        chat_jid = _resolve_chat_or_exit(conn, chat)
        rows = members_for(conn, chat_jid, include_history=history)
    finally:
        conn.close()

    if fmt == "json":
        _emit_json(rows)
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("name")
    table.add_column("role")
    table.add_column("joined", justify="right")
    table.add_column("jid", style="dim", overflow="fold")
    for r in rows:
        table.add_row(r["name"] or "?", r["role"] or "", (r["joined"] or "")[:19], r["jid"])
    console.print(table)


@chat_app.command("digest")
def chat_digest(
    chat: Annotated[str, typer.Argument(help="Chat name substring or JID.")],
    last: Annotated[int, typer.Option("--last", "-n", help="Last N messages.")] = 200,
    output: Annotated[Optional[Path], typer.Option("--output", "-o")] = None,
    pseudonymise: Annotated[
        bool,
        typer.Option(
            "--pseudonymise",
            help="Replace names with stable pseudo-IDs (P1, P2, ...).",
        ),
    ] = False,
    fmt: Annotated[
        str,
        typer.Option("--format", "-f", help="'markdown' (default) or 'jsonl'."),
    ] = "markdown",
) -> None:
    """Render the last N messages of a chat as Markdown or JSONL."""
    from .exports.digest import render_digest, render_digest_jsonl
    from .queries.chats import resolve_chat

    if fmt not in ("markdown", "jsonl"):
        err_console.print(f"[red]--format must be 'markdown' or 'jsonl' (got {fmt!r}).[/]")
        raise typer.Exit(code=2)
    if fmt == "jsonl" and pseudonymise:
        err_console.print("[red]--pseudonymise is markdown-only for now.[/]")
        raise typer.Exit(code=2)

    paths = Paths.default()
    conn = _open_db(paths, read_only=True)
    try:
        chat_jid = _resolve_chat_or_exit(conn, chat)
        if fmt == "jsonl":
            text = render_digest_jsonl(conn, chat_jid, last=last)
        else:
            text = render_digest(conn, chat_jid, last=last, pseudonymise=pseudonymise)
    finally:
        conn.close()

    if output:
        output.write_text(text, encoding="utf-8")
        console.print(f"[green]✓[/] {output} ({len(text)} bytes)")
    else:
        sys.stdout.write(text)


@chat_app.command("export")
def chat_export(
    chat: Annotated[str, typer.Argument(help="Chat name substring or JID.")],
    out: Annotated[Path, typer.Option("--out", "-o", help="Output directory.")] = Path("./export"),
    last: Annotated[
        int,
        typer.Option("--last", "-n", help="Last N messages (default 999999 = all)."),
    ] = 999_999,
    include_media: Annotated[
        bool,
        typer.Option("--include-media/--no-include-media", help="Hardlink media files into <out>/media/."),
    ] = True,
) -> None:
    """One-shot export of a single chat: digest.md + digest.jsonl + media/ folder."""
    import os as _os
    from .exports.digest import render_digest, render_digest_jsonl
    from .queries.chats import resolve_chat

    from .identities import NameResolver

    paths = Paths.default()
    conn = _open_db(paths, read_only=True)
    try:
        chat_jid = _resolve_chat_or_exit(conn, chat)
        info = conn.execute(
            "SELECT subject, kind FROM chats WHERE jid = ?", (chat_jid,)
        ).fetchone()
        title = info["subject"] or (
            NameResolver(conn).resolve(chat_jid)
            if info["kind"] in ("user", "lid") else chat_jid
        )
        md_text = render_digest(conn, chat_jid, last=last, pseudonymise=False)
        jsonl_text = render_digest_jsonl(conn, chat_jid, last=last)

        media_rows: list[dict] = []
        if include_media:
            media_rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT m.id, mm.file_path, mm.mirrored_path, mm.mime "
                    "FROM messages m JOIN message_media mm ON mm.message_id = m.id "
                    "WHERE m.chat_jid = ? AND (mm.file_path IS NOT NULL OR mm.mirrored_path IS NOT NULL) "
                    "ORDER BY m.ts LIMIT ?",
                    (chat_jid, last),
                )
            ]
    finally:
        conn.close()

    out.mkdir(parents=True, exist_ok=True)
    (out / "digest.md").write_text(md_text, encoding="utf-8")
    (out / "digest.jsonl").write_text(jsonl_text, encoding="utf-8")

    media_copied = media_missing = 0
    if include_media and media_rows:
        media_dir = out / "media"
        media_dir.mkdir(exist_ok=True)
        for r in media_rows:
            src = None
            if r["mirrored_path"] and Path(r["mirrored_path"]).exists():
                src = Path(r["mirrored_path"])
            elif r["file_path"]:
                fp = r["file_path"]
                rel = fp[len("Media/") :] if fp.startswith("Media/") else fp
                cand_mirror = paths.media_dir / rel
                cand_live = DEFAULT_MEDIA_ROOT / rel
                src = cand_mirror if cand_mirror.exists() else (cand_live if cand_live.exists() else None)
            if not src:
                media_missing += 1
                continue
            dst = media_dir / src.name
            if dst.exists():
                continue
            try:
                _os.link(src, dst)
            except OSError:
                import shutil as _sh
                _sh.copy2(src, dst)
            media_copied += 1

    console.print(f"[green]✓[/] {title} → {out}")
    console.print(f"  digest.md     {len(md_text):>10} bytes")
    console.print(f"  digest.jsonl  {len(jsonl_text):>10} bytes")
    if include_media:
        console.print(f"  media/        {media_copied} files (+{media_missing} missing on disk)")


@chat_app.command("info")
def chat_info(
    chat: Annotated[str, typer.Argument(help="Chat name substring or JID.")],
) -> None:
    """Show metadata for a single chat."""
    from .queries.chats import chat_info as _info, resolve_chat

    conn = _open_db(Paths.default(), read_only=True)
    try:
        chat_jid = _resolve_chat_or_exit(conn, chat)
        info = _info(conn, chat_jid)
    finally:
        conn.close()

    # Group fields into sections for readability.
    sections = [
        ("Identity", ["jid", "kind", "subject", "display_name", "created_ts"]),
        ("State", ["archived", "hidden", "locked", "pinned", "muted_until_ts",
                  "ephemeral_seconds", "is_contact"]),
        ("Group", ["group_type", "group_member_count"]),
        ("Activity", ["message_count", "first_message_ts", "last_message_ts",
                       "reaction_count", "last_seen_ts"]),
    ]
    seen: set[str] = set()
    for title, keys in sections:
        printed_header = False
        for k in keys:
            if k in info:
                if not printed_header:
                    console.print(f"\n[bold cyan]{title}[/]")
                    printed_header = True
                seen.add(k)
                console.print(f"  {k:<22} {info[k]!s}")
    other = [k for k in info if k not in seen and k != "raw_json"]
    if other:
        console.print("\n[bold cyan]Other[/]")
        for k in other:
            console.print(f"  {k:<22} {info[k]!s}")


@chat_app.command("why")
def chat_why(
    chat: Annotated[str, typer.Argument(help="Chat name substring or JID.")],
    member: Annotated[str, typer.Argument(help="Member JID, e.g. 196323…@lid.")],
) -> None:
    """Trace why a member's name resolves the way it does (debugging tool)."""
    from .queries.chats import resolve_chat
    from .queries.timeline import chat_member_explain

    conn = _open_db(Paths.default(), read_only=True)
    try:
        chat_jid = _resolve_chat_or_exit(conn, chat)
        result = chat_member_explain(conn, chat_jid, member)
    finally:
        conn.close()
    console.print(f"[bold]resolved as[/] {result['resolved_name']}\n")
    for line in result["chain"]:
        console.print(f"  {line}")


def _format_media_tag(r: dict) -> str:
    fname = r.get("media_mirrored_path") or r.get("media_file_path")
    mime = r.get("media_mime")
    if not (fname or mime):
        return ""
    tag = f" [{mime or '?'}"
    if fname:
        tag += f" {fname}"
    return tag + "]"


@app.command()
def timeline(
    name: Annotated[str, typer.Argument(help="Contact name (substring match).")],
    since: Annotated[Optional[str], typer.Option("--since")] = None,
    until: Annotated[Optional[str], typer.Option("--until")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Last messages from any JID that maps to a given contact name."""
    _check_format(fmt)
    from .queries.timeline import timeline_for_member

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = timeline_for_member(conn, name, since=since, until=until, limit=limit)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    if not rows:
        err_console.print(f"[yellow]No messages found for {name!r}.[/]")
        return
    for r in rows:
        body = (r["text"] or "")[:200]
        media = _format_media_tag(r)
        if not body and not media:
            body = "<no body>"
        console.print(
            f"[dim]{r['ts'][:19]}[/] [bold]{r['subject'] or r['chat_jid']}[/]: "
            f"{body}{media}"
        )


@app.command()
def forgotten(
    days: Annotated[int, typer.Option("--days", help="Threshold in days since last outgoing.")] = 365,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Contacts you haven't initiated a message with in N days."""
    _check_format(fmt)
    from .queries.timeline import forgotten_contacts

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = forgotten_contacts(conn, days=days, limit=limit)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("name")
    table.add_column("last outgoing", justify="right")
    table.add_column("phone", style="dim")
    for r in rows:
        table.add_row(r["name"] or "?", (r["last_ts"] or "never")[:19], r["phone_jid"])
    console.print(table)


# ---------------------------------------------------------------------------
# search / link / receipts / stats
# ---------------------------------------------------------------------------


@app.command()
def search(
    query: Annotated[str, typer.Argument()],
    chat: Annotated[Optional[str], typer.Option("--chat")] = None,
    since: Annotated[Optional[str], typer.Option("--since", help="ISO date or datetime.")] = None,
    until: Annotated[Optional[str], typer.Option("--until")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Full-text search across messages."""
    _check_format(fmt)
    from .queries.search import search_messages

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = search_messages(conn, query, chat=chat, since=since, until=until, limit=limit)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    for r in rows:
        body = r["text"] or ""
        media = _format_media_tag(r)
        console.print(
            f"[dim]{r['ts'][:19]}[/] [bold]{r['chat_subject'] or r['chat_jid']}[/] "
            f"[cyan]{r['sender_name']}[/]: {body or '<media>'}{media}"
        )


@link_app.command("list")
def link_list(
    chat: Annotated[Optional[str], typer.Option("--chat")] = None,
    since: Annotated[Optional[str], typer.Option("--since")] = None,
    until: Annotated[Optional[str], typer.Option("--until")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 200,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """List links shared in chats."""
    _check_format(fmt)
    from .queries.links import list_links

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = list_links(conn, chat=chat, since=since, until=until, limit=limit)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    for r in rows:
        console.print(f"[dim]{r['ts'][:19]}[/] {r['url']}")
        if r.get("title"):
            console.print(f"   [italic]{r['title']}[/]")


@status_app.command("list")
def status_list(
    kind: Annotated[Optional[str], typer.Option("--kind", help="'own' or 'received'.")] = None,
    sender: Annotated[Optional[str], typer.Option("--sender", help="Substring of resolved sender name.")] = None,
    since: Annotated[Optional[str], typer.Option("--since")] = None,
    until: Annotated[Optional[str], typer.Option("--until")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """List status posts (own + received) with media hints."""
    _check_format(fmt)
    from .queries.status import list_status

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = list_status(conn, kind=kind, sender=sender, since=since, until=until, limit=limit)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    if not rows:
        err_console.print("[yellow]no status posts found.[/]")
        return
    for r in rows:
        ts = (r["ts"] or "")[:19]
        body = r["text"] or ""
        media = ""
        if r["mime"]:
            dur = f" {r['duration_s']}s" if r["duration_s"] else ""
            fname = r.get("file_path")
            fpart = f" {fname}" if fname else ""
            media = f" [{r['mime']}{dur}{fpart}]"
        console.print(
            f"[dim]{ts}[/] [cyan]{r['kind'][:4]}[/] [bold]{r['sender_name']}[/]"
            f"{media}: {body[:200]}"
        )


@status_app.command("views")
def status_views(
    status_id: Annotated[
        str,
        typer.Argument(
            help="Status post id, or a shortcut: 'latest', 'top', or an index (1 = newest).",
        ),
    ] = "latest",
) -> None:
    """List viewers + reactions for an own status post."""
    from .queries.status import reactions_for_status, resolve_own_status_id, views_for_status

    conn = _open_db(Paths.default(), read_only=True)
    try:
        resolved = resolve_own_status_id(conn, status_id)
        if not resolved:
            err_console.print(f"[red]could not resolve status: {status_id!r}[/]")
            raise typer.Exit(code=2)
        if resolved != status_id:
            console.print(f"[dim]→ {resolved}[/]")
        views = views_for_status(conn, resolved)
        reactions = reactions_for_status(conn, resolved)
    finally:
        conn.close()

    if reactions:
        console.print(f"\n[bold]Reactions ({len(reactions)})[/]")
        for r in reactions:
            ts = (r["sender_ts"] or "")[:19]
            console.print(f"  {r['emoji']}  [bold]{r['sender_name']}[/]  [dim]{ts}[/]")
    else:
        console.print("[yellow]no reactions on this status.[/]")

    if not views:
        console.print("[yellow]no view receipts for this status.[/]")
        return
    table = Table(show_header=True, header_style="bold", title=f"\nViews ({len(views)})")
    table.add_column("viewer")
    table.add_column("read", justify="right")
    table.add_column("received", justify="right")
    table.add_column("jid", style="dim")
    for r in views:
        table.add_row(
            r["viewer_name"] or "?",
            (r["read_ts"] or "")[:19],
            (r["received_ts"] or "")[:19],
            r["viewer_jid"],
        )
    console.print(table)


@status_app.command("media")
def status_media(
    status_id: Annotated[
        str,
        typer.Argument(help="Status id, or shortcut: 'latest', 'top', or index. Defaults to 'latest'."),
    ] = "latest",
    save_thumbnail_to: Annotated[
        Optional[Path],
        typer.Option("--save-thumbnail", help="Write the thumbnail BLOB to this path."),
    ] = None,
) -> None:
    """Show resolved local file paths and CDN URL for a status post's media."""
    from .queries.status import resolve_own_status_id

    paths = Paths.default()
    conn = _open_db(paths, read_only=True)
    try:
        # For received: ID lookup is direct; for own: shortcuts apply.
        resolved = resolve_own_status_id(conn, status_id) or status_id
        sa = conn.execute(
            "SELECT s.id, s.kind, s.ts, s.text, "
            "       m.mime, m.file_path, m.media_url, m.direct_path, "
            "       m.file_size, m.duration_s, m.accessibility_label, "
            "       (SELECT 1 FROM status_archive_thumbnails t WHERE t.status_id = s.id) AS has_thumb "
            "FROM status_archive s LEFT JOIN status_archive_media m ON s.id = m.status_id "
            "WHERE s.id = ?",
            (resolved,),
        ).fetchone()
        if not sa:
            err_console.print(f"[red]status not found: {resolved!r}[/]")
            raise typer.Exit(code=2)

        thumb = None
        if save_thumbnail_to:
            row = conn.execute(
                "SELECT thumbnail FROM status_archive_thumbnails WHERE status_id = ?",
                (resolved,),
            ).fetchone()
            thumb = row["thumbnail"] if row else None
    finally:
        conn.close()

    console.print(f"[bold]{sa['kind']}[/] status — {(sa['ts'] or '')[:19]}")
    if sa["text"]:
        console.print(f"  text: {sa['text'][:200]}")
    if sa["mime"]:
        console.print(f"  mime: {sa['mime']}  size={sa['file_size']}  dur={sa['duration_s']}")
    if sa["accessibility_label"]:
        console.print(f"  alt:  {sa['accessibility_label']}")

    if sa["file_path"]:
        rel = sa["file_path"]
        # Try chatvault mirror first (durable), then live source.
        candidates: list[tuple[str, Path]] = []
        if rel.startswith("Media/"):
            candidates.append(("chatvault mirror", paths.media_dir / rel[len("Media/") :]))
        candidates.append(
            ("live source", DEFAULT_MEDIA_ROOT / rel[len("Media/") :] if rel.startswith("Media/") else DEFAULT_MEDIA_ROOT.parent / rel)
        )
        console.print("\n[bold]Local paths[/]")
        for label, p in candidates:
            mark = "[green]✓[/]" if p.exists() else "[red]✗[/]"
            console.print(f"  {mark} {label}: {p}")

    if sa["media_url"]:
        console.print(f"\n[bold]CDN[/] {sa['media_url']}")
    if sa["direct_path"]:
        console.print(f"  direct_path: {sa['direct_path']}")

    console.print(f"\n[bold]Thumbnail[/] {'present' if sa['has_thumb'] else 'none'}")
    if save_thumbnail_to and thumb:
        save_thumbnail_to.write_bytes(thumb)
        console.print(f"  → wrote {save_thumbnail_to} ({len(thumb)} bytes)")


@status_app.command("summary")
def status_summary(
    since: Annotated[Optional[str], typer.Option("--since")] = None,
    until: Annotated[Optional[str], typer.Option("--until")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 50,
    sort: Annotated[str, typer.Option("--sort", help="'ts' (newest first) or 'views' (most-viewed first).")] = "ts",
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Own status posts with view + reaction counts."""
    _check_format(fmt)
    from .queries.status import own_status_view_summary

    if sort not in ("ts", "views"):
        err_console.print(f"[red]--sort must be 'ts' or 'views' (got {sort!r}).[/]")
        raise typer.Exit(code=2)
    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = own_status_view_summary(conn, since=since, until=until, limit=limit, sort=sort)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json(rows)
        return
    if not rows:
        err_console.print("[yellow]no own status posts.[/]")
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("when")
    table.add_column("type")
    table.add_column("views", justify="right")
    table.add_column("likes", justify="right")
    table.add_column("text")
    table.add_column("id", style="dim", overflow="fold")
    for r in rows:
        table.add_row(
            (r["ts"] or "")[:19],
            r["type"] or "",
            str(r["view_count"] or 0),
            str(r["reaction_count"] or 0),
            (r["text"] or "")[:60],
            r["id"],
        )
    console.print(table)


@app.command()
def receipts(
    message: Annotated[
        str,
        typer.Argument(
            help=(
                "Message reference. Forms: full message id; <chat>:<int> (Python "
                "indexing — 1=oldest in chat, -1=newest); plain <chat> (= newest)."
            ),
        ),
    ],
    source_db: Annotated[
        Path,
        typer.Option(
            "--source-db",
            help="Path to the decrypted source msgstore.db (read-only). Defaults to staging.",
        ),
    ] = Path(),
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Look up read/delivery receipts for a single message in the original source DB."""
    _check_format(fmt)
    from .queries.chats import resolve_message
    from .queries.receipts import receipts_for

    paths = Paths.default()
    if not source_db or str(source_db) == ".":
        source_db = paths.staging_dir / "msgstore.db"
    if not source_db.exists():
        err_console.print(f"[red]source db not found:[/] {source_db}")
        raise typer.Exit(code=2)
    conn = _open_db(paths, read_only=True)
    try:
        try:
            message_id = resolve_message(conn, message)
        except (LookupError, ValueError) as exc:
            err_console.print(f"[red]{exc}[/]")
            raise typer.Exit(code=2) from None
        rows = receipts_for(conn, source_db, message_id)
    finally:
        conn.close()
    if fmt == "json":
        for r in rows:
            r["message_id"] = message_id
        _emit_json(rows)
        return
    console.print(f"[dim]message:[/] {message_id}")
    for r in rows:
        console.print(
            f"[dim]{r['ts'][:19]}[/] [cyan]{r['recipient']}[/] {r['status']}"
        )


@app.command()
def stats(
    fmt: Annotated[str, typer.Option("--format", "-f", help="'table' or 'json'.")] = "table",
) -> None:
    """Quick row-count overview of the archive."""
    _check_format(fmt)
    from .queries.stats import quick_stats

    conn = _open_db(Paths.default(), read_only=True)
    try:
        rows = quick_stats(conn)
    finally:
        conn.close()
    if fmt == "json":
        _emit_json([{"label": l, "count": c} for l, c in rows])
        return
    for label, count in rows:
        console.print(f"  {label:<30} {count:>10}")


# ---------------------------------------------------------------------------
# mirror
# ---------------------------------------------------------------------------


@mirror_app.command("snapshot")
def mirror_snapshot(
    media_root: Annotated[
        Path,
        typer.Option("--media-root", help="Source app's Media root."),
    ] = Path("/storage/emulated/0/Android/media/com.whatsapp/WhatsApp/Media"),
) -> None:
    """One-off mirror pass: hardlink any new media files into the archive."""
    from .media_mirror import snapshot_pass

    paths = Paths.default()
    conn = _open_db(paths)
    try:
        result = snapshot_pass(conn, media_root=media_root, archive_root=paths.media_dir)
    finally:
        conn.close()
    console.print(f"[green]✓[/] mirrored {result.new_files} new files ({result.bytes:,} bytes)")


@mirror_app.command("start")
def mirror_start(
    media_root: Annotated[
        Path, typer.Option("--media-root")
    ] = Path("/storage/emulated/0/Android/media/com.whatsapp/WhatsApp/Media"),
) -> None:
    """Run the inotify-based mirror daemon in the foreground."""
    from .media_mirror import run_daemon

    paths = Paths.default()
    run_daemon(db_path=paths.db_path, media_root=media_root, archive_root=paths.media_dir)


if __name__ == "__main__":
    app()
