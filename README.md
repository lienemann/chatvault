# chatvault

A local SQLite archive of personal chat history, built around a decrypted
msgstore database. The schema is the project's own design rather than a
mirror of the source app, which means upstream schema drift doesn't break
the archive.

> Alpha. The schema may change before 1.0.

## Status

Reads from a decrypted `msgstore.db` and writes into
`~/.local/share/chatvault/archive.db`. There's a CLI for the everyday
queries (digest a chat, list group members, find old links, search) and
a Markdown export for handing slices to an external assistant. A small
inotify watcher mirrors media files so the archive doesn't lose them when
the source app's storage management cleans up.

What it doesn't do: anything over the network. The decrypted database, the
backup key, message bodies and media all stay on the device.

## Quickstart

Tested on Android/Termux. Needs Python 3.11+, `wa-crypt-tools` for
decryption, and `inotify-tools` if you want the mirror daemon.

```sh
pip install --user uv
uv tool install chatvault    # once published
# from a checkout:
uv pip install -e '.[decrypt,dev]'

chatvault init               # creates dirs, runs migrations
chatvault key set            # paste the 64-char hex backup key
chatvault extract --backup ~/storage/.../msgstore.db.crypt15
chatvault contact sync      # if termux-contact-list is around
chatvault contact import-vcard ~/storage/downloads/contacts.vcf  # full multi-number import
```

Then ask things. **Anywhere a chat name is needed, you can pass a group
subject substring, a contact name, a phone number, or a literal JID.**

```sh
chatvault chat list                                    # groups + 1:1 mixed
chatvault chat list --kind user                        # 1:1 phone chats
chatvault chat members "Girls Night"
chatvault chat members "Andrea Wirz"                    # 1:1 → owner + peer
chatvault chat digest "Birthday Anna" --last 1000 -o digest.md
chatvault chat digest "+41 79 937 24 33" --format jsonl # 1:1 by phone, JSONL
chatvault chat export "Andrea Wirz" --out ./andrea      # md + jsonl + media/
chatvault search "Restaurant" --since 2026-01-01
chatvault timeline "Anna" --limit 20
chatvault forgotten --days 365
chatvault status summary --sort views                   # top own status posts
chatvault status views top                              # who saw the most-viewed
chatvault contact unresolved --csv pins.csv            # bulk-pin pipeline
chatvault chat why "Girls Night" "196323038986257@lid"
```

`chat why` is the debugging tool — it prints the resolution chain for a
member's name (LID → phone → contact), which is the part that's least
obvious when something looks off.

## CLI conventions

All sub-namespaces are singular: `chat`, `contact`, `link`, `status`, `key`,
`mirror`. Operations on the collection live under their entity (e.g.
`chat list`, `contact list`).

Every list-style command supports the same options:

- `--limit N` / `-n N`
- `--since <iso>` / `--until <iso>` (where time-filtering applies)
- `--format table|json` / `-f` (default `table`; `json` streams JSON Lines)

See [docs/recipes/queries.md](docs/recipes/queries.md) for the full command
reference.

## What gets archived

Messages, reactions, edits, system events with reconstructed bodies, group
memberships with history, calls and call participants, polls and votes,
status post metadata, newsletter messages, voice-note transcription
segments, vcards, locations, view-once metadata, albums, revoked-message
tombstones, bot/AI threads, templates, communities, and identity mappings.

What stays in the source DB and is fetched on demand: read/delivery
receipts (`chatvault receipts <message-id>` joins the source on `key_id`
when you ask). They're huge and rarely useful enough to justify copying.

## Multi-number contacts

`termux-contact-list` returns one number per contact, which loses the
linkage when someone has work + private + foreign numbers. The fix is to
export the address book as vCards from the Contacts app — that gives you
every number — and run

```sh
chatvault contact import-vcard ~/storage/downloads/contacts.vcf
```

The import is additive, so you can run it whenever the address book grows.

## Privacy

The archive is private data. A few things to keep in mind:

- The backup key decrypts everything. chatvault stores it in the XDG config
  dir at `chmod 600` and never logs it.
- No outbound network calls. If you want to feed a chat slice to an external
  assistant, that's an explicit `chat digest` step, and you choose between
  `--pseudonymise` and named output.
- Don't paste real backup files into bug reports. There's a synthetic
  fixture under `tests/` for reductions.

## Layout

Migrations are in `src/chatvault/migrations/` as forward-only `00N_*.sql`
files; the runner picks up the next version on startup and updates
`_meta.schema_version`. Each domain extractor lives in its own module under
`src/chatvault/extractors/` and follows a single contract:

```python
def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult: ...
```

Convenience queries are under `queries/`, exports under `exports/`, the
mirror daemon at `media_mirror.py`. `docs/architecture.md` has the longer
explanation.

## License

Apache 2.0.

## Trademarks

Not affiliated with, endorsed by, or sponsored by Meta Platforms or any of
its products. Source-format names are referenced only for technical
interoperability.
