# Architecture

chatvault is a thin pipeline that reads from a decrypted source database and
writes structured rows into our own SQLite archive. There are no long-running
services in the core; the optional `media_mirror` daemon is the only exception.

```
┌──────────────────────┐    decrypt     ┌──────────────────┐
│  msgstore.db.crypt15 │ ─────────────▶ │  staging/        │
│  (source app)        │  wa-crypt-     │  msgstore.db     │
└──────────────────────┘  tools         └────────┬─────────┘
                                                 │ read-only
                                                 ▼
                                        ┌──────────────────┐
                                        │  extractors/     │
                                        │  one per domain  │
                                        └────────┬─────────┘
                                                 │ upserts
                                                 ▼
                                        ┌──────────────────┐
                                        │  archive.db      │ ◀─── chatvault tables
                                        │  (chatvault)     │      (own schema, versioned)
                                        └──────────────────┘
```

## Layers

### 1. Decrypt

`chatvault.decrypt` shells out to `wa-crypt-tools` to turn a `.crypt15` blob
into a plaintext SQLite file in `$XDG_CACHE_HOME/chatvault/staging/`. The
plaintext is deleted after the run unless `--keep-decrypted` is passed.

### 2. Source DB (read-only)

The source schema is volatile — column names and tables change between source
app versions. We do not depend on stable schemas; we depend on stable concepts
(messages, chats, reactions, ...) and adapt the SQL when we have to. The
`extractors/` package is where this coupling lives.

### 3. Extractors

Each extractor is a single module in `extractors/`. Public contract:

```python
def extract(source: sqlite3.Connection, archive: sqlite3.Connection) -> ExtractorResult: ...
```

Order in `pipeline.py` matters: identities and chats come first because later
extractors reference their JIDs, then messages (which create the rows the
extension tables hang off), then everything else.

Each extractor is **idempotent**: running it twice on the same source produces
the same archive state. Most use `INSERT OR REPLACE` keyed on a stable id;
some use `DELETE WHERE message_id = ? + INSERT` for has-many relations.

### 4. archive.db

The chatvault SQLite file. Our own schema (see `docs/schema.md`), in
`$XDG_DATA_HOME/chatvault/archive.db`. Schema versions are tracked in `_meta`
and applied as forward-only `migrations/NNN_*.sql` files at startup.

### 5. Queries / exports

`queries/` contains pure-SQL convenience functions. `exports/` formats data
into Markdown / JSONL for handing to an external assistant. Both layers depend
only on the chatvault schema, never the source.

### 6. Media mirror

`media_mirror.py` runs in two modes: `snapshot` (one-off, cron-friendly) and
`start` (long-running inotify watcher). It hardlinks new media files from the
source app's Media directory into `$XDG_DATA_HOME/chatvault/media/` so the
archive remains intact when the source app's storage management removes its
own copies.

### 7. Receipt lookup (lazy)

`receipt_*` tables in the source DB are huge (~1M rows for a personal archive)
and rarely queried. Rather than copying all of them, `chatvault receipts <id>`
opens the source DB read-only and joins by `key_id` on demand.

## Identity resolution

JIDs are the stable identifiers across the whole archive. `identities.py`
centralises the resolution chain:

1. address-book name (table `contacts`)
2. `+phone-number` (when only the phone JID is known)
3. for LIDs: source-app display name if it looks like a real name
4. `lid:{user_part}` (we know an identity but no name)
5. raw JID (last resort)

The resolver loads the lookup tables (5–10k rows total) into RAM at construction
time; per-call resolution is a dict lookup.

## Extending

- New domain ⇒ new module in `extractors/`, registered in `pipeline.py:DEFAULT_EXTRACTORS`.
- Schema change ⇒ new file `migrations/00N_*.sql`, never edit a released migration.
- New convenience query ⇒ module in `queries/`, exposed on the CLI in `cli.py`.

## Why SQLite over JSONL

We've been here before. JSONL is excellent for append-only journals and
git-friendly inspection, but the data is highly relational (group →
participants → identities; message → reactions/edits/quoted/media), and
analysis queries on a 4000-day archive don't tolerate full-stream rescans.
SQLite gives us indexes, joins, and atomic transactions; we keep the
schema-stability promise by owning the schema (it's not a mirror of the
source app's tables) and using `migrations/`.

JSONL still has a job: it's the export format for handing slices to external
assistants (`chatvault chat digest --output …`).
