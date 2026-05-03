# Common queries

The CLI covers the everyday questions. These are the building blocks for more.

## Common options across list commands

Every list-style command (`chat list`, `contact list`, `contact unresolved`,
`link list`, `status list`, `status summary`, `timeline`, `search`,
`forgotten`, `stats`) supports the same shape:

| Option | Meaning |
|---|---|
| `--limit N` / `-n N` | Cap result count. |
| `--since ISO` | Only entries at or after this timestamp. |
| `--until ISO` | Only entries at or before this timestamp. |
| `--format table\|json` / `-f` | Default `table` (Rich); `json` streams JSON Lines on stdout — pipe-friendly. |

Not every command supports all four (e.g. `forgotten` uses `--days` instead
of `--since`), but anywhere a filter conceptually applies, the name is the
same.

## Identifying a chat

Anywhere a command takes a `<chat>` argument, you can pass any of:

| Form | Example |
|---|---|
| Group subject substring | `"Girls Night"`, `"Daygame)"` |
| Contact name substring | `"Andrea"`, `"Andrea Wirz"` (resolves to that person's 1:1 chat) |
| Phone number (any format) | `"+41 79 937 24 33"`, `"0041799372433"` |
| Literal JID | `41799372433@s.whatsapp.net`, `120363000@g.us` |
| Stable alias | `mom`, `work` (set with `chat alias <name> <chat>`, persisted in `~/.config/chatvault/chat_aliases.json`; must start with a letter) |
| Index from last `chat list` | `3` (the # column — only stable until you re-run `chat list`; cached in `~/.cache/chatvault/last_chat_list.json`) |

If multiple matches exist, the CLI prints the candidates and asks you to be
more specific.

## Identifying a single message

Wherever a command takes a `<message>` argument (currently `receipts`), you
can pass any of:

| Form | Example |
|---|---|
| Full message id | `41799372433@s.whatsapp.net:0:ACDD7C38…` |
| `<chat-ref>:<int>` | `Andrea:1` (oldest in chat), `Andrea:-1` (newest), `Andrea:-2` (next-to-newest); chat-ref accepts every form above, including `3:-1` (newest message in chat #3) |
| `<chat-ref>` alone | shorthand for `<chat-ref>:-1` |

`chat digest` prints a `[#N]` per message — the same N you can use as the
positive integer in `<chat-ref>:N`. The number is **stable**: 1 = oldest
message ever in that chat, regardless of slice. It doesn't shift when newer
messages arrive.

## Quick stats

```sh
chatvault stats
```

Row-count overview of the archive — useful as a smoke test after `extract`.

## Browse chats — groups AND people

```sh
chatvault chat list                        # most recent first; mixes groups + 1:1
chatvault chat list --kind group           # groups only
chatvault chat list --kind user            # 1:1 phone-keyed person chats
chatvault chat list --kind lid             # 1:1 LID-keyed person chats
chatvault chat list --kind newsletter
```

For 1:1 chats with empty `subject`, the `name` column is filled from your
contacts table (or pretty-formatted phone if unknown).

## Stable chat aliases

When you use a chat often, give it a short handle. Aliases are persistent
and immune to re-running `chat list`:

```sh
chatvault chat alias mom "Manuel Liemann"     # set
chatvault chat alias work "Geld & Finanzen"
chatvault chat alias                          # list all
chatvault chat alias --remove mom

# Use anywhere a chat-ref is accepted:
chatvault chat digest mom --last 100
chatvault receipts mom:-1
chatvault chat export work --out ./work
```

Constraints: alias must start with a letter; no `@` or `:` allowed (those
clash with JIDs and the message-index syntax).

## Members of a chat

```sh
chatvault chat members "Girls Night"
chatvault chat members "Girls Night" --history    # include left members
chatvault chat members "Andrea Wirz"              # 1:1 → owner + peer
```

## Why is this name showing up?

Trace the resolution chain for one member of a chat — useful when an LID
resolves to an unexpected name.

```sh
chatvault chat why "Girls Night" "196323038986257@lid"
```

## Per-person timeline

Recent messages from anyone matching a contact name (across all chats and
across all phone numbers / LIDs that name maps to).

```sh
chatvault timeline "Anna" --since 2026-01-01
```

## Forgotten contacts

```sh
chatvault forgotten --days 365
```

## Search

```sh
chatvault search "Restaurant"
chatvault search "rezept" --chat "Geld" --since 2026-01-01
```

## Links

```sh
chatvault link list --chat "Familie Wandertag" --since 2026-01-01
```

## Receipts (lazy)

```sh
chatvault receipts "120363000@g.us:0:3EB07DD1"
```

Joins against the source DB on the fly — no large copy.

## Status posts (your own + last 24h received)

```sh
chatvault status list                      # received in current 24h backup window
chatvault status list --kind own           # your own status posts (full history)
chatvault status summary --sort views      # top by view count
chatvault status views                     # latest status — shows viewers + reactions
chatvault status views top                 # most-viewed status
chatvault status media latest              # local file paths + CDN URL + thumbnail
```

## Export a single chat (full text + media)

See [digest.md](digest.md).

## Digest for an assistant

See [digest.md](digest.md).

## Custom SQL

For anything not covered, open the archive directly:

```sh
sqlite3 ~/.local/share/chatvault/archive.db
```

Useful starters:

```sql
-- top 10 most reacted-to messages of last 30 days, with text snippet
SELECT m.ts, c.subject, m.text,
       COUNT(*) AS rxs
FROM messages m
JOIN reactions r ON r.parent_message_id = m.id
JOIN chats c     ON c.jid = m.chat_jid
WHERE m.ts >= date('now', '-30 days')
GROUP BY m.id
ORDER BY rxs DESC
LIMIT 10;

-- chats I haven't engaged in for a year
SELECT c.subject, MAX(m.ts) AS last
FROM chats c
LEFT JOIN messages m ON m.chat_jid = c.jid AND m.from_me = 1
GROUP BY c.jid
HAVING last < date('now', '-365 days') OR last IS NULL
ORDER BY last;

-- voice-note transcripts with a substring
SELECT m.ts, c.subject, GROUP_CONCAT(s.text, ' ') AS transcript
FROM message_transcription_segments s
JOIN messages m ON m.id = s.message_id
JOIN chats c    ON c.jid = m.chat_jid
WHERE s.text LIKE '%vintage%'
GROUP BY m.id
ORDER BY m.ts DESC
LIMIT 20;
```
