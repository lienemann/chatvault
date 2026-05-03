# Digesting and exporting a chat

Two commands cover this:

| Command | Output | When |
|---|---|---|
| `chat digest` | one Markdown or JSONL stream | quick read or LLM input |
| `chat export` | folder with both formats + linked media | offline backup of a single chat |

Both accept any chat identifier (group subject, contact name, phone number,
JID — see [queries.md](queries.md)).

## `chat digest`

Renders the last N messages of a chat. Default format is Markdown, friendly
for pasting into another assistant. Sender names are real (resolved via the
contacts table); pass `--pseudonymise` for stable `P1, P2, …` IDs.

```sh
# Group, last 1 000 messages, Markdown
chatvault chat digest "Birthday Anna" --last 1000 -o birthday.md

# 1:1 chat by contact name
chatvault chat digest "Andrea Wirz" --last 200 -o andrea.md

# 1:1 by phone number
chatvault chat digest "+41 79 937 24 33" --last 200 -o andrea.md

# JSONL — one message per line, machine-parsable
chatvault chat digest "Daygame)" --last 100 --format jsonl -o daygame.jsonl
```

## `chat export`

One-shot full export of a single chat into a directory:

```sh
chatvault chat export "Andrea Wirz" --out ./andrea
# ./andrea/digest.md          (markdown digest, all messages by default)
# ./andrea/digest.jsonl       (jsonl digest)
# ./andrea/media/             (hardlinked media files)
```

Options:

- `--last N` — cap to the last N messages (default 999 999 = all).
- `--no-include-media` — skip the media folder.
- `-o <dir>` — output directory (default `./export`).

## A starter prompt for the receiving assistant

```
You will receive a Markdown digest of a single group chat. Sender names are
real (resolved from the owner's address book) unless the digest header says
otherwise.

Format per message:

  **[ts] sender** _(type)_
    ↪ replying to NAME: "snippet"        (optional)
    text lines…
    [media: mime caption="..."]          (optional)
    [link: URL]                          (optional, plus title)
    reactions: 👍×3 (Anna, Berit, Carla) (optional)
    [edited Nx — current text above]    (optional)

Task:

1. Topical clusters (3–8). One paragraph each: what it was about, what the
   outcome was. Chronological where it helps, otherwise by importance.
2. Tips with consensus. Concrete recommendations the group endorsed.
   Strength ranking: reactions (👍 ❤️ 🔥 💯 ✅ 🎯) > affirmative replies
   > multiple voices repeating > no contradiction. Note disagreements.
3. All links. Every URL in the digest, with one-sentence context per link.

Skip smalltalk, raw reaction listings without content, and content the
digest doesn't actually support. Don't invent.
```

Save the prompt as `~/.local/share/chatvault/prompts/digest.md` and pipe both
together when you hand off:

```sh
cat ~/.local/share/chatvault/prompts/digest.md birthday-anna.md | pbcopy
```
