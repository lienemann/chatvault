"""Two-pass LLM summary of a chat via the Claude Code CLI (`claude -p`).

Pass 1: raw JSONL → structured JSON extract (links/images/audio/decisions/
opinions/tips/unresolved). Pass 2: extract → polished Markdown report.

Both passes invoke `claude -p --bare`. `--bare` strips Claude Code's default
system prompt and uses ANTHROPIC_API_KEY strictly — letting the caller route
requests through a personal key instead of a shared/company OAuth account.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chatvault.config import Paths
from chatvault.exports.digest import render_digest_jsonl

PASS1_SYSTEM = """\
You receive JSONL of WhatsApp chat messages (one per line, chronological).
Per-line fields: idx, ts, sender_name, type, text, transcription (audio only),
image_description (vision-derived; present for images that were processed by
`chatvault describe-images`), media (with file_path/mirrored_path/mime/
caption), quoted, reactions.

image_description has shape:
  { "kind": "chat_screenshot" | "social_profile" | "image",
    "summary": str,
    "details": {…full structured content — see below…} }

USE image_description.details aggressively — it is primary content.

For kind == "chat_screenshot":
  details.transcript is a verbatim conversation forwarded INTO this chat.
  Treat its messages like first-class messages: the chat sharer is reacting
  to / discussing them. Quote prompts/answers in "opinions" and "decisions"
  when participants respond to them.

For kind == "social_profile":
  details.prompts is the ordered list of prompt Q&A cards — strongest signal
  of who the person is and what hooks others to react.
  details.vitals carries demographic/lifestyle fields.
  details.photos describes each photo with `shows` tags and `vibe`.
  details.observations contains notable signals, contradictions, humour.
  When the chat is discussing a profile, surface specific prompt answers,
  vitals contradictions, and observations in "opinions" and "tips".
  Refer to the profile subject by `details.name` (or "the profile" when null).

Multiple consecutive screenshots from the same person are likely fragments
of one profile/conversation — group them via sender + adjacent ts.

For kind == "image":
  Use details.description + text_in_image. Memes' punchlines often drive
  reactions — include them when reactions cluster.

Extract and categorise content. Output ONLY a single JSON object — no prose,
no markdown fences — with exactly these keys:

{
  "links":      [{"url": str, "title": str|null, "sender": str, "ts": str,
                  "context": str, "reaction_count": int}],
  "images":     [{"sender": str, "ts": str, "caption": str|null,
                  "image_ref": str, "context": str}],
  "audio":      [{"sender": str, "ts": str,
                  "transcription_summary": str, "context": str}],
  "decisions":  [{"topic": str, "outcome": str, "originator": str,
                  "accepted_by": [str], "dissent": [str], "ts_range": str}],
  "opinions":   [{"topic": str, "majority_view": str,
                  "supporters": [{"name": str, "argument": str}],
                  "dissenters": [{"name": str, "argument": str}]}],
  "tips":       [{"tip": str, "source": str, "ts": str, "reaction_count": int}],
  "unresolved": [{"topic": str, "positions": [str], "ts_range": str}]
}

Rules:
- Drop pure greetings, single-word agreements, logistics dust ("on my way"),
  duplicated forwards, personal fights — UNLESS a substantive outcome emerged.
- Reaction counts are an agreement signal; weight them.
- A "decision" requires a real resolution. Threads that fizzled or stayed
  contested go in "unresolved", not "decisions".
- Use the `transcription` field verbatim as input — never invent audio content.
- For "image_ref", use exactly the basename of media.mirrored_path
  (e.g. "IMG-20260101-WA0001.jpg") — no path prefix.
- Skip images with no mirrored_path.
- If a category has no items, return [] — do not omit the key.
"""

PASS2_SYSTEM = """\
You receive a JSON object summarising a WhatsApp chat. Produce a clean
Markdown report with these sections, in this order. SKIP any section whose
input array is empty.

## Links
- [Title or URL](url) — sender, ts. Reactions: 👍×N. <context>

## Audio (transcribed)
- ts — sender: <transcription_summary>. <context>

## Images
- ts — sender. <caption or context>. ![](media/<image_ref>)

## Decisions
- **<topic>**: <outcome>. Originator: <originator>. Accepted by: <accepted_by joined with ", ">. Dissent: <dissent joined with ", " or "none">. (<ts_range>)

## Opinions
- **<topic>**. Majority: <majority_view>.
  - Supporters: <name (1-line arg)>, …
  - Dissenters: <name (1-line arg)>, … or "none"

## Tips & tricks
- <tip> — <source>, ts. 👍×N.

## Unresolved threads
- **<topic>** (<ts_range>): <positions joined with " vs ">.

Rules:
- Output ONLY the Markdown — no prose framing, no "the chat shows…".
- H2 headings (`##`), bullets, bold for keys. Plain markdown, no HTML.
- Preserve image_ref values exactly — they map to files on disk.
- Omit reaction info entirely when count is 0.
"""


def _call_claude(
    prompt: str,
    *,
    model: str,
    system: str,
    api_key_env: str,
    timeout: int = 600,
) -> str:
    """Invoke `claude -p --bare` with an explicit API key from env."""
    api_key = os.environ.get(api_key_env)
    if not api_key:
        msg = (
            f"environment variable {api_key_env!r} is not set. "
            "Export an Anthropic API key (personal recommended for company-managed accounts)."
        )
        raise RuntimeError(msg)

    cmd = [
        "claude",
        "-p",
        "--bare",
        "--model",
        model,
        "--system-prompt",
        system,
    ]
    env = {**os.environ, "ANTHROPIC_API_KEY": api_key}
    res = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        errors="replace",
        env=env,
        timeout=timeout,
        check=False,
    )
    if res.returncode != 0:
        stderr = (res.stderr or "").strip()
        msg = f"claude failed (rc={res.returncode}): {stderr[:500]}"
        raise RuntimeError(msg)
    return res.stdout


def _strip_json_fences(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    lines = t.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _filter_jsonl_since(jsonl: str, since: str | None) -> str:
    if not since:
        return jsonl
    kept: list[str] = []
    for line in jsonl.splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = rec.get("ts") or ""
        if ts >= since:
            kept.append(line)
    return ("\n".join(kept) + "\n") if kept else ""


@dataclass(slots=True)
class SummaryResult:
    extract: dict[str, Any] = field(default_factory=dict)
    markdown: str = ""
    msg_count: int = 0


def summarise_chat(
    conn: sqlite3.Connection,
    chat_jid: str,
    *,
    last: int = 1000,
    since: str | None = None,
    model: str = "opus",
    api_key_env: str = "ANTHROPIC_API_KEY",
    extract_only: bool = False,
) -> SummaryResult:
    """Run the two-pass summary. `extract_only=True` skips pass 2 (debugging)."""
    jsonl = render_digest_jsonl(conn, chat_jid, last=last)
    jsonl = _filter_jsonl_since(jsonl, since)
    msg_count = sum(1 for line in jsonl.splitlines() if line.strip())
    if msg_count == 0:
        return SummaryResult()

    extract_text = _call_claude(
        jsonl, model=model, system=PASS1_SYSTEM, api_key_env=api_key_env
    )
    try:
        extract = json.loads(_strip_json_fences(extract_text))
    except json.JSONDecodeError as exc:
        msg = f"pass 1 returned non-JSON: {exc}; head={extract_text[:300]!r}"
        raise RuntimeError(msg) from exc

    markdown = ""
    if not extract_only:
        markdown = _call_claude(
            json.dumps(extract, ensure_ascii=False, indent=2),
            model=model,
            system=PASS2_SYSTEM,
            api_key_env=api_key_env,
        )
    return SummaryResult(extract=extract, markdown=markdown, msg_count=msg_count)


def collect_image_refs(extract: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    for img in extract.get("images") or []:
        ref = img.get("image_ref")
        if isinstance(ref, str) and ref:
            refs.add(Path(ref).name)
    return refs


def copy_referenced_media(
    refs: set[str],
    conn: sqlite3.Connection,
    paths: Paths,
    out_dir: Path,
) -> tuple[int, int]:
    """Copy mirrored files matching `refs` (basenames) into out_dir/media/.

    Returns (copied, missing). Hard-link when possible, copy otherwise (sdcard
    is FUSE-mounted and rejects hard links — copy falls back automatically).
    """
    if not refs:
        return 0, 0
    media_out = out_dir / "media"
    media_out.mkdir(parents=True, exist_ok=True)
    by_name: dict[str, Path] = {}
    rows = conn.execute(
        "SELECT mirrored_path FROM message_media "
        "WHERE mirrored_path IS NOT NULL AND mirrored_path != ''"
    )
    for r in rows:
        p = Path(r["mirrored_path"])
        if not p.is_absolute():
            p = paths.media_dir / p
        if p.name in refs and p.name not in by_name:
            by_name[p.name] = p

    copied = 0
    for name, src in by_name.items():
        if not src.exists():
            continue
        dst = media_out / name
        if dst.exists():
            copied += 1
            continue
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)
        copied += 1
    return copied, len(refs) - copied
