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
import sqlite3
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chatvault.config import Paths
from chatvault.exports.digest import render_digest_jsonl
from chatvault.fs import link_or_copy

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
  "tips":       [{
                    "tip": str,                          // verbatim or near-verbatim
                    "category": "recommendation" | "recipe" | "habit_routine"
                                | "setting_config" | "shortcut_hack" | "warning_avoid"
                                | "tool_service" | "place_venue" | "skill_technique"
                                | "data_point" | "other",
                    "subject": str,                      // what it concerns (app/book/place/exercise/...)
                    "source": str,
                    "ts": str,
                    "evidence": str | null,              // outcome claim or anecdote attached
                    "endorsed_by": [str],                // others who later confirmed it worked
                    "reaction_count": int
                  }],
  "unresolved": [{"topic": str, "positions": [str], "ts_range": str}]
}

Rules:
- Use `sender_name` (and any names inside image_description.details) verbatim
  throughout. Do NOT pseudonymise, abbreviate, or paraphrase to "the sharer" /
  "user A". Real names are expected.
- Drop pure greetings, single-word agreements, logistics dust ("on my way"),
  duplicated forwards, personal fights — UNLESS a substantive outcome emerged.

TIPS — be liberal. Anything below counts; capture them ALL, not just obvious
"tips". Each goes into the `tips` array with the matching `category`:
  • recommendation   — apps, books, podcasts, movies, products, supplements
  • recipe           — ingredient list / step / technique for cooking, drinks
  • habit_routine    — daily/weekly practice ("cold shower every morning")
  • setting_config   — phone/app settings, automation, keyboard shortcuts
  • shortcut_hack    — lifehack, workaround, clever combination
  • warning_avoid    — what NOT to do, brands/services to skip, anti-patterns
  • tool_service     — tool, SaaS, website, command, library, hardware
  • place_venue      — restaurant, bar, route, hotel, viewpoint, shop
  • skill_technique  — exercise form, language tactic, study method, drill
  • data_point       — concrete number/result worth remembering ("I lost 5kg
                       on X over 8 weeks", "Y costs €40 vs €120 at Z")
  • other            — useful enough to remember but doesn't fit above
Capture `subject` precisely (brand/product/place name), `evidence` if the
sender attached a result or anecdote, and `endorsed_by` for anyone who later
confirmed it worked. Reactions count as a soft endorsement signal but the
sender must add value, not just react.
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
Group items by `category`. Use these subheadings (skip empty groups, in this order):
"### Recommendations" (recommendation), "### Recipes" (recipe),
"### Habits & routines" (habit_routine), "### Settings & configs"
(setting_config), "### Shortcuts & hacks" (shortcut_hack), "### Tools & services"
(tool_service), "### Places & venues" (place_venue), "### Skills & techniques"
(skill_technique), "### Data points" (data_point), "### What to avoid"
(warning_avoid), "### Other" (other).

Per-item line:
- **<subject>** — <tip>. <evidence if any>. — <source>, ts. 👍×N
  Endorsed by: <endorsed_by joined with ", "> (omit line if list is empty).

## Unresolved threads
- **<topic>** (<ts_range>): <positions joined with " vs ">.

Rules:
- Output ONLY the Markdown — no prose framing, no "the chat shows…".
- H2 headings (`##`), bullets, bold for keys. Plain markdown, no HTML.
- Preserve image_ref values exactly — they map to files on disk.
- Omit reaction info entirely when count is 0.
"""


CHAT_UI_PROMPT = """\
You are summarising a WhatsApp chat. The conversation data is attached as
`data.jsonl` (one message per line). Internally:

  1. extract the structured categories listed in PART 1 (mentally; do not show);
  2. render the Markdown report in PART 2, using that extract.

Output ONLY the final Markdown — no preamble, no JSON, no fences.

NAMES
-----

`sender_name` values and any names inside `image_description.details` are
REAL names from the chat. Use them verbatim. Do NOT pseudonymise, abbreviate
(e.g. "M.") or paraphrase ("the sharer", "user A", "person 1"). If a name is
missing or shows as a JID/phone, fall back to that string unchanged.

────────────────────────────  PART 1: EXTRACTION RULES  ────────────────────────────

{pass1}

────────────────────────────  PART 2: REPORT FORMAT  ────────────────────────────

{pass2}
"""


def build_chat_ui_prompt() -> str:
    """Standalone prompt text (no data) — pair with the data.jsonl attachment
    when pasting into Claude.ai / ChatGPT."""
    return CHAT_UI_PROMPT.format(pass1=PASS1_SYSTEM.strip(), pass2=PASS2_SYSTEM.strip())


def _resolve_key(api_key_env: str, api_key_file: str | None) -> str:
    """Prefer env var; fall back to chmod-600 file. Hard-fail on loose perms."""
    val = os.environ.get(api_key_env, "")
    if val:
        return val
    if api_key_file:
        from chatvault.config import read_api_key_from_file

        return read_api_key_from_file(Path(os.path.expanduser(api_key_file)))
    msg = (
        f"No API key: env var {api_key_env!r} unset and no api_key_file given. "
        f"Either export ${api_key_env} or set [<section>].api_key_file in config.toml."
    )
    raise RuntimeError(msg)


def _call_claude(
    prompt: str,
    *,
    model: str,
    system: str,
    api_key_env: str,
    api_key_file: str | None = None,
    timeout: int = 600,
) -> str:
    """Invoke `claude -p --bare` with an explicit API key (env or chmod-600 file)."""
    api_key = _resolve_key(api_key_env, api_key_file)

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


def _call_openai(
    prompt: str,
    *,
    model: str,
    system: str,
    base_url: str,
    api_key_env: str,
    api_key_file: str | None = None,
    timeout: int = 600,
    json_mode: bool = False,
) -> str:
    """POST to an OpenAI-compatible /chat/completions endpoint.

    Works with Ollama (`http://localhost:11434/v1`), llama.cpp `server`,
    LM Studio, vLLM, LocalAI, and OpenAI itself. Local servers usually accept
    any non-empty bearer token — `api_key_env` may be unset in that case.

    `json_mode=True` requests `response_format={"type": "json_object"}`, which
    the better local runtimes honour (llama.cpp grammar, vLLM guided JSON).
    """
    import urllib.error
    import urllib.request

    # Local servers usually accept any non-empty token. Only enforce a real key
    # when a file is configured or the env var holds something.
    try:
        api_key = _resolve_key(api_key_env, api_key_file)
    except RuntimeError:
        api_key = ""
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "stream": False,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    url = base_url.rstrip("/") + "/chat/completions"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key or 'sk-local'}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        msg = f"openai-compat HTTP {exc.code} from {url}: {detail}"
        raise RuntimeError(msg) from exc
    except urllib.error.URLError as exc:
        msg = f"openai-compat could not reach {url}: {exc.reason}"
        raise RuntimeError(msg) from exc

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        msg = f"openai-compat unexpected response shape: {str(data)[:500]}"
        raise RuntimeError(msg) from exc
    if not isinstance(content, str):
        msg = f"openai-compat content is not a string: {type(content).__name__}"
        raise RuntimeError(msg)
    return content


def _call_llm(
    prompt: str,
    *,
    backend: str,
    model: str,
    system: str,
    api_key_env: str,
    api_key_file: str | None,
    base_url: str | None,
    json_mode: bool = False,
) -> str:
    if backend == "claude":
        return _call_claude(
            prompt,
            model=model,
            system=system,
            api_key_env=api_key_env,
            api_key_file=api_key_file,
        )
    if backend == "openai":
        if not base_url:
            msg = "backend='openai' requires base_url (e.g. http://localhost:11434/v1)"
            raise RuntimeError(msg)
        return _call_openai(
            prompt,
            model=model,
            system=system,
            base_url=base_url,
            api_key_env=api_key_env,
            api_key_file=api_key_file,
            json_mode=json_mode,
        )
    msg = f"unknown backend: {backend!r} (expected 'claude' or 'openai')"
    raise RuntimeError(msg)


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
    backend: str = "claude",
    base_url: str | None = None,
    api_key_file: str | None = None,
) -> SummaryResult:
    """Run the two-pass summary. `extract_only=True` skips pass 2 (debugging).

    backend="claude" runs `claude -p --bare`. backend="openai" POSTs to an
    OpenAI-compatible /chat/completions endpoint (`base_url` required —
    e.g. http://localhost:11434/v1 for Ollama).
    """
    jsonl = render_digest_jsonl(conn, chat_jid, last=last)
    jsonl = _filter_jsonl_since(jsonl, since)
    msg_count = sum(1 for line in jsonl.splitlines() if line.strip())
    if msg_count == 0:
        return SummaryResult()

    extract_text = _call_llm(
        jsonl,
        backend=backend,
        model=model,
        system=PASS1_SYSTEM,
        api_key_env=api_key_env,
        api_key_file=api_key_file,
        base_url=base_url,
        json_mode=True,
    )
    try:
        extract = json.loads(_strip_json_fences(extract_text))
    except json.JSONDecodeError as exc:
        msg = f"pass 1 returned non-JSON: {exc}; head={extract_text[:300]!r}"
        raise RuntimeError(msg) from exc

    markdown = ""
    if not extract_only:
        markdown = _call_llm(
            json.dumps(extract, ensure_ascii=False, indent=2),
            backend=backend,
            model=model,
            system=PASS2_SYSTEM,
            api_key_env=api_key_env,
            api_key_file=api_key_file,
            base_url=base_url,
        )
    return SummaryResult(extract=extract, markdown=markdown, msg_count=msg_count)


def prepare_chat_ui_bundle(
    conn: sqlite3.Connection,
    chat_jid: str,
    *,
    last: int = 1000,
    since: str | None = None,
) -> tuple[str, str, int]:
    """Build the chat-UI artefacts. Returns (prompt_text, data_jsonl, msg_count)."""
    jsonl = render_digest_jsonl(conn, chat_jid, last=last)
    jsonl = _filter_jsonl_since(jsonl, since)
    msg_count = sum(1 for line in jsonl.splitlines() if line.strip())
    return build_chat_ui_prompt(), jsonl, msg_count


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
        link_or_copy(src, dst)
        copied += 1
    return copied, len(refs) - copied
