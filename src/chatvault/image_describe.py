"""Vision-derived descriptions of image messages via the Anthropic API.

Each row in ``image_descriptions`` covers one image message. The model
classifies the image and emits structured JSON; we store both a
human-readable summary (for digest/markdown rendering) and the full raw JSON
(for downstream processing). Two extra pieces of context are passed to the
model per call:

* ``poster_hint``  — the chat sender of *this* message. When the image is a
                     chat screenshot forwarded from another conversation, the
                     right-aligned side of that screenshot is this person.
* ``chat_context`` — a one-line caption/quoted-text snippet so the model can
                     disambiguate ambiguous content.

Stickers (mime ``image/webp`` or path under ``Media/WhatsApp Stickers/``) are
filtered out — they're noise.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Paths
from .db import transaction
from .identities import NameResolver

log = logging.getLogger(__name__)

_MEDIA_PREFIX = "Media/"

# What we ask the model to return. Kept verbatim in the prompt so the model
# knows the schema exactly.
_PROMPT_SYSTEM = """\
You receive ONE image shared inside a WhatsApp chat, plus minimal context about
the person who shared it. Classify and extract content. Output ONLY a single
JSON object — no prose, no markdown fences — with this shape:

{
  "kind": "chat_screenshot" | "social_profile" | "image",
  "summary": str,                 // 2-5 dense lines, content-bearing, no fluff
  "fragment_position": "top" | "middle" | "bottom" | "full" | null,
                                  // The card may continue above/below this screenshot.

  /* ─────────  kind == "chat_screenshot"  ───────── */
  "platform": str | null,         // chat/app name as visible (e.g. "WhatsApp",
                                  //  "iMessage", "Instagram DM"), or null.
  "transcript": [
    {
      "sender": str,              // the screenshot poster's name for right-aligned
                                  // bubbles (provided in user prompt as "me"),
                                  // visible name above their bubble for others,
                                  // or "other" when no name is shown.
      "text": str,                // verbatim, preserve emoji + punctuation
      "type": "text" | "image" | "voice" | "sticker" | "video" | "system",
      "time": str | null,         // visible timestamp if any
      "reactions": [str]          // emojis stuck to the bubble
    }
  ],
  "context_hints": [str],         // day labels, online status, time-of-day cues

  /* ─────────  kind == "social_profile"  ─────────
     Profile pages typically interleave PROMPT CARDS (question + answer),
     PHOTO CARDS, a VITALS TILE, and CAPTIONS. Capture all of them. */
  "platform": str | null,
  "name": str | null,
  "age": int | null,
  "vitals": {                     // vitals tile fields — null when not visible
    "height": str | null,
    "location": str | null,
    "hometown": str | null,
    "ethnicity": str | null,
    "religion": str | null,
    "politics": str | null,
    "languages": [str],
    "education": str | null,
    "job_title": str | null,
    "workplace": str | null,
    "kids": str | null,
    "wants_kids": str | null,
    "drinking": str | null,
    "smoking": str | null,
    "marijuana": str | null,
    "drugs": str | null,
    "pronouns": str | null,
    "relationship_type": str | null,
    "dating_intentions": str | null,
    "looking_for": str | null,
    "zodiac": str | null,
    "pets": str | null
  },
  "prompts": [                    // prompt cards (question + answer). ORDER MATTERS.
    {
      "question": str,            // verbatim prompt text
      "answer": str,              // verbatim answer text; for voice/video the
                                  //  icon label is fine ("voice prompt").
      "type": "text" | "voice" | "video" | "poll"
    }
  ],
  "photos": [                     // one entry per photo visible in this screenshot
    {
      "position": int,            // visual index within the screenshot (1-based)
      "description": str,         // what the photo actually shows (specific, not generic)
      "shows": [str],             // tags: "gym", "outdoors", "group", "pet", "drink",
                                  //  "child", "filter", "selfie", "professional shot", …
      "vibe": str | null          // photographic + social signal in one short phrase
    }
  ],
  "captions": [str],              // text captions attached to specific photos
  "other_text": [str],            // anything else visible (badges, "verified", etc.)
  "observations": [str],          // red flags, green flags, contradictions, humor,
                                  //  conversation hooks, off-putting bits. BE BLUNT.

  /* ─────────  kind == "image"  ───────── */
  "description": str,             // 1-2 sentences, specific
  "text_in_image": str | null,    // OCR of any visible text, verbatim
  "subjects": [str]               // people, objects, places visible
}

Rules:
- Be specific. Avoid generic phrases ("a picture of a person").
- Use the poster's name (provided in the user prompt) as the "me"/right-side
  sender in chat_screenshot.transcript. Never emit the literal token "me".
- For social profiles, nuance matters more than coverage. Capture humour,
  sarcasm, contradictions between prompts and photos, conversation hooks,
  and notable signals in `observations`. Transcribe prompt Q&A verbatim —
  these are the strongest content a reader needs.
- Photos and prompts INTERLEAVE on the page. Preserve the order they appear
  in the screenshot (top-to-bottom).
- If part of the card extends beyond the screenshot, set
  `fragment_position` accordingly so a future screenshot can be stitched.
- Transcribe chat screenshots verbatim; preserve emoji and punctuation.
- When a field doesn't apply or isn't visible, set it to null (or [] for lists).
  Do NOT invent.
- Output ONLY the JSON object. No prose, no markdown fences.
"""

# Mimes the Anthropic vision API accepts. Anything else we skip.
_VALID_MIMES = frozenset({"image/jpeg", "image/png", "image/gif", "image/webp"})


@dataclass(slots=True)
class ImageRow:
    message_id: str
    chat_jid: str
    ts: str
    sender_name: str
    file_path: str | None
    mirrored_path: str | None
    mime: str | None
    caption: str | None


@dataclass(slots=True)
class DescribeResult:
    candidates: int = 0
    described: int = 0
    skipped_missing: int = 0
    skipped_sticker: int = 0
    failed: int = 0


ProgressCb = Callable[[ImageRow, "Path | None", str], None]


def resolve_image_path(row: ImageRow, paths: Paths, live_root: Path | None) -> Path | None:
    """Mirror copy wins; falls back to live source. Same layout as audio resolver."""
    if row.mirrored_path:
        p = Path(row.mirrored_path)
        if p.exists():
            return p
    rel = row.file_path
    if not rel:
        return None
    mirror_rel = rel[len(_MEDIA_PREFIX) :] if rel.startswith(_MEDIA_PREFIX) else rel
    mirror = paths.media_dir / mirror_rel
    if mirror.exists():
        return mirror
    if live_root is not None:
        live = live_root / mirror_rel
        if live.exists():
            return live
    return None


def _is_sticker(file_path: str | None, mime: str | None) -> bool:
    if mime == "image/webp":
        return True
    if file_path and file_path.startswith("Media/WhatsApp Stickers/"):
        return True
    return False


def _build_query(
    chat_jid: str | None,
    *,
    force: bool,
    since: str | None,
    last: int | None,
) -> tuple[str, list[object]]:
    where = [
        "mm.mime LIKE 'image/%'",
        "(mm.file_path IS NOT NULL OR mm.mirrored_path IS NOT NULL)",
    ]
    params: list[object] = []
    if chat_jid is not None:
        where.append("m.chat_jid = ?")
        params.append(chat_jid)
    if since is not None:
        where.append("m.ts >= ?")
        params.append(since)
    if not force:
        where.append(
            "NOT EXISTS (SELECT 1 FROM image_descriptions d WHERE d.message_id = m.id)"
        )
    sql = (
        "SELECT m.id AS message_id, m.chat_jid, m.ts, m.sender_jid, m.from_me, "
        "       mm.file_path, mm.mirrored_path, mm.mime, mm.caption "
        "FROM messages m JOIN message_media mm ON mm.message_id = m.id "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY m.ts DESC"
    )
    if last is not None:
        sql += " LIMIT ?"
        params.append(last)
    return sql, params


def iter_image_candidates(
    conn: sqlite3.Connection,
    *,
    chat_jid: str | None = None,
    force: bool = False,
    since: str | None = None,
    last: int | None = None,
) -> Iterator[ImageRow]:
    """Yield image rows in scope, newest first. Stickers and non-vision mimes pre-filtered."""
    sql, params = _build_query(chat_jid, force=force, since=since, last=last)
    resolver = NameResolver(conn)
    for r in conn.execute(sql, params):
        if _is_sticker(r["file_path"], r["mime"]):
            continue
        if r["mime"] not in _VALID_MIMES:
            continue
        sender = resolver.resolve(r["sender_jid"], from_me=bool(r["from_me"]))
        yield ImageRow(
            message_id=r["message_id"],
            chat_jid=r["chat_jid"],
            ts=r["ts"],
            sender_name=sender,
            file_path=r["file_path"],
            mirrored_path=r["mirrored_path"],
            mime=r["mime"],
            caption=r["caption"],
        )


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _load_anthropic_client(api_key_env: str) -> Any:
    """Construct an anthropic.Anthropic client. Late import for optional dep."""
    api_key = os.environ.get(api_key_env)
    if not api_key:
        msg = (
            f"environment variable {api_key_env!r} is not set. "
            "Export a personal Anthropic API key from console.anthropic.com."
        )
        raise RuntimeError(msg)
    try:
        import anthropic  # type: ignore[import-not-found]
    except ImportError as exc:
        msg = (
            "The 'anthropic' package is required for image description. "
            "Install with: pip install 'chatvault[vision]'  or  pip install anthropic"
        )
        raise RuntimeError(msg) from exc
    return anthropic.Anthropic(api_key=api_key)


_MODEL_ALIASES = {
    "sonnet": "claude-sonnet-4-6",
    "opus": "claude-opus-4-7",
    "haiku": "claude-haiku-4-5",
}


def _resolve_model(model: str) -> str:
    return _MODEL_ALIASES.get(model, model)


def _describe_one(
    client: Any,
    *,
    image_bytes: bytes,
    mime: str,
    model: str,
    poster_hint: str,
    chat_context: str,
    max_tokens: int = 3072,
) -> dict[str, Any]:
    """Single vision call. Returns parsed JSON dict."""
    b64 = base64.standard_b64encode(image_bytes).decode("ascii")
    user_prompt = (
        f"The person who shared this image is: {poster_hint}.\n"
        f"Chat context (caption / surrounding hint): {chat_context or '(none)'}\n\n"
        "Classify and extract per the schema."
    )
    resp = client.messages.create(
        model=_resolve_model(model),
        max_tokens=max_tokens,
        system=_PROMPT_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {"type": "base64", "media_type": mime, "data": b64},
                    },
                    {"type": "text", "text": user_prompt},
                ],
            }
        ],
    )
    # Extract first text block.
    text_blocks = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
    if not text_blocks:
        msg = "vision response contained no text block"
        raise RuntimeError(msg)
    raw = text_blocks[0].strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    parsed: dict[str, Any] = json.loads(raw)
    if "kind" not in parsed or "summary" not in parsed:
        msg = f"vision response missing required keys: {list(parsed)}"
        raise RuntimeError(msg)
    return parsed


def _store(
    conn: sqlite3.Connection,
    *,
    row: ImageRow,
    src: Path,
    parsed: dict[str, Any],
    model: str,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO image_descriptions "
        "(message_id, image_path, kind, summary, raw_json, model, described_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            row.message_id,
            str(src),
            parsed["kind"],
            parsed["summary"],
            json.dumps(parsed, ensure_ascii=False),
            model,
            _now_iso(),
        ),
    )


def describe_all(
    conn: sqlite3.Connection,
    paths: Paths,
    *,
    model: str = "sonnet",
    chat_jid: str | None = None,
    since: str | None = None,
    last: int | None = None,
    force: bool = False,
    api_key_env: str = "ANTHROPIC_API_KEY",
    live_media_root: Path | None = None,
    progress: ProgressCb | None = None,
) -> DescribeResult:
    """Describe every image in scope that doesn't yet have a row in image_descriptions."""
    client = _load_anthropic_client(api_key_env)
    result = DescribeResult()

    candidates = list(
        iter_image_candidates(
            conn,
            chat_jid=chat_jid,
            force=force,
            since=since,
            last=last,
        )
    )
    result.candidates = len(candidates)

    for row in candidates:
        src = resolve_image_path(row, paths, live_media_root)
        if src is None:
            result.skipped_missing += 1
            if progress:
                progress(row, None, "missing")
            continue
        try:
            data = src.read_bytes()
            parsed = _describe_one(
                client,
                image_bytes=data,
                mime=row.mime or "image/jpeg",
                model=model,
                poster_hint=row.sender_name,
                chat_context=row.caption or "",
            )
        except Exception as exc:  # network / API / JSON errors all funnel here
            log.warning("describe failed for %s: %s", row.message_id, exc)
            result.failed += 1
            if progress:
                progress(row, src, f"failed: {exc}")
            continue
        with transaction(conn):
            _store(conn, row=row, src=src, parsed=parsed, model=model)
        result.described += 1
        if progress:
            progress(row, src, f"ok ({parsed['kind']})")
    return result
