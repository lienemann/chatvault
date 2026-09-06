# Summarising chats with a local LLM

`chatvault chat summarize` supports two backends:

| `--backend` | Calls | Needs |
|---|---|---|
| `claude` (default) | `claude -p --bare` subprocess | `ANTHROPIC_API_KEY` |
| `openai` | HTTP POST to `{base-url}/chat/completions` | OpenAI-compatible server |

The `openai` backend talks to anything that speaks the OpenAI chat-completions
shape: Ollama, llama.cpp `server`, vLLM, LM Studio, LocalAI, or OpenAI itself.
No new Python dependency — uses stdlib `urllib`.

## Quick start — Ollama on a desktop, phone hits it over LAN

```sh
# On the desktop (one-off)
ollama pull qwen2.5:7b-instruct-q4_K_M
ollama serve                                   # listens on :11434

# From the phone (or wherever chatvault lives)
chatvault chat summarize "Birthday Alice" \
    --backend openai \
    --base-url http://<desktop-ip>:11434/v1 \
    --model qwen2.5:7b-instruct-q4_K_M \
    --last 200 \
    -o summary
```

Local servers usually accept any non-empty bearer token, so `--api-key-env` can
be left at its default (the request still sends `Authorization: Bearer sk-local`
if the env var is unset).

## On-device on a Samsung Galaxy S25

The S25 line ships with 12 GB RAM and the Snapdragon 8 Elite for Galaxy. After
Android reserves its share, ~7 GB is realistically free for an LLM process.

Recommended on-device model: **Qwen2.5-7B-Instruct Q4_K_M** (~4.7 GB on disk).

Why this one:
- Honours `response_format={"type":"json_object"}` reliably — pass 1 needs
  strict JSON, and weaker 7B models drift here.
- Multilingual (German + English chats work).
- Fits in RAM at 4 K context with room for the OS.

Setup via Termux + Ollama (proot-distro):

```sh
pkg install proot-distro
proot-distro install debian
proot-distro login debian
# inside debian:
curl -fsSL https://ollama.com/install.sh | sh
ollama serve &
ollama pull qwen2.5:7b-instruct-q4_K_M
```

Then from the Termux side:

```sh
chatvault chat summarize "John" \
    --backend openai \
    --base-url http://127.0.0.1:11434/v1 \
    --model qwen2.5:7b-instruct-q4_K_M \
    --last 150
```

### Caveats on phone

- **Context window**: 7 B-class local models give ~8–32 K usable tokens. Long
  chats blow that. Use `--last 100…300`, not 1000.
- **Speed**: pass 1 on a 200-message chat takes ~1–3 min on the S25 CPU. The
  NPU is not used (Ollama is CPU/GPU on Android; Qualcomm's NPU runtime is not
  OpenAI-compatible).
- **Quality**: noticeable drop vs Claude Opus on `decisions` / `opinions`
  consolidation. `links` / `tips` / `images` extraction holds up well.
- **Battery + thermals**: sustained inference will throttle. Plug in.

If quality matters more than locality, run the model on a desktop / NAS and
point `--base-url` at it from the phone — same command, network round-trip is
negligible compared with token generation.

## Bigger boxes

If you have a desktop GPU with ≥24 GB VRAM, swap to a 14 B–32 B model — same
flags, different `--model`. Suggestions:

| Hardware | Model |
|---|---|
| 16 GB VRAM | `qwen2.5:14b-instruct-q4_K_M` |
| 24 GB VRAM | `qwen2.5:32b-instruct-q4_K_M` |
| Apple Silicon (≥32 GB unified) | `qwen2.5:32b-instruct-q4_K_M` via Ollama |
| OpenAI proper | `--base-url https://api.openai.com/v1 --model gpt-4o-mini` (set `OPENAI_API_KEY` and `--api-key-env OPENAI_API_KEY`) |

## JSON-mode behaviour

Pass 1 sends `response_format={"type":"json_object"}`. Servers that don't
recognise the field ignore it; servers that do (llama.cpp grammar, vLLM guided
JSON, Ollama recent) constrain output, which is what keeps the pipeline
non-flaky on smaller models. If you see `pass 1 returned non-JSON`, switch to
a server build that supports constrained decoding or use a bigger model.
