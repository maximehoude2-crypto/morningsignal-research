from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env", override=False)

DEFAULT_MODEL = os.getenv("CLAUDE_MODEL", "claude-opus-4-8")
# Thinking depth / token spend. low | medium | high | max (Opus-tier).
DEFAULT_EFFORT = os.getenv("CLAUDE_EFFORT", "medium")

_client: Any | None = None


def claude_enabled() -> bool:
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def get_client():
    global _client
    if _client is None:
        from anthropic import Anthropic

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not configured.")
        _client = Anthropic(api_key=api_key)
    return _client


def complete_text(
    prompt: str,
    *,
    system: str | None = None,
    model: str | None = None,
    max_output_tokens: int = 16000,
    reasoning_effort: str | None = None,
    retries: int = 2,
) -> str:
    """Generate text from Claude via the Messages API.

    Streams the response (so large `max_output_tokens` values don't trip the
    SDK's non-streaming timeout guard) and returns the concatenated text blocks.
    When `system` is supplied it is sent as a cached prefix — a stable system
    prompt shared across calls is served from cache on repeat requests; the
    volatile `prompt` stays in the user turn and is never cached, so callers
    that pass a unique one-shot `prompt` pay no cache-write premium.
    """
    last_error: Exception | None = None

    system_blocks = None
    if system:
        system_blocks = [{
            "type": "text",
            "text": system,
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
        }]

    for attempt in range(retries + 1):
        try:
            kwargs: dict[str, Any] = dict(
                model=model or DEFAULT_MODEL,
                max_tokens=max_output_tokens,
                thinking={"type": "adaptive"},
                output_config={"effort": reasoning_effort or DEFAULT_EFFORT},
                messages=[{"role": "user", "content": prompt}],
            )
            if system_blocks is not None:
                kwargs["system"] = system_blocks

            with get_client().messages.stream(**kwargs) as stream:
                message = stream.get_final_message()

            text = "".join(
                block.text
                for block in message.content
                if getattr(block, "type", None) == "text"
            ).strip()
            if text:
                return text

            raise RuntimeError("Claude returned no text output.")
        except Exception as exc:  # pragma: no cover - provider/network failures
            last_error = exc
            if attempt >= retries:
                raise
            time.sleep(2 * (attempt + 1))

    raise RuntimeError(str(last_error or "Claude request failed"))


def extract_json(text: str) -> dict:
    raw = text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(raw[start:end + 1])
