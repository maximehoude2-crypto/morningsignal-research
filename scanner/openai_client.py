from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env", override=False)

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")
DEFAULT_REASONING = os.getenv("OPENAI_REASONING_EFFORT", "medium")

_client: Any | None = None


def openai_enabled() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


def get_client():
    global _client
    if _client is None:
        from openai import OpenAI

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        # Retries are handled in complete_text — disable the SDK's internal
        # retries so they don't multiply with ours, and bound each request.
        _client = OpenAI(api_key=api_key, timeout=300.0, max_retries=0)
    return _client


def _is_retryable(exc: Exception) -> bool:
    """Only rate limits, connection issues, timeouts, and 5xx are retryable."""
    try:
        import openai
    except ImportError:  # pragma: no cover
        return False
    if isinstance(exc, (openai.RateLimitError, openai.APIConnectionError, openai.APITimeoutError)):
        return True
    if isinstance(exc, openai.APIStatusError):
        return exc.status_code >= 500
    return False


def complete_text(
    prompt: str,
    *,
    model: str | None = None,
    max_output_tokens: int = 4000,
    reasoning_effort: str | None = None,
    retries: int = 2,
) -> str:
    for attempt in range(retries + 1):
        try:
            response = get_client().responses.create(
                model=model or DEFAULT_MODEL,
                input=prompt,
                max_output_tokens=max_output_tokens,
                reasoning={"effort": reasoning_effort or DEFAULT_REASONING},
            )

            text = getattr(response, "output_text", None)
            if text:
                return text.strip()

            fragments: list[str] = []
            for item in getattr(response, "output", []) or []:
                if getattr(item, "type", None) != "message":
                    continue
                for content in getattr(item, "content", []) or []:
                    if getattr(content, "type", None) in {"output_text", "text"}:
                        fragments.append(getattr(content, "text", ""))

            merged = "\n".join(part for part in fragments if part).strip()
            if merged:
                return merged

            raise RuntimeError("OpenAI returned no text output.")
        except Exception as exc:  # pragma: no cover - provider/network failures
            # Fail fast on deterministic errors (auth, 400, context length…).
            if attempt >= retries or not _is_retryable(exc):
                raise
            time.sleep(2 * (attempt + 1))


def extract_json(text: str) -> dict:
    raw = text.strip()
    # Strip the first fenced code block (```json … ``` or ``` … ```), if any.
    fence = re.search(r"```(?:json)?\s*\n(.*?)```", raw, re.DOTALL)
    if fence:
        raw = fence.group(1).strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        parsed = json.loads(raw[start:end + 1])

    if not isinstance(parsed, dict):
        raise ValueError(f"Expected a JSON object, got {type(parsed).__name__}")
    return parsed
