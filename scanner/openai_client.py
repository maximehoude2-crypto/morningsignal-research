from __future__ import annotations

import json
import os
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
        _client = OpenAI(api_key=api_key)
    return _client


def complete_text(
    prompt: str,
    *,
    model: str | None = None,
    max_output_tokens: int = 4000,
    reasoning_effort: str | None = None,
    retries: int = 2,
) -> str:
    last_error: Exception | None = None

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
            last_error = exc
            if attempt >= retries:
                raise
            time.sleep(2 * (attempt + 1))

    raise RuntimeError(str(last_error or "OpenAI request failed"))


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
