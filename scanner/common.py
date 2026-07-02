"""
Shared helpers used across scanner modules: date resolution, atomic state
JSON IO, and the common scraping User-Agent.
"""

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def resolve_target_date(target_date: str | None = None) -> str:
    """Return target_date if given, else the most recent weekday (ISO)."""
    if target_date:
        return target_date
    d = date.today()
    while d.isoweekday() > 5:
        d -= timedelta(days=1)
    return d.isoformat()


def save_json(path: Path, payload) -> None:
    """Atomically write JSON: write to a temp file, then os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str))
    os.replace(tmp, path)


def load_json(path: Path, default=None):
    """Read JSON, returning `default` on missing or corrupt files."""
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  Warning: could not read {path}: {exc}")
        return default
