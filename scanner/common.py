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


def acquire_pipeline_lock():
    """Take an exclusive, non-blocking lock on state/.pipeline.lock.

    Returns the open file handle — the caller must keep a reference to it for
    the lifetime of the run (the lock is released when the handle is closed,
    i.e. at process exit). Raises SystemExit if another run holds the lock.
    """
    import fcntl

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_DIR / ".pipeline.lock"
    handle = open(lock_path, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise SystemExit("Another pipeline run is in progress")
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def step(name: str, fn, *args, **kwargs):
    """Run a pipeline step, print status. Returns (success, result)."""
    import time
    import traceback

    print(f"\n{'─' * 50}")
    print(f"▶  {name}")
    print(f"{'─' * 50}")
    t0 = time.time()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.time() - t0
        print(f"✓  {name} completed in {elapsed:.1f}s")
        return True, result
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"✗  {name} FAILED in {elapsed:.1f}s: {exc}")
        traceback.print_exc()
        return False, None
