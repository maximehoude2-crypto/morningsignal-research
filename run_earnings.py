#!/usr/bin/env python3
"""
Standalone earnings-brief generator.

Generates the AM/PM earnings deep dives and writes them as Obsidian notes
(see scanner/obsidian_export.py). This is the entrypoint used by the cloud
schedule — it does not touch the website or the rest of the daily pipeline.

Usage:
    python3 run_earnings.py                      # today, AM + PM
    python3 run_earnings.py --sessions AM        # today, AM only
    python3 run_earnings.py --date 2026-06-02    # a specific date
    python3 run_earnings.py --regenerate         # overwrite existing briefs
"""

import argparse
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))


def parse_args():
    parser = argparse.ArgumentParser(description="Generate earnings briefs into the Obsidian vault.")
    parser.add_argument("--date", default=None, help="Date to process (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--sessions", default="AM,PM", help="Comma-separated sessions to generate (AM, PM).")
    parser.add_argument("--regenerate", action="store_true", help="Regenerate even if a brief already exists.")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date or date.today().isoformat()
    sessions = tuple(part.strip().upper() for part in args.sessions.split(",") if part.strip())

    invalid = sorted(set(sessions) - {"AM", "PM"})
    if invalid:
        raise SystemExit(f"Invalid session(s): {', '.join(invalid)}. Use AM and/or PM.")

    print(f"Generating earnings briefs for {target_date} ({', '.join(sessions)})")

    from scanner.earnings_sync import sync_earnings

    # Raises EarningsDataError if the calendar source is down, so the cloud job
    # fails loudly instead of silently committing nothing.
    sync_earnings(target_date, sessions=sessions, regenerate=args.regenerate)
    return 0


if __name__ == "__main__":
    sys.exit(main())
