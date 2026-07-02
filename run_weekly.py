#!/usr/bin/env python3
"""
MorningSignal Weekly Digest Sender
Run at 6 PM Fridays (see README for launchd setup).

Usage:
    python3 run_weekly.py            # live send via Resend
    python3 run_weekly.py --dry-run  # save preview HTML, don't send
"""

import argparse
import inspect
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from scanner.common import acquire_pipeline_lock


def parse_args():
    parser = argparse.ArgumentParser(description="Send the MorningSignal weekly digest.")
    parser.add_argument(
        "--date",
        default=None,
        help="Reference date for the digest week (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Save a preview HTML instead of sending via Resend.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    # Held for the life of the process; prevents overlapping pipeline runs.
    _lock = acquire_pipeline_lock()

    from newsletter.weekly_digest import run_weekly_digest

    print(f"\n{'═' * 50}")
    print(f"  MorningSignal Weekly Digest")
    print(f"  {'[DRY RUN]' if args.dry_run else '[LIVE SEND]'}")
    if args.date:
        print(f"  Date: {args.date}")
    print(f"{'═' * 50}")

    t0 = time.time()
    ok = False
    error = None
    try:
        kwargs = {"dry_run": args.dry_run}
        if args.date:
            # Pass the reference date through when the digest supports it.
            if "target_date" in inspect.signature(run_weekly_digest).parameters:
                kwargs["target_date"] = args.date
            else:
                print("  Warning: run_weekly_digest does not accept a date; --date ignored")
        ok = run_weekly_digest(**kwargs)
    except Exception as exc:
        error = exc
        import traceback
        traceback.print_exc()
    elapsed = time.time() - t0

    print(f"\n{'═' * 50}")
    if ok:
        print(f"  ✓ Weekly digest {'previewed' if args.dry_run else 'sent'} in {elapsed:.1f}s")
        if args.dry_run:
            print(f"  Preview → state/weekly_digest_preview.html")
    else:
        print(f"  ✗ Weekly digest FAILED in {elapsed:.1f}s")
    print(f"{'═' * 50}\n")

    if not ok and not args.dry_run:
        from scanner.alerts import send_failure_alert

        detail = f"Error: {error}" if error else "run_weekly_digest returned falsy."
        send_failure_alert(
            "Weekly digest failure",
            f"The weekly digest run failed.\n{detail}\nSee the run log for details.",
        )

    return ok


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
