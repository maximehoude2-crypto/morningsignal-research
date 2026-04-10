#!/usr/bin/env python3
"""
MorningSignal Weekly Digest Sender
Run at 6 PM Fridays (see README for launchd setup).

Usage:
    python3 run_weekly.py            # live send via Resend
    python3 run_weekly.py --dry-run  # save preview HTML, don't send
"""

import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

DRY_RUN = "--dry-run" in sys.argv


def main():
    from newsletter.weekly_digest import run_weekly_digest

    print(f"\n{'═' * 50}")
    print(f"  MorningSignal Weekly Digest")
    print(f"  {'[DRY RUN]' if DRY_RUN else '[LIVE SEND]'}")
    print(f"{'═' * 50}")

    t0 = time.time()
    ok = run_weekly_digest(dry_run=DRY_RUN)
    elapsed = time.time() - t0

    print(f"\n{'═' * 50}")
    if ok:
        print(f"  ✓ Weekly digest {'previewed' if DRY_RUN else 'sent'} in {elapsed:.1f}s")
        if DRY_RUN:
            print(f"  Preview → state/weekly_digest_preview.html")
    else:
        print(f"  ✗ Weekly digest FAILED in {elapsed:.1f}s")
    print(f"{'═' * 50}\n")
    return ok


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
