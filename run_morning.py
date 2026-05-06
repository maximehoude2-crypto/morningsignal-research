#!/usr/bin/env python3
"""
MorningSignal pre-market refresh.

Usage:
    python3 run_morning.py            # live run
    python3 run_morning.py --dry-run  # preview actions without OpenAI or git push
"""

import argparse
import sys
import time
import traceback
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

# 'site' is a stdlib module; remove it so our local site/ package is found instead
sys.modules.pop("site", None)


def step(name: str, fn, *args, **kwargs):
    """Run a step, print status. Returns (success, result)."""
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


def parse_args():
    parser = argparse.ArgumentParser(description="Run the MorningSignal pre-market refresh.")
    parser.add_argument(
        "--date",
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the morning run without OpenAI generation or git push.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date or date.today().isoformat()
    mode = "[DRY RUN]" if args.dry_run else "[LIVE]"

    print(f"\n{'═' * 50}")
    print(f"  MorningSignal Morning Refresh {mode}")
    print(f"  Date: {target_date}")
    print(f"{'═' * 50}")

    results = {}
    state_earnings = BASE_DIR / "state" / "earnings" / f"earnings_{target_date}_AM.md"

    def run_am_earnings():
        if args.dry_run:
            print(f"  [dry-run] Would generate AM earnings brief → {state_earnings}")
            return state_earnings

        from scanner.earnings_sync import generate_earnings_brief

        return generate_earnings_brief(target_date, "AM")

    ok1, earnings_path = step("AM Earnings Brief", run_am_earnings)
    results["earnings_am"] = ok1

    def run_site():
        import importlib.util as _ilu

        spec = _ilu.spec_from_file_location("_site_gen", BASE_DIR / "site" / "generate_site.py")
        mod = _ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.generate_site(target_date)

    ok2, _ = step("Site Generation", run_site)
    results["site"] = ok2

    from deploy.push_to_github import deploy

    ok3, _ = step("Deploy to GitHub", deploy, dry_run=args.dry_run)
    results["deploy"] = ok3

    print(f"\n{'═' * 50}")
    print("  Morning refresh complete")
    for name, ok in results.items():
        icon = "✓" if ok else "✗"
        print(f"  {icon} {name}")
    if earnings_path:
        print(f"  AM brief → {earnings_path}")
    if ok3 and not args.dry_run:
        print("  Live → https://research.morningsignal.xyz")
    print(f"{'═' * 50}\n")

    return all(results.values())


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
