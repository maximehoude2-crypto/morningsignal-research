#!/usr/bin/env python3
"""
MorningSignal pre-market refresh.

Usage:
    python3 run_morning.py            # live run
    python3 run_morning.py --dry-run  # preview actions without OpenAI or git push
"""

import argparse
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

# 'site' is a stdlib module; remove it so our local site/ package is found instead
sys.modules.pop("site", None)

from scanner.common import acquire_pipeline_lock, step


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
    # Held for the life of the process; prevents overlapping pipeline runs.
    _lock = acquire_pipeline_lock()
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

    # Deploy is gated on site generation: never push a half-written docs/.
    ok3 = None
    if ok2:
        from deploy.push_to_github import deploy

        ok3, _ = step("Deploy to GitHub", deploy, dry_run=args.dry_run)
        results["deploy"] = ok3
    else:
        print("\n  ⚠  Skipping Deploy — site generation failed")

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

    # Consolidated failure alert (skipped in dry-run)
    failed_steps = [name for name, ok in results.items() if not ok]
    if failed_steps and not args.dry_run:
        from scanner.alerts import send_failure_alert

        send_failure_alert(
            f"Morning refresh failure ({target_date}): {', '.join(failed_steps)}",
            "The following morning refresh steps failed:\n"
            + "\n".join(f"  - {name}" for name in failed_steps)
            + f"\n\nRun date: {target_date}\nSee the run log for tracebacks.",
        )

    return all(results.values())


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
