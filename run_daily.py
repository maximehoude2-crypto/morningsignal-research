#!/usr/bin/env python3
"""
MorningSignal Daily Orchestrator
Run at 4:30 PM weekdays (see README for launchd setup).

Usage:
    python3 run_daily.py            # live run
    python3 run_daily.py --dry-run  # mock data, no yfinance, no git push
"""

import sys
import time
import traceback
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

# 'site' is a stdlib module; remove it so our local site/ package is found instead
sys.modules.pop("site", None)

DRY_RUN = "--dry-run" in sys.argv


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
    except Exception as e:
        elapsed = time.time() - t0
        print(f"✗  {name} FAILED in {elapsed:.1f}s: {e}")
        traceback.print_exc()
        return False, None


def main():
    start = time.time()
    today = date.today().isoformat()
    mode = "[DRY RUN]" if DRY_RUN else "[LIVE]"
    print(f"\n{'═' * 50}")
    print(f"  MorningSignal Daily Run {mode}")
    print(f"  Date: {today}")
    print(f"{'═' * 50}")

    results = {}

    # ── Step 1: Market Brief ───────────────────────────────────────────────
    from scanner.market_brief import run_market_brief
    ok1, brief = step("Market Brief", run_market_brief, dry_run=DRY_RUN)
    results["brief"] = ok1

    # ── Step 2: Breakout Scanner ───────────────────────────────────────────
    from scanner.breakout_scanner import run_scanner
    ok2, breakouts = step("Breakout Scanner", run_scanner, dry_run=DRY_RUN)
    results["scanner"] = ok2

    if not ok2:
        print("\n  ⚠  Scanner failed — will use yesterday's breakout data if available")

    # ── Step 3: Site Generation ────────────────────────────────────────────
    # Use importlib to avoid shadowing by stdlib 'site' module
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location("_site_gen", BASE_DIR / "site" / "generate_site.py")
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    generate_site = _mod.generate_site
    ok3, _ = step("Site Generation", generate_site, today if (ok1 and ok2) else None)
    results["site"] = ok3

    # ── Step 4: Deploy to GitHub ───────────────────────────────────────────
    from deploy.push_to_github import deploy
    ok4, _ = step("Deploy to GitHub", deploy, dry_run=DRY_RUN)
    results["deploy"] = ok4

    # ── Summary ───────────────────────────────────────────────────────────
    elapsed = time.time() - start
    n_breakouts = len(breakouts) if breakouts else 0
    print(f"\n{'═' * 50}")
    print(f"  MorningSignal update complete in {elapsed:.1f}s")
    print(f"  {n_breakouts} breakout setups found")
    for name, ok in results.items():
        icon = "✓" if ok else "✗"
        print(f"  {icon} {name}")
    if ok3:
        print(f"\n  Site → docs/index.html")
    if ok4 and not DRY_RUN:
        print(f"  Live → https://research.morningsignal.xyz")
    print(f"{'═' * 50}\n")

    return all(results.values())


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
