#!/usr/bin/env python3
"""
MorningSignal Daily Orchestrator
Run at 4:30 PM weekdays (see README for launchd setup).

Usage:
    python3 run_daily.py            # live run
    python3 run_daily.py --dry-run  # mock data, no yfinance, no git push
"""

import argparse
import json
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
STATE_DIR = BASE_DIR / "state"
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
    except Exception as e:
        elapsed = time.time() - t0
        print(f"✗  {name} FAILED in {elapsed:.1f}s: {e}")
        traceback.print_exc()
        return False, None


def parse_args():
    parser = argparse.ArgumentParser(description="Run the MorningSignal daily pipeline.")
    parser.add_argument(
        "--date",
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Use mock/deferred actions where supported and skip git push.",
    )
    parser.add_argument(
        "--earnings-sessions",
        default="AM,PM",
        help="Comma-separated earnings sessions to generate (AM, PM). Defaults to both.",
    )
    parser.add_argument(
        "--refresh-earnings",
        action="store_true",
        help="Regenerate the selected earnings sessions even if files already exist.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start = time.time()
    today = args.date or date.today().isoformat()
    mode = "[DRY RUN]" if args.dry_run else "[LIVE]"
    sessions = tuple(
        part.strip().upper()
        for part in args.earnings_sessions.split(",")
        if part.strip()
    )
    invalid_sessions = sorted(set(sessions) - {"AM", "PM"})
    if invalid_sessions:
        raise SystemExit(
            f"Invalid earnings session(s): {', '.join(invalid_sessions)}. Use AM and/or PM."
        )

    print(f"\n{'═' * 50}")
    print(f"  MorningSignal Daily Run {mode}")
    print(f"  Date: {today}")
    print(f"{'═' * 50}")

    results = {}
    write_state_dir = STATE_DIR / "dry_run" if args.dry_run else STATE_DIR
    write_state_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Market Brief ───────────────────────────────────────────────
    from scanner.market_brief import run_market_brief
    ok1, brief = step("Market Brief", run_market_brief, dry_run=args.dry_run, target_date=today)
    results["brief"] = ok1

    # ── Step 1b: Thematic Scan ────────────────────────────────────────────
    from scanner.thematic_scanner import run_thematic_scan
    ok1b, thematic = step("Thematic Scan", run_thematic_scan, dry_run=args.dry_run)
    results["thematic"] = ok1b
    if ok1b and ok1 and brief and thematic:
        brief.update(thematic)
        out_path = write_state_dir / f"market_brief_{today}.json"
        out_path.write_text(json.dumps(brief, indent=2, default=str))

    # ── Step 2: Breakout Scanner ───────────────────────────────────────────
    from scanner.breakout_scanner import run_scanner
    ok2, breakouts = step("Breakout Scanner", run_scanner, dry_run=args.dry_run, target_date=today)
    results["scanner"] = ok2

    if not ok2:
        print("\n  ⚠  Scanner failed — will use yesterday's breakout data if available")

    # ── Step 2a: Industry / Sub-Sector Scanner ─────────────────────────────
    # Runs after the breakout scanner so the parquet price cache is already warm.
    from scanner.industry_scanner import run_industry_scan
    ok2a, industries = step("Industry Scan", run_industry_scan, dry_run=args.dry_run, target_date=today)
    results["industries"] = ok2a

    # Merge industry data into the brief so the narrative step can use it
    if ok2a and ok1 and brief and industries:
        brief["industries"] = industries
        out_path = write_state_dir / f"market_brief_{today}.json"
        out_path.write_text(json.dumps(brief, indent=2, default=str))
        # Re-run the narrative now that we have richer context.
        # If OpenAI is configured this regenerates a full LLM narrative; otherwise
        # we rebuild the deterministic fallback so it now includes the industry
        # rotation / MA-event bullets.
        try:
            from scanner.market_brief import (
                _generate_narrative, fallback_narrative, narrative_has_content,
            )
            print("\n  Regenerating narrative with industry context...")
            new_narr = None if args.dry_run else _generate_narrative(brief)
            current = brief.get("narrative") or {}
            current_is_fallback = (
                isinstance(current, dict) and current.get("source") == "fallback"
            )
            if narrative_has_content(new_narr):
                brief["narrative"] = new_narr
                out_path.write_text(json.dumps(brief, indent=2, default=str))
                print("  ✓ Narrative refreshed (LLM with industry data)")
            elif current_is_fallback or not narrative_has_content(current):
                brief["narrative"] = fallback_narrative(brief)
                out_path.write_text(json.dumps(brief, indent=2, default=str))
                print("  ✓ Fallback narrative rebuilt with industry data")
            else:
                print("  Narrative regen returned empty; keeping prior narrative.")
        except Exception as exc:
            print(f"  Narrative regen skipped: {exc}")

    # ── Step 2b: Dashboard Data Aggregator ─────────────────────────────────
    # Computes 52w highs/lows, regime score, cross-asset grid, style box,
    # crowdedness — feeds the dashboard page.
    from scanner.dashboard_data import run_dashboard_data
    ok2b, dashboard = step("Dashboard Data", run_dashboard_data, dry_run=args.dry_run, target_date=today)
    results["dashboard"] = ok2b

    # ── Step 2bb: InvestorDebate Index ────────────────────────────────────
    # Parses any new reports under state/investordebate/ and refreshes the
    # consolidated index + per-sector diffs.
    from scanner.investordebate_index import run_investordebate_index
    ok2bb, _id_index = step("InvestorDebate Index", run_investordebate_index, dry_run=args.dry_run)
    results["investordebate"] = ok2bb

    # ── Step 2c: News Intelligence ────────────────────────────────────────
    # Multi-source headline scrape + theme/sector tagging.
    from scanner.news_intelligence import run_news_intelligence
    ok2c, news = step("News Intelligence", run_news_intelligence, dry_run=args.dry_run, target_date=today)
    results["news"] = ok2c

    # Merge news into the brief so narrative/templates can reference it
    if ok2c and brief and news:
        brief["news"] = news
        out_path = write_state_dir / f"market_brief_{today}.json"
        out_path.write_text(json.dumps(brief, indent=2, default=str))
        # Refresh the fallback narrative now that we have news context
        try:
            from scanner.market_brief import (
                _generate_narrative, fallback_narrative, narrative_has_content,
            )
            new_narr = None if args.dry_run else _generate_narrative(brief)
            current = brief.get("narrative") or {}
            current_is_fallback = (
                isinstance(current, dict) and current.get("source") == "fallback"
            )
            if narrative_has_content(new_narr):
                brief["narrative"] = new_narr
                out_path.write_text(json.dumps(brief, indent=2, default=str))
                print("  ✓ Narrative refreshed (LLM with news + industry context)")
            elif current_is_fallback or not narrative_has_content(current):
                brief["narrative"] = fallback_narrative(brief)
                out_path.write_text(json.dumps(brief, indent=2, default=str))
                print("  ✓ Fallback narrative rebuilt with news + industry data")
        except Exception as exc:
            print(f"  Narrative refresh skipped: {exc}")

    # ── Step 2b: Weekly Summary (Friday only) ──────────────────────────────
    if datetime.strptime(today, "%Y-%m-%d").date().isoweekday() == 5:  # Friday
        from scanner.weekly_summary import run_weekly_summary
        ok_weekly, _ = step("Weekly Summary", run_weekly_summary)
        results["weekly"] = ok_weekly

    # ── Step 2c: Earnings Briefs ───────────────────────────────────────────
    from scanner.earnings_sync import sync_earnings
    ok_earnings, _ = step(
        "Earnings Briefs",
        sync_earnings,
        today,
        sessions=sessions,
        dry_run=args.dry_run,
        regenerate=args.refresh_earnings,
    )
    results["earnings"] = ok_earnings

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
    ok4, _ = step("Deploy to GitHub", deploy, dry_run=args.dry_run)
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
    if ok4 and not args.dry_run:
        print(f"  Live → https://research.morningsignal.xyz")
    print(f"{'═' * 50}\n")

    return all(results.values())


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
