"""
Weekly Summary Generator — aggregates Mon-Fri daily briefs into a
1-2 page institutional weekly report, synthesized via OpenAI.
Runs every Friday as part of the daily pipeline.
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path

from scanner.openai_client import complete_text, extract_json, openai_enabled

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"


def _get_week_dates(ref_date: date = None) -> list[date]:
    """Get Mon-Fri dates for the week containing ref_date."""
    if ref_date is None:
        ref_date = date.today()
    # Find Monday of this week
    monday = ref_date - timedelta(days=ref_date.weekday())
    return [monday + timedelta(days=i) for i in range(5)]


def _load_daily_briefs(week_dates: list[date]) -> list[dict]:
    """Load all available daily briefs for the given dates."""
    briefs = []
    for d in week_dates:
        path = STATE_DIR / f"market_brief_{d.isoformat()}.json"
        if path.exists():
            try:
                brief = json.loads(path.read_text())
                briefs.append(brief)
            except Exception:
                pass
    return briefs


def _aggregate_week_data(briefs: list[dict]) -> dict:
    """Aggregate daily data into weekly summaries."""
    if not briefs:
        return {}

    # Index performance across the week
    first = briefs[0]
    last = briefs[-1]

    # Weekly index returns (last day vs first day's previous close, approximated by day changes)
    weekly_indices = {}
    for sym in ["SPY", "QQQ", "IWM", "DIA"]:
        cumulative = 1.0
        for b in briefs:
            day_chg = b.get("indices", {}).get(sym, {}).get("day_change", 0)
            cumulative *= (1 + day_chg / 100)
        weekly_indices[sym] = {
            "name": first.get("indices", {}).get(sym, {}).get("name", sym),
            "weekly_change": round((cumulative - 1) * 100, 2),
            "last_price": last.get("indices", {}).get(sym, {}).get("price", 0),
        }

    # Sector performance across the week
    sector_weekly = {}
    for b in briefs:
        for s in b.get("sectors", []):
            name = s["name"]
            if name not in sector_weekly:
                sector_weekly[name] = {"cumulative": 1.0, "symbol": s.get("symbol", "")}
            sector_weekly[name]["cumulative"] *= (1 + s["day_change"] / 100)
    sectors = []
    for name, data in sector_weekly.items():
        sectors.append({
            "name": name,
            "symbol": data["symbol"],
            "weekly_change": round((data["cumulative"] - 1) * 100, 2),
        })
    sectors.sort(key=lambda x: x["weekly_change"], reverse=True)

    # Collect all daily narratives
    daily_narratives = []
    for b in briefs:
        narr = b.get("narrative", {})
        if isinstance(narr, dict) and narr.get("bullets"):
            daily_narratives.append({
                "date": b.get("date", ""),
                "summary": narr.get("summary", ""),
                "bullets": narr.get("bullets", []),
                "overnight": narr.get("overnight", ""),
                "cross_sector": narr.get("cross_sector", ""),
            })

    # Top weekly gainers/losers across all days
    all_gainers = []
    all_losers = []
    for b in briefs:
        for g in b.get("top_gainers", []):
            all_gainers.append({**g, "date": b.get("date", "")})
        for l in b.get("top_losers", []):
            all_losers.append({**l, "date": b.get("date", "")})
    all_gainers.sort(key=lambda x: x.get("day_change", 0), reverse=True)
    all_losers.sort(key=lambda x: x.get("day_change", 0))

    # Macro at week end
    macro = last.get("macro", {})

    # Market signal at week end
    signal = last.get("market_signal", {})

    # Factor performance at week end
    factors = last.get("factors", {}).get("performance", [])

    # Thematic ETFs at week end
    thematic = last.get("thematic_etfs", [])

    return {
        "week_start": briefs[0].get("date", ""),
        "week_end": briefs[-1].get("date", ""),
        "trading_days": len(briefs),
        "indices": weekly_indices,
        "sectors": sectors,
        "macro": macro,
        "signal": signal,
        "factors": factors,
        "thematic": thematic,
        "daily_narratives": daily_narratives,
        "top_gainers": all_gainers[:10],
        "top_losers": all_losers[:10],
    }


def _generate_weekly_narrative(agg: dict) -> dict:
    """Call OpenAI to synthesize a weekly summary from aggregated data."""

    indices_str = "\n".join(
        f"  - {v['name']}: {v['weekly_change']:+.2f}% (close: ${v['last_price']})"
        for v in agg.get("indices", {}).values()
    )

    sectors_str = "\n".join(
        f"  - {s['name']}: {s['weekly_change']:+.2f}%"
        for s in agg.get("sectors", [])
    )

    daily_str = ""
    for d in agg.get("daily_narratives", []):
        daily_str += f"\n### {d['date']}\n{d['summary']}\n"
        for b in d.get("bullets", []):
            daily_str += f"  - {b['sector']} ({b['change']:+.1f}%): {b['narrative']}\n"
        if d.get("cross_sector"):
            daily_str += f"  Cross-sector: {d['cross_sector']}\n"

    gainers_str = ", ".join(
        f"{g['ticker']} {g['day_change']:+.2f}% ({g['date']})"
        for g in agg.get("top_gainers", [])[:8]
    )

    losers_str = ", ".join(
        f"{l['ticker']} {l['day_change']:+.2f}% ({l['date']})"
        for l in agg.get("top_losers", [])[:8]
    )

    signal = agg.get("signal", {})
    factor_str = "\n".join(
        f"  - {f['name']}: 1d {f.get('1d', 0):+.2f}%, 5d {f.get('5d', 0):+.2f}%, MTD {f.get('mtd', 0):+.2f}%"
        for f in agg.get("factors", [])
    )

    # Build thematic ETF context
    thematic_str = ""
    for e in agg.get("thematic", []):
        thematic_str += f"  - {e['symbol']} ({e['name']}, {e.get('category','')}): 1d {e.get('1d',0):+.2f}%, 5d {e.get('5d',0):+.2f}%, MTD {e.get('mtd',0):+.2f}%\n"

    prompt = f"""You are a senior thematic portfolio strategist at a top-tier investment firm writing the weekly intelligence report. This covers the week of {agg.get('week_start', '')} to {agg.get('week_end', '')} ({agg.get('trading_days', 0)} trading days). Your audience is financial advisers who manage thematic and sector-focused portfolios.

## WEEKLY INDEX PERFORMANCE
{indices_str}

## WEEKLY SECTOR PERFORMANCE (best to worst)
{sectors_str}

## THEMATIC ETF PERFORMANCE (end of week)
{thematic_str}

## COMPLETE DAILY NARRATIVES FROM THIS WEEK
(These contain the specific events, catalysts, earnings, and data points that moved markets each day)
{daily_str}

## BIGGEST SINGLE-DAY MOVERS THIS WEEK
Top gainers: {gainers_str}
Top losers: {losers_str}

## FACTOR PERFORMANCE (end of week)
{factor_str}

## MARKET SIGNAL (end of week)
Signal: {signal.get('signal', 'N/A')} (score: {signal.get('score', 0):+.3f})

## TASK
Produce a JSON object with this structure. Return ONLY raw JSON — no markdown, no code fences.

{{
  "headline": "One punchy sentence — the week's defining narrative. E.g. 'Anthropic Mythos Splits Tech in Two: Software Reckoning Meets Semi Supercycle'",

  "executive_summary": "5-6 paragraph executive summary (~500 words) that tells the STORY of this week as a coherent narrative arc. Structure it as: (1) Open with the single most important event/catalyst of the week and its market impact, (2) How that catalyst rippled across sectors — name specific stocks and their moves, (3) Key earnings or economic data releases and what they signaled, (4) The thematic rotation story — which investment themes gained/lost traction and why (AI infra vs SaaS, private credit risk, energy transition, etc.), (5) Cross-asset context: what rates, credit, vol, and factor rotation tell us about market regime, (6) What this means for thematic portfolio positioning going into next week. Be SPECIFIC — name companies, cite numbers, reference actual events from the daily narratives. This should read like a Goldman Sachs or Morgan Stanley weekly note.",

  "key_events": [
    {{
      "event": "Anthropic Mythos Model Launch",
      "date": "Apr 10",
      "impact": "3-4 sentence analysis of what happened, which stocks/sectors it impacted, the magnitude of moves, and the second-order implications. E.g. 'Anthropic released Mythos, its most capable model yet, triggering an immediate repricing of software names as the market reassessed AI displacement risk. SNOW -8.42%, NOW -7.58%, PANW -6.74%, with the entire IGV ETF down -2.57%. Simultaneously, semiconductor names rallied as the same catalyst validated the AI infrastructure buildout thesis — SMCI +8.79%, MRVL +7.19%. This divergence marks the clearest market signal yet that investors are distinguishing AI beneficiaries from AI casualties.'"
    }}
  ],

  "thematic_analysis": [
    {{
      "theme": "AI Infrastructure vs Software Disruption",
      "narrative": "4-5 sentence deep analysis of this investment theme as it played out this week. Reference specific thematic ETFs (SMH, IGV, WCLD, HACK, BOTZ), individual stocks, and the catalysts. Explain what this means for thematic positioning — is this a rotation to lean into or fade? What are the leading indicators to watch?"
    }}
  ],

  "sector_review": [
    {{
      "sector": "Technology — Software vs Semis",
      "weekly_change": -2.3,
      "narrative": "3-4 sentence review. Name the key stocks that drove the sector, the specific catalysts (earnings, product launches, regulatory actions), and what this means for positioning. Don't just say 'tech was mixed' — explain the SOFTWARE vs SEMIS divergence with specific names and numbers."
    }}
  ],

  "earnings_and_data": [
    {{
      "event": "TSMC Q1 Revenue Beat (+35% YoY)",
      "impact": "2-3 sentences on what this data point revealed and how the market reacted"
    }}
  ],

  "factor_commentary": "3-4 sentences analyzing factor performance through a thematic lens. Which factors outperformed, what does that tell us about market regime (risk-on vs risk-off, growth vs value rotation, quality premium or discount), and what it implies for thematic positioning.",

  "signal_commentary": "2-3 sentences interpreting the market signal score and its components — what's driving the reading and what would change it.",

  "week_ahead": "4-5 sentences on what to watch next week. Be specific: name upcoming earnings (with dates if known), economic releases (CPI, PPI, retail sales, Fed speakers), geopolitical catalysts, and key technical levels. Flag which thematic exposures are most at risk or have the most upside optionality.",

  "notable_moves": [
    {{
      "ticker": "SMCI",
      "move": "+8.79%",
      "date": "Apr 11",
      "context": "2 sentences: what drove this move and why it matters for the broader theme"
    }}
  ]
}}

RULES:
1. This is written for THEMATIC INVESTORS — every section should have a thematic angle (AI, energy transition, private credit, etc.)
2. Reference SPECIFIC events from the daily narratives — Anthropic Mythos, TSMC revenue, HHS budget cuts, Iran pipeline attacks, etc.
3. Name specific stocks with their moves (ticker + percentage)
4. Reference thematic ETFs (SMH, IGV, WCLD, HACK, XBI, KRE, etc.) when relevant
5. The executive summary must tell a STORY, not list events — connect the dots between catalysts
6. Key events: pick the 3-4 most market-moving events of the week, with deep analysis
7. Thematic analysis: 2-3 cross-cutting investment themes with actionable positioning insight
8. Sector review: cover the 5-6 most important sectors with sub-sector granularity (semis vs software, banks vs insurers)
9. Notable moves: 6-8 stocks with 2-sentence context each
10. Do NOT be generic — every sentence should contain a specific name, number, or catalyst

Return ONLY the JSON object."""

    if not openai_enabled():
        print("  OPENAI_API_KEY not set, skipping GPT-5.4 weekly summary generation")
        return {
            "headline": f"Weekly Market Summary: {agg.get('week_start', '')} — {agg.get('week_end', '')}",
            "executive_summary": "OpenAI is not configured yet, so the weekly commentary was skipped.",
            "sector_review": [],
            "thematic_analysis": [],
            "key_themes": [],
            "factor_commentary": "",
            "week_ahead": "",
            "notable_moves": [],
            "earnings_and_data": [],
            "signal_commentary": "",
        }

    print("  Generating weekly summary via OpenAI GPT-5.4...")
    try:
        raw = complete_text(prompt, max_output_tokens=8000)
        narrative = extract_json(raw)
        print(f"  Weekly narrative: {len(narrative.get('sector_review', []))} sectors, "
              f"{len(narrative.get('thematic_analysis', [])) or len(narrative.get('key_themes', []))} themes, "
              f"{len(narrative.get('notable_moves', []))} notable moves")
        return narrative

    except Exception as e:
        print(f"  Warning: weekly narrative generation failed: {e}")
        return {
            "headline": f"Weekly Market Summary: {agg.get('week_start', '')} — {agg.get('week_end', '')}",
            "executive_summary": "Weekly summary generation failed. Please refer to daily reports.",
            "sector_review": [],
            "thematic_analysis": [],
            "key_themes": [],
            "factor_commentary": "",
            "week_ahead": "",
            "notable_moves": [],
            "earnings_and_data": [],
            "signal_commentary": "",
        }


def run_weekly_summary(ref_date: date = None) -> dict | None:
    """Generate weekly summary if today is Friday (or forced via ref_date)."""
    if ref_date is None:
        ref_date = date.today()

    week_dates = _get_week_dates(ref_date)
    briefs = _load_daily_briefs(week_dates)

    if not briefs:
        print("  No daily briefs found for this week")
        return None

    print(f"  Aggregating {len(briefs)} daily briefs ({week_dates[0]} to {week_dates[-1]})...")

    agg = _aggregate_week_data(briefs)
    narrative = _generate_weekly_narrative(agg)

    result = {
        "generated_at": datetime.now().isoformat(),
        **agg,
        "narrative": narrative,
    }

    # Save
    week_str = week_dates[0].isoformat()
    out_path = STATE_DIR / f"weekly_summary_{week_str}.json"
    out_path.write_text(json.dumps(result, indent=2, default=str))
    print(f"  Saved weekly summary → {out_path}")

    return result
