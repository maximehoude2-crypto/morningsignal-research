"""
Daily Market Brief — fetches index, sector, and macro data via yfinance.
Saves to state/market_brief_YYYY-MM-DD.json.
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from io import StringIO

import pandas as pd
import requests

from scanner.openai_client import complete_text, extract_json, openai_enabled

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
STATE_DIR.mkdir(exist_ok=True)

INDICES = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq 100",
    "IWM": "Russell 2000",
    "DIA": "Dow Jones",
}

SECTORS = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLY": "Cons. Discretionary",
    "XLP": "Cons. Staples",
    "XLC": "Communication Svcs",
}

MACRO = {
    "^VIX": "VIX",
    "^TNX": "10Y Yield",
    "^IRX": "2Y Yield",
}


def _pct_change(series: pd.Series, periods: int = 1) -> float:
    if len(series) <= periods:
        return 0.0
    prev = series.iloc[-(periods + 1)]
    curr = series.iloc[-1]
    if prev == 0 or pd.isna(prev):
        return 0.0
    return round((curr / prev - 1) * 100, 2)


def _ytd_change(series: pd.Series) -> float:
    """YTD return: current price vs first trading day of the current year."""
    if len(series) < 2:
        return 0.0
    current_year = date.today().year
    # Build a tz-aware or tz-naive timestamp to match the series index
    jan1 = pd.Timestamp(f"{current_year}-01-01")
    if series.index.tz is not None:
        jan1 = jan1.tz_localize(series.index.tz)
    ytd_data = series.loc[series.index >= jan1]
    if len(ytd_data) < 2:
        return 0.0
    start_val = float(ytd_data.iloc[0])
    end_val = float(ytd_data.iloc[-1])
    if start_val == 0 or pd.isna(start_val):
        return 0.0
    return round((end_val / start_val - 1) * 100, 2)


def _mock_ticker(symbol: str, base_price: float, day_chg: float, ytd_chg: float) -> dict:
    return {
        "symbol": symbol,
        "price": base_price,
        "day_change": day_chg,
        "ytd_change": ytd_chg,
    }


def _scrape_news_headlines() -> str:
    """Scrape today's financial news headlines from multiple sources."""
    from bs4 import BeautifulSoup

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    all_headlines = []
    today_str = date.today().strftime("%Y/%m/%d")

    sources = [
        ("https://www.cnbc.com/market-insider/", "CNBC"),
        ("https://www.cnbc.com/markets/", "CNBC"),
        ("https://finance.yahoo.com/topic/stock-market-news/", "Yahoo"),
        ("https://finance.yahoo.com/topic/earnings/", "Yahoo"),
    ]

    kw_filter = [
        'stock', 'market', 'why', 'fall', 'rise', 'drop', 'surge', 'plunge',
        'rally', 'sell', 'buy', 'ai ', 'nvidia', 'sector', 'etf', 'bond',
        'yield', 'fed', 'inflation', 'earnings', 'trade', 'tariff', 'oil',
        'software', 'semi', 'insurance', 'bank', 'credit', 'energy',
        'biotech', 'pharma', 'defense', 'retail', 'consumer', 'gold',
        'china', 'rate', 'cpi', 'ppi', 'housing', 'jobs',
    ]

    for url, source in sources:
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, 'html.parser')

            for a in soup.select('a'):
                t = a.get_text(strip=True)
                href = a.get('href', '')
                if not t or len(t) < 25 or len(t) > 250:
                    continue
                # Today's articles (by date in URL) get priority
                if today_str in href:
                    all_headlines.append(f"[{source}] {t}")
                # General headlines filtered by keywords
                elif any(kw in t.lower() for kw in kw_filter):
                    all_headlines.append(f"[{source}] {t}")
        except Exception:
            pass

    # Deduplicate
    seen = set()
    unique = []
    for h in all_headlines:
        key = h.lower()[:60]
        if key not in seen:
            seen.add(key)
            unique.append(h)

    return "\n".join(unique[:60])


def _generate_narrative(brief: dict) -> dict:
    """
    Scrape today's financial news, then call OpenAI Responses API
    to produce a comprehensive institutional-grade narrative package.
    """
    print("  Scraping today's financial news headlines...")
    news = _scrape_news_headlines()
    n_headlines = len(news.strip().splitlines()) if news.strip() else 0
    print(f"  Found {n_headlines} headlines")

    if not openai_enabled():
        print("  OPENAI_API_KEY not set, skipping GPT-5.4 narrative generation")
        return _empty_narrative()

    try:
        indices = brief.get("indices", {})
        sectors = brief.get("sectors", [])
        macro = brief.get("macro", {})
        top_gainers = brief.get("top_gainers", [])
        top_losers = brief.get("top_losers", [])

        spx = indices.get("SPY", {})
        qqq = indices.get("QQQ", {})
        iwm = indices.get("IWM", {})

        sector_str = "\n".join(
            f"  - {s['name']} ({s['symbol']}): {s['day_change']:+.2f}%" for s in sectors
        )

        gainers_str = ", ".join(
            f"{m['ticker']} {m['day_change']:+.2f}%" for m in top_gainers
        ) or "N/A"

        losers_str = ", ".join(
            f"{m['ticker']} {m['day_change']:+.2f}%" for m in top_losers
        ) or "N/A"

        vix_level = macro.get("vix", {}).get("level", 0)
        vix_5d = macro.get("vix", {}).get("5d_change", 0)
        tnx = macro.get("tnx", {}).get("level", 0)
        spread = macro.get("spread_2s10s", 0)

        thematic_str = ""
        for e in brief.get("thematic_etfs", []):
            thematic_str += f"  - {e['symbol']} ({e['name']}): {e.get('1d', 0):+.2f}%\n"

        signal_ctx = ""
        sig = brief.get("market_signal", {})
        if sig:
            signal_ctx = f"\nMarket Signal: {sig.get('signal', 'N/A')} (score: {sig.get('score', 0):+.3f})\n"
            for name, comp in sig.get("components", {}).items():
                signal_ctx += f"  - {name}: {comp.get('score', 0):+.2f} ({comp.get('detail', '')})\n"

        factor_ctx = ""
        for f in brief.get("factors", {}).get("performance", []):
            factor_ctx += f"  - {f['name']} ({f['symbol']}): {f.get('1d', 0):+.2f}%\n"

        # ── Sub-sector / industry context ────────────────────────────────
        industries_payload = brief.get("industries") or {}
        industry_ctx = ""
        if industries_payload:
            ind_list = industries_payload.get("industries", [])
            top_inds = ind_list[:8]
            bottom_inds = sorted(ind_list, key=lambda x: x.get("performance", {}).get("1d", 0))[:5]
            rotation = industries_payload.get("rotation", {})
            ind_events = industries_payload.get("industry_events", {})
            stock_events = industries_payload.get("stock_events", {})

            def _fmt_ind(rec: dict) -> str:
                p = rec.get("performance", {})
                rrg = rec.get("rrg") or {}
                return (
                    f"  - {rec.get('industry')} ({rec.get('sector')}): "
                    f"1d {p.get('1d', 0):+.2f}%, 5d {p.get('5d', 0):+.2f}%, "
                    f"{rrg.get('quadrant', 'N/A')} quadrant, "
                    f"%>50d {rec.get('breadth', {}).get('pct_above_50d', 0):.0f}%, "
                    f"%>200d {rec.get('breadth', {}).get('pct_above_200d', 0):.0f}%"
                )

            industry_ctx = "\n## SUB-SECTOR (GICS INDUSTRY) MOVES\n"
            summary_b = industries_payload.get("summary", {})
            industry_ctx += (
                f"Universe breadth: {summary_b.get('pct_above_50d', 0):.0f}% above 50d, "
                f"{summary_b.get('pct_above_200d', 0):.0f}% above 200d.\n"
            )
            industry_ctx += "Top industries (1d):\n" + "\n".join(_fmt_ind(r) for r in top_inds) + "\n"
            industry_ctx += "Bottom industries (1d):\n" + "\n".join(_fmt_ind(r) for r in bottom_inds) + "\n"

            if rotation.get("rotation_breakout"):
                industry_ctx += f"Rotation BREAKOUTS into Leading: {', '.join(rotation['rotation_breakout'])}\n"
            if rotation.get("rotation_recovery"):
                industry_ctx += f"Rotation recoveries (Lagging->Improving): {', '.join(rotation['rotation_recovery'])}\n"
            if rotation.get("rotation_breakdown"):
                industry_ctx += f"Rotation breakdowns into Lagging: {', '.join(rotation['rotation_breakdown'])}\n"
            if rotation.get("rotation_topping"):
                industry_ctx += f"Rotation topping (Leading->Weakening): {', '.join(rotation['rotation_topping'])}\n"

            for label, key in [
                ("Industry golden crosses", "golden_cross"),
                ("Industry death crosses", "death_cross"),
                ("Industry reclaimed 200d", "reclaim_200d"),
                ("Industry lost 200d", "lost_200d"),
                ("Industry EMA(12/26) bull crosses", "ema_bull_cross"),
                ("Industry EMA(12/26) bear crosses", "ema_bear_cross"),
            ]:
                items = ind_events.get(key, [])
                if items:
                    industry_ctx += f"{label}: {', '.join(i['industry'] for i in items[:6])}\n"

            for label, key in [
                ("Stock-level golden crosses", "golden_cross"),
                ("Stock-level death crosses", "death_cross"),
                ("Stock-level reclaimed 200d", "reclaim_200d"),
                ("Stock-level lost 200d", "lost_200d"),
            ]:
                bucket = stock_events.get(key, {})
                if bucket and bucket.get("count"):
                    industry_ctx += (
                        f"{label}: {bucket['count']} stocks "
                        f"(e.g. {', '.join(bucket.get('tickers', [])[:8])})\n"
                    )

        # ── Categorized news intelligence ────────────────────────────────
        news_payload = brief.get("news") or {}
        news_intel_ctx = ""
        if news_payload:
            news_intel_ctx = "\n## CATEGORIZED NEWS INTELLIGENCE (multi-source, theme-tagged)\n"
            news_intel_ctx += f"Total relevant headlines: {news_payload.get('total', 0)} from {', '.join(news_payload.get('sources', []))}.\n"
            by_theme = news_payload.get("by_theme", {})
            for theme in ["Earnings", "Fed/Macro", "Geopolitics", "AI/Tech",
                          "Energy/Oil", "China/Trade", "Healthcare", "Crypto",
                          "M&A", "Regulation", "Layoffs/Labor"]:
                items = by_theme.get(theme, [])
                if items:
                    news_intel_ctx += f"\n### {theme} ({len(items)})\n"
                    for h in items[:8]:
                        urgency_marker = "!" * h.get("urgency", 1)
                        sec_tag = f" [{', '.join(h.get('sectors', []))}]" if h.get("sectors") else ""
                        news_intel_ctx += f"  - [{h['source']}]{urgency_marker} {h['title']}{sec_tag}\n"
            earnings = news_payload.get("earnings", [])
            if earnings:
                news_intel_ctx += "\n### Earnings reporting today\n"
                for e in earnings[:15]:
                    surp = f" (surprise {e.get('surprise_pct', 0):+.1f}%)" if e.get("surprise_pct") else ""
                    news_intel_ctx += f"  - {e['ticker']} ({e.get('sector', 'Unknown')}): {e.get('name', '')}{surp}\n"

        prompt = f"""You are a senior portfolio strategist at a top-tier prime brokerage writing the morning intelligence note. Today is {brief.get('date', 'today')}.

## TODAY'S MARKET DATA
- S&P 500 (SPY): {spx.get('day_change', 0):+.2f}% | Nasdaq 100 (QQQ): {qqq.get('day_change', 0):+.2f}% | Russell 2000 (IWM): {iwm.get('day_change', 0):+.2f}%
- Sector returns:
{sector_str}
- Top S&P 500 gainers: {gainers_str}
- Top S&P 500 losers: {losers_str}
- VIX: {vix_level:.2f} ({vix_5d:+.1f}% 5d) | 10Y: {tnx:.2f}% | 2s/10s: {spread*100:+.0f}bps

## THEMATIC ETF MOVES
{thematic_str}
## FACTOR RETURNS
{factor_ctx}
## MARKET SIGNAL
{signal_ctx}
{industry_ctx}
## TODAY'S NEWS HEADLINES (scraped from financial media)
{news}
{news_intel_ctx}

## TASK
Produce a JSON object with exactly this structure. Return ONLY raw JSON — no markdown, no code fences, no commentary.

{{
  "summary": "One sentence: the dominant theme driving markets today. Be specific — name the catalyst.",

  "bullets": [
    {{
      "sector": "Technology — Software vs Semis",
      "change": -1.4,
      "narrative": "ZS -8%, CRWD -5%, PANW -4% as Anthropic Mythos model launch triggers AI displacement repricing across SaaS; semis diverge higher (NVDA +2.6%, AVGO +4.7%) on TSMC revenue beat and CoreWeave infra deal"
    }}
  ],

  "sector_annotations": {{
    "Technology": "software repricing on Anthropic Mythos AI displacement fears",
    "Financials": "private credit mark-to-market fears hit insurance sub-sector",
    "Health Care": "HHS budget cuts + CMS prior auth rules weigh on managed care",
    "Energy": "Iranian attacks on Saudi pipeline; WTI bounces off support"
  }},

  "cross_sector": "Two-paragraph essay describing the cross-sector / cross-asset linkages today: cyclicals vs defensives, growth vs value, semis vs software, energy vs utilities, small caps vs large caps, stocks vs bonds, dollar vs equities, gold vs equities, credit vs treasuries. Cite specific magnitudes and explain WHAT EACH SPREAD MEANS for positioning.",

  "cross_sector_dynamics": [
    {{
      "label": "Cyclicals vs Defensives",
      "observation": "XLY +0.8% / XLI +0.6% / XLF +0.4% vs XLP -0.3% / XLU -0.5% / XLV -0.4%, spread of +0.95pp.",
      "implication": "Risk appetite firm — the tape is paying for growth, leverage and operating cycle exposure.",
      "signal": "risk-on"
    }}
  ],

  "factor_interpretation": "Momentum outperforming while Min Vol sells off = classic risk-on factor rotation...",

  "signal_interpretation": "Risk-on at +0.45 driven by falling VIX and steepening curve, but breadth divergence...",

  "industry_rotation": "Specific GICS industries that moved today and their RRG-quadrant transitions. Lead with rotation breakouts (industries that just entered Leading) and cross events (golden/death cross, 200d reclaim, EMA(12/26) momentum cross). Reference specific industry names from the SUB-SECTOR section above.",

  "ma_events": "Most important moving-average cross events at the industry level: golden crosses, death crosses, 200d reclaims, EMA momentum crosses. Be specific about which industries triggered them and what it implies for positioning.",

  "morning_note": "300-350 word top-of-page Strategist Letter. Open with one sentence calling the regime (risk-on / risk-off / regime-shift / consolidation). Paragraph 2: equity leadership at the GICS-industry level — name 3-4 industries and the underlying catalysts. Paragraph 3: cross-asset color (rates, dollar, oil, gold, bitcoin) and what it tells you about positioning. Paragraph 4: the key risk to monitor and a concrete positioning takeaway (e.g., 'fade tech-laggard pairs', 'rotate from large-cap defensives into industrials', 'add credit hedges'). Write in the voice of a senior macro strategist at a top-tier hedge fund — precise, opinionated, no filler. NO bullet points. Plain prose.",

  "overnight": "Nikkei -0.4% on yen strength... Hang Seng +1.2%... STOXX 600... WTI... Gold... DXY... US futures..."
}}

## RULES FOR BULLETS
1. Produce 8-10 bullets. Each must have specific ticker callouts with magnitudes (e.g., "ZS -8%, CRWD -5%")
2. Explain the CAUSAL CHAIN — every bullet must reference a specific catalyst from the CATEGORIZED NEWS INTELLIGENCE section above (cite the source and headline, e.g. "[Reuters] OPEC+ extends production cuts → Energy +1.4%, XLE leadership confirmed by KMI/OXY/SLB"). Anchor sector moves to specific news.
3. Distinguish sub-sectors AT THE GICS-INDUSTRY LEVEL using the SUB-SECTOR data above (e.g., Application Software vs Systems Software vs Semiconductors vs Semiconductor Equipment, not just "Tech")
4. Include AT LEAST 2 bullets that focus specifically on industries that triggered a moving-average event (golden/death cross, 200d reclaim/loss, EMA bull/bear cross) or a RRG rotation transition (entering Leading, falling into Lagging)
5. When multiple thematic ETFs move together (IGV -2.5%, WCLD -5%, HACK -4.8%), call out the shared catalyst — link it to a specific headline from the news section
6. Sort by absolute magnitude of move (biggest movers first); use the "sector" field to label "GICS Sector — GICS Industry" (e.g., "Information Technology — Application Software")
7. Do NOT hallucinate — only reference events from headlines or that you are confident happened today
8. If a sector moved more than 0.5% but no obvious news catalyst exists in the headlines, say so explicitly (e.g., "no clear single catalyst — likely positioning / month-end rebalance")

## RULES FOR SECTOR_ANNOTATIONS
- One terse phrase per GICS sector explaining today's move
- Key format: use the sector name exactly as listed in sector returns above

## RULES FOR CROSS_SECTOR_DYNAMICS
- Produce 6-10 entries covering: Cyclicals vs Defensives, Discretionary vs Staples, Semis vs Software, Energy vs Utilities, Small caps vs Large caps (IWM vs SPY), Stocks vs Bonds (SPY vs TLT), Dollar vs Equities (UUP vs SPY), Gold vs Equities (GLD vs SPY), Credit vs Treasuries (HYG vs IEF), Growth vs Value, Momentum vs Min-Vol — pick the entries where the spread is meaningful (>0.20pp typically).
- Each entry: 1) `label` (the pair name), 2) `observation` (numerical observation with magnitudes), 3) `implication` (what positioning takeaway it gives), 4) `signal` ∈ {{ "risk-on", "risk-off", "neutral", "mixed" }}
- The observation must include the actual numbers; the implication must say what to DO about it.

## RULES FOR OVERNIGHT
- Cover Asia (Nikkei, Hang Seng), Europe (STOXX 600, DAX), commodities (oil, gold), FX (DXY), US futures
- Be specific about catalysts, not just direction

Return ONLY the JSON object."""

        print("  Calling OpenAI GPT-5.4 via Responses API...")
        raw = complete_text(prompt, max_output_tokens=5000)
        result = extract_json(raw)

        n_bullets = len(result.get("bullets", []))
        print(f"  Narrative generated: {n_bullets} bullets, "
              f"{len(result.get('sector_annotations', {}))} annotations")
        return result

    except Exception as e:
        print(f"  Warning: narrative generation failed: {e}")
        return _empty_narrative()


def _empty_narrative() -> dict:
    return {"summary": "", "bullets": [], "sector_annotations": {},
            "cross_sector": "", "cross_sector_dynamics": [],
            "factor_interpretation": "",
            "signal_interpretation": "",
            "industry_rotation": "", "ma_events": "",
            "morning_note": "",
            "overnight": ""}


def narrative_has_content(narrative) -> bool:
    if isinstance(narrative, str):
        return bool(narrative.strip())
    if not isinstance(narrative, dict):
        return False
    return any(
        bool(narrative.get(key))
        for key in (
            "summary",
            "bullets",
            "sector_annotations",
            "cross_sector",
            "cross_sector_dynamics",
            "factor_interpretation",
            "signal_interpretation",
            "industry_rotation",
            "ma_events",
            "morning_note",
            "overnight",
        )
    )


def compute_cross_sector_dynamics(brief: dict) -> tuple[list[dict], str]:
    """
    Compute the classic cross-sector / cross-asset linkages a market strategist
    looks at every morning. Returns (structured_list, prose_narrative).

    Each item in structured_list is:
        {label, observation, implication, signal: 'risk-on'|'risk-off'|'neutral'|'mixed'}
    """
    sectors = {s["name"]: s for s in brief.get("sectors", [])}
    indices = brief.get("indices", {})
    macro = brief.get("macro", {})
    factors = {f["name"]: f for f in brief.get("factors", {}).get("performance", [])}

    def _theme_local(symbol: str) -> dict:
        for e in brief.get("thematic_etfs", []):
            if e.get("symbol") == symbol:
                return e
        return {}

    spx = indices.get("SPY", {}).get("day_change", 0)
    qqq = indices.get("QQQ", {}).get("day_change", 0)
    iwm = indices.get("IWM", {}).get("day_change", 0)

    dynamics: list[dict] = []

    # 1. Cyclicals vs Defensives → Risk Appetite
    cyc_avg = 0.0
    cyc_n = 0
    for n in ("Cons. Discretionary", "Industrials", "Financials", "Technology"):
        if n in sectors:
            cyc_avg += sectors[n]["day_change"]; cyc_n += 1
    cyc_avg = cyc_avg / cyc_n if cyc_n else 0.0
    def_avg = 0.0
    def_n = 0
    for n in ("Cons. Staples", "Utilities", "Health Care", "Real Estate"):
        if n in sectors:
            def_avg += sectors[n]["day_change"]; def_n += 1
    def_avg = def_avg / def_n if def_n else 0.0
    spread_cd = cyc_avg - def_avg
    if abs(spread_cd) > 0.05:
        dynamics.append({
            "label": "Cyclicals vs Defensives",
            "observation": (
                f"Cyclicals ({cyc_avg:+.2f}%) are "
                f"{'leading' if spread_cd > 0 else 'lagging'} Defensives ({def_avg:+.2f}%) "
                f"by {abs(spread_cd):.2f}pp."
            ),
            "implication": (
                "Risk appetite firm — the tape is paying for growth, leverage and operating cycle exposure."
                if spread_cd > 0.30 else
                "Defensive rotation underway — money is rotating into Staples / Utilities / REITs / Health Care."
                if spread_cd < -0.30 else
                "Mixed leadership — the tape can't pick a regime."
            ),
            "signal": "risk-on" if spread_cd > 0.30 else "risk-off" if spread_cd < -0.30 else "mixed",
        })

    # 2. Discretionary vs Staples → Consumer health
    if "Cons. Discretionary" in sectors and "Cons. Staples" in sectors:
        disc = sectors["Cons. Discretionary"]["day_change"]
        stap = sectors["Cons. Staples"]["day_change"]
        spread = disc - stap
        if abs(spread) > 0.10:
            dynamics.append({
                "label": "Discretionary vs Staples",
                "observation": f"XLY {disc:+.2f}% vs XLP {stap:+.2f}% (spread {spread:+.2f}pp).",
                "implication": (
                    "Consumer is risk-on — the household balance-sheet trade is working."
                    if spread > 0.30 else
                    "Consumer rotating defensive — Staples bid is a late-cycle warning."
                    if spread < -0.30 else
                    "Consumer mixed — no clear signal on household risk appetite."
                ),
                "signal": "risk-on" if spread > 0.30 else "risk-off" if spread < -0.30 else "mixed",
            })

    # 3. Semis vs Software → AI capex theme
    smh = _theme_local("SMH")
    igv = _theme_local("IGV")
    if smh and igv:
        s = smh.get("1d", 0); g = igv.get("1d", 0)
        spread = s - g
        if abs(spread) > 0.30:
            dynamics.append({
                "label": "Semis vs Software (AI capex theme)",
                "observation": f"SMH {s:+.2f}% vs IGV {g:+.2f}% — spread {spread:+.2f}pp.",
                "implication": (
                    "Hardware/infrastructure leg of AI is bid — capex theme intact, hyperscaler spend cycle on."
                    if spread > 0.50 else
                    "Software is leading semis — AI monetisation narrative dominating, or semis exhaustion."
                    if spread < -0.50 else
                    "AI complex moving in tandem — no relative dispersion within the theme."
                ),
                "signal": "risk-on" if spread > 0 else "risk-off" if spread < -0.5 else "neutral",
            })

    # 4. Energy vs Utilities → Inflation re-acceleration
    if "Energy" in sectors and "Utilities" in sectors:
        e = sectors["Energy"]["day_change"]
        u = sectors["Utilities"]["day_change"]
        spread = e - u
        if abs(spread) > 0.30:
            dynamics.append({
                "label": "Energy vs Utilities",
                "observation": f"XLE {e:+.2f}% vs XLU {u:+.2f}% — spread {spread:+.2f}pp.",
                "implication": (
                    "Inflation reflation/commodity bid; favours real-asset exposure and pricing-power names."
                    if spread > 0.50 else
                    "Disinflation/duration bid; long-bond proxy outperformance signals lower-rates regime."
                    if spread < -0.50 else
                    "No clear inflation impulse from the energy/utilities pair."
                ),
                "signal": "neutral",
            })

    # 5. IWM vs SPY → Breadth / risk
    if iwm != 0 or spx != 0:
        spread = iwm - spx
        if abs(spread) > 0.20:
            dynamics.append({
                "label": "Small caps vs Large caps (IWM vs SPY)",
                "observation": f"IWM {iwm:+.2f}% vs SPY {spx:+.2f}% — spread {spread:+.2f}pp.",
                "implication": (
                    "Rally is broadening — small caps participating; risk-on with breadth confirmation."
                    if spread > 0.30 else
                    "Mega-caps doing the lifting — narrow tape, breadth divergence is a yellow flag."
                    if spread < -0.30 else
                    "Cap-weighted vs equal-weighted in line — no breadth distortion."
                ),
                "signal": "risk-on" if spread > 0.30 else "risk-off" if spread < -0.30 else "neutral",
            })

    # 6. Bonds vs Stocks → Correlation regime
    tlt = _theme_local("TLT")
    if tlt:
        t = tlt.get("1d", 0)
        if abs(spx) > 0.10 or abs(t) > 0.10:
            both_up = spx > 0.10 and t > 0.10
            both_down = spx < -0.10 and t < -0.10
            inverse = (spx > 0.10 and t < -0.10) or (spx < -0.10 and t > 0.10)
            if both_up:
                obs = f"SPY {spx:+.2f}% AND TLT {t:+.2f}% — both bid."
                impl = "Liquidity tape — stocks and bonds rallying together signals easing financial conditions."
                sig = "risk-on"
            elif both_down:
                obs = f"SPY {spx:+.2f}% AND TLT {t:+.2f}% — both red."
                impl = "Cross-asset stress — equities and duration selling in tandem; classic liquidity drain."
                sig = "risk-off"
            elif inverse:
                obs = f"SPY {spx:+.2f}% / TLT {t:+.2f}% — inverse."
                impl = "Classic stocks/bonds correlation working — risk-on means TLT down, risk-off means TLT up."
                sig = "neutral"
            else:
                obs = f"SPY {spx:+.2f}% / TLT {t:+.2f}%."
                impl = "Stocks/bonds modest moves — no clear cross-asset signal."
                sig = "neutral"
            dynamics.append({
                "label": "Stocks vs Bonds (SPY vs TLT)",
                "observation": obs,
                "implication": impl,
                "signal": sig,
            })

    # 7. Dollar vs Equities
    uup = _theme_local("UUP")
    if uup:
        u = uup.get("1d", 0)
        if abs(u) > 0.10 or abs(spx) > 0.10:
            sign = (spx > 0) != (u > 0)  # opposite signs = classic risk relationship
            dynamics.append({
                "label": "Dollar vs Equities",
                "observation": f"UUP {u:+.2f}% / SPY {spx:+.2f}%.",
                "implication": (
                    "Dollar weakness with equities up = risk-on, EM and commodities should follow."
                    if u < -0.05 and spx > 0.05 else
                    "Dollar bid with equities down = classic flight-to-quality, USD safe-haven."
                    if u > 0.05 and spx < -0.05 else
                    "Dollar strong with equities up = US exceptionalism / yield-driven, watch EM."
                    if u > 0.05 and spx > 0.05 else
                    "Dollar weak with equities down = unusual; could be growth scare in US specifically."
                ),
                "signal": "risk-on" if (u < -0.05 and spx > 0.05) else "risk-off" if (u > 0.05 and spx < -0.05) else "neutral",
            })

    # 8. Gold vs Equities
    gld = _theme_local("GLD")
    if gld:
        g = gld.get("1d", 0)
        if abs(g) > 0.20 and abs(spx) > 0.10:
            if g > 0.30 and spx < -0.10:
                impl = "Defensive rotation into gold while equities sell — flight-to-quality, watch credit too."
                sig = "risk-off"
            elif g > 0.30 and spx > 0.10:
                impl = "Both gold AND equities bid — debasement / liquidity trade, watch the dollar."
                sig = "neutral"
            elif g < -0.30 and spx > 0.10:
                impl = "Gold sold while equities rally — risk-on rotation out of safe-haven."
                sig = "risk-on"
            else:
                impl = "Gold and equities both off — broad de-leveraging."
                sig = "risk-off"
            dynamics.append({
                "label": "Gold vs Equities",
                "observation": f"GLD {g:+.2f}% / SPY {spx:+.2f}%.",
                "implication": impl,
                "signal": sig,
            })

    # 9. Credit (HYG/IEF spread) → Risk premia
    hyg = _theme_local("HYG"); ief = _theme_local("IEF")
    if hyg and ief:
        h = hyg.get("5d", 0); i = ief.get("5d", 0)
        spread = h - i
        if abs(spread) > 0.20:
            dynamics.append({
                "label": "Credit vs Treasuries (HYG vs IEF, 5d)",
                "observation": f"HYG 5d {h:+.2f}% vs IEF 5d {i:+.2f}% — spread {spread:+.2f}pp.",
                "implication": (
                    "Credit tightening / risk premia compressing — bullish for equities."
                    if spread > 0.30 else
                    "Credit widening relative to Treasuries — risk premia expanding, defensive signal."
                    if spread < -0.30 else
                    "Credit and rates roughly in line — no credit stress signal."
                ),
                "signal": "risk-on" if spread > 0.30 else "risk-off" if spread < -0.30 else "neutral",
            })

    # 10. Growth vs Value factor
    grw = factors.get("Growth"); val = factors.get("Value")
    if grw and val:
        g = grw.get("1d", 0); v = val.get("1d", 0)
        spread = g - v
        if abs(spread) > 0.30:
            dynamics.append({
                "label": "Growth vs Value (factors)",
                "observation": f"VUG {g:+.2f}% vs VLUE {v:+.2f}% — spread {spread:+.2f}pp.",
                "implication": (
                    "Growth factor leading — duration / long-multiple stocks bid; lower-rates regime."
                    if spread > 0.50 else
                    "Value factor leading — short-duration / cash-flow names bid; cyclical / re-flation tape."
                    if spread < -0.50 else
                    "Growth/Value roughly tied — no factor regime signal."
                ),
                "signal": "neutral",
            })

    # 11. Momentum vs Min-Vol
    mom = factors.get("Momentum"); mv = factors.get("Min Vol")
    if mom and mv:
        m = mom.get("1d", 0); v = mv.get("1d", 0)
        spread = m - v
        if abs(spread) > 0.30:
            dynamics.append({
                "label": "Momentum vs Min Vol",
                "observation": f"MTUM {m:+.2f}% vs USMV {v:+.2f}% — spread {spread:+.2f}pp.",
                "implication": (
                    "Trend-followers in control — leadership names extend, junk reflation often follows."
                    if spread > 0.50 else
                    "Defensive crowding — Min-Vol bid signals investors paying for stability over upside."
                    if spread < -0.50 else
                    "Momentum / Min-Vol balanced — neutral risk preference."
                ),
                "signal": "risk-on" if spread > 0.50 else "risk-off" if spread < -0.50 else "neutral",
            })

    # ── Aggregate prose narrative ────────────────────────────────────────
    if not dynamics:
        prose = (
            "Cross-sector spreads are tight today — there is no clear leadership theme between "
            "cyclicals and defensives, no growth/value rotation of note, and no cross-asset stress. "
            "The tape is in consolidation."
        )
    else:
        risk_on = sum(1 for d in dynamics if d["signal"] == "risk-on")
        risk_off = sum(1 for d in dynamics if d["signal"] == "risk-off")
        regime_phrase = (
            "The cross-sector tape is unambiguously risk-on" if risk_on >= risk_off + 2 else
            "The cross-sector tape is leaning risk-on" if risk_on > risk_off else
            "The cross-sector tape is unambiguously risk-off" if risk_off >= risk_on + 2 else
            "The cross-sector tape is leaning risk-off" if risk_off > risk_on else
            "The cross-sector tape is mixed — neither risk-on nor risk-off has the upper hand"
        )
        # First three dynamics inline
        first_few = [d for d in dynamics[:3]]
        observations = " ".join(
            f"{d['label']}: {d['observation']} {d['implication']}"
            for d in first_few
        )
        prose = f"{regime_phrase}. {observations}"

    return dynamics, prose


def fallback_narrative(brief: dict) -> dict:
    """Build a deterministic market narrative when the LLM step fails."""
    sectors = list(brief.get("sectors", []))
    gainers = list(brief.get("top_gainers", []))
    losers = list(brief.get("top_losers", []))
    thematics = sorted(
        brief.get("thematic_etfs", []),
        key=lambda item: abs(item.get("1d", 0)),
        reverse=True,
    )
    factors = brief.get("factors", {}).get("performance", [])
    signal = brief.get("market_signal", {})
    macro = brief.get("macro", {})

    leaders = sorted(sectors, key=lambda item: item.get("day_change", 0), reverse=True)
    laggards = sorted(sectors, key=lambda item: item.get("day_change", 0))
    leader = leaders[0] if leaders else None
    laggard = laggards[0] if laggards else None

    def ticker_string(items: list[dict], limit: int = 3) -> str:
        picked = items[:limit]
        return ", ".join(
            f"{item.get('ticker', item.get('symbol', 'N/A'))} {item.get('day_change', item.get('1d', 0)):+.2f}%"
            for item in picked
        )

    top_theme = thematics[0] if thematics else None
    top_factor = factors[0] if factors else None
    low_factor = factors[-1] if factors else None
    vix = macro.get("vix", {})
    spread_bps = macro.get("spread_2s10s", 0) * 100

    if leader and laggard and top_theme and laggard["name"] != leader["name"]:
        summary = (
            f"{leader['name']} led the session at {leader['day_change']:+.2f}%, "
            f"while {laggard['name']} lagged at {laggard['day_change']:+.2f}%, "
            f"with {top_theme['symbol']} {top_theme['1d']:+.2f}% setting the thematic tone."
        )
    elif leader and laggard and laggard["name"] != leader["name"]:
        summary = (
            f"{leader['name']} led the session at {leader['day_change']:+.2f}%, "
            f"while {laggard['name']} lagged at {laggard['day_change']:+.2f}%."
        )
    elif leader:
        summary = f"{leader['name']} led the session at {leader['day_change']:+.2f}%."
    else:
        summary = "Market data was available, but the AI narrative step failed, so this fallback summary was generated from the tape."

    bullets = []
    if leader:
        bullets.append({
            "sector": leader["name"],
            "change": leader["day_change"],
            "narrative": (
                f"{leader['name']} led the sector leaderboard at {leader['day_change']:+.2f}% "
                f"as the strongest large-cap gainers were {ticker_string(gainers)}."
                if gainers else
                f"{leader['name']} led the sector leaderboard at {leader['day_change']:+.2f}%."
            ),
        })
    if laggard and (not leader or laggard["name"] != leader["name"]):
        bullets.append({
            "sector": laggard["name"],
            "change": laggard["day_change"],
            "narrative": (
                f"{laggard['name']} was the weakest major group at {laggard['day_change']:+.2f}% "
                f"with pressure concentrated in {ticker_string(losers)}."
                if losers else
                f"{laggard['name']} was the weakest major group at {laggard['day_change']:+.2f}%."
            ),
        })
    if top_theme:
        bullets.append({
            "sector": f"{top_theme['category']} — {top_theme['name']}",
            "change": top_theme["1d"],
            "narrative": (
                f"{top_theme['symbol']} moved {top_theme['1d']:+.2f}% on the day and "
                f"{top_theme.get('5d', 0):+.2f}% over 5 days, giving the market a clear read "
                f"on thematic leadership."
            ),
        })
    if top_factor or signal:
        factor_text = ""
        if top_factor and low_factor:
            factor_text = (
                f"{top_factor['name']} led the factor complex at {top_factor.get('1d', 0):+.2f}% "
                f"while {low_factor['name']} trailed at {low_factor.get('1d', 0):+.2f}%."
            )
        signal_text = ""
        if signal:
            signal_text = (
                f" Market signal registered {signal.get('signal', 'N/A')} "
                f"at {signal.get('score', 0):+.2f}."
            )
        bullets.append({
            "sector": "Factor & Regime",
            "change": top_factor.get("1d", 0) if top_factor else 0,
            "narrative": (factor_text + signal_text).strip(),
        })

    sector_annotations = {}
    if leader:
        sector_annotations[leader["name"]] = "session leadership"
    if laggard and laggard["name"] not in sector_annotations:
        sector_annotations[laggard["name"]] = "session laggard"

    factor_interpretation = ""
    if top_factor and low_factor:
        factor_interpretation = (
            f"{top_factor['name']} outperformed while {low_factor['name']} lagged, "
            "based on the live factor tape rather than the missing AI summary."
        )

    signal_interpretation = ""
    if signal:
        signal_interpretation = (
            f"{signal.get('signal', 'N/A')} with a score of {signal.get('score', 0):+.2f}, "
            "generated from the quantitative market signal inputs."
        )

    overnight = (
        f"Fallback note from live macro data: VIX {vix.get('level', 0):.2f} "
        f"({vix.get('5d_change', 0):+.2f}% over 5d), 10Y {macro.get('tnx', {}).get('level', 0):.2f}%, "
        f"2s/10s {spread_bps:+.0f}bps."
    )

    # Real cross-sector dynamics (replaces the old placeholder string)
    cross_sector_dynamics, cross_sector = compute_cross_sector_dynamics(brief)

    # ── Industry / sub-sector commentary (from industry_scanner output) ──
    industries_payload = brief.get("industries") or {}
    industry_rotation_text = ""
    ma_events_text = ""
    if industries_payload:
        ind_list = industries_payload.get("industries", [])
        rotation = industries_payload.get("rotation", {})
        ind_events = industries_payload.get("industry_events", {})
        stock_events = industries_payload.get("stock_events", {})
        breadth = industries_payload.get("stock_breadth", {})

        # Top/bottom industries
        top_three = ind_list[:3]
        bottom_three = sorted(ind_list, key=lambda x: x.get("performance", {}).get("1d", 0))[:3]

        if top_three:
            top_str = ", ".join(
                f"{i['industry']} {i['performance']['1d']:+.2f}%" for i in top_three
            )
            bullets.append({
                "sector": f"Industry Leaders — {top_three[0]['industry']}",
                "change": top_three[0]["performance"]["1d"],
                "narrative": (
                    f"At the GICS sub-industry level, leadership came from {top_str}. "
                    f"Universe breadth: {breadth.get('pct_above_50d', 0):.0f}% of S&P 1500 names above their 50-day MA, "
                    f"{breadth.get('pct_above_200d', 0):.0f}% above the 200-day."
                ),
            })

        if bottom_three:
            bot_str = ", ".join(
                f"{i['industry']} {i['performance']['1d']:+.2f}%" for i in bottom_three
            )
            bullets.append({
                "sector": f"Industry Laggards — {bottom_three[0]['industry']}",
                "change": bottom_three[0]["performance"]["1d"],
                "narrative": f"Weakest sub-industries today: {bot_str}.",
            })

        # Rotation breakouts
        if rotation.get("rotation_breakout"):
            bullets.append({
                "sector": "Rotation — into Leading",
                "change": 0,
                "narrative": (
                    f"{len(rotation['rotation_breakout'])} industr"
                    f"{'y' if len(rotation['rotation_breakout']) == 1 else 'ies'} just crossed into the Leading "
                    f"RRG quadrant: {', '.join(rotation['rotation_breakout'])}."
                ),
            })
            industry_rotation_text = (
                f"Rotation breakouts into Leading quadrant: {', '.join(rotation['rotation_breakout'])}."
            )
        if rotation.get("rotation_breakdown"):
            bullets.append({
                "sector": "Rotation — into Lagging",
                "change": 0,
                "narrative": (
                    f"Falling into Lagging: {', '.join(rotation['rotation_breakdown'])}."
                ),
            })
            industry_rotation_text += (
                f" Industries breaking down into Lagging: {', '.join(rotation['rotation_breakdown'])}."
            )

        # MA events: pick the most prominent across categories
        ma_lines = []
        for label, key in [
            ("Industry golden crosses", "golden_cross"),
            ("Industry death crosses", "death_cross"),
            ("Industry 200d reclaims", "reclaim_200d"),
            ("Industry 200d losses", "lost_200d"),
            ("Industry EMA(12/26) bull crosses", "ema_bull_cross"),
            ("Industry EMA(12/26) bear crosses", "ema_bear_cross"),
        ]:
            items = ind_events.get(key, [])
            if items:
                names = ", ".join(i["industry"] for i in items[:5])
                ma_lines.append(f"{label}: {names}")

        if ma_lines:
            bullets.append({
                "sector": "Moving-Average Cross Events",
                "change": 0,
                "narrative": "; ".join(ma_lines) + ".",
            })
            ma_events_text = "; ".join(ma_lines) + "."

        # Stock-level breadth count
        gc_count = stock_events.get("golden_cross", {}).get("count", 0)
        dc_count = stock_events.get("death_cross", {}).get("count", 0)
        if gc_count or dc_count:
            ma_events_text += (
                f" Stock-level: {gc_count} new golden crosses, {dc_count} new death crosses across "
                "the S&P 1500 in the last 5 sessions."
            )

    # ── News-driven catalyst bullets (deterministic) ─────────────────────
    news_payload = brief.get("news") or {}
    if news_payload:
        by_theme = news_payload.get("by_theme", {})
        # Inject up to 3 bullets summarising the loudest themes
        priority_themes = [
            ("Geopolitics", "Geopolitics"),
            ("Fed/Macro", "Fed / Macro"),
            ("AI/Tech", "AI / Tech"),
            ("Earnings", "Earnings Tape"),
            ("Energy/Oil", "Energy / Oil"),
            ("China/Trade", "China / Trade"),
            ("Healthcare", "Healthcare"),
            ("M&A", "M&A"),
            ("Regulation", "Regulation"),
        ]
        added = 0
        for theme_key, label in priority_themes:
            if added >= 4:
                break
            items = by_theme.get(theme_key, [])
            if not items:
                continue
            # Pick the top-2 highest-urgency headlines
            top = items[:2]
            heads = "; ".join(f"[{h['source']}] {h['title']}" for h in top)
            bullets.append({
                "sector": label,
                "change": 0,
                "narrative": f"{len(items)} active headlines tagged {theme_key}. Top items: {heads}",
            })
            added += 1

    # ── Strategist Letter (deterministic) ────────────────────────────────
    morning_note_parts: list[str] = []
    sig_label = signal.get("signal", "")
    sig_score = signal.get("score", 0)

    spx = brief.get("indices", {}).get("SPY", {})
    qqq = brief.get("indices", {}).get("QQQ", {})
    iwm = brief.get("indices", {}).get("IWM", {})
    spx_chg = spx.get("day_change", 0)
    qqq_chg = qqq.get("day_change", 0)
    iwm_chg = iwm.get("day_change", 0)

    breadth = (industries_payload.get("stock_breadth", {})
               if industries_payload else {})
    pct_50 = breadth.get("pct_above_50d", 0)
    pct_200 = breadth.get("pct_above_200d", 0)

    # Opening regime sentence
    if sig_label:
        morning_note_parts.append(
            f"The tape is trading {sig_label.lower()} (composite score {sig_score:+.2f}), with SPY {spx_chg:+.2f}%, "
            f"QQQ {qqq_chg:+.2f}% and IWM {iwm_chg:+.2f}% on the session. "
            f"Universe breadth sits at {pct_50:.0f}% above 50-day and {pct_200:.0f}% above 200-day, "
            f"and VIX prints {vix.get('level', 0):.2f} ({vix.get('5d_change', 0):+.1f}% over five sessions)."
        )
    else:
        morning_note_parts.append(
            f"Equities closed with SPY {spx_chg:+.2f}%, QQQ {qqq_chg:+.2f}% and IWM {iwm_chg:+.2f}%. "
            f"VIX is {vix.get('level', 0):.2f}, {vix.get('5d_change', 0):+.1f}% over five sessions."
        )

    # Industry leadership paragraph
    if industries_payload:
        ind_list = industries_payload.get("industries", [])
        top4 = ind_list[:4]
        if top4:
            top_str = ", ".join(
                f"{i['industry']} ({i['performance']['1d']:+.2f}%)" for i in top4
            )
            morning_note_parts.append(
                f"At the GICS sub-industry level, leadership concentrated in {top_str}. "
                f"On the laggard side, {bottom_three[0]['industry']} "
                f"{bottom_three[0]['performance']['1d']:+.2f}% headed the weak list. "
                f"{', '.join(rotation.get('rotation_breakout', [])) or 'No industries crossed into the Leading RRG quadrant in the last five sessions'}"
                f"{', and ' + str(len(rotation.get('rotation_breakdown', []))) + ' broke down into Lagging' if rotation.get('rotation_breakdown') else ''}."
            )

    # Cross-asset paragraph
    def _theme_local(symbol: str) -> dict:
        for e in brief.get("thematic_etfs", []):
            if e.get("symbol") == symbol:
                return e
        return {}
    tlt = _theme_local("TLT"); hyg = _theme_local("HYG")
    uup = _theme_local("UUP"); gld = _theme_local("GLD"); uso = _theme_local("USO"); bito = _theme_local("BITO")
    macro_block = []
    if uup: macro_block.append(f"the dollar (UUP) {uup.get('1d', 0):+.2f}%")
    if tlt: macro_block.append(f"long bonds (TLT) {tlt.get('1d', 0):+.2f}%")
    if hyg and tlt: macro_block.append(f"high yield (HYG) {hyg.get('1d', 0):+.2f}% — credit stress {'tightening' if hyg.get('5d', 0) > 0 else 'widening'}")
    if uso: macro_block.append(f"crude (USO) {uso.get('1d', 0):+.2f}%")
    if gld: macro_block.append(f"gold (GLD) {gld.get('1d', 0):+.2f}%")
    if bito: macro_block.append(f"bitcoin (BITO) {bito.get('1d', 0):+.2f}%")
    if macro_block:
        morning_note_parts.append(
            "Cross-asset color: " + "; ".join(macro_block) +
            f". The 2s10s curve sits at {macro.get('spread_2s10s', 0)*100:+.0f}bps."
        )

    # Risk + positioning takeaway
    risk_lines = []
    if industries_payload:
        ind_events = industries_payload.get("industry_events", {})
        if ind_events.get("death_cross"):
            risk_lines.append(
                f"watch the recent death-cross prints in "
                f"{', '.join(i['industry'] for i in ind_events['death_cross'][:3])}"
            )
        if ind_events.get("lost_200d"):
            risk_lines.append(
                f"and 200-day breaks in "
                f"{', '.join(i['industry'] for i in ind_events['lost_200d'][:3])}"
            )
        stock_events = industries_payload.get("stock_events", {})
        gc = stock_events.get("golden_cross", {}).get("count", 0)
        dc = stock_events.get("death_cross", {}).get("count", 0)
        if gc or dc:
            risk_lines.append(
                f"the breadth tape shows {gc} new stock-level golden crosses against {dc} death crosses"
            )
    if risk_lines:
        morning_note_parts.append(
            "Risks worth tracking: " + "; ".join(risk_lines) + "."
        )

    # News catalyst paragraph
    if news_payload:
        catalyst_lines = []
        by_theme = news_payload.get("by_theme", {})
        for theme_key, label in [
            ("Geopolitics", "geopolitics"),
            ("Fed/Macro", "Fed / macro"),
            ("AI/Tech", "AI / tech"),
            ("Energy/Oil", "energy"),
            ("Earnings", "the earnings tape"),
            ("M&A", "M&A"),
        ]:
            items = by_theme.get(theme_key, [])
            if items:
                top = items[0]["title"]
                catalyst_lines.append(f"on {label}: '{top}' [{items[0]['source']}]")
        if catalyst_lines:
            morning_note_parts.append(
                "Catalysts in the wires: " + "; ".join(catalyst_lines[:4]) + "."
            )

    if leader and laggard:
        morning_note_parts.append(
            f"Positioning takeaway: lean into {leader['name']} pockets where breadth is improving "
            f"and trim exposure in {laggard['name']} until 200-day support is reclaimed."
        )

    morning_note = " ".join(morning_note_parts)

    return {
        "source": "fallback",
        "summary": summary,
        "bullets": bullets,
        "sector_annotations": sector_annotations,
        "cross_sector": cross_sector,
        "cross_sector_dynamics": cross_sector_dynamics,
        "factor_interpretation": factor_interpretation,
        "signal_interpretation": signal_interpretation,
        "industry_rotation": industry_rotation_text,
        "ma_events": ma_events_text,
        "morning_note": morning_note,
        "overnight": overnight,
    }


def mock_market_brief() -> dict:
    """Generate realistic mock data for dry run."""
    import random
    random.seed(99)

    def rnd(lo, hi): return round(random.uniform(lo, hi), 2)

    indices = {
        "SPY": {"name": "S&P 500", "price": 521.43, "day_change": rnd(-1.5, 1.5), "ytd_change": rnd(-5, 15)},
        "QQQ": {"name": "Nasdaq 100", "price": 441.22, "day_change": rnd(-1.5, 1.5), "ytd_change": rnd(-5, 18)},
        "IWM": {"name": "Russell 2000", "price": 198.77, "day_change": rnd(-2, 2), "ytd_change": rnd(-8, 10)},
        "DIA": {"name": "Dow Jones", "price": 389.15, "day_change": rnd(-1, 1), "ytd_change": rnd(-3, 12)},
    }

    sectors_raw = []
    for sym, name in SECTORS.items():
        sectors_raw.append({
            "symbol": sym,
            "name": name,
            "day_change": rnd(-2.5, 2.5),
            "ytd_change": rnd(-10, 20),
        })
    sectors_raw.sort(key=lambda x: x["day_change"], reverse=True)

    sp500_names = {
        "NVDA": "NVIDIA Corp", "META": "Meta Platforms", "AAPL": "Apple Inc",
        "MSFT": "Microsoft Corp", "GOOGL": "Alphabet Inc",
        "INTC": "Intel Corp", "PFE": "Pfizer Inc", "BA": "Boeing Co",
        "KHC": "Kraft Heinz", "T": "AT&T Inc",
    }
    tickers = list(sp500_names.keys())
    gainers = [{"ticker": t, "name": sp500_names[t], "day_change": rnd(2, 8)} for t in tickers[:5]]
    losers = [{"ticker": t, "name": sp500_names[t], "day_change": rnd(-8, -2)} for t in tickers[5:]]
    gainers.sort(key=lambda x: x["day_change"], reverse=True)
    losers.sort(key=lambda x: x["day_change"])

    vix_level = round(random.uniform(14, 28), 2)
    tnx = round(random.uniform(3.8, 5.0), 3)
    irx = round(random.uniform(3.5, 5.2), 3)

    return {
        "date": date.today().isoformat(),
        "generated_at": datetime.now().isoformat(),
        "indices": indices,
        "sectors": sectors_raw,
        "macro": {
            "vix": {"level": vix_level, "5d_change": rnd(-3, 3)},
            "tnx": {"level": tnx, "label": "10Y Yield"},
            "irx": {"level": irx, "label": "2Y Yield"},
            "spread_2s10s": round(tnx - irx, 3),
        },
        "top_gainers": gainers,
        "top_losers": losers,
    }


def _resolve_target_date(target_date: str | date | None) -> date:
    if target_date is None:
        return date.today()
    if isinstance(target_date, date):
        return target_date
    return datetime.strptime(target_date, "%Y-%m-%d").date()


def run_market_brief(
    dry_run: bool = False,
    target_date: str | date | None = None,
) -> dict:
    target_dt = _resolve_target_date(target_date)
    today = target_dt.isoformat()
    out_path = STATE_DIR / f"market_brief_{today}.json"

    if dry_run:
        print("[DRY RUN] Generating mock market brief...")
        brief = mock_market_brief()
        brief["date"] = today
        print("  Building fallback market narrative (no external API calls)")
        brief["narrative"] = fallback_narrative(brief)
        out_path = STATE_DIR / "dry_run" / f"market_brief_{today}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(brief, indent=2))
        print(f"  Saved mock market brief → {out_path}")
        return brief

    import yfinance as yf

    all_symbols = list(INDICES.keys()) + list(SECTORS.keys()) + list(MACRO.keys())
    print(f"Fetching data for {len(all_symbols)} symbols...")

    end = datetime.combine(target_dt + timedelta(days=1), datetime.min.time())
    start = end - timedelta(days=380)

    data = yf.download(
        all_symbols,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    def get_close(sym: str) -> pd.Series:
        try:
            if len(all_symbols) == 1:
                return data["Close"].dropna()
            return data[sym]["Close"].dropna()
        except (KeyError, TypeError):
            return pd.Series(dtype=float)

    # Indices
    indices = {}
    for sym, name in INDICES.items():
        close = get_close(sym)
        if close.empty:
            continue
        indices[sym] = {
            "name": name,
            "price": round(float(close.iloc[-1]), 2),
            "day_change": _pct_change(close, 1),
            "ytd_change": _ytd_change(close),
        }

    # Sectors
    sectors_raw = []
    for sym, name in SECTORS.items():
        close = get_close(sym)
        if close.empty:
            continue
        sectors_raw.append({
            "symbol": sym,
            "name": name,
            "day_change": _pct_change(close, 1),
            "ytd_change": _ytd_change(close),
        })
    sectors_raw.sort(key=lambda x: x["day_change"], reverse=True)

    # Macro
    vix_close = get_close("^VIX")
    tnx_close = get_close("^TNX")
    irx_close = get_close("^IRX")

    tnx_val = round(float(tnx_close.iloc[-1]), 3) if not tnx_close.empty else 0
    irx_val = round(float(irx_close.iloc[-1]), 3) if not irx_close.empty else 0

    macro = {
        "vix": {
            "level": round(float(vix_close.iloc[-1]), 2) if not vix_close.empty else 0,
            "5d_change": _pct_change(vix_close, 5) if not vix_close.empty else 0,
        },
        "tnx": {"level": tnx_val, "label": "10Y Yield"},
        "irx": {"level": irx_val, "label": "2Y Yield"},
        "spread_2s10s": round(tnx_val - irx_val, 3),
    }

    # Top gainers / losers from top 100 liquid S&P 500 names
    SP100 = [
        "AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","BRK-B","UNH","JNJ",
        "V","XOM","JPM","PG","MA","HD","CVX","MRK","ABBV","LLY",
        "PEP","KO","COST","AVGO","WMT","MCD","CSCO","TMO","ABT","CRM",
        "ACN","DHR","NKE","TXN","NEE","UPS","PM","MS","BMY","UNP",
        "RTX","HON","INTC","QCOM","LOW","AMGN","BA","CAT","GS","BLK",
        "SPGI","DE","ISRG","MDT","ADP","GILD","SYK","BKNG","VRTX","REGN",
        "ADI","LRCX","ZTS","SCHW","CB","CI","MO","SO","DUK","CME",
        "PLD","BDX","CL","APD","HUM","ANET","ICE","MCK","EW","SLB",
        "EOG","PXD","MPC","PSX","VLO","CRWD","ZS","PANW","SNOW","NOW",
        "WDAY","DDOG","PLTR","COIN","MRVL","ARM","SMCI","ABNB","DASH","UBER",
    ]
    try:
        print("Fetching S&P 100 movers...")
        sp500_data = yf.download(
            SP100,
            start=(target_dt - timedelta(days=10)).strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by="ticker",
        )
        moves = []
        for t in SP100:
            try:
                close = sp500_data[t]["Close"].dropna()
                if len(close) >= 2:
                    chg = _pct_change(close, 1)
                    moves.append({"ticker": t, "name": t, "day_change": chg})
            except Exception:
                pass
        moves.sort(key=lambda x: x["day_change"], reverse=True)
        top_gainers = moves[:10]
        top_losers = moves[-10:][::-1]
        print(f"  Found {len(moves)} movers ({len(top_gainers)} gainers, {len(top_losers)} losers)")
    except Exception as e:
        print(f"  Warning: could not fetch S&P movers: {e}")
        top_gainers, top_losers = [], []

    brief = {
        "date": today,
        "generated_at": datetime.now().isoformat(),
        "indices": indices,
        "sectors": sectors_raw,
        "macro": macro,
        "top_gainers": top_gainers,
        "top_losers": top_losers,
    }

    # Generate AI market narrative via OpenAI GPT-5.4
    print("Generating AI market narrative...")
    narrative = _generate_narrative(brief)

    # Only use the new narrative if it has actual content. Otherwise rebuild
    # the deterministic fallback from the current live brief so we never carry
    # stale commentary forward from an older or dry-run render.
    if narrative_has_content(narrative):
        brief["narrative"] = narrative
    else:
        print("  Falling back to deterministic narrative")
        brief["narrative"] = fallback_narrative(brief)

    out_path.write_text(json.dumps(brief, indent=2, default=str))
    print(f"Saved market brief → {out_path}")
    return brief


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    run_market_brief(dry_run=dry)
