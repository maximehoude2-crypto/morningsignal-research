"""
Dashboard Data Aggregator.

Reads the existing market_brief + industry_scanner outputs and computes the
extra signals a best-in-class market dashboard needs:

  • Cross-asset grid (stocks / bonds / commodities / FX / crypto) with
    1d / 5d / MTD / YTD perf.
  • Regime composite:  risk-on / risk-off score with component breakdown.
  • Vol regime:        VIX level + 5d direction.
  • Credit stress:     HYG/IEF behavior.
  • Yield curve:       2s10s level + steepening trend.
  • Dollar regime:     UUP 1d/5d/YTD.
  • 52-week highs / lows count across the S&P 1500 universe (from the price
    cache the breakout scanner already maintains).
  • Factor crowdedness: % of breakouts in top-RS quintile vs market.
  • Style box:         large/small × growth/value (uses VUG/VLUE/IWN/SLY etc.)

Everything is saved into state/dashboard_YYYY-MM-DD.json.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
CACHE_DIR = STATE_DIR / "prices_cache"
STATE_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_target_date(target_date: str | date | None) -> date:
    if target_date is None:
        return date.today()
    if isinstance(target_date, date):
        return target_date
    return datetime.strptime(target_date, "%Y-%m-%d").date()


def _load_brief(target_dt: date) -> dict | None:
    p = STATE_DIR / f"market_brief_{target_dt.isoformat()}.json"
    if p.exists():
        return json.loads(p.read_text())
    return None


def _load_breakouts(target_dt: date) -> list:
    p = STATE_DIR / f"breakouts_{target_dt.isoformat()}.json"
    if p.exists():
        return json.loads(p.read_text())
    return []


def _load_industries(target_dt: date) -> dict | None:
    p = STATE_DIR / f"industries_{target_dt.isoformat()}.json"
    if p.exists():
        return json.loads(p.read_text())
    return None


def _theme(brief: dict, symbol: str) -> dict | None:
    for e in brief.get("thematic_etfs", []):
        if e.get("symbol") == symbol:
            return e
    return None


# ---------------------------------------------------------------------------
# 52-week highs / lows
# ---------------------------------------------------------------------------

def _fifty_two_week_extremes(target_dt: date) -> dict:
    """
    Walk every cached parquet for `target_dt` and count tickers within 1% of
    their 52-week high/low. Returns counts + sample tickers.
    """
    new_highs: list[str] = []
    new_lows: list[str] = []
    near_highs: list[str] = []
    near_lows: list[str] = []
    total = 0
    for p in CACHE_DIR.glob(f"*_{target_dt.isoformat()}.parquet"):
        try:
            df = pd.read_parquet(p)
        except Exception:  # noqa: BLE001
            continue
        if "Close" not in df:
            continue
        close = df["Close"].dropna()
        if len(close) < 200:
            continue
        ticker = p.stem.rsplit("_", 1)[0]
        total += 1
        last = float(close.iloc[-1])
        hi52 = float(close.tail(252).max())
        lo52 = float(close.tail(252).min())
        if hi52 <= 0:
            continue
        # Within last 5 trading days hitting a new 52w high?
        recent = close.tail(5)
        if len(recent) and float(recent.max()) >= hi52 * 0.999:
            new_highs.append(ticker)
        elif last >= hi52 * 0.99:
            near_highs.append(ticker)
        if len(recent) and float(recent.min()) <= lo52 * 1.001:
            new_lows.append(ticker)
        elif last <= lo52 * 1.01:
            near_lows.append(ticker)
    return {
        "universe": total,
        "new_highs": {"count": len(new_highs), "tickers": new_highs[:20]},
        "near_highs": {"count": len(near_highs), "tickers": near_highs[:20]},
        "new_lows":  {"count": len(new_lows),  "tickers": new_lows[:20]},
        "near_lows": {"count": len(near_lows), "tickers": near_lows[:20]},
    }


# ---------------------------------------------------------------------------
# Regime classification
# ---------------------------------------------------------------------------

def _regime_classifier(brief: dict, industries: dict | None) -> dict:
    """
    Composite regime score on a -100 to +100 scale.

    Components (each scaled -1..+1 then averaged):
      • VIX level + 5d direction (lower / falling = bullish)
      • Yield curve (steepening = bullish)
      • Credit stress (HYG/IEF rising = risk-on)
      • Breadth (% above 50d / 200d)
      • Universe momentum (SPY above 50d & 200d)
      • Dollar (DXY weakening = risk-on for equities)
    """
    macro = brief.get("macro", {})
    vix_level = macro.get("vix", {}).get("level", 0)
    vix_5d = macro.get("vix", {}).get("5d_change", 0)
    spread_2s10s = macro.get("spread_2s10s", 0)
    sig = brief.get("market_signal", {})

    # VIX score: prefer level <16 (=+1), >24 (=-1)
    if vix_level <= 0:
        vix_score = 0
    else:
        vix_score = max(-1.0, min(1.0, (20 - vix_level) / 8))
    if vix_5d < -5:
        vix_score = min(1.0, vix_score + 0.2)
    elif vix_5d > 5:
        vix_score = max(-1.0, vix_score - 0.2)

    # Curve: positive slope above ~0.5% is bullish
    curve_score = max(-1.0, min(1.0, (spread_2s10s - 0.0) / 1.0))

    # Credit: HYG move
    hyg = _theme(brief, "HYG")
    credit_score = 0
    if hyg:
        d5 = hyg.get("5d", 0)
        credit_score = max(-1.0, min(1.0, d5 / 2.0))

    # Breadth from industries
    breadth_score = 0
    stock_breadth = (industries or {}).get("stock_breadth", {})
    if industries:
        b50 = stock_breadth.get("pct_above_50d", 50)
        b200 = stock_breadth.get("pct_above_200d", 50)
        breadth_score = max(-1.0, min(1.0, ((b50 + b200) / 2 - 50) / 30))

    # Momentum: market_signal already has Momentum component
    momentum_score = 0
    comps = sig.get("components", {})
    if "Momentum" in comps:
        momentum_score = max(-1.0, min(1.0, comps["Momentum"].get("score", 0)))

    # Dollar
    uup = _theme(brief, "UUP")
    dollar_score = 0
    if uup:
        # falling dollar = risk-on (typically), so invert sign
        dollar_score = max(-1.0, min(1.0, -uup.get("5d", 0) / 2.0))

    components = {
        "VIX":        {"score": round(vix_score, 2),       "detail": f"{vix_level:.2f} ({vix_5d:+.1f}% 5d)"},
        "Curve":      {"score": round(curve_score, 2),     "detail": f"2s10s {spread_2s10s*100:+.0f}bps"},
        "Credit":     {"score": round(credit_score, 2),    "detail": f"HYG 5d {hyg.get('5d', 0):+.2f}%" if hyg else "n/a"},
        "Breadth":    {"score": round(breadth_score, 2),   "detail": (f"50d {stock_breadth.get('pct_above_50d', 0):.0f}% / "
                                                                       f"200d {stock_breadth.get('pct_above_200d', 0):.0f}%") if industries else "n/a"},
        "Momentum":   {"score": round(momentum_score, 2),  "detail": comps.get("Momentum", {}).get("detail", "n/a")},
        "Dollar":     {"score": round(dollar_score, 2),    "detail": f"UUP 5d {uup.get('5d', 0):+.2f}%" if uup else "n/a"},
    }

    avg = sum(c["score"] for c in components.values()) / max(1, len(components))
    score_100 = round(avg * 100, 1)

    if score_100 >= 35:
        regime = "Risk-On"
    elif score_100 >= 10:
        regime = "Constructive"
    elif score_100 >= -10:
        regime = "Neutral"
    elif score_100 >= -35:
        regime = "Defensive"
    else:
        regime = "Risk-Off"

    return {
        "regime": regime,
        "score": score_100,
        "components": components,
        "vix_level": vix_level,
        "vix_5d_change": vix_5d,
    }


# ---------------------------------------------------------------------------
# Cross-asset grid
# ---------------------------------------------------------------------------

CROSS_ASSET_LAYOUT = [
    ("Equities", [
        ("SPY",  "S&P 500",    "indices"),
        ("QQQ",  "Nasdaq 100", "indices"),
        ("IWM",  "Russell 2k", "indices"),
        ("DIA",  "Dow Jones",  "indices"),
        ("EFA",  "Dev ex-US",  "thematic"),
        ("EEM",  "EM",         "thematic"),
        ("FXI",  "China",      "thematic"),
    ]),
    ("Rates / Credit", [
        ("TLT",  "20+y UST",   "thematic"),
        ("IEF",  "7-10y UST",  "thematic"),
        ("LQD",  "Inv Grade",  "thematic"),
        ("HYG",  "High Yield", "thematic"),
        ("JNK",  "Junk",       "thematic"),
    ]),
    ("Commodities", [
        ("GLD",  "Gold",       "thematic"),
        ("SLV",  "Silver",     "thematic"),
        ("USO",  "Crude",      "thematic"),
        ("UNG",  "Nat Gas",    "thematic"),
        ("DBA",  "Agriculture","thematic"),
    ]),
    ("FX / Crypto", [
        ("UUP",  "US Dollar",  "thematic"),
        ("FXE",  "Euro",       "thematic"),
        ("FXY",  "Yen",        "thematic"),
        ("BITO", "Bitcoin",    "thematic"),
    ]),
]


def _build_cross_asset(brief: dict) -> list[dict]:
    rows = []
    for group, items in CROSS_ASSET_LAYOUT:
        group_rows = []
        for symbol, label, source in items:
            if source == "indices":
                idx = brief.get("indices", {}).get(symbol, {})
                if not idx:
                    continue
                group_rows.append({
                    "symbol": symbol,
                    "label": label,
                    "price": idx.get("price"),
                    "1d": idx.get("day_change", 0),
                    "5d": None,
                    "mtd": None,
                    "ytd": idx.get("ytd_change", 0),
                })
            else:
                t = _theme(brief, symbol)
                if not t:
                    continue
                group_rows.append({
                    "symbol": symbol,
                    "label": label,
                    "price": t.get("price"),
                    "1d": t.get("1d", 0),
                    "5d": t.get("5d", 0),
                    "mtd": t.get("mtd", 0),
                    "ytd": t.get("ytd", 0),
                })
        if group_rows:
            rows.append({"group": group, "rows": group_rows})
    return rows


# ---------------------------------------------------------------------------
# Style box (large/small × growth/value)
# ---------------------------------------------------------------------------

def _style_box(brief: dict) -> dict:
    """
    Approximate Morningstar-style style box. We don't have IWO/IWN/IWF/IWD
    fetched, so we approximate using the factor ETFs we DO have:
      • Growth: VUG, Value: VLUE, Size: SIZE, MinVol: USMV
      • Use SPY as benchmark
    Returns a 2x2 grid with day/5d performance per cell label.
    """
    factors = brief.get("factors", {}).get("performance", [])
    by_name = {f["name"]: f for f in factors}
    return {
        "growth_value": {
            "Growth": by_name.get("Growth"),
            "Value":  by_name.get("Value"),
        },
        "size_quality": {
            "Size":     by_name.get("Size"),
            "Quality":  by_name.get("Quality"),
            "Min Vol":  by_name.get("Min Vol"),
            "Momentum": by_name.get("Momentum"),
            "High Beta": by_name.get("High Beta"),
            "Low Vol":   by_name.get("Low Vol"),
        },
    }


# ---------------------------------------------------------------------------
# Crowdedness — among breakout-scanner top names, how concentrated are they?
# ---------------------------------------------------------------------------

def _crowdedness(brief: dict, breakouts: list, industries: dict | None) -> dict:
    """
    Three crowdedness indicators:
      • % of breakouts in the top-leadership sector
      • Industry concentration (Herfindahl) of breakouts
      • Top RS-leadership streak count (3+ days)
    """
    if not breakouts:
        return {"top_sector_pct": 0, "herfindahl": 0, "streak3plus": 0,
                "top_sector": "n/a"}

    sector_counts: dict[str, int] = {}
    industry_counts: dict[str, int] = {}
    industry_map = {}
    if industries:
        # Build ticker→industry mapping from the industries payload (if present)
        for rec in industries.get("industries", []):
            for t in rec.get("constituent_events", {}).get("golden_cross", []) + \
                     rec.get("constituent_events", {}).get("reclaim_50d", []):
                industry_map[t] = rec["industry"]

    for b in breakouts:
        sec = b.get("sector", "Unknown")
        sector_counts[sec] = sector_counts.get(sec, 0) + 1

    top_sector = max(sector_counts, key=sector_counts.get)
    top_sector_pct = round(sector_counts[top_sector] / len(breakouts) * 100, 1)

    # Herfindahl on sector concentration
    n = len(breakouts)
    herf = sum((c / n) ** 2 for c in sector_counts.values())
    herf_norm = round(herf * 10000)

    streak = sum(1 for b in breakouts if b.get("streak_days", 0) >= 3)

    return {
        "top_sector": top_sector,
        "top_sector_pct": top_sector_pct,
        "herfindahl": herf_norm,
        "streak3plus": streak,
        "sector_distribution": sector_counts,
    }


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def run_dashboard_data(
    dry_run: bool = False,
    target_date: str | date | None = None,
) -> dict:
    target_dt = _resolve_target_date(target_date)
    today = target_dt.isoformat()
    out_path = STATE_DIR / f"dashboard_{today}.json"
    if dry_run:
        out_path = STATE_DIR / "dry_run" / f"dashboard_{today}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)

    brief = _load_brief(target_dt)
    if brief is None:
        print("ERROR: no market brief found — dashboard data skipped.")
        return {}

    breakouts = _load_breakouts(target_dt)
    industries = _load_industries(target_dt)

    print("Building regime composite...")
    regime = _regime_classifier(brief, industries)

    print("Counting 52-week extremes from price cache...")
    if dry_run:
        extremes = {"universe": 0,
                    "new_highs": {"count": 0, "tickers": []},
                    "near_highs": {"count": 0, "tickers": []},
                    "new_lows":  {"count": 0, "tickers": []},
                    "near_lows": {"count": 0, "tickers": []}}
    else:
        extremes = _fifty_two_week_extremes(target_dt)

    print("Building cross-asset grid...")
    cross_asset = _build_cross_asset(brief)

    print("Building style box...")
    style_box = _style_box(brief)

    print("Computing crowdedness...")
    crowdedness = _crowdedness(brief, breakouts, industries)

    payload = {
        "date": today,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "regime": regime,
        "extremes_52w": extremes,
        "cross_asset": cross_asset,
        "style_box": style_box,
        "crowdedness": crowdedness,
    }
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Saved dashboard data → {out_path.relative_to(BASE_DIR)}")
    print(f"  Regime: {regime['regime']} (score {regime['score']}) · "
          f"52w highs: {extremes['new_highs']['count']} · 52w lows: {extremes['new_lows']['count']}")
    return payload


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    run_dashboard_data(dry_run=dry)
