"""
Industry / Sub-Sector Scanner.

For each ticker in the S&P 1500 universe (already cached by breakout_scanner)
we look up its GICS Sub-Industry, build an equal-weighted industry price index,
and compute:

  • Multi-timeframe performance (1d / 5d / MTD / QTD / YTD)
  • RRG coordinates (rs-ratio, rs-momentum) vs SPY with 4-week tail
  • Quadrant + quadrant transition (rotation breakouts)
  • Moving-average events on the industry index (within last 5 trading days):
        - Golden cross (50d crosses above 200d)
        - Death cross  (50d crosses below 200d)
        - Price reclaiming 50d / 200d
        - Price losing  50d / 200d
        - EMA(12/26) momentum cross (MACD-style)
  • Constituent breadth at industry level
        - % of constituents above 50d MA
        - % of constituents above 200d MA
        - # of constituent golden / death crosses today / last 5d

Everything is saved to state/industries_YYYY-MM-DD.json.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
CACHE_DIR = STATE_DIR / "prices_cache"
INDUSTRY_MAP_PATH = STATE_DIR / "industry_map.json"
STATE_DIR.mkdir(exist_ok=True)

BENCHMARK = "SPY"
RECENT_WINDOW = 5            # how many trading days back qualifies as "just"
MIN_INDUSTRY_SIZE = 3        # minimum constituents to include an industry
MIN_HISTORY = 220            # need ~1y for golden/death cross logic


# ---------------------------------------------------------------------------
# Wikipedia GICS scraping
# ---------------------------------------------------------------------------

WIKI_SOURCES = [
    {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "ticker_col": "Symbol",
        "sector_col": "GICS Sector",
        "industry_col": "GICS Sub-Industry",
        "name_col": "Security",
        "label": "S&P500",
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "ticker_col": "Symbol",
        "sector_col": "GICS sector",
        "industry_col": "GICS sub-industry",
        "name_col": "Company",
        "label": "S&P400",
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
        "ticker_col": "Symbol",
        "sector_col": "GICS sector",
        "industry_col": "GICS sub-industry",
        "name_col": "Company",
        "label": "S&P600",
    },
]


def _coerce_col(df: pd.DataFrame, candidate: str) -> str | None:
    """Return the first column whose name contains `candidate` (case-insensitive)."""
    for col in df.columns:
        if candidate.lower() in str(col).lower():
            return col
    return None


def _scrape_wiki_industries(source: dict) -> dict[str, dict]:
    """Return {ticker: {sector, industry, name}} for one Wikipedia source."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        r = requests.get(source["url"], headers=headers, timeout=15)
        r.raise_for_status()
        tables = pd.read_html(StringIO(r.text))
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: could not fetch {source['url']}: {exc}")
        return {}

    out: dict[str, dict] = {}
    for tbl in tables:
        ticker_col = _coerce_col(tbl, source["ticker_col"])
        industry_col = _coerce_col(tbl, source["industry_col"])
        sector_col = _coerce_col(tbl, source["sector_col"])
        name_col = _coerce_col(tbl, source["name_col"])
        if not ticker_col or not industry_col:
            continue
        for _, row in tbl[[c for c in [ticker_col, industry_col, sector_col, name_col] if c]].iterrows():
            raw = row[ticker_col]
            if pd.isna(raw):
                continue
            ticker = str(raw).split(".")[0].strip().replace("-", ".")
            if not ticker:
                continue
            industry = str(row[industry_col]).strip() if industry_col and not pd.isna(row[industry_col]) else "Unknown"
            sector = str(row[sector_col]).strip() if sector_col and not pd.isna(row[sector_col]) else "Unknown"
            name = str(row[name_col]).strip() if name_col and not pd.isna(row[name_col]) else ticker
            out[ticker] = {"industry": industry, "sector": sector, "name": name}
        if out:
            break  # first usable table is enough per page
    return out


def get_industry_map(force_refresh: bool = False, max_age_days: int = 7) -> dict[str, dict]:
    """Build/load a {ticker: {sector, industry, name}} mapping."""
    if INDUSTRY_MAP_PATH.exists() and not force_refresh:
        try:
            payload = json.loads(INDUSTRY_MAP_PATH.read_text())
            ts = datetime.fromisoformat(payload.get("generated_at", "1970-01-01T00:00:00"))
            age_days = (datetime.now() - ts).days
            if age_days <= max_age_days and payload.get("map"):
                return payload["map"]
        except Exception as exc:  # noqa: BLE001
            print(f"  Warning: industry map cache unreadable ({exc}); refetching.")

    print("Building GICS industry map from Wikipedia...")
    merged: dict[str, dict] = {}
    for source in WIKI_SOURCES:
        chunk = _scrape_wiki_industries(source)
        merged.update({k: v for k, v in chunk.items() if k not in merged})
        print(f"  {source['label']}: {len(chunk)} tickers (running total {len(merged)})")

    if not merged:
        # Fallback: try to load whatever exists, even if stale
        if INDUSTRY_MAP_PATH.exists():
            payload = json.loads(INDUSTRY_MAP_PATH.read_text())
            print("  Using stale industry_map.json as fallback.")
            return payload.get("map", {})
        return {}

    INDUSTRY_MAP_PATH.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(merged),
        "map": merged,
    }, indent=2))
    return merged


# ---------------------------------------------------------------------------
# Price loading (reuses breakout_scanner's parquet cache)
# ---------------------------------------------------------------------------

def _load_cached_prices(ticker: str, target_dt: date) -> pd.DataFrame | None:
    p = CACHE_DIR / f"{ticker}_{target_dt.isoformat()}.parquet"
    if p.exists():
        try:
            return pd.read_parquet(p)
        except Exception:  # noqa: BLE001
            return None
    return None


def _load_benchmark(target_dt: date) -> pd.Series | None:
    """Try cache first; fall back to a fresh yfinance fetch."""
    cached = _load_cached_prices(BENCHMARK, target_dt)
    if cached is not None and "Close" in cached:
        return cached["Close"].dropna()
    try:
        import yfinance as yf
        end = datetime.combine(target_dt + timedelta(days=1), datetime.min.time())
        start = end - timedelta(days=380)
        hist = yf.Ticker(BENCHMARK).history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            auto_adjust=True,
        )
        if hist is None or hist.empty:
            return None
        return hist["Close"].dropna()
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: benchmark fetch failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _safe_pct(curr: float, ref: float) -> float:
    if ref is None or ref == 0 or pd.isna(ref):
        return 0.0
    return round((curr / ref - 1) * 100, 2)


def _multi_timeframe(close: pd.Series, target_dt: date) -> dict:
    if close is None or len(close) < 2:
        return {"price": 0.0, "1d": 0.0, "5d": 0.0, "mtd": 0.0, "qtd": 0.0, "ytd": 0.0}
    price = float(close.iloc[-1])

    def back(n: int) -> float:
        return _safe_pct(price, float(close.iloc[-n - 1])) if len(close) > n else 0.0

    d1 = back(1)
    d5 = back(5)

    idx = close.index
    if idx.tz is not None:
        anchor_today = pd.Timestamp(target_dt).tz_localize(idx.tz)
    else:
        anchor_today = pd.Timestamp(target_dt)

    def first_at_or_after(ts: pd.Timestamp) -> float | None:
        sub = close.loc[close.index >= ts]
        return float(sub.iloc[0]) if len(sub) else None

    mtd_val = first_at_or_after(pd.Timestamp(target_dt.replace(day=1)).tz_localize(idx.tz) if idx.tz is not None else pd.Timestamp(target_dt.replace(day=1)))
    q_month = ((target_dt.month - 1) // 3) * 3 + 1
    qtd_val = first_at_or_after(pd.Timestamp(target_dt.replace(month=q_month, day=1)).tz_localize(idx.tz) if idx.tz is not None else pd.Timestamp(target_dt.replace(month=q_month, day=1)))
    ytd_val = first_at_or_after(pd.Timestamp(target_dt.replace(month=1, day=1)).tz_localize(idx.tz) if idx.tz is not None else pd.Timestamp(target_dt.replace(month=1, day=1)))

    return {
        "price": round(price, 2),
        "1d": d1,
        "5d": d5,
        "mtd": _safe_pct(price, mtd_val) if mtd_val else 0.0,
        "qtd": _safe_pct(price, qtd_val) if qtd_val else 0.0,
        "ytd": _safe_pct(price, ytd_val) if ytd_val else 0.0,
    }


def _ma_events(close: pd.Series, window: int = RECENT_WINDOW) -> dict:
    """
    Detect MA cross / reclaim / loss events that occurred within `window` days.
    Returns a dict of bool flags + days_since for each.
    """
    out = {
        "golden_cross": False, "golden_cross_age": None,
        "death_cross": False, "death_cross_age": None,
        "reclaim_50d": False, "reclaim_50d_age": None,
        "lost_50d": False,    "lost_50d_age": None,
        "reclaim_200d": False, "reclaim_200d_age": None,
        "lost_200d": False,    "lost_200d_age": None,
        "ema_bull_cross": False, "ema_bear_cross": False, "ema_cross_age": None,
        "above_50d": False, "above_200d": False,
        "ma50_rising": False, "ma200_rising": False,
    }
    if close is None or len(close) < MIN_HISTORY:
        return out

    ma50 = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    last_idx = len(close) - 1
    out["above_50d"]  = bool(close.iloc[-1] > ma50.iloc[-1]) if not pd.isna(ma50.iloc[-1]) else False
    out["above_200d"] = bool(close.iloc[-1] > ma200.iloc[-1]) if not pd.isna(ma200.iloc[-1]) else False
    out["ma50_rising"]  = bool(ma50.iloc[-1] > ma50.iloc[-10]) if last_idx >= 10 and not pd.isna(ma50.iloc[-10]) else False
    out["ma200_rising"] = bool(ma200.iloc[-1] > ma200.iloc[-20]) if last_idx >= 20 and not pd.isna(ma200.iloc[-20]) else False

    def _find_recent_cross(series_a: pd.Series, series_b: pd.Series, direction: str) -> int | None:
        """direction = 'up' (a crosses above b) or 'down' (a crosses below b)."""
        a = series_a.iloc[-(window + 1):]
        b = series_b.iloc[-(window + 1):]
        for i in range(1, len(a)):
            prev_a, prev_b = a.iloc[i - 1], b.iloc[i - 1]
            cur_a, cur_b = a.iloc[i], b.iloc[i]
            if any(pd.isna(x) for x in (prev_a, prev_b, cur_a, cur_b)):
                continue
            if direction == "up" and prev_a <= prev_b and cur_a > cur_b:
                return len(a) - 1 - i
            if direction == "down" and prev_a >= prev_b and cur_a < cur_b:
                return len(a) - 1 - i
        return None

    age = _find_recent_cross(ma50, ma200, "up")
    if age is not None:
        out["golden_cross"], out["golden_cross_age"] = True, age
    age = _find_recent_cross(ma50, ma200, "down")
    if age is not None:
        out["death_cross"], out["death_cross_age"] = True, age

    age = _find_recent_cross(close, ma50, "up")
    if age is not None:
        out["reclaim_50d"], out["reclaim_50d_age"] = True, age
    age = _find_recent_cross(close, ma50, "down")
    if age is not None:
        out["lost_50d"], out["lost_50d_age"] = True, age

    age = _find_recent_cross(close, ma200, "up")
    if age is not None:
        out["reclaim_200d"], out["reclaim_200d_age"] = True, age
    age = _find_recent_cross(close, ma200, "down")
    if age is not None:
        out["lost_200d"], out["lost_200d_age"] = True, age

    # EMA momentum cross (12/26)
    ema_fast = _ema(close, 12)
    ema_slow = _ema(close, 26)
    age = _find_recent_cross(ema_fast, ema_slow, "up")
    if age is not None:
        out["ema_bull_cross"], out["ema_cross_age"] = True, age
    age2 = _find_recent_cross(ema_fast, ema_slow, "down")
    if age2 is not None and (out["ema_cross_age"] is None or age2 < out["ema_cross_age"]):
        out["ema_bear_cross"], out["ema_bull_cross"], out["ema_cross_age"] = True, False, age2

    return out


def _compute_rrg(idx_close: pd.Series, bench_close: pd.Series,
                 rs_period: int = 10, mom_period: int = 10) -> dict | None:
    """JdK RS-Ratio / RS-Momentum (mirrors thematic_scanner._compute_rrg)."""
    if idx_close is None or bench_close is None or len(idx_close) < 60 or len(bench_close) < 60:
        return None
    common = idx_close.index.intersection(bench_close.index)
    s = idx_close.loc[common]
    b = bench_close.loc[common]
    if len(s) < 60:
        return None

    rs_line = (s / b) * 100
    rs_sma = rs_line.rolling(rs_period).mean()
    rs_ratio = ((rs_line / rs_sma - 1) * 100).ewm(span=rs_period, adjust=False).mean()
    rs_ratio_final = (100 + rs_ratio).dropna()

    rs_mom = (rs_ratio_final - rs_ratio_final.shift(mom_period)).ewm(span=mom_period, adjust=False).mean()
    rs_momentum_final = (100 + rs_mom).dropna()

    if len(rs_ratio_final) < 5 or len(rs_momentum_final) < 5:
        return None

    def quadrant(r: float, m: float) -> str:
        if r >= 100 and m >= 100:
            return "Leading"
        if r >= 100:
            return "Weakening"
        if m >= 100:
            return "Improving"
        return "Lagging"

    latest_r = round(float(rs_ratio_final.iloc[-1]), 2)
    latest_m = round(float(rs_momentum_final.iloc[-1]), 2)
    cur_q = quadrant(latest_r, latest_m)

    # quadrant 5d ago — used to detect rotation events
    prev_q = None
    if len(rs_ratio_final) >= 6 and len(rs_momentum_final) >= 6:
        prev_q = quadrant(float(rs_ratio_final.iloc[-6]), float(rs_momentum_final.iloc[-6]))

    tail = []
    for offset in [15, 10, 5, 0]:
        idx = -(offset + 1) if offset > 0 else -1
        if abs(idx) <= len(rs_ratio_final) and abs(idx) <= len(rs_momentum_final):
            tail.append({
                "r": round(float(rs_ratio_final.iloc[idx]), 2),
                "m": round(float(rs_momentum_final.iloc[idx]), 2),
            })

    return {
        "rs_ratio": latest_r,
        "rs_momentum": latest_m,
        "quadrant": cur_q,
        "prev_quadrant": prev_q,
        "tail": tail,
    }


def _classify_rotation(rrg: dict) -> str | None:
    """
    Identify rotation 'breakouts'.
    Returns one of: 'rotation_breakout', 'rotation_recovery', 'rotation_topping',
                    'rotation_breakdown', or None when no transition.
    """
    if not rrg:
        return None
    cur = rrg.get("quadrant")
    prev = rrg.get("prev_quadrant")
    if not cur or not prev or cur == prev:
        return None
    # Bullish rotation: industry just crossed into Leading from Improving / Lagging
    if cur == "Leading" and prev in ("Improving", "Lagging"):
        return "rotation_breakout"
    # Recovery: just left Lagging into Improving
    if cur == "Improving" and prev == "Lagging":
        return "rotation_recovery"
    # Topping: Leading → Weakening
    if cur == "Weakening" and prev == "Leading":
        return "rotation_topping"
    # Breakdown: Weakening / Improving → Lagging
    if cur == "Lagging" and prev in ("Weakening", "Improving"):
        return "rotation_breakdown"
    return None


# ---------------------------------------------------------------------------
# Industry aggregation
# ---------------------------------------------------------------------------

def _build_industry_index(constituent_closes: list[pd.Series]) -> pd.Series | None:
    """
    Equal-weighted price index. Each constituent normalised to 100 at first common
    date, then averaged across constituents.
    """
    if not constituent_closes:
        return None
    aligned = [s for s in constituent_closes if s is not None and len(s) >= MIN_HISTORY]
    if len(aligned) < MIN_INDUSTRY_SIZE:
        return None
    df = pd.concat(aligned, axis=1).dropna(how="all")
    df = df.ffill().bfill()
    if df.empty:
        return None
    base = df.iloc[0]
    base = base.replace(0, np.nan)
    if base.isna().all():
        return None
    rebased = (df.divide(base) * 100).mean(axis=1)
    return rebased.dropna()


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def _resolve_target_date(target_date: str | date | None) -> date:
    if target_date is None:
        return date.today()
    if isinstance(target_date, date):
        return target_date
    return datetime.strptime(target_date, "%Y-%m-%d").date()


def run_industry_scan(
    dry_run: bool = False,
    target_date: str | date | None = None,
) -> dict:
    target_dt = _resolve_target_date(target_date)
    today = target_dt.isoformat()
    out_path = STATE_DIR / f"industries_{today}.json"

    if dry_run:
        print("[DRY RUN] Industry scanner — skipping live aggregation.")
        summary = {
            "n_industries": 0,
            "n_constituents": 0,
            "pct_above_50d": 0,
            "pct_above_200d": 0,
            "top_industries_1d": [],
            "bottom_industries_1d": [],
            "rotation_breakouts": [],
            "rotation_breakdowns": [],
        }
        payload = {
            "date": today,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "summary": summary,
            "industries": [],
            "rotation": {"rotation_breakout": [], "rotation_breakdown": []},
            "industry_events": {},
            "stock_events": {},
            "stock_breadth": {
                "pct_above_50d": summary["pct_above_50d"],
                "pct_above_200d": summary["pct_above_200d"],
            },
        }
        out_path = STATE_DIR / "dry_run" / f"industries_{today}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2))
        return payload

    industry_map = get_industry_map()
    if not industry_map:
        print("ERROR: industry map empty — cannot build industries.")
        return {"industries": [], "events": {}, "rotation": {}, "summary": {}}

    bench = _load_benchmark(target_dt)
    if bench is None:
        print("ERROR: benchmark unavailable — cannot compute RRG.")

    # Group tickers by industry, but only those with cached price data for today.
    print("Aggregating constituents by GICS Sub-Industry...")
    industry_buckets: dict[str, list[str]] = {}
    industry_meta: dict[str, dict] = {}
    for ticker, meta in industry_map.items():
        df = _load_cached_prices(ticker, target_dt)
        if df is None or "Close" not in df or len(df["Close"].dropna()) < MIN_HISTORY:
            continue
        ind = meta.get("industry") or "Unknown"
        sect = meta.get("sector") or "Unknown"
        industry_buckets.setdefault(ind, []).append(ticker)
        industry_meta.setdefault(ind, {"sector": sect, "tickers": []})
        industry_meta[ind]["tickers"].append(ticker)

    print(f"  {len(industry_buckets)} industries with ≥{MIN_INDUSTRY_SIZE} usable tickers (filtered next).")

    industries_out: list[dict] = []
    rotation_events: dict[str, list[str]] = {
        "rotation_breakout": [], "rotation_recovery": [],
        "rotation_topping": [],  "rotation_breakdown": [],
    }
    cross_events: dict[str, list[dict]] = {
        "golden_cross": [], "death_cross": [],
        "reclaim_50d": [], "lost_50d": [],
        "reclaim_200d": [], "lost_200d": [],
        "ema_bull_cross": [], "ema_bear_cross": [],
    }

    # Pre-compute MA events for every constituent (for breadth + stock-level events)
    print("Computing constituent MA events...")
    constituent_events: dict[str, dict] = {}
    constituent_closes: dict[str, pd.Series] = {}
    for ticker in industry_map:
        df = _load_cached_prices(ticker, target_dt)
        if df is None or "Close" not in df:
            continue
        close = df["Close"].dropna()
        if len(close) < MIN_HISTORY:
            continue
        constituent_closes[ticker] = close
        constituent_events[ticker] = _ma_events(close)

    print(f"  {len(constituent_events)} constituents have ≥{MIN_HISTORY} days of price data.")

    for industry, tickers in industry_buckets.items():
        usable_tickers = [t for t in tickers if t in constituent_closes]
        if len(usable_tickers) < MIN_INDUSTRY_SIZE:
            continue
        closes = [constituent_closes[t] for t in usable_tickers]
        idx_close = _build_industry_index(closes)
        if idx_close is None or len(idx_close) < MIN_HISTORY:
            continue

        perf = _multi_timeframe(idx_close, target_dt)
        events = _ma_events(idx_close)
        rrg = _compute_rrg(idx_close, bench) if bench is not None else None
        rotation = _classify_rotation(rrg) if rrg else None

        # Constituent breadth
        n = len(usable_tickers)
        above_50 = sum(1 for t in usable_tickers if constituent_events[t]["above_50d"])
        above_200 = sum(1 for t in usable_tickers if constituent_events[t]["above_200d"])
        gc = [t for t in usable_tickers if constituent_events[t]["golden_cross"]]
        dc = [t for t in usable_tickers if constituent_events[t]["death_cross"]]
        r50 = [t for t in usable_tickers if constituent_events[t]["reclaim_50d"]]
        l50 = [t for t in usable_tickers if constituent_events[t]["lost_50d"]]
        r200 = [t for t in usable_tickers if constituent_events[t]["reclaim_200d"]]
        l200 = [t for t in usable_tickers if constituent_events[t]["lost_200d"]]
        ebull = [t for t in usable_tickers if constituent_events[t]["ema_bull_cross"]]
        ebear = [t for t in usable_tickers if constituent_events[t]["ema_bear_cross"]]

        sector = industry_meta[industry]["sector"]

        record = {
            "industry": industry,
            "sector": sector,
            "n_constituents": n,
            "performance": perf,
            "ma": events,
            "rrg": rrg,
            "rotation": rotation,
            "breadth": {
                "pct_above_50d": round(above_50 / n * 100, 1),
                "pct_above_200d": round(above_200 / n * 100, 1),
                "golden_cross_count": len(gc),
                "death_cross_count": len(dc),
                "reclaim_50d_count": len(r50),
                "lost_50d_count": len(l50),
                "reclaim_200d_count": len(r200),
                "lost_200d_count": len(l200),
                "ema_bull_count": len(ebull),
                "ema_bear_count": len(ebear),
            },
            "constituent_events": {
                "golden_cross": gc[:10],
                "death_cross": dc[:10],
                "reclaim_50d": r50[:10],
                "lost_50d": l50[:10],
                "reclaim_200d": r200[:10],
                "lost_200d": l200[:10],
                "ema_bull_cross": ebull[:10],
                "ema_bear_cross": ebear[:10],
            },
        }
        industries_out.append(record)

        if rotation:
            rotation_events[rotation].append(industry)

        # Industry-level MA events
        for key in ("golden_cross", "death_cross", "reclaim_50d", "lost_50d",
                    "reclaim_200d", "lost_200d", "ema_bull_cross", "ema_bear_cross"):
            if events.get(key):
                cross_events[key].append({
                    "industry": industry, "sector": sector,
                    "age": events.get(f"{key}_age") or events.get("ema_cross_age"),
                    "perf_5d": perf.get("5d", 0.0),
                })

    # Sort outputs
    industries_out.sort(key=lambda x: x["performance"].get("1d", 0), reverse=True)
    for key, lst in cross_events.items():
        cross_events[key] = sorted(lst, key=lambda x: x.get("age") or 99)

    # Aggregate stock-level events across the whole universe
    stock_event_counts = {
        "golden_cross": [t for t, e in constituent_events.items() if e["golden_cross"]],
        "death_cross":  [t for t, e in constituent_events.items() if e["death_cross"]],
        "reclaim_50d":  [t for t, e in constituent_events.items() if e["reclaim_50d"]],
        "lost_50d":     [t for t, e in constituent_events.items() if e["lost_50d"]],
        "reclaim_200d": [t for t, e in constituent_events.items() if e["reclaim_200d"]],
        "lost_200d":    [t for t, e in constituent_events.items() if e["lost_200d"]],
        "ema_bull_cross": [t for t, e in constituent_events.items() if e["ema_bull_cross"]],
        "ema_bear_cross": [t for t, e in constituent_events.items() if e["ema_bear_cross"]],
    }

    # Universe-wide breadth
    total = len(constituent_events)
    breadth_above_50 = sum(1 for e in constituent_events.values() if e["above_50d"])
    breadth_above_200 = sum(1 for e in constituent_events.values() if e["above_200d"])

    summary = {
        "n_industries": len(industries_out),
        "n_constituents": total,
        "pct_above_50d": round(breadth_above_50 / total * 100, 1) if total else 0,
        "pct_above_200d": round(breadth_above_200 / total * 100, 1) if total else 0,
        "top_industries_1d":  [i["industry"] for i in industries_out[:5]],
        "bottom_industries_1d": [i["industry"] for i in sorted(industries_out, key=lambda x: x["performance"]["1d"])[:5]],
        "rotation_breakouts": rotation_events["rotation_breakout"],
        "rotation_breakdowns": rotation_events["rotation_breakdown"],
    }

    payload = {
        "date": today,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "industries": industries_out,
        "rotation": rotation_events,
        "industry_events": cross_events,
        "stock_events": {
            k: {"count": len(v), "tickers": v[:25]}
            for k, v in stock_event_counts.items()
        },
        "stock_breadth": {
            "pct_above_50d": summary["pct_above_50d"],
            "pct_above_200d": summary["pct_above_200d"],
        },
    }

    out_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Saved industry scan → {out_path.relative_to(BASE_DIR)}")
    print(f"  Industries: {summary['n_industries']} · "
          f"Rotation breakouts: {len(summary['rotation_breakouts'])} · "
          f"Golden crosses (industry-level): {len(cross_events['golden_cross'])} · "
          f"Death crosses: {len(cross_events['death_cross'])}")
    return payload


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    run_industry_scan(dry_run=dry)
