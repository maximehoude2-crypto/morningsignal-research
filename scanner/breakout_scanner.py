"""
Breakout Scanner — scans S&P 1500 universe for high-scoring breakout setups.
Saves results to state/breakouts_YYYY-MM-DD.json.
"""

import time
from datetime import date, datetime, timedelta
from pathlib import Path
from io import StringIO
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from scanner.common import USER_AGENT, load_json, resolve_target_date, save_json
from scanner.indicators import composite_from_components, score_components

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
CACHE_DIR = STATE_DIR / "prices_cache"
STATE_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

BENCHMARK = "SPY"
TOP_N = 15
MIN_SCORE = 60
MIN_UNIVERSE = 1200
BATCH_SIZE = 100
BATCH_SLEEP = 1.0
INFO_SLEEP = 0.5
CACHE_MAX_AGE_DAYS = 10
MARKET_TZ = ZoneInfo("America/New_York")
MARKET_CLOSE_HOUR = 16


# ---------------------------------------------------------------------------
# Ticker universe
# ---------------------------------------------------------------------------

def _fetch_wikipedia_tickers(url: str, ticker_col: str) -> list[str]:
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(2):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            tables = pd.read_html(StringIO(response.text))
            for tbl in tables:
                cols = [c for c in tbl.columns if ticker_col.lower() in str(c).lower()]
                if cols:
                    raw = tbl[cols[0]].dropna().tolist()
                    # Wikipedia writes class shares with dots (BRK.B);
                    # yfinance wants dashes (BRK-B)
                    return [str(t).strip().replace(".", "-") for t in raw if str(t).strip()]
        except Exception as e:
            print(f"  Warning: could not fetch {url} (attempt {attempt + 1}/2): {e}")
            if attempt == 0:
                time.sleep(2)
    return []


def get_sp1500_tickers() -> list[str]:
    """Download S&P 500 + 400 + 600 tickers from Wikipedia."""
    print("Fetching ticker universe from Wikipedia...")
    sp500 = _fetch_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol"
    )
    sp400 = _fetch_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", "Ticker"
    )
    sp600 = _fetch_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", "Ticker"
    )
    combined = list(dict.fromkeys(sp500 + sp400 + sp600))  # deduplicate preserving order
    print(f"  Universe: {len(sp500)} S&P500 + {len(sp400)} S&P400 + {len(sp600)} S&P600 = {len(combined)} unique tickers")
    if len(combined) < MIN_UNIVERSE:
        raise RuntimeError(
            f"Ticker universe sanity check failed: {len(combined)} tickers fetched "
            f"(expected >= {MIN_UNIVERSE}) — refusing to scan a shrunken universe"
        )
    return combined


# ---------------------------------------------------------------------------
# Price data with caching
# ---------------------------------------------------------------------------

def _resolve_target_date(target_date: str | date | None) -> date:
    if isinstance(target_date, date):
        return target_date
    return date.fromisoformat(resolve_target_date(target_date))


def _cache_path(ticker: str, target_date: str | date | None = None) -> Path:
    target_dt = _resolve_target_date(target_date)
    return CACHE_DIR / f"{ticker}_{target_dt.isoformat()}.parquet"


def _cache_is_fresh(path: Path, target_dt: date) -> bool:
    """
    A cache file for a past date is always valid. For today's date it is
    only valid if written after the 4:00 PM ET close — otherwise a midday
    run would freeze a partial intraday bar for the after-close run.
    """
    if not path.exists():
        return False
    if target_dt != date.today():
        return True
    close = datetime(target_dt.year, target_dt.month, target_dt.day,
                     MARKET_CLOSE_HOUR, 0, tzinfo=MARKET_TZ)
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=MARKET_TZ)
    return mtime > close


def load_cached(ticker: str, target_date: str | date | None = None) -> pd.DataFrame | None:
    target_dt = _resolve_target_date(target_date)
    p = _cache_path(ticker, target_dt)
    if _cache_is_fresh(p, target_dt):
        return pd.read_parquet(p)
    return None


def save_cache(ticker: str, df: pd.DataFrame, target_date: str | date | None = None):
    df.to_parquet(_cache_path(ticker, target_date))


def prune_cache(max_age_days: int = CACHE_MAX_AGE_DAYS) -> int:
    """Delete parquet cache files older than max_age_days. Returns count."""
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    for f in CACHE_DIR.glob("*.parquet"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        except OSError:
            pass
    if removed:
        print(f"  Pruned {removed} cache files older than {max_age_days} days")
    return removed


def fetch_prices_batch(
    tickers: list[str],
    dry_run: bool = False,
    target_date: str | date | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch 1 year of daily OHLCV for a batch of tickers."""
    if dry_run or not tickers:
        return {}
    import yfinance as yf
    target_dt = _resolve_target_date(target_date)
    end = datetime.combine(target_dt + timedelta(days=1), datetime.min.time())
    start = end - timedelta(days=380)

    data = None
    for attempt in range(2):
        try:
            data = yf.download(
                tickers,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"  Warning: batch download failed (attempt {attempt + 1}/2): {e}")
            data = None
        if data is not None and not data.empty:
            break
        if attempt == 0:
            time.sleep(5)  # backoff before the single retry
    if data is None or data.empty:
        return {}

    result = {}
    if len(tickers) == 1:
        t = tickers[0]
        df = data
        if isinstance(df.columns, pd.MultiIndex):
            try:
                df = df[t]
            except KeyError:
                df = df.droplevel(0, axis=1)
        df = df.dropna(how="all")
        if not df.empty:
            result[t] = df
    else:
        for t in tickers:
            try:
                df = data[t].dropna(how="all")
                if not df.empty:
                    result[t] = df
            except (KeyError, TypeError):
                pass
    return result


def get_price_data(
    ticker: str,
    all_data: dict,
    target_date: str | date | None = None,
) -> pd.DataFrame | None:
    cached = load_cached(ticker, target_date)
    if cached is not None:
        return cached
    df = all_data.get(ticker)
    if df is not None and not df.empty:
        save_cache(ticker, df, target_date)
    return df


def _normalize_index(obj):
    """Coerce a price index to tz-naive normalized dates for reliable joins."""
    idx = pd.DatetimeIndex(obj.index)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    obj.index = idx.normalize()
    return obj


# ---------------------------------------------------------------------------
# Consecutive-day flagging
# ---------------------------------------------------------------------------

STREAK_STATE_PATH = STATE_DIR / "streak_state.json"
DEEP_DIVE_QUEUE_PATH = STATE_DIR / "deep_dive_queue.json"


def load_streak_state() -> dict:
    return load_json(STREAK_STATE_PATH, default={}) or {}


def save_streak_state(state: dict):
    save_json(STREAK_STATE_PATH, state)


def update_streaks(top_tickers: list[str], target_date: str | date | None = None) -> dict:
    """
    Track how many consecutive days each ticker has been in top 15.
    Idempotent per date: a ticker already counted for the target date is
    not incremented again on a same-day rerun.
    """
    streak = load_streak_state()
    target_dt = _resolve_target_date(target_date)
    today_str = target_dt.isoformat()
    # Expire old entries (not in top 15 today)
    new_streak = {}
    for t in top_tickers:
        prev = streak.get(t)
        if prev and prev.get("last_counted") == today_str:
            new_streak[t] = prev  # already counted for this date
        elif prev:
            new_streak[t] = {"count": prev["count"] + 1,
                             "since": prev.get("since", today_str),
                             "last_counted": today_str}
        else:
            new_streak[t] = {"count": 1, "since": today_str,
                             "last_counted": today_str}
    save_streak_state(new_streak)
    return new_streak


def update_deep_dive_queue(streaks: dict, target_date: str | date | None = None):
    """Flag tickers with 3+ consecutive days for deep dive."""
    queue = load_json(DEEP_DIVE_QUEUE_PATH, default=[]) or []
    existing = {e["ticker"] for e in queue}
    target_dt = _resolve_target_date(target_date)
    for ticker, info in streaks.items():
        if info["count"] >= 3 and ticker not in existing:
            queue.append({"ticker": ticker, "flagged_date": target_dt.isoformat(),
                          "streak_days": info["count"]})
            print(f"  >> Auto-flagged {ticker} for deep dive ({info['count']} consecutive days)")
    save_json(DEEP_DIVE_QUEUE_PATH, queue)


# ---------------------------------------------------------------------------
# Main scanner
# ---------------------------------------------------------------------------

MOCK_TICKERS = [
    ("NVDA", "NVIDIA Corp", "Technology"),
    ("META", "Meta Platforms", "Communication Services"),
    ("AAPL", "Apple Inc", "Technology"),
    ("MSFT", "Microsoft Corp", "Technology"),
    ("GOOGL", "Alphabet Inc", "Communication Services"),
    ("AMZN", "Amazon.com Inc", "Consumer Discretionary"),
    ("TSLA", "Tesla Inc", "Consumer Discretionary"),
    ("AMD", "Advanced Micro Devices", "Technology"),
    ("CRWD", "CrowdStrike Holdings", "Technology"),
    ("PANW", "Palo Alto Networks", "Technology"),
    ("ANET", "Arista Networks", "Technology"),
    ("AXON", "Axon Enterprise", "Industrials"),
    ("CELH", "Celsius Holdings", "Consumer Staples"),
    ("SMCI", "Super Micro Computer", "Technology"),
    ("MELI", "MercadoLibre", "Consumer Discretionary"),
]


def mock_breakout_data() -> list[dict]:
    """Generate realistic-looking mock breakout results for dry run."""
    import random
    random.seed(42)
    results = []
    for i, (ticker, name, sector) in enumerate(MOCK_TICKERS):
        score = round(95 - i * 2.3 + random.uniform(-1, 1), 1)
        rs = round(min(100, score + random.uniform(-5, 10)), 1)
        base = round(min(100, score + random.uniform(-10, 5)), 1)
        trend = round(min(100, 75 + random.uniform(-5, 25)), 1)
        price = round(random.uniform(80, 800), 2)
        high_52w = round(price * random.uniform(1.0, 1.12), 2)
        pct_from_high = round((price / high_52w - 1) * 100, 1)
        avg_vol = int(random.uniform(2e6, 50e6))
        vol_ratio = round(random.uniform(1.1, 2.5), 2)
        results.append({
            "rank": i + 1,
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "score": score,
            "rs": rs,
            "base": base,
            "trend": trend,
            "stage2": True,
            "price": price,
            "high_52w": high_52w,
            "pct_from_high": pct_from_high,
            "avg_volume": avg_vol,
            "vol_ratio": vol_ratio,
        })
    return results


def run_scanner(
    dry_run: bool = False,
    target_date: str | date | None = None,
) -> list[dict]:
    """
    Main entry point. Returns list of top breakout dicts.
    Two-pass scoring: pass 1 computes per-ticker components (stage 2 gate,
    raw RS, base, trend); pass 2 percentile-ranks raw RS across the scanned
    universe and combines into the final composite.
    """
    target_dt = _resolve_target_date(target_date)
    today = target_dt.isoformat()
    out_path = STATE_DIR / f"breakouts_{today}.json"

    if dry_run:
        print("[DRY RUN] Generating mock breakout data...")
        results = mock_breakout_data()
        out_path = STATE_DIR / "dry_run" / f"breakouts_{today}.json"
        save_json(out_path, results)
        print(f"  Saved {len(results)} mock breakouts → {out_path}")
        return results

    prune_cache()

    tickers = get_sp1500_tickers()

    import yfinance as yf

    # Fetch benchmark using Ticker API (avoids MultiIndex issue with yf.download)
    print(f"Fetching benchmark ({BENCHMARK})...")
    bench_end = datetime.combine(target_dt + timedelta(days=1), datetime.min.time())
    bench_start = bench_end - timedelta(days=380)
    bench_hist = yf.Ticker(BENCHMARK).history(
        start=bench_start.strftime("%Y-%m-%d"),
        end=bench_end.strftime("%Y-%m-%d"),
        auto_adjust=True,
    )
    if bench_hist is None or bench_hist.empty:
        print("ERROR: Could not fetch benchmark data")
        return []
    bench_prices = _normalize_index(bench_hist["Close"].dropna()).rename("Benchmark")

    # Pass 1: per-ticker components
    candidates = []
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    print(f"Scanning {len(tickers)} tickers in {len(batches)} batches...")
    for batch_idx, batch in enumerate(batches):
        print(f"  Batch {batch_idx + 1}/{len(batches)}: {batch[:3]}...")

        # Check cache first
        uncached = [t for t in batch if load_cached(t, target_dt) is None]
        batch_data = (
            fetch_prices_batch(uncached, target_date=target_dt) if uncached else {}
        )

        for ticker in batch:
            df = get_price_data(ticker, batch_data, target_dt)
            if df is None or len(df) < 200:
                continue

            # Align price, volume and benchmark on the DatetimeIndex
            pv = _normalize_index(df[["Close", "Volume"]].dropna())
            joined = pv.join(bench_prices, how="inner")
            if len(joined) < 200:
                continue

            prices = joined["Close"]
            volumes = joined["Volume"]
            bench_aligned = joined["Benchmark"]

            comp = score_components(prices, volumes, bench_aligned)

            high_52w = prices.tail(252).max()
            # Average volume excludes the last 5 (breakout) sessions
            avg_vol = int(volumes.iloc[-55:-5].mean())
            vol_ratio = round(float(volumes.tail(5).mean()) / avg_vol, 2) if avg_vol > 0 else 0
            candidates.append({
                "ticker": ticker,
                "stage2": comp["stage2"],
                "rs_raw": comp["rs_raw"],
                "base": comp["base"],
                "trend": comp["trend"],
                "price": round(float(prices.iloc[-1]), 2),
                "high_52w": round(float(high_52w), 2),
                "pct_from_high": round((float(prices.iloc[-1]) / float(high_52w) - 1) * 100, 1),
                "avg_volume": avg_vol,
                "vol_ratio": vol_ratio,
            })

        time.sleep(BATCH_SLEEP)

    if not candidates:
        print("ERROR: no tickers produced usable price data")
        save_json(out_path, [])
        return []

    # Pass 2: percentile-rank raw RS across the scanned universe (0-100),
    # then combine into the final composite. Stage 2 is a hard gate.
    rs_percentiles = pd.Series([c["rs_raw"] for c in candidates]).rank(pct=True) * 100

    scored = []
    for cand, rs_pct in zip(candidates, rs_percentiles):
        if not cand["stage2"]:
            continue
        rs_score = round(float(rs_pct), 1)
        total = composite_from_components(rs_score, cand["base"], cand["trend"])
        if total >= MIN_SCORE:
            scored.append({
                "ticker": cand["ticker"],
                "score": total,
                "rs": rs_score,
                "rs_raw": round(float(cand["rs_raw"]), 4),
                "base": round(float(cand["base"]), 1),
                "trend": round(float(cand["trend"]), 1),
                "stage2": True,
                "price": cand["price"],
                "high_52w": cand["high_52w"],
                "pct_from_high": cand["pct_from_high"],
                "avg_volume": cand["avg_volume"],
                "vol_ratio": cand["vol_ratio"],
            })

    # Sort and take top N
    scored.sort(key=lambda x: x["score"], reverse=True)
    top = scored[:TOP_N]

    # Enrich with name/sector
    print("Enriching top results with metadata...")
    for item in top:
        try:
            info = yf.Ticker(item["ticker"]).info
            item["name"] = info.get("longName", item["ticker"])
            item["sector"] = info.get("sector", "Unknown")
        except Exception:
            item.setdefault("name", item["ticker"])
            item.setdefault("sector", "Unknown")
        time.sleep(INFO_SLEEP)  # .info is the most rate-limited endpoint

    # Add rank
    for i, item in enumerate(top):
        item["rank"] = i + 1

    # Update streaks and deep dive queue
    streaks = update_streaks([item["ticker"] for item in top], target_dt)
    update_deep_dive_queue(streaks, target_dt)
    for item in top:
        item["streak_days"] = streaks.get(item["ticker"], {}).get("count", 1)

    save_json(out_path, top)
    print(f"Saved {len(top)} breakouts → {out_path}")
    return top


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    results = run_scanner(dry_run=dry)
    print(f"\nTop breakouts: {[r['ticker'] for r in results[:5]]}")
