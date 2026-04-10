"""
Breakout Scanner — scans S&P 1500 universe for high-scoring breakout setups.
Saves results to state/breakouts_YYYY-MM-DD.json.
"""

import json
import os
import time
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from io import StringIO

import pandas as pd
import numpy as np
import requests

from scanner.indicators import composite_score

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
CACHE_DIR = STATE_DIR / "prices_cache"
STATE_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

BENCHMARK = "SPY"
TOP_N = 15
MIN_SCORE = 60
BATCH_SIZE = 100
BATCH_SLEEP = 0.1


# ---------------------------------------------------------------------------
# Ticker universe
# ---------------------------------------------------------------------------

def _fetch_wikipedia_tickers(url: str, ticker_col: str) -> list[str]:
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        for tbl in tables:
            cols = [c for c in tbl.columns if ticker_col.lower() in str(c).lower()]
            if cols:
                raw = tbl[cols[0]].dropna().tolist()
                # Clean tickers (remove exchange suffix like .NYSE)
                return [str(t).split(".")[0].strip().replace("-", ".") for t in raw if str(t).strip()]
    except Exception as e:
        print(f"  Warning: could not fetch {url}: {e}")
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
    return combined


# ---------------------------------------------------------------------------
# Price data with caching
# ---------------------------------------------------------------------------

def _cache_path(ticker: str) -> Path:
    return CACHE_DIR / f"{ticker}_{date.today().isoformat()}.parquet"


def load_cached(ticker: str) -> pd.DataFrame | None:
    p = _cache_path(ticker)
    if p.exists():
        return pd.read_parquet(p)
    return None


def save_cache(ticker: str, df: pd.DataFrame):
    df.to_parquet(_cache_path(ticker))


def fetch_prices_batch(tickers: list[str], dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """Fetch 1 year of daily OHLCV for a batch of tickers."""
    if dry_run:
        return {}
    import yfinance as yf
    end = datetime.now()
    start = end - timedelta(days=380)
    data = yf.download(
        tickers,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    result = {}
    if len(tickers) == 1:
        t = tickers[0]
        if not data.empty:
            result[t] = data
    else:
        for t in tickers:
            try:
                df = data[t].dropna(how="all")
                if not df.empty:
                    result[t] = df
            except (KeyError, TypeError):
                pass
    return result


def get_price_data(ticker: str, all_data: dict) -> pd.DataFrame | None:
    cached = load_cached(ticker)
    if cached is not None:
        return cached
    df = all_data.get(ticker)
    if df is not None and not df.empty:
        save_cache(ticker, df)
    return df


# ---------------------------------------------------------------------------
# Consecutive-day flagging
# ---------------------------------------------------------------------------

def load_streak_state() -> dict:
    p = STATE_DIR / "streak_state.json"
    if p.exists():
        return json.loads(p.read_text())
    return {}


def save_streak_state(state: dict):
    p = STATE_DIR / "streak_state.json"
    p.write_text(json.dumps(state, indent=2))


def update_streaks(top_tickers: list[str]) -> dict:
    """Track how many consecutive days each ticker has been in top 15."""
    streak = load_streak_state()
    today_str = date.today().isoformat()
    # Expire old entries (not in top 15 today)
    new_streak = {}
    for t in top_tickers:
        prev = streak.get(t, {"count": 0, "since": today_str})
        new_streak[t] = {"count": prev["count"] + 1, "since": prev["since"]}
    save_streak_state(new_streak)
    return new_streak


def update_deep_dive_queue(streaks: dict):
    """Flag tickers with 3+ consecutive days for deep dive."""
    p = STATE_DIR / "deep_dive_queue.json"
    queue = json.loads(p.read_text()) if p.exists() else []
    existing = {e["ticker"] for e in queue}
    for ticker, info in streaks.items():
        if info["count"] >= 3 and ticker not in existing:
            queue.append({"ticker": ticker, "flagged_date": date.today().isoformat(),
                          "streak_days": info["count"]})
            print(f"  >> Auto-flagged {ticker} for deep dive ({info['count']} consecutive days)")
    p.write_text(json.dumps(queue, indent=2))


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


def run_scanner(dry_run: bool = False) -> list[dict]:
    """
    Main entry point. Returns list of top breakout dicts.
    """
    today = date.today().isoformat()
    out_path = STATE_DIR / f"breakouts_{today}.json"

    if dry_run:
        print("[DRY RUN] Generating mock breakout data...")
        results = mock_breakout_data()
        out_path.write_text(json.dumps(results, indent=2))
        print(f"  Saved {len(results)} mock breakouts → {out_path}")
        return results

    tickers = get_sp1500_tickers()
    if not tickers:
        print("ERROR: Could not fetch ticker universe")
        return []

    # Fetch benchmark
    print(f"Fetching benchmark ({BENCHMARK})...")
    bench_data = fetch_prices_batch([BENCHMARK])
    bench_df = bench_data.get(BENCHMARK)
    if bench_df is None or bench_df.empty:
        print("ERROR: Could not fetch benchmark data")
        return []
    bench_prices = bench_df["Close"].dropna()

    # Fetch ticker info (sector/name) from yfinance
    import yfinance as yf

    scored = []
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    print(f"Scanning {len(tickers)} tickers in {len(batches)} batches...")
    for batch_idx, batch in enumerate(batches):
        print(f"  Batch {batch_idx + 1}/{len(batches)}: {batch[:3]}...")

        # Check cache first
        uncached = [t for t in batch if load_cached(t) is None]
        batch_data = fetch_prices_batch(uncached) if uncached else {}

        for ticker in batch:
            df = get_price_data(ticker, batch_data)
            if df is None or len(df) < 200:
                continue

            prices = df["Close"].dropna()
            volumes = df["Volume"].dropna()

            # Align lengths
            min_len = min(len(prices), len(volumes), len(bench_prices))
            if min_len < 200:
                continue

            prices = prices.iloc[-min_len:]
            volumes = volumes.iloc[-min_len:]
            bench_aligned = bench_prices.iloc[-min_len:]

            sc = composite_score(prices, volumes, bench_aligned)
            if sc["total"] >= MIN_SCORE and sc["stage2"]:
                high_52w = prices.tail(252).max()
                avg_vol = int(volumes.tail(50).mean())
                vol_ratio = round(volumes.tail(5).mean() / avg_vol, 2) if avg_vol > 0 else 0
                scored.append({
                    "ticker": ticker,
                    "score": sc["total"],
                    "rs": sc["rs"],
                    "base": sc["base"],
                    "trend": sc["trend"],
                    "stage2": sc["stage2"],
                    "price": round(float(prices.iloc[-1]), 2),
                    "high_52w": round(float(high_52w), 2),
                    "pct_from_high": round((float(prices.iloc[-1]) / float(high_52w) - 1) * 100, 1),
                    "avg_volume": avg_vol,
                    "vol_ratio": vol_ratio,
                })

        time.sleep(BATCH_SLEEP)

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

    # Add rank
    for i, item in enumerate(top):
        item["rank"] = i + 1

    # Update streaks and deep dive queue
    streaks = update_streaks([item["ticker"] for item in top])
    update_deep_dive_queue(streaks)
    for item in top:
        item["streak_days"] = streaks.get(item["ticker"], {}).get("count", 1)

    out_path.write_text(json.dumps(top, indent=2))
    print(f"Saved {len(top)} breakouts → {out_path}")
    return top


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    results = run_scanner(dry_run=dry)
    print(f"\nTop breakouts: {[r['ticker'] for r in results[:5]]}")
