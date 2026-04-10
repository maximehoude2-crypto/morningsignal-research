"""
Daily Market Brief — fetches index, sector, and macro data via yfinance.
Saves to state/market_brief_YYYY-MM-DD.json.
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

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
    """Approximate YTD as change since ~252 trading days ago (1 year)."""
    if len(series) < 2:
        return 0.0
    start_val = series.iloc[0]  # start of fetched data ≈ year-start for 1yr fetch
    end_val = series.iloc[-1]
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


def run_market_brief(dry_run: bool = False) -> dict:
    today = date.today().isoformat()
    out_path = STATE_DIR / f"market_brief_{today}.json"

    if dry_run:
        print("[DRY RUN] Generating mock market brief...")
        brief = mock_market_brief()
        out_path.write_text(json.dumps(brief, indent=2))
        print(f"  Saved mock market brief → {out_path}")
        return brief

    import yfinance as yf

    all_symbols = list(INDICES.keys()) + list(SECTORS.keys()) + list(MACRO.keys())
    print(f"Fetching data for {len(all_symbols)} symbols...")

    end = datetime.now() + timedelta(days=1)
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

    tnx_val = round(float(tnx_close.iloc[-1]) / 10, 3) if not tnx_close.empty else 0
    irx_val = round(float(irx_close.iloc[-1]) / 10, 3) if not irx_close.empty else 0

    macro = {
        "vix": {
            "level": round(float(vix_close.iloc[-1]), 2) if not vix_close.empty else 0,
            "5d_change": _pct_change(vix_close, 5) if not vix_close.empty else 0,
        },
        "tnx": {"level": tnx_val, "label": "10Y Yield"},
        "irx": {"level": irx_val, "label": "2Y Yield"},
        "spread_2s10s": round(tnx_val - irx_val, 3),
    }

    # Top gainers / losers from S&P 500
    try:
        sp500_tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        sp500_list = sp500_tickers["Symbol"].tolist()[:100]  # limit for speed
        sp500_data = yf.download(sp500_list, period="2d", auto_adjust=True, progress=False, threads=True)
        moves = []
        for t in sp500_list:
            try:
                close = sp500_data[t]["Close"].dropna()
                if len(close) >= 2:
                    chg = _pct_change(close, 1)
                    moves.append({"ticker": t, "name": t, "day_change": chg})
            except Exception:
                pass
        moves.sort(key=lambda x: x["day_change"], reverse=True)
        top_gainers = moves[:5]
        top_losers = moves[-5:][::-1]
    except Exception as e:
        print(f"  Warning: could not fetch S&P 500 movers: {e}")
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

    out_path.write_text(json.dumps(brief, indent=2))
    print(f"Saved market brief → {out_path}")
    return brief


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    run_market_brief(dry_run=dry)
