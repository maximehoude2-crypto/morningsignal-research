"""
Thematic ETF Scanner — fetches thematic/factor ETFs, computes multi-timeframe
returns, RRG coordinates, factor heatmap, and market signal indicator.
Saves enriched data into the market brief JSON.
"""

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"

# ---------------------------------------------------------------------------
# ETF Universes
# ---------------------------------------------------------------------------

THEMATIC_ETFS = {
    # AI / Tech Themes
    "SMH":  ("Semiconductors", "AI / Tech"),
    "IGV":  ("Software", "AI / Tech"),
    "BOTZ": ("Robotics & AI", "AI / Tech"),
    "WCLD": ("Cloud Computing", "AI / Tech"),
    "HACK": ("Cybersecurity", "AI / Tech"),
    "ARKK": ("ARK Innovation", "AI / Tech"),
    # Fixed Income
    "TLT":  ("20+ Yr Treasury", "Fixed Income"),
    "HYG":  ("High Yield Corp", "Fixed Income"),
    "LQD":  ("Inv Grade Corp", "Fixed Income"),
    "JNK":  ("Junk Bonds", "Fixed Income"),
    "IEF":  ("7-10 Yr Treasury", "Fixed Income"),
    # Commodities
    "GLD":  ("Gold", "Commodities"),
    "SLV":  ("Silver", "Commodities"),
    "USO":  ("Crude Oil", "Commodities"),
    "UNG":  ("Natural Gas", "Commodities"),
    "DBA":  ("Agriculture", "Commodities"),
    # Crypto
    "BITO": ("Bitcoin Futures", "Crypto"),
    # Currency
    "UUP":  ("US Dollar Index", "Currency"),
    "FXE":  ("Euro", "Currency"),
    "FXY":  ("Japanese Yen", "Currency"),
    # International
    "EEM":  ("Emerging Markets", "International"),
    "EFA":  ("Developed ex-US", "International"),
    "FXI":  ("China Large Cap", "International"),
    # Other Themes
    "XHB":  ("Homebuilders", "Themes"),
    "JETS": ("Airlines", "Themes"),
    "XBI":  ("Biotech", "Themes"),
    "KRE":  ("Regional Banks", "Themes"),
    "ITB":  ("Home Construction", "Themes"),
}

FACTOR_ETFS = {
    "Momentum":  "MTUM",
    "Value":     "VLUE",
    "Growth":    "VUG",
    "Quality":   "QUAL",
    "Min Vol":   "USMV",
    "Size":      "SIZE",
    "High Beta": "SPHB",
    "Low Vol":   "SPLV",
}

# Sectors for RRG (already fetched in market_brief, but we need them here too)
SECTOR_ETFS = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLY": "Cons. Discr.",
    "XLP": "Cons. Staples",
    "XLC": "Comm. Svcs",
}

SIGNAL_EXTRA = ["RSP"]  # Equal-weight S&P for breadth proxy


# ---------------------------------------------------------------------------
# Multi-timeframe returns
# ---------------------------------------------------------------------------

def _compute_returns(close: pd.Series, today_dt: date) -> dict:
    """Compute 1d, 5d, MTD, QTD, YTD returns from a close price series."""
    if close.empty or len(close) < 2:
        return {}

    current = float(close.iloc[-1])
    price = round(current, 2)

    def safe_ret(ref):
        if ref == 0 or pd.isna(ref):
            return 0.0
        return round((current / ref - 1) * 100, 2)

    # 1d
    d1 = safe_ret(float(close.iloc[-2])) if len(close) >= 2 else 0.0

    # 5d
    d5 = safe_ret(float(close.iloc[-6])) if len(close) >= 6 else 0.0

    # MTD
    mtd_mask = close.index >= pd.Timestamp(today_dt.replace(day=1))
    mtd_vals = close.loc[mtd_mask]
    mtd = safe_ret(float(mtd_vals.iloc[0])) if len(mtd_vals) >= 1 else 0.0

    # QTD
    q_month = ((today_dt.month - 1) // 3) * 3 + 1
    qtd_mask = close.index >= pd.Timestamp(today_dt.replace(month=q_month, day=1))
    qtd_vals = close.loc[qtd_mask]
    qtd = safe_ret(float(qtd_vals.iloc[0])) if len(qtd_vals) >= 1 else 0.0

    # YTD
    ytd_mask = close.index >= pd.Timestamp(today_dt.replace(month=1, day=1))
    ytd_vals = close.loc[ytd_mask]
    ytd = safe_ret(float(ytd_vals.iloc[0])) if len(ytd_vals) >= 1 else 0.0

    return {"price": price, "1d": d1, "5d": d5, "mtd": mtd, "qtd": qtd, "ytd": ytd}


# ---------------------------------------------------------------------------
# Relative Rotation Graph (JdK methodology)
# ---------------------------------------------------------------------------

def _compute_rrg(etf_close: pd.Series, bench_close: pd.Series,
                 rs_period: int = 10, mom_period: int = 10) -> dict | None:
    """Compute JdK RS-Ratio and RS-Momentum for RRG plotting."""
    if len(etf_close) < 60 or len(bench_close) < 60:
        return None

    # Align series
    common = etf_close.index.intersection(bench_close.index)
    etf = etf_close.loc[common]
    bench = bench_close.loc[common]

    if len(etf) < 60:
        return None

    # Raw relative strength
    rs_line = (etf / bench) * 100

    # RS-Ratio: smoothed ratio vs its own SMA
    rs_sma = rs_line.rolling(rs_period).mean()
    rs_ratio_raw = (rs_line / rs_sma - 1) * 100
    rs_ratio = rs_ratio_raw.ewm(span=rs_period, adjust=False).mean()
    rs_ratio_final = 100 + rs_ratio

    # RS-Momentum: rate of change of RS-Ratio, smoothed
    rs_mom_raw = rs_ratio_final - rs_ratio_final.shift(mom_period)
    rs_momentum = rs_mom_raw.ewm(span=mom_period, adjust=False).mean()
    rs_momentum_final = 100 + rs_momentum

    # Drop NaN
    rs_ratio_final = rs_ratio_final.dropna()
    rs_momentum_final = rs_momentum_final.dropna()

    if len(rs_ratio_final) < 5 or len(rs_momentum_final) < 5:
        return None

    latest_r = round(float(rs_ratio_final.iloc[-1]), 2)
    latest_m = round(float(rs_momentum_final.iloc[-1]), 2)

    # Quadrant
    if latest_r >= 100 and latest_m >= 100:
        quadrant = "Leading"
    elif latest_r >= 100:
        quadrant = "Weakening"
    elif latest_m >= 100:
        quadrant = "Improving"
    else:
        quadrant = "Lagging"

    # Trailing tail (4 weekly snapshots)
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
        "quadrant": quadrant,
        "tail": tail,
    }


# ---------------------------------------------------------------------------
# Factor heatmap (weekly spreads vs SPY)
# ---------------------------------------------------------------------------

def _compute_factor_heatmap(factor_closes: dict, bench_close: pd.Series,
                            weeks: int = 12) -> tuple[list, list]:
    """Weekly factor-vs-benchmark spreads for heatmap. Returns (rows, week_labels)."""
    rows = []
    week_labels = []

    bench_weekly = bench_close.resample('W-FRI').last().dropna()

    for name, close in factor_closes.items():
        weekly = close.resample('W-FRI').last().dropna()
        common = weekly.index.intersection(bench_weekly.index)
        if len(common) < weeks + 1:
            continue
        common = common[-(weeks + 1):]
        f_ret = weekly.loc[common].pct_change().iloc[1:] * 100
        b_ret = bench_weekly.loc[common].pct_change().iloc[1:] * 100
        spread = (f_ret - b_ret).round(2)

        if not week_labels:
            week_labels = [d.strftime("%b %d") for d in spread.index]

        rows.append({
            "factor": name,
            "spreads": spread.tolist(),
        })

    return rows, week_labels


# ---------------------------------------------------------------------------
# Market Signal Indicator
# ---------------------------------------------------------------------------

def _compute_market_signal(closes: dict, spy_close: pd.Series) -> dict:
    """Composite signal: VIX, yield curve, credit, breadth, momentum."""
    scores = {}

    # 1. VIX Level + Trend (20%)
    try:
        vix = closes["^VIX"]
        vix_level = float(vix.iloc[-1])
        vix_sma50 = float(vix.rolling(50).mean().iloc[-1])
        if vix_level < 15:
            vs = 1.0
        elif vix_level < 20:
            vs = 0.5
        elif vix_level < 25:
            vs = -0.25
        elif vix_level < 30:
            vs = -0.5
        else:
            vs = -1.0
        vs += 0.25 if vix_level < vix_sma50 else -0.25
        vs = max(-1, min(1, vs))
        vix_trend = "falling" if vix_level < vix_sma50 else "rising"
        scores["VIX"] = {"score": round(vs, 2), "detail": f"{vix_level:.1f} ({vix_trend})"}
    except Exception:
        scores["VIX"] = {"score": 0, "detail": "N/A"}

    # 2. Yield Curve (20%)
    try:
        tnx = closes["^TNX"]
        irx = closes["^IRX"]
        spread_now = float(tnx.iloc[-1] - irx.iloc[-1])
        spread_20d = float(tnx.iloc[-20] - irx.iloc[-20]) if len(tnx) >= 20 else spread_now
        if spread_now > 0.5:
            cs = 1.0
        elif spread_now > 0:
            cs = 0.5
        elif spread_now > -0.5:
            cs = -0.5
        else:
            cs = -1.0
        cs += 0.25 if spread_now > spread_20d else -0.25
        cs = max(-1, min(1, cs))
        direction = "steepening" if spread_now > spread_20d else "flattening"
        scores["Yield Curve"] = {"score": round(cs, 2), "detail": f"{spread_now:+.2f}% ({direction})"}
    except Exception:
        scores["Yield Curve"] = {"score": 0, "detail": "N/A"}

    # 3. Credit Spreads: HYG/IEF ratio (20%)
    try:
        hyg = closes["HYG"]
        ief = closes["IEF"]
        ratio = hyg / ief
        ratio_now = float(ratio.iloc[-1])
        ratio_sma = float(ratio.rolling(50).mean().iloc[-1])
        pct = (ratio_now / ratio_sma - 1) * 100
        cs = 1.0 if ratio_now > ratio_sma else -1.0
        if abs(pct) < 1:
            cs *= 0.5
        scores["Credit"] = {"score": round(cs, 2), "detail": f"HYG/IEF {'above' if cs > 0 else 'below'} 50d ({pct:+.1f}%)"}
    except Exception:
        scores["Credit"] = {"score": 0, "detail": "N/A"}

    # 4. Breadth: RSP/SPY ratio (20%)
    try:
        rsp = closes["RSP"]
        ratio = rsp / spy_close
        ratio_now = float(ratio.iloc[-1])
        ratio_sma = float(ratio.rolling(50).mean().iloc[-1])
        pct = (ratio_now / ratio_sma - 1) * 100
        bs = 1.0 if ratio_now > ratio_sma else -1.0
        if abs(pct) < 0.5:
            bs *= 0.5
        scores["Breadth"] = {"score": round(bs, 2), "detail": f"RSP/SPY {'above' if bs > 0 else 'below'} 50d ({pct:+.1f}%)"}
    except Exception:
        scores["Breadth"] = {"score": 0, "detail": "N/A"}

    # 5. Momentum: SPY vs MAs (20%)
    try:
        spy_now = float(spy_close.iloc[-1])
        spy_50d = float(spy_close.rolling(50).mean().iloc[-1])
        spy_200d = float(spy_close.rolling(200).mean().iloc[-1])
        ms = 0
        ms += 0.5 if spy_now > spy_50d else -0.5
        ms += 0.5 if spy_now > spy_200d else -0.5
        above_50 = "above" if spy_now > spy_50d else "below"
        above_200 = "above" if spy_now > spy_200d else "below"
        scores["Momentum"] = {"score": round(ms, 2), "detail": f"SPY {above_50} 50d, {above_200} 200d"}
    except Exception:
        scores["Momentum"] = {"score": 0, "detail": "N/A"}

    # Composite
    composite = sum(s["score"] for s in scores.values()) / max(len(scores), 1)
    if composite > 0.25:
        signal = "RISK ON"
    elif composite < -0.25:
        signal = "RISK OFF"
    else:
        signal = "NEUTRAL"

    return {
        "signal": signal,
        "score": round(composite, 3),
        "components": scores,
    }


# ---------------------------------------------------------------------------
# Catalyst Calendar (earnings + economic releases)
# ---------------------------------------------------------------------------

def _scrape_catalyst_calendar() -> list:
    """Scrape upcoming earnings (Zacks) and economic events for next 5 trading days."""
    import json
    import requests
    from datetime import timedelta
    from bs4 import BeautifulSoup

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    events = []
    today_dt = date.today()

    # Zacks earnings calendar
    for offset in range(5):
        d = today_dt + timedelta(days=offset)
        # Skip weekends
        if d.isoweekday() > 5:
            continue
        d_str = d.strftime("%Y-%m-%d")
        try:
            url = f"https://www.zacks.com/includes/classes/z2_class_calendarfunctions_702.php?calltype=eventscal&date={d_str}&type=1"
            r = requests.get(url, headers={**headers, 'Referer': 'https://www.zacks.com/earnings/earnings-calendar'}, timeout=10)
            if r.status_code == 200 and r.text.strip():
                try:
                    data = json.loads(r.text)
                    for row in data.get("data", [])[:10]:
                        if isinstance(row, list) and len(row) >= 4:
                            # Zacks returns HTML in cells — extract text
                            ticker_html = row[0] if isinstance(row[0], str) else ""
                            ticker_match = re.search(r'>([A-Z]{1,5})<', ticker_html) if 'import re' or True else None
                            import re as _re
                            ticker_match = _re.search(r'>([A-Z]{1,5})<', ticker_html)
                            ticker = ticker_match.group(1) if ticker_match else ""
                            company = BeautifulSoup(row[1], 'html.parser').get_text(strip=True) if isinstance(row[1], str) else str(row[1])
                            time_str = row[2] if isinstance(row[2], str) else ""
                            eps_est = row[3] if isinstance(row[3], str) else ""

                            if ticker and len(ticker) <= 5:
                                events.append({
                                    "date": d.strftime("%b %d"),
                                    "time": time_str[:20] if time_str else "TBD",
                                    "event": f"{ticker} earnings",
                                    "detail": f"{company[:40]} — EPS est: {eps_est}" if eps_est else company[:40],
                                    "type": "earnings",
                                    "ticker": ticker,
                                })
                except (json.JSONDecodeError, KeyError):
                    pass
        except Exception:
            pass

    # Fallback: if Zacks failed, try scraping Zacks HTML page
    if not events:
        try:
            r = requests.get("https://www.zacks.com/earnings/earnings-calendar", headers=headers, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                for row in soup.select('table#earnings_calendar_table tbody tr, table.earnings_table tbody tr')[:15]:
                    cells = row.select('td')
                    if len(cells) >= 3:
                        ticker = cells[0].get_text(strip=True)[:5]
                        company = cells[1].get_text(strip=True)[:40] if len(cells) > 1 else ""
                        if ticker and ticker.isalpha() and len(ticker) <= 5:
                            events.append({
                                "date": today_dt.strftime("%b %d"),
                                "time": "TBD",
                                "event": f"{ticker} earnings",
                                "detail": company,
                                "type": "earnings",
                                "ticker": ticker,
                            })
        except Exception:
            pass

    # ForexFactory economic calendar
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                         headers=headers, timeout=10)
        if r.status_code == 200:
            cal_data = json.loads(r.text)
            for ev in cal_data:
                if ev.get("impact", "") in ("High", "Medium"):
                    ev_date_str = ev.get("date", "")
                    try:
                        ev_date = datetime.strptime(ev_date_str, "%Y-%m-%dT%H:%M:%S%z").date()
                        if today_dt <= ev_date <= today_dt + timedelta(days=7):
                            events.append({
                                "date": ev_date.strftime("%b %d"),
                                "time": ev.get("time", ""),
                                "event": ev.get("title", ""),
                                "detail": f"Forecast: {ev.get('forecast', 'N/A')} | Prior: {ev.get('previous', 'N/A')}",
                                "type": "economic",
                                "ticker": "",
                            })
                    except Exception:
                        pass
    except Exception:
        pass

    # Deduplicate and limit
    seen = set()
    unique = []
    for e in events:
        key = f"{e['date']}_{e['event']}"
        if key not in seen:
            seen.add(key)
            unique.append(e)

    return unique[:20]


# ---------------------------------------------------------------------------
# Notable Options Flow
# ---------------------------------------------------------------------------

def _scrape_notable_flow() -> list:
    """Scrape unusual options activity from Barchart."""
    import requests
    from bs4 import BeautifulSoup

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    flows = []

    try:
        r = requests.get("https://www.barchart.com/options/unusual-activity/stocks",
                         headers=headers, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for row in soup.select('table tbody tr')[:10]:
                cells = row.select('td')
                if len(cells) >= 7:
                    ticker = cells[0].get_text(strip=True)
                    exp = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    strike = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    call_put = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                    volume = cells[5].get_text(strip=True) if len(cells) > 5 else ""
                    oi = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                    if ticker and len(ticker) <= 5:
                        direction = "bullish" if "Call" in call_put else "bearish"
                        flows.append({
                            "ticker": ticker,
                            "type": call_put,
                            "strike": strike,
                            "expiry": exp,
                            "volume": volume,
                            "open_interest": oi,
                            "direction": direction,
                            "summary": f"{ticker} {exp} ${strike} {call_put}s — Vol: {volume} vs OI: {oi}",
                        })
    except Exception:
        pass

    # Fallback: if Barchart blocked, try scraping Yahoo Finance options movers
    if not flows:
        try:
            r = requests.get("https://finance.yahoo.com/markets/options/most-active/",
                             headers=headers, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                for a in soup.select('a'):
                    t = a.get_text(strip=True)
                    if t and 3 <= len(t) <= 5 and t.isupper():
                        href = a.get('href', '')
                        if '/quote/' in href:
                            flows.append({
                                "ticker": t,
                                "type": "Active",
                                "strike": "",
                                "expiry": "",
                                "volume": "",
                                "open_interest": "",
                                "direction": "neutral",
                                "summary": f"{t} — among most active options today",
                            })
                            if len(flows) >= 5:
                                break
        except Exception:
            pass

    return flows[:8]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_thematic_scan(dry_run: bool = False) -> dict:
    """Fetch thematic ETF data and compute all derived metrics."""
    today_dt = date.today()

    if dry_run:
        return _mock_thematic_data()

    import yfinance as yf

    # Collect all unique symbols
    all_syms = set()
    all_syms.update(THEMATIC_ETFS.keys())
    all_syms.update(FACTOR_ETFS.values())
    all_syms.update(SECTOR_ETFS.keys())
    all_syms.update(SIGNAL_EXTRA)
    all_syms.update(["SPY", "^VIX", "^TNX", "^IRX"])
    all_syms = sorted(all_syms)

    print(f"  Fetching {len(all_syms)} thematic/factor/signal symbols...")

    end = datetime.now() + timedelta(days=1)
    start = end - timedelta(days=380)

    data = yf.download(
        all_syms,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    def get_close(sym: str) -> pd.Series:
        try:
            if len(all_syms) == 1:
                return data["Close"].dropna()
            return data[sym]["Close"].dropna()
        except (KeyError, TypeError):
            return pd.Series(dtype=float)

    spy_close = get_close("SPY")

    # --- Feature 1: Thematic ETF returns ---
    print("  Computing thematic ETF returns...")
    thematic_list = []
    for sym, (name, category) in THEMATIC_ETFS.items():
        close = get_close(sym)
        if close.empty:
            continue
        rets = _compute_returns(close, today_dt)
        if rets:
            thematic_list.append({
                "symbol": sym, "name": name, "category": category, **rets
            })
    thematic_list.sort(key=lambda x: x.get("1d", 0), reverse=True)

    # --- Feature 2: RRG ---
    print("  Computing Relative Rotation Graph...")
    rrg_data = []
    # Sectors
    for sym, name in SECTOR_ETFS.items():
        close = get_close(sym)
        rrg = _compute_rrg(close, spy_close)
        if rrg:
            rrg_data.append({"symbol": sym, "name": name, "group": "sector", **rrg})
    # Thematic
    for sym, (name, cat) in THEMATIC_ETFS.items():
        close = get_close(sym)
        rrg = _compute_rrg(close, spy_close)
        if rrg:
            rrg_data.append({"symbol": sym, "name": name, "group": "thematic", **rrg})

    # --- Feature 3: Factor dashboard ---
    print("  Computing factor performance...")
    factor_list = []
    factor_closes = {}
    for fname, sym in FACTOR_ETFS.items():
        close = get_close(sym)
        if close.empty:
            continue
        rets = _compute_returns(close, today_dt)
        if rets:
            factor_list.append({"name": fname, "symbol": sym, **rets})
        factor_closes[fname] = close

    heatmap_rows, heatmap_weeks = _compute_factor_heatmap(factor_closes, spy_close)

    # --- Feature 4: Market Signal ---
    print("  Computing market signal...")
    signal_closes = {}
    for sym in ["^VIX", "^TNX", "^IRX", "HYG", "IEF", "RSP"]:
        signal_closes[sym] = get_close(sym)

    market_signal = _compute_market_signal(signal_closes, spy_close)
    print(f"  Signal: {market_signal['signal']} ({market_signal['score']:+.3f})")

    # --- Feature 5: Catalyst Calendar ---
    print("  Scraping catalyst calendar...")
    catalyst_calendar = _scrape_catalyst_calendar()
    print(f"  Found {len(catalyst_calendar)} upcoming catalysts")

    # --- Feature 6: Notable Options Flow ---
    print("  Scraping notable options flow...")
    notable_flow = _scrape_notable_flow()
    print(f"  Found {len(notable_flow)} notable flow entries")

    result = {
        "thematic_etfs": thematic_list,
        "rrg": {"benchmark": "SPY", "data": rrg_data},
        "factors": {
            "performance": factor_list,
            "heatmap": heatmap_rows,
            "heatmap_weeks": heatmap_weeks,
        },
        "market_signal": market_signal,
        "catalyst_calendar": catalyst_calendar,
        "notable_flow": notable_flow,
    }

    print(f"  Thematic scan complete: {len(thematic_list)} ETFs, {len(rrg_data)} RRG points, {len(factor_list)} factors")
    return result


def _mock_thematic_data() -> dict:
    """Mock data for dry-run mode."""
    import random
    random.seed(42)

    def rnd(lo, hi):
        return round(random.uniform(lo, hi), 2)

    thematic = []
    for sym, (name, cat) in THEMATIC_ETFS.items():
        thematic.append({
            "symbol": sym, "name": name, "category": cat,
            "price": rnd(20, 500),
            "1d": rnd(-3, 3), "5d": rnd(-5, 5),
            "mtd": rnd(-8, 8), "qtd": rnd(-10, 15), "ytd": rnd(-15, 30),
        })

    rrg_data = []
    for sym, name in SECTOR_ETFS.items():
        rrg_data.append({
            "symbol": sym, "name": name, "group": "sector",
            "rs_ratio": rnd(97, 103), "rs_momentum": rnd(97, 103),
            "quadrant": random.choice(["Leading", "Weakening", "Lagging", "Improving"]),
            "tail": [{"r": rnd(97, 103), "m": rnd(97, 103)} for _ in range(4)],
        })

    factors = []
    for fname, sym in FACTOR_ETFS.items():
        factors.append({
            "name": fname, "symbol": sym, "price": rnd(50, 200),
            "1d": rnd(-2, 2), "5d": rnd(-4, 4),
            "mtd": rnd(-5, 5), "qtd": rnd(-8, 8), "ytd": rnd(-10, 20),
        })

    heatmap = [{"factor": f, "spreads": [rnd(-2, 2) for _ in range(12)]} for f in FACTOR_ETFS]
    weeks = [f"W{i}" for i in range(1, 13)]

    signal = {
        "signal": "NEUTRAL",
        "score": 0.1,
        "components": {
            "VIX": {"score": 0.5, "detail": "18.5 (falling)"},
            "Yield Curve": {"score": 0.25, "detail": "+0.45% (steepening)"},
            "Credit": {"score": 0.5, "detail": "HYG/IEF above 50d (+0.8%)"},
            "Breadth": {"score": -0.5, "detail": "RSP/SPY below 50d (-0.3%)"},
            "Momentum": {"score": 1.0, "detail": "SPY above 50d, above 200d"},
        },
    }

    return {
        "thematic_etfs": thematic,
        "rrg": {"benchmark": "SPY", "data": rrg_data},
        "factors": {"performance": factors, "heatmap": heatmap, "heatmap_weeks": weeks},
        "market_signal": signal,
    }
