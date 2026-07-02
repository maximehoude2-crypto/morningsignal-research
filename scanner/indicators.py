"""
Composite Breakout Score (0-100)
Built from 4 frameworks: IBD-style relative strength (raw excess return,
percentile-ranked across the scanned universe by the scanner), Weinstein
Stage Analysis, Minervini SEPA base detection, and trend structure.
"""

import pandas as pd

# Composite weights: RS / base quality / trend structure.
RS_WEIGHT = 0.40
BASE_WEIGHT = 0.35
TREND_WEIGHT = 0.25

# Today-anchored RS legs (IBD convention): (lookback sessions, weight).
# 3 months = 40%, 6 / 9 / 12 months = 20% each.
RS_LEGS = ((63, 0.40), (126, 0.20), (189, 0.20), (252, 0.20))

# Sessions treated as the breakout window — excluded from base/volume baselines.
BREAKOUT_SESSIONS = 5


def period_return(series: pd.Series, lookback: int) -> float | None:
    """
    Cumulative return over the trailing `lookback` sessions, anchored at the
    most recent close. Returns None when history is insufficient or either
    endpoint is NaN (or the start is zero).
    """
    if len(series) <= lookback:
        return None
    start = series.iloc[-(lookback + 1)]
    end = series.iloc[-1]
    if pd.isna(start) or pd.isna(end) or start == 0:
        return None
    return float(end) / float(start) - 1.0


def relative_strength_rating(stock_prices: pd.Series, benchmark_prices: pd.Series) -> float:
    """
    Raw IBD-style relative strength: weighted sum of today-anchored cumulative
    returns (3m = 40%, 6m/9m/12m = 20% each), each leg measured as excess
    return over the benchmark's same-period return. Legs with insufficient
    history are dropped and the remaining weights renormalized, so short
    histories aren't dragged toward zero.

    The result is a raw excess-return number, NOT a 0-100 score — the scanner
    percentile-ranks it across the scanned universe to get the RS rating.
    """
    total = 0.0
    weight_sum = 0.0
    for lookback, weight in RS_LEGS:
        stock_ret = period_return(stock_prices, lookback)
        bench_ret = period_return(benchmark_prices, lookback)
        if stock_ret is None or bench_ret is None:
            continue
        total += (stock_ret - bench_ret) * weight
        weight_sum += weight
    if weight_sum == 0:
        return 0.0
    return total / weight_sum


def weinstein_stage2(prices: pd.Series) -> bool:
    """
    Weinstein Stage 2 gate: price above rising 30-week MA.
    Returns True only if stock is in Stage 2 uptrend.
    Needs 159 sessions: 150 for the MA plus 10 for the slope check.
    """
    if len(prices) < 159:  # 30 weeks * 5 days + 10-session slope lookback
        return False
    ma30w = prices.rolling(150).mean()
    last_ma = ma30w.iloc[-1]
    prev_ma = ma30w.iloc[-10]  # MA level 2 weeks ago
    if pd.isna(last_ma) or pd.isna(prev_ma):
        return False
    price_above_ma = prices.iloc[-1] > last_ma
    ma_rising = last_ma > prev_ma
    return bool(price_above_ma and ma_rising)


def base_breakout_score(prices: pd.Series, volumes: pd.Series) -> float:
    """
    Minervini SEPA base breakout detection (0-100).
    Tightness of the consolidation preceding the last 5 sessions (so the
    breakout move itself isn't counted as base "depth"), plus a continuous
    volume-expansion ramp on the breakout window.
    """
    if len(prices) < 60 or len(volumes) < 60:
        return 0

    high_52w = prices.tail(252).max()
    current = prices.iloc[-1]

    # Must be within 15% of 52-week high
    if current < high_52w * 0.85:
        return 0

    # Base: up to 15 weeks of consolidation, excluding the breakout window
    base_window = prices.iloc[-80:-BREAKOUT_SESSIONS]
    base_high = base_window.max()
    base_low = base_window.min()
    base_depth = (base_high - base_low) / base_high if base_high > 0 else 1

    # Tight base = depth < 30%, ideal < 15%
    tightness_score = max(0, (0.30 - base_depth) / 0.30 * 50)

    # Volume on breakout: last 5 days vs the 50-day average preceding them
    avg_vol = volumes.iloc[-55:-BREAKOUT_SESSIONS].mean()
    recent_vol = volumes.tail(BREAKOUT_SESSIONS).mean()
    vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 0
    # Continuous ramp: 0 pts at 1.0x average volume up to 50 pts at 2.0x
    volume_score = max(0.0, min(50.0, (vol_ratio - 1.0) * 50))

    return float(min(100, tightness_score + volume_score))


def trend_structure_score(prices: pd.Series) -> float:
    """
    Trend alignment score (0-100).
    Price > 50d MA > 200d MA, both MAs rising.
    Needs 219 sessions: 200 for the MA plus 20 for the slope check.
    """
    if len(prices) < 219:  # 200d MA + 20-session slope lookback
        return 0

    ma50_series = prices.rolling(50).mean()
    ma200_series = prices.rolling(200).mean()
    ma50 = ma50_series.iloc[-1]
    ma200 = ma200_series.iloc[-1]
    ma50_prev = ma50_series.iloc[-10]
    ma200_prev = ma200_series.iloc[-20]
    current = prices.iloc[-1]

    score = 0
    if current > ma50:
        score += 25
    if current > ma200:
        score += 25
    if ma50 > ma200:  # golden cross
        score += 25
    if ma50 > ma50_prev and ma200 > ma200_prev:  # both rising
        score += 25
    return float(score)


def score_components(prices: pd.Series, volumes: pd.Series,
                     benchmark_prices: pd.Series) -> dict:
    """
    First pass of the two-pass scoring: per-ticker components.
    `rs_raw` is a raw excess return; the scanner percentile-ranks it across
    the scanned universe (0-100) before combining via composite_from_components.
    Stage 2 is a hard gate — callers should drop tickers where stage2 is False.
    """
    return {
        "stage2": weinstein_stage2(prices),
        "rs_raw": relative_strength_rating(prices, benchmark_prices),
        "base": base_breakout_score(prices, volumes),
        "trend": trend_structure_score(prices),
    }


def composite_from_components(rs_score: float, base: float, trend: float) -> float:
    """
    Final composite score (0-100) from a 0-100 RS percentile, base quality,
    and trend structure. Weights: RS 40%, base 35%, trend 25%.
    """
    return round(float(rs_score * RS_WEIGHT + base * BASE_WEIGHT + trend * TREND_WEIGHT), 1)
