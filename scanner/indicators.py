"""
Composite Breakout Score (0-100)
Built from 4 frameworks: IBD RS Rating, Weinstein Stage Analysis,
Minervini SEPA base detection, and trend structure.
"""

import pandas as pd
import numpy as np


def relative_strength_rating(stock_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    """
    IBD-style Relative Strength Rating (0-99).
    Weights: last 3 months = 40%, months 3-6 = 20%, months 6-9 = 20%, months 9-12 = 20%.
    """
    def period_return(series, start, end):
        if len(series) < end:
            return 0
        val_end = series.iloc[-end]
        if val_end == 0 or pd.isna(val_end):
            return 0
        return (series.iloc[-start] / val_end) - 1

    stock_score = (
        period_return(stock_returns, 1, 63) * 0.40 +
        period_return(stock_returns, 63, 126) * 0.20 +
        period_return(stock_returns, 126, 189) * 0.20 +
        period_return(stock_returns, 189, 252) * 0.20
    )
    bench_score = (
        period_return(benchmark_returns, 1, 63) * 0.40 +
        period_return(benchmark_returns, 63, 126) * 0.20 +
        period_return(benchmark_returns, 126, 189) * 0.20 +
        period_return(benchmark_returns, 189, 252) * 0.20
    )
    return stock_score - bench_score


def weinstein_stage2(prices: pd.Series) -> bool:
    """
    Weinstein Stage 2 gate: price above rising 30-week MA.
    Returns True only if stock is in Stage 2 uptrend.
    """
    if len(prices) < 150:  # 30 weeks * 5 days
        return False
    ma30w = prices.rolling(150).mean()
    last_ma = ma30w.iloc[-1]
    if last_ma is None or pd.isna(last_ma):
        return False
    price_above_ma = prices.iloc[-1] > last_ma
    ma_rising = last_ma > ma30w.iloc[-10]  # MA rising over last 2 weeks
    return bool(price_above_ma and ma_rising)


def base_breakout_score(prices: pd.Series, volumes: pd.Series) -> float:
    """
    Minervini SEPA base breakout detection (0-100).
    Looks for: tight consolidation near 52-week high, then breakout on volume.
    """
    if len(prices) < 60:
        return 0

    high_52w = prices.tail(252).max()
    current = prices.iloc[-1]

    # Must be within 15% of 52-week high
    if current < high_52w * 0.85:
        return 0

    # Base: look back 3-15 weeks for consolidation
    base_window = prices.tail(75)  # up to 15 weeks
    base_high = base_window.max()
    base_low = base_window.min()
    base_depth = (base_high - base_low) / base_high if base_high > 0 else 1

    # Tight base = depth < 30%, ideal < 15%
    tightness_score = max(0, (0.30 - base_depth) / 0.30 * 50)

    # Volume on breakout: last 5 days vs 50-day average
    avg_vol = volumes.tail(50).mean()
    recent_vol = volumes.tail(5).mean()
    vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 0
    volume_score = min(50, (vol_ratio - 1.0) * 50) if vol_ratio > 1.4 else 0

    return min(100, tightness_score + volume_score)


def trend_structure_score(prices: pd.Series) -> float:
    """
    Trend alignment score (0-100).
    Price > 50d MA > 200d MA, both MAs rising.
    """
    if len(prices) < 200:
        return 0

    ma50 = prices.rolling(50).mean().iloc[-1]
    ma200 = prices.rolling(200).mean().iloc[-1]
    ma50_prev = prices.rolling(50).mean().iloc[-10]
    ma200_prev = prices.rolling(200).mean().iloc[-20]
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


def composite_score(prices: pd.Series, volumes: pd.Series,
                    benchmark_prices: pd.Series) -> dict:
    """
    Final composite score (0-100). Weights:
    RS Rating: 40%, Base quality: 35%, Trend: 25%
    Stage 2 is a hard gate — fails = score 0.
    """
    if not weinstein_stage2(prices):
        return {"total": 0, "stage2": False, "rs": 0, "base": 0, "trend": 0}

    rs = relative_strength_rating(prices, benchmark_prices)
    # Normalize RS to 0-100 (typical range -0.5 to +0.5)
    rs_score = min(100, max(0, (rs + 0.3) / 0.6 * 100))

    base = base_breakout_score(prices, volumes)
    trend = trend_structure_score(prices)

    total = rs_score * 0.40 + base * 0.35 + trend * 0.25

    return {
        "total": round(total, 1),
        "stage2": True,
        "rs": round(rs_score, 1),
        "base": round(base, 1),
        "trend": round(trend, 1),
    }
