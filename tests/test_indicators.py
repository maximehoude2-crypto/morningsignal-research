"""
Unit tests for scanner.indicators — deterministic synthetic series, no network.
"""

import pandas as pd
import pytest

from scanner.indicators import (
    base_breakout_score,
    composite_from_components,
    period_return,
    relative_strength_rating,
    score_components,
    trend_structure_score,
    weinstein_stage2,
)


def _uptrend(n: int, start: float = 50.0, step: float = 0.5) -> pd.Series:
    return pd.Series([start + i * step for i in range(n)])


# ---------------------------------------------------------------------------
# period_return
# ---------------------------------------------------------------------------

def test_period_return_basic():
    s = pd.Series([float(i) for i in range(1, 101)])  # 1..100
    # 10-session return anchored at the last close: 100 / 90 - 1
    assert period_return(s, 10) == pytest.approx(100 / 90 - 1)


def test_period_return_insufficient_history():
    s = pd.Series([100.0] * 63)
    assert period_return(s, 63) is None


def test_period_return_nan_numerator():
    s = pd.Series([100.0] * 64)
    s.iloc[-1] = float("nan")
    assert period_return(s, 63) is None


def test_period_return_nan_denominator():
    s = pd.Series([100.0] * 64)
    s.iloc[0] = float("nan")
    assert period_return(s, 63) is None


def test_period_return_zero_denominator():
    s = pd.Series([0.0] + [100.0] * 63)
    assert period_return(s, 63) is None


# ---------------------------------------------------------------------------
# weinstein_stage2 history boundaries
# ---------------------------------------------------------------------------

def test_weinstein_stage2_one_row_short():
    # 158 rows: ma30w.iloc[-10] would be NaN — guard must reject
    assert weinstein_stage2(_uptrend(158)) is False


def test_weinstein_stage2_exact_boundary():
    # 159 rows: exactly enough for the 150d MA plus the 10-session slope check
    assert weinstein_stage2(_uptrend(159)) is True


def test_weinstein_stage2_downtrend_rejected():
    prices = pd.Series([200.0 - i * 0.5 for i in range(300)])
    assert weinstein_stage2(prices) is False


def test_trend_structure_boundary():
    # 218 rows: rolling(200).iloc[-20] is NaN — guard must reject
    assert trend_structure_score(_uptrend(218)) == 0
    # 219 rows: fully aligned uptrend scores the maximum
    assert trend_structure_score(_uptrend(219)) == 100.0


# ---------------------------------------------------------------------------
# base_breakout_score
# ---------------------------------------------------------------------------

def test_base_score_not_penalized_by_breakout():
    # Tight base oscillating +/-1 around 100, then a +20% breakout
    # in the last 5 sessions. The base window excludes the breakout,
    # so tightness must stay high.
    base = [100.0 + ((-1) ** i) for i in range(95)]
    breakout = [110.0, 114.0, 117.0, 119.0, 120.0]
    prices = pd.Series(base + breakout)
    volumes = pd.Series([1e6] * 100)  # flat volume: no volume points
    score = base_breakout_score(prices, volumes)
    # Base depth ~2% -> tightness ~46.7 of 50. The old tail(75) window
    # measured the breakout as ~17.5% "depth" (score ~21).
    assert score > 40


def _flat_base_with_breakout_volume(ratio: float):
    prices = pd.Series([100.0] * 95 + [102.0] * 5)  # depth 0 -> tightness 50
    volumes = pd.Series([1e6] * 95 + [ratio * 1e6] * 5)
    return prices, volumes


def test_volume_ramp_no_cliff_at_former_threshold():
    lo = base_breakout_score(*_flat_base_with_breakout_volume(1.39))
    hi = base_breakout_score(*_flat_base_with_breakout_volume(1.41))
    assert hi > lo  # still monotonic
    # Old code jumped 0 -> ~20.5 points across 1.40; now the step is ~1 point
    assert hi - lo < 2.0


def test_volume_ramp_starts_at_one():
    # ratio 1.0 earns no volume points; only tightness (50) remains
    assert base_breakout_score(*_flat_base_with_breakout_volume(1.0)) == pytest.approx(50.0)


def test_volume_ramp_capped():
    # ratio 3.0 caps the volume component at 50
    assert base_breakout_score(*_flat_base_with_breakout_volume(3.0)) == pytest.approx(100.0)


def test_base_score_far_from_high_rejected():
    prices = pd.Series([200.0] * 50 + [100.0] * 50)  # 50% off the 52w high
    volumes = pd.Series([1e6] * 100)
    assert base_breakout_score(prices, volumes) == 0


# ---------------------------------------------------------------------------
# relative_strength_rating
# ---------------------------------------------------------------------------

def test_rs_zero_when_matching_benchmark():
    s = pd.Series([100.0 + i * 0.1 for i in range(260)])
    assert relative_strength_rating(s, s.copy()) == pytest.approx(0.0)


def test_rs_positive_for_outperformer():
    stock = pd.Series([100.0 * (1.002 ** i) for i in range(260)])
    bench = pd.Series([100.0 * (1.0005 ** i) for i in range(260)])
    assert relative_strength_rating(stock, bench) > 0


def test_rs_renormalization_short_history():
    # 130 rows: only the 3m and 6m legs exist; weights must renormalize
    # (0.4 + 0.2 -> /0.6) instead of scoring the missing legs as 0.
    n = 130
    stock = pd.Series([100.0 * (1.002 ** i) for i in range(n)])
    bench = pd.Series([100.0] * n)  # flat benchmark: legs are pure stock return
    r63 = period_return(stock, 63)
    r126 = period_return(stock, 126)
    expected = (0.40 * r63 + 0.20 * r126) / 0.60
    rating = relative_strength_rating(stock, bench)
    assert rating == pytest.approx(expected)
    # Without renormalization the missing legs would drag the score down
    assert rating > 0.40 * r63 + 0.20 * r126


def test_rs_no_history_returns_zero():
    s = pd.Series([100.0] * 10)
    assert relative_strength_rating(s, s.copy()) == 0.0


# ---------------------------------------------------------------------------
# composite plumbing
# ---------------------------------------------------------------------------

def test_composite_weights():
    assert composite_from_components(100, 100, 100) == 100.0
    assert composite_from_components(100, 0, 0) == 40.0
    assert composite_from_components(0, 100, 0) == 35.0
    assert composite_from_components(0, 0, 100) == 25.0


def test_score_components_stage2_gate_and_keys():
    n = 260
    prices = pd.Series([50.0 + i * 0.5 for i in range(n)])
    volumes = pd.Series([1e6] * n)
    bench = pd.Series([100.0 + i * 0.05 for i in range(n)])
    comp = score_components(prices, volumes, bench)
    assert set(comp) == {"stage2", "rs_raw", "base", "trend"}
    assert comp["stage2"] is True
    assert comp["rs_raw"] > 0
    assert comp["trend"] == 100.0

    downtrend = pd.Series([200.0 - i * 0.5 for i in range(n)])
    assert score_components(downtrend, volumes, bench)["stage2"] is False
