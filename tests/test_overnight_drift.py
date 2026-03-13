"""Tests for the overnight drift backtesting engine.

All synthetic data — no file or network I/O.
"""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from strategies.overnight_drift import (
    annual_returns_table,
    cagr,
    compute_overnight_returns,
    compute_vix_filter,
    ibkr_roundtrip_cost,
    load_vix_from_cboe,
    max_drawdown,
    sharpe,
    simulate_strategy,
    var_95,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _spy_df(opens, closes):
    """Build a minimal SPY-like DataFrame."""
    n = len(opens)
    dates = pd.date_range("2024-01-02", periods=n, freq="B")
    return pd.DataFrame(
        {
            "trade_date": dates,
            "open": opens,
            "high": [max(o, c) + 1 for o, c in zip(opens, closes)],
            "low": [min(o, c) - 1 for o, c in zip(opens, closes)],
            "close": closes,
            "volume": [1_000_000] * n,
        }
    )


# ---------------------------------------------------------------------------
# test_compute_overnight_returns
# ---------------------------------------------------------------------------
class TestComputeOvernightReturns:
    def test_basic_log_returns(self):
        df = _spy_df([100, 102, 104], [101, 103, 105])
        result = compute_overnight_returns(df)
        # ln(open[1] / close[0]) = ln(102/101)
        expected_0 = np.log(102 / 101)
        expected_1 = np.log(104 / 103)
        np.testing.assert_almost_equal(result.iloc[0], expected_0)
        np.testing.assert_almost_equal(result.iloc[1], expected_1)
        assert np.isnan(result.iloc[2])  # last row is NaN

    def test_single_row(self):
        df = _spy_df([100], [101])
        result = compute_overnight_returns(df)
        assert np.isnan(result.iloc[0])


# ---------------------------------------------------------------------------
# test_compute_vix_filter
# ---------------------------------------------------------------------------
class TestComputeVixFilter:
    def test_ma_crossover(self):
        # 210 days: first 200 are high (30), last 10 are low (15)
        n = 210
        closes = [30.0] * 200 + [15.0] * 10
        dates = pd.date_range("2020-01-01", periods=n, freq="B")
        vix_df = pd.DataFrame({"trade_date": dates, "close": closes})
        result = compute_vix_filter(vix_df, lookback=200)
        # First 199 rows: MA not available -> NaN -> vix_filter is False
        assert result["vix_filter"].iloc[198] == False  # noqa: E712
        # Row 199: MA = 30, close = 30 -> not less than -> False
        assert result["vix_filter"].iloc[199] == False  # noqa: E712
        # Last 10 rows: close=15 < MA(~29.x) -> True
        assert result["vix_filter"].iloc[-1] == True  # noqa: E712

    def test_boundary_equal(self):
        # VIX == MA exactly -> filter is False (not strictly less)
        n = 200
        closes = [20.0] * n
        dates = pd.date_range("2020-01-01", periods=n, freq="B")
        vix_df = pd.DataFrame({"trade_date": dates, "close": closes})
        result = compute_vix_filter(vix_df, lookback=200)
        assert result["vix_filter"].iloc[-1] == False  # noqa: E712


# ---------------------------------------------------------------------------
# test_ibkr_roundtrip_cost
# ---------------------------------------------------------------------------
class TestIBKRCost:
    def test_normal_cost(self):
        # $100K equity, $500/share -> 200 shares
        cost = ibkr_roundtrip_cost(100_000, 500)
        shares = 200
        per_side = shares * 0.0065
        assert cost == pytest.approx(per_side * 2)

    def test_min_order(self):
        # Very small position: 1 share at $100
        cost = ibkr_roundtrip_cost(100, 100)
        # 1 share * $0.0065 = $0.0065 < $0.35 min -> $0.35 per side
        assert cost == pytest.approx(0.35 * 2)

    def test_max_cap(self):
        # Very cheap stock, many shares: cost would exceed 1% cap
        # $10K equity, $0.10/share -> 100,000 shares
        cost = ibkr_roundtrip_cost(10_000, 0.10)
        shares = 100_000
        trade_value = shares * 0.10  # $10,000
        max_per_side = 0.01 * trade_value  # $100
        raw_per_side = shares * 0.0065  # $650
        # Capped at $100 per side
        assert cost == pytest.approx(max_per_side * 2)
        assert raw_per_side > max_per_side  # confirm it was actually capped

    def test_zero_shares(self):
        assert ibkr_roundtrip_cost(0, 100) == 0.0
        assert ibkr_roundtrip_cost(50, 100) == 0.0  # can't buy 1 share


# ---------------------------------------------------------------------------
# test_simulate_strategy
# ---------------------------------------------------------------------------
class TestSimulateStrategy:
    def test_basic_equity_tracking(self):
        # 3 days: known returns
        closes = np.array([100.0, 101.0, 102.0])
        opens_next = np.array([101.5, 102.5, 103.5])
        returns = np.log(opens_next / closes)
        mask = np.array([True, True, True])

        equity = simulate_strategy(returns, closes, opens_next, mask, capital=10_000, fee_fn=lambda e, p: 0)
        assert len(equity) == 4
        assert equity[0] == 10_000
        # Day 0: buy 100 shares at 100, sell at 101.5 -> pnl = 150
        assert equity[1] == pytest.approx(10_150)

    def test_mask_skips_trades(self):
        closes = np.array([100.0, 100.0])
        opens_next = np.array([110.0, 110.0])
        returns = np.log(opens_next / closes)
        mask = np.array([False, True])

        equity = simulate_strategy(returns, closes, opens_next, mask, capital=10_000, fee_fn=lambda e, p: 0)
        assert equity[1] == 10_000  # skipped day 0
        assert equity[2] > 10_000  # traded day 1

    def test_fees_reduce_equity(self):
        closes = np.array([100.0])
        opens_next = np.array([100.0])  # zero return
        returns = np.log(opens_next / closes)
        mask = np.array([True])

        equity = simulate_strategy(returns, closes, opens_next, mask, capital=10_000)
        # With zero price change, equity decreases by fees
        assert equity[1] < 10_000


# ---------------------------------------------------------------------------
# test_cagr
# ---------------------------------------------------------------------------
class TestCAGR:
    def test_known_cagr(self):
        # Double in 10 years -> CAGR = 2^(1/10) - 1 ~= 7.18%
        equity = np.array([100, 200])  # start, end
        result = cagr(equity, 10.0)
        assert result == pytest.approx(2 ** (1 / 10) - 1, rel=1e-6)

    def test_zero_years(self):
        assert cagr(np.array([100, 200]), 0) == 0.0

    def test_zero_start(self):
        assert cagr(np.array([0, 200]), 5) == 0.0


# ---------------------------------------------------------------------------
# test_sharpe
# ---------------------------------------------------------------------------
class TestSharpe:
    def test_known_sharpe(self):
        # Constant daily return of 0.1% -> low vol, high Sharpe
        daily_ret = np.full(252, 0.001)
        s = sharpe(daily_ret, rf=0.0)
        # mean = 0.001, std ≈ 0, so Sharpe -> infinity... use small noise
        rng = np.random.default_rng(42)
        daily_ret = rng.normal(0.001, 0.005, 252)
        s = sharpe(daily_ret, rf=0.0)
        assert s > 0  # positive mean -> positive Sharpe

    def test_zero_vol(self):
        daily_ret = np.full(10, 0.001)
        assert sharpe(daily_ret) == 0.0

    def test_empty(self):
        assert sharpe(np.array([0.01])) == 0.0


# ---------------------------------------------------------------------------
# test_max_drawdown
# ---------------------------------------------------------------------------
class TestMaxDrawdown:
    def test_known_drawdown(self):
        equity = np.array([100, 120, 90, 110, 80])
        # Peak 120, trough 80 -> DD = 40/120 = 33.3%
        assert max_drawdown(equity) == pytest.approx(40 / 120)

    def test_no_drawdown(self):
        equity = np.array([100, 110, 120, 130])
        assert max_drawdown(equity) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# test_var_95
# ---------------------------------------------------------------------------
class TestVaR95:
    def test_known_var(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, 10000)
        v = var_95(returns)
        # 5th percentile of N(0, 0.01) ≈ -0.01645
        assert v == pytest.approx(-0.01645, abs=0.001)

    def test_with_nans(self):
        returns = np.array([0.01, -0.02, np.nan, 0.005, -0.03, 0.0, -0.01])
        v = var_95(returns)
        assert np.isfinite(v)


# ---------------------------------------------------------------------------
# test_annual_returns_table
# ---------------------------------------------------------------------------
class TestAnnualReturnsTable:
    def test_year_filtering(self):
        dates = pd.date_range("2014-01-02", periods=600, freq="B")
        equity_values = np.linspace(100, 200, 600)
        equity = np.concatenate([[100], equity_values])  # +1 for initial

        tbl = annual_returns_table(equity, dates, start_year=2015)
        assert 2014 not in tbl["year"].values
        assert 2015 in tbl["year"].values

    def test_return_calculation(self):
        # All of 2020 at equity=100, all of 2021 at equity=120
        dates_2020 = pd.bdate_range("2020-01-02", "2020-12-31")
        dates_2021 = pd.bdate_range("2021-01-04", "2021-12-31")
        dates = dates_2020.append(dates_2021)
        eq_vals = np.concatenate([np.full(len(dates_2020), 100.0), np.full(len(dates_2021), 120.0)])
        equity = np.concatenate([[100], eq_vals])

        tbl = annual_returns_table(equity, pd.Series(dates), start_year=2020)
        # Year 2020: starts 100, ends 100 -> 0%
        yr_2020 = tbl[tbl["year"] == 2020]["return"].iloc[0]
        assert yr_2020 == pytest.approx(0.0, abs=0.001)
        # Year 2021: starts 120, ends 120 -> 0%
        yr_2021 = tbl[tbl["year"] == 2021]["return"].iloc[0]
        assert yr_2021 == pytest.approx(0.0, abs=0.001)


# ---------------------------------------------------------------------------
# test_load_vix_from_cboe
# ---------------------------------------------------------------------------
class TestLoadVixFromCBOE:
    def test_mock_download(self, tmp_path):
        csv_content = (
            " DATE, OPEN, HIGH, LOW, CLOSE\n"
            "01/02/2020, 13.50, 14.00, 13.00, 13.78\n"
            "01/03/2020, 14.00, 14.50, 13.50, 14.02\n"
        )

        mock_resp = MagicMock()
        mock_resp.read.return_value = csv_content.encode("utf-8")
        mock_opener = MagicMock(return_value=mock_resp)

        cache = tmp_path / "vix.csv"
        df = load_vix_from_cboe(url="http://fake", cache_path=cache, _opener=mock_opener)

        assert len(df) == 2
        assert "trade_date" in df.columns
        assert "close" in df.columns
        assert df["close"].iloc[0] == pytest.approx(13.78)
        mock_opener.assert_called_once_with("http://fake")

    def test_uses_cache(self, tmp_path):
        csv_content = "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2020,13.50,14.00,13.00,13.78\n"
        cache = tmp_path / "vix.csv"
        cache.write_text(csv_content)

        mock_opener = MagicMock()
        # stale_seconds very large -> should use cache
        df = load_vix_from_cboe(url="http://fake", cache_path=cache, stale_seconds=999999, _opener=mock_opener)

        assert len(df) == 1
        mock_opener.assert_not_called()
