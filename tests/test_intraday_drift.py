"""Tests for the intraday drift backtesting engine.

All synthetic data — no file or network I/O.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategies.intraday_drift import (
    annual_returns_table,
    cagr,
    compute_intraday_returns,
    ibkr_roundtrip_cost,
    load_ticker_from_parquet,
    max_drawdown,
    sharpe,
    simulate_strategy,
    var_95,
)


def _spy_df(opens, closes):
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


class TestComputeIntradayReturns:
    def test_basic_log_returns(self):
        df = _spy_df([100, 102, 104], [101, 103, 105])
        result = compute_intraday_returns(df)
        np.testing.assert_almost_equal(result.iloc[0], np.log(101 / 100))
        np.testing.assert_almost_equal(result.iloc[1], np.log(103 / 102))
        np.testing.assert_almost_equal(result.iloc[2], np.log(105 / 104))

    def test_negative_return(self):
        df = _spy_df([105], [100])
        result = compute_intraday_returns(df)
        assert result.iloc[0] < 0

    def test_flat_day(self):
        df = _spy_df([100], [100])
        result = compute_intraday_returns(df)
        np.testing.assert_almost_equal(result.iloc[0], 0.0)


class TestIBKRCost:
    def test_normal_cost(self):
        cost = ibkr_roundtrip_cost(100_000, 500)
        shares = 200
        per_side = shares * 0.0065
        assert cost == pytest.approx(per_side * 2)

    def test_min_order(self):
        cost = ibkr_roundtrip_cost(100, 100)
        assert cost == pytest.approx(0.35 * 2)

    def test_max_cap(self):
        cost = ibkr_roundtrip_cost(10_000, 0.10)
        shares = 100_000
        trade_value = shares * 0.10
        max_per_side = 0.01 * trade_value
        assert cost == pytest.approx(max_per_side * 2)

    def test_zero_shares(self):
        assert ibkr_roundtrip_cost(0, 100) == 0.0
        assert ibkr_roundtrip_cost(50, 100) == 0.0


class TestSimulateStrategy:
    def test_basic_equity_tracking(self):
        opens = np.array([100.0, 101.0, 102.0])
        closes = np.array([101.0, 102.0, 103.0])
        mask = np.array([True, True, True])

        equity = simulate_strategy(opens, closes, mask, capital=10_000, fee_fn=lambda e, p: 0)
        assert len(equity) == 4
        assert equity[0] == 10_000
        # Day 0: buy 100 shares at 100, sell at 101 -> pnl = 100
        assert equity[1] == pytest.approx(10_100)

    def test_mask_skips_trades(self):
        opens = np.array([100.0, 100.0])
        closes = np.array([110.0, 110.0])
        mask = np.array([False, True])

        equity = simulate_strategy(opens, closes, mask, capital=10_000, fee_fn=lambda e, p: 0)
        assert equity[1] == 10_000  # skipped day 0
        assert equity[2] > 10_000  # traded day 1

    def test_loss_day(self):
        opens = np.array([100.0])
        closes = np.array([95.0])
        mask = np.array([True])

        equity = simulate_strategy(opens, closes, mask, capital=10_000, fee_fn=lambda e, p: 0)
        # 100 shares * (95-100) = -500
        assert equity[1] == pytest.approx(9_500)

    def test_fees_reduce_equity(self):
        opens = np.array([100.0])
        closes = np.array([100.0])  # flat day
        mask = np.array([True])

        equity = simulate_strategy(opens, closes, mask, capital=10_000)
        assert equity[1] < 10_000


class TestCAGR:
    def test_known_cagr(self):
        equity = np.array([100, 200])
        result = cagr(equity, 10.0)
        assert result == pytest.approx(2 ** (1 / 10) - 1, rel=1e-6)

    def test_zero_years(self):
        assert cagr(np.array([100, 200]), 0) == 0.0


class TestSharpe:
    def test_positive_mean(self):
        rng = np.random.default_rng(42)
        daily_ret = rng.normal(0.001, 0.005, 252)
        s = sharpe(daily_ret, rf=0.0)
        assert s > 0

    def test_zero_vol(self):
        daily_ret = np.full(10, 0.001)
        assert sharpe(daily_ret) == 0.0


class TestMaxDrawdown:
    def test_known_drawdown(self):
        equity = np.array([100, 120, 90, 110, 80])
        assert max_drawdown(equity) == pytest.approx(40 / 120)

    def test_no_drawdown(self):
        equity = np.array([100, 110, 120, 130])
        assert max_drawdown(equity) == pytest.approx(0.0)


class TestVaR95:
    def test_known_var(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, 10000)
        v = var_95(returns)
        assert v == pytest.approx(-0.01645, abs=0.001)


class TestAnnualReturnsTable:
    def test_year_filtering(self):
        dates = pd.date_range("2014-01-02", periods=600, freq="B")
        equity = np.concatenate([[100], np.linspace(100, 200, 600)])
        tbl = annual_returns_table(equity, dates, start_year=2015)
        assert 2014 not in tbl["year"].values
        assert 2015 in tbl["year"].values
