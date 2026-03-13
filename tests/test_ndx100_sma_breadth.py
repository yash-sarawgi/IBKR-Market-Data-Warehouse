"""Tests for the Nasdaq-100 SMA breadth analysis."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from strategies.ndx100_sma_breadth import (
    analyze_breadth,
    compute_forward_returns,
    compute_breadth,
    compute_point_in_time_breadth,
    select_trailing_sessions,
    summarize_conditioned_forward_returns,
)


def _close_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2026-03-02", periods=8)
    frame = pd.DataFrame(
        {
            "AAA": [10, 10, 10, 10, 11, 12, 13, 14],
            "BBB": [10, 10, 10, 10, 9, 8, 7, 6],
            "CCC": [10, 10, 10, 10, 10, 10, 10, 10],
        },
        index=dates,
    )
    frame.index.name = "trade_date"
    return frame


def _write_symbol_parquet(warehouse: Path, symbol: str, closes: list[float]) -> None:
    path = warehouse / "data-lake" / "bronze" / "asset_class=equity" / f"symbol={symbol}"
    path.mkdir(parents=True, exist_ok=True)
    dates = pd.bdate_range("2026-03-02", periods=len(closes))
    frame = pd.DataFrame({"trade_date": dates, "close": closes})
    frame.to_parquet(path / "data.parquet", index=False)


class TestComputeBreadth:
    def test_counts_and_percentages(self):
        result = compute_breadth(_close_frame(), lookback=5)

        row = result.loc[result["trade_date"] == pd.Timestamp("2026-03-06")].iloc[0]
        assert row["eligible_count"] == 3
        assert row["above_count"] == 1
        assert row["below_or_equal_count"] == 2
        assert row["unavailable_count"] == 0
        assert row["pct_above"] == pytest.approx(100 / 3)
        assert row["pct_below_or_equal"] == pytest.approx(200 / 3)

    def test_universe_size_accounts_for_missing_symbols(self):
        result = compute_breadth(_close_frame()[["AAA", "BBB"]], lookback=5, universe_size=3)

        row = result.loc[result["trade_date"] == pd.Timestamp("2026-03-06")].iloc[0]
        assert row["eligible_count"] == 2
        assert row["above_count"] == 1
        assert row["below_or_equal_count"] == 1
        assert row["unavailable_count"] == 1
        assert row["pct_above"] == pytest.approx(50.0)


class TestSelectTrailingSessions:
    def test_returns_last_n_sessions_through_end_date(self):
        breadth = compute_breadth(_close_frame(), lookback=5)

        trailing = select_trailing_sessions(breadth, end_date="2026-03-11", sessions=3)
        assert trailing["trade_date"].tolist() == [
            pd.Timestamp("2026-03-09"),
            pd.Timestamp("2026-03-10"),
            pd.Timestamp("2026-03-11"),
        ]

    def test_requires_requested_end_date(self):
        breadth = compute_breadth(_close_frame(), lookback=5)

        with pytest.raises(ValueError, match="Requested end date 2026-03-12 is not present"):
            select_trailing_sessions(breadth, end_date="2026-03-12", sessions=3)


class TestPointInTimeBreadth:
    def test_uses_date_specific_membership(self):
        prices = _close_frame()[["AAA", "BBB", "CCC"]]
        memberships = {
            "2026-03-06": {"AAA", "BBB"},
            "2026-03-09": {"AAA", "CCC"},
            "2026-03-10": {"AAA", "CCC"},
            "2026-03-11": {"AAA", "CCC"},
        }

        result = compute_point_in_time_breadth(prices, memberships, lookback=5)

        row_first = result.loc[result["trade_date"] == pd.Timestamp("2026-03-06")].iloc[0]
        assert row_first["eligible_count"] == 2
        assert row_first["above_count"] == 1
        assert row_first["below_or_equal_count"] == 1
        assert row_first["unavailable_count"] == 0
        assert row_first["pct_above"] == pytest.approx(50.0)

        row_second = result.loc[result["trade_date"] == pd.Timestamp("2026-03-09")].iloc[0]
        assert row_second["eligible_count"] == 2
        assert row_second["above_count"] == 1
        assert row_second["below_or_equal_count"] == 1
        assert row_second["unavailable_count"] == 0
        assert row_second["pct_above"] == pytest.approx(50.0)


class TestAnalyzeBreadth:
    def test_end_to_end_with_temp_parquet(self, tmp_path: Path):
        warehouse = tmp_path / "warehouse"
        preset_path = tmp_path / "ndx100-test.json"

        _write_symbol_parquet(warehouse, "AAA", [10, 10, 10, 10, 11, 12, 13, 14])
        _write_symbol_parquet(warehouse, "BBB", [10, 10, 10, 10, 9, 8, 7, 6])
        preset_path.write_text(json.dumps({"tickers": ["AAA", "BBB", "CCC"]}))

        trailing, target_row, summary, histogram, missing = analyze_breadth(
            preset_path=preset_path,
            warehouse=warehouse,
            end_date="2026-03-11",
            sessions=4,
            lookback=5,
        )

        assert trailing["trade_date"].tolist() == [
            pd.Timestamp("2026-03-06"),
            pd.Timestamp("2026-03-09"),
            pd.Timestamp("2026-03-10"),
            pd.Timestamp("2026-03-11"),
        ]
        assert target_row["above_count"] == 1
        assert target_row["below_or_equal_count"] == 1
        assert target_row["unavailable_count"] == 1
        assert target_row["pct_above"] == pytest.approx(50.0)
        assert missing == ["CCC"]
        assert summary["observations"] == 4
        assert summary["mean"] == pytest.approx(50.0)
        assert summary["std"] == pytest.approx(0.0)

        histogram = histogram.set_index("breadth_band")
        assert histogram.loc["40-50%", "days"] == 4
        assert histogram["days"].sum() == 4


class TestForwardReturns:
    def test_compute_forward_returns(self):
        dates = pd.bdate_range("2026-03-02", periods=5)
        prices = pd.Series([100.0, 110.0, 121.0, 133.1, 146.41], index=dates)

        result = compute_forward_returns(prices, horizons={"1d": 1, "2d": 2})
        assert result.loc[dates[0], "1d"] == pytest.approx(0.10)
        assert result.loc[dates[0], "2d"] == pytest.approx(0.21)
        assert pd.isna(result.loc[dates[-1], "1d"])
        assert pd.isna(result.loc[dates[-2], "2d"])

    def test_summarize_conditioned_forward_returns(self):
        dates = pd.bdate_range("2026-03-02", periods=6)
        breadth = pd.DataFrame(
            {
                "trade_date": dates,
                "pct_below_or_equal": [70.0, 60.0, 80.0, 50.0, 72.0, 40.0],
            }
        )
        closes = pd.DataFrame(
            {
                "SPY": [100.0, 110.0, 121.0, 133.1, 146.41, 161.051],
                "SPXL": [50.0, 60.0, 72.0, 86.4, 103.68, 124.416],
            },
            index=dates,
        )

        triggered, summary = summarize_conditioned_forward_returns(
            breadth,
            closes,
            min_pct_below=65.0,
            horizons={"1d": 1, "2d": 2},
        )

        assert triggered["trade_date"].tolist() == [
            pd.Timestamp("2026-03-02"),
            pd.Timestamp("2026-03-04"),
            pd.Timestamp("2026-03-06"),
        ]

        spy_1d = summary[(summary["asset"] == "SPY") & (summary["horizon"] == "1d")].iloc[0]
        assert spy_1d["signals"] == 3
        assert spy_1d["observations"] == 3
        assert spy_1d["mean_return_pct"] == pytest.approx(10.0)
        assert spy_1d["median_return_pct"] == pytest.approx(10.0)
        assert spy_1d["positive_rate_pct"] == pytest.approx(100.0)

        spxl_2d = summary[(summary["asset"] == "SPXL") & (summary["horizon"] == "2d")].iloc[0]
        assert spxl_2d["signals"] == 3
        assert spxl_2d["observations"] == 2
        assert spxl_2d["mean_return_pct"] == pytest.approx(44.0)
        assert spxl_2d["median_return_pct"] == pytest.approx(44.0)
