"""Tests for the generic breadth washout strategy."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

import strategies.breadth_washout as breadth_washout
from strategies.breadth_washout import (
    BreadthWashoutConfig,
    build_config_from_args,
    build_membership_change_table,
    build_static_memberships,
    fetch_price_panel,
    load_preset_metadata,
    expand_snapshot_memberships,
    format_strategy_report,
    normalize_symbol_for_yahoo,
    parse_horizons,
    resolve_static_universe_symbols,
    save_strategy_outputs,
    summarize_signal_forward_returns,
    slugify,
)


class TestParseHorizons:
    def test_defaults_when_missing(self):
        assert parse_horizons(None) == {"1d": 1, "1w": 5, "1m": 21, "3m": 63}

    def test_custom_horizons(self):
        assert parse_horizons(["2d=2", "2w=10"]) == {"2d": 2, "2w": 10}

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="expected label=periods"):
            parse_horizons(["bad"])


class TestStaticUniverseHelpers:
    def test_normalize_symbol_for_yahoo(self):
        assert normalize_symbol_for_yahoo("BRK.B") == "BRK-B"
        assert normalize_symbol_for_yahoo("msft") == "MSFT"

    def test_load_preset_metadata(self, tmp_path: Path):
        preset = tmp_path / "custom.json"
        preset.write_text(json.dumps({"name": "custom-set", "tickers": ["spy", "qqq"]}))

        name, tickers = load_preset_metadata(preset)
        assert name == "custom-set"
        assert tickers == ["SPY", "QQQ"]

    def test_resolve_static_universe_from_tickers(self):
        config = BreadthWashoutConfig(
            universe_mode="tickers",
            universe_label="my-list",
            explicit_tickers=("aapl", "msft"),
        )

        label, tickers = resolve_static_universe_symbols(config)
        assert label == "my-list"
        assert tickers == ["AAPL", "MSFT"]

    def test_resolve_static_universe_from_preset(self, tmp_path: Path):
        preset = tmp_path / "custom.json"
        preset.write_text(json.dumps({"name": "custom-set", "tickers": ["spy", "qqq"]}))
        config = BreadthWashoutConfig(
            universe_mode="preset",
            universe_label="preset-alias",
            preset_path=str(preset),
        )

        label, tickers = resolve_static_universe_symbols(config)
        assert label == "preset-alias"
        assert tickers == ["SPY", "QQQ"]

    def test_build_static_memberships(self):
        trade_dates = pd.to_datetime(["2026-01-02", "2026-01-05"])
        memberships = build_static_memberships(trade_dates, ["AAA", "BBB"])
        assert memberships[pd.Timestamp("2026-01-02")] == {"AAA", "BBB"}
        assert memberships[pd.Timestamp("2026-01-05")] == {"AAA", "BBB"}

    def test_resolve_all_stocks_uses_discovery(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(breadth_washout, "discover_all_stocks", lambda bronze_dir=None: ["AAA", "BBB"])
        config = BreadthWashoutConfig(universe_mode="all-stocks", universe_label="all-stocks")

        label, tickers = resolve_static_universe_symbols(config)
        assert label == "all-stocks"
        assert tickers == ["AAA", "BBB"]

    def test_fetch_price_panel_preserves_canonical_symbol_names(self, monkeypatch: pytest.MonkeyPatch):
        requested: list[str] = []

        def fake_fetch(symbol: str, start_date, end_date, adjusted, session):
            requested.append(symbol)
            return pd.Series([100.0, 101.0], index=pd.to_datetime(["2026-03-10", "2026-03-11"]), name=symbol)

        monkeypatch.setattr(breadth_washout, "fetch_yahoo_daily_series", fake_fetch)

        panel, missing = fetch_price_panel(
            ["BRK.B", "MSFT"],
            start_date="2026-03-10",
            end_date="2026-03-11",
            adjusted=False,
            max_workers=2,
        )

        assert sorted(requested) == ["BRK-B", "MSFT"]
        assert missing == []
        assert panel.columns.tolist() == ["BRK.B", "MSFT"]


class TestBuildConfigFromArgs:
    def test_named_preset_universe(self):
        args = argparse.Namespace(
            end_date="2026-03-11",
            sessions=252,
            lookback=5,
            signal_mode="oversold",
            threshold=None,
            min_pct_below=65.0,
            universe="sp500",
            preset=None,
            tickers=None,
            universe_label=None,
            membership_time_of_day="EOD",
            snapshot_date=None,
            bronze_dir=None,
            assets=["SPY", "SPXL"],
            horizon=None,
            price_returns=False,
            max_workers=12,
        )

        config = build_config_from_args(args)
        assert config.universe_mode == "preset"
        assert config.universe_label == "sp500"
        assert config.signal_mode == "oversold"
        assert config.signal_threshold == 65.0
        assert config.preset_path is not None and config.preset_path.endswith("presets/sp500.json")

    def test_explicit_tickers_override_named_universe(self):
        args = argparse.Namespace(
            end_date="2026-03-11",
            sessions=252,
            lookback=5,
            signal_mode="oversold",
            threshold=None,
            min_pct_below=65.0,
            universe="ndx100",
            preset=None,
            tickers=["aapl", "msft"],
            universe_label="tech-pair",
            membership_time_of_day="EOD",
            snapshot_date=None,
            bronze_dir=None,
            assets=["SPY"],
            horizon=["2d=2"],
            price_returns=True,
            max_workers=4,
        )

        config = build_config_from_args(args)
        assert config.universe_mode == "tickers"
        assert config.universe_label == "tech-pair"
        assert config.explicit_tickers == ("AAPL", "MSFT")
        assert config.adjusted_forward_returns is False
        assert config.horizons == {"2d": 2}

    def test_overbought_threshold_maps_correctly(self):
        args = argparse.Namespace(
            end_date="2026-03-11",
            sessions=252,
            lookback=5,
            signal_mode="overbought",
            threshold=70.0,
            min_pct_below=65.0,
            universe="r2k",
            preset=None,
            tickers=None,
            universe_label=None,
            membership_time_of_day="EOD",
            snapshot_date=None,
            bronze_dir=None,
            assets=["QQQ", "TQQQ"],
            horizon=None,
            price_returns=False,
            max_workers=8,
        )

        config = build_config_from_args(args)
        assert config.signal_mode == "overbought"
        assert config.signal_threshold == 70.0
        assert config.forward_assets == ("QQQ", "TQQQ")


class TestSummarizeSignalForwardReturns:
    def test_oversold_filters_on_pct_below_or_equal(self):
        breadth = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"]),
                "pct_above": [40.0, 80.0, 35.0],
                "pct_below_or_equal": [60.0, 20.0, 65.0],
            }
        )
        prices = pd.DataFrame(
            {"SPY": [100.0, 101.0, 103.0, 102.0]},
            index=pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]),
        )

        triggered, summary = summarize_signal_forward_returns(
            breadth,
            prices,
            signal_mode="oversold",
            threshold=65.0,
            horizons={"1d": 1},
        )

        assert triggered["trade_date"].tolist() == [pd.Timestamp("2026-01-06")]
        assert summary["signals"].tolist() == [1]

    def test_overbought_filters_on_pct_above(self):
        breadth = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"]),
                "pct_above": [40.0, 80.0, 35.0],
                "pct_below_or_equal": [60.0, 20.0, 65.0],
            }
        )
        prices = pd.DataFrame(
            {"QQQ": [100.0, 101.0, 103.0, 102.0]},
            index=pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]),
        )

        triggered, summary = summarize_signal_forward_returns(
            breadth,
            prices,
            signal_mode="overbought",
            threshold=70.0,
            horizons={"1d": 1},
        )

        assert triggered["trade_date"].tolist() == [pd.Timestamp("2026-01-05")]
        assert summary["signals"].tolist() == [1]


class TestCliBootstrap:
    def test_generic_script_help_runs_from_repo_root(self):
        repo_root = Path(__file__).resolve().parent.parent
        result = subprocess.run(
            [sys.executable, "strategies/breadth_washout.py", "--help"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0
        assert "Generic breadth washout strategy" in result.stdout

    def test_ndx_wrapper_help_runs_from_repo_root(self):
        repo_root = Path(__file__).resolve().parent.parent
        result = subprocess.run(
            [sys.executable, "strategies/ndx100_breadth_washout.py", "--help"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0
        assert "Generic breadth washout strategy" in result.stdout


class TestBuildMembershipChangeTable:
    def test_detects_changes(self):
        memberships = {
            pd.Timestamp("2026-01-02"): {"AAA", "BBB"},
            pd.Timestamp("2026-01-05"): {"AAA", "CCC"},
            pd.Timestamp("2026-01-06"): {"AAA", "CCC"},
            pd.Timestamp("2026-01-07"): {"AAA", "CCC", "DDD"},
        }

        result = build_membership_change_table(memberships)

        assert result["trade_date"].tolist() == [
            pd.Timestamp("2026-01-05"),
            pd.Timestamp("2026-01-07"),
        ]
        assert result["added"].tolist() == ["CCC", "DDD"]
        assert result["removed"].tolist() == ["BBB", ""]


class TestExpandSnapshotMemberships:
    def test_expands_latest_snapshot_forward(self):
        trade_dates = pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"])
        snapshots = {
            pd.Timestamp("2026-01-02"): {"AAA", "BBB"},
            pd.Timestamp("2026-01-06"): {"AAA", "CCC"},
        }

        expanded = expand_snapshot_memberships(trade_dates, snapshots)

        assert expanded[pd.Timestamp("2026-01-02")] == {"AAA", "BBB"}
        assert expanded[pd.Timestamp("2026-01-05")] == {"AAA", "BBB"}
        assert expanded[pd.Timestamp("2026-01-06")] == {"AAA", "CCC"}
        assert expanded[pd.Timestamp("2026-01-07")] == {"AAA", "CCC"}


class TestFormatAndSave:
    def _results(self):
        config = BreadthWashoutConfig(
            end_date="2026-03-11",
            universe_label="sp500",
            universe_mode="preset",
            signal_mode="overbought",
            signal_threshold=70.0,
        )
        trailing = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-03-09", "2026-03-10", "2026-03-11"]),
                "pct_above": [30.0, 20.0, 34.0],
                "pct_below_or_equal": [70.0, 80.0, 66.34],
                "above_count": [30, 20, 34],
                "below_or_equal_count": [71, 81, 67],
                "eligible_count": [101, 101, 101],
                "unavailable_count": [0, 0, 0],
            }
        )
        triggered = trailing.copy()
        summary = pd.DataFrame(
            {
                "asset": ["SPY", "SPXL"],
                "horizon": ["1d", "1d"],
                "signals": [3, 3],
                "observations": [2, 2],
                "mean_return_pct": [0.5, 1.2],
                "median_return_pct": [0.4, 1.0],
                "positive_rate_pct": [50.0, 50.0],
            }
        )
        changes = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-01-20"]),
                "member_count": [101],
                "added": ["WMT"],
                "removed": ["AZN"],
            }
        )
        target_row = pd.Series(
            {
                "trade_date": pd.Timestamp("2026-03-11"),
                "above_count": 34,
                "below_or_equal_count": 67,
                "pct_below_or_equal": 66.336634,
            }
        )
        return {
            "config": config,
            "universe_label": "sp500",
            "target_row": target_row,
            "trailing_breadth": trailing,
            "triggered": triggered,
            "forward_summary": summary,
            "membership_changes": changes,
            "missing_constituent_prices": ["ANSS"],
            "missing_forward_assets": [],
        }

    def test_report_contains_key_sections(self):
        report = format_strategy_report(self._results())
        assert "Breadth Washout Strategy (sp500)" in report
        assert "overbought when >= 70.00% of universe is above 5-day SMA" in report
        assert "Signals in trailing window: 3" in report
        assert "WMT" in report
        assert "Forward-return summary" in report

    def test_save_strategy_outputs_writes_files(self, tmp_path: Path):
        paths = save_strategy_outputs(self._results(), output_dir=tmp_path)

        assert sorted(paths.keys()) == ["membership_changes", "meta", "summary", "triggers"]
        for path in paths.values():
            assert path.exists()
        assert "overbought_70pct" in paths["summary"].name
        assert slugify("S&P 500") == "s-p-500"
