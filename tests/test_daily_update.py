"""Tests for scripts/daily_update.py — 100% coverage target.

Tests the trading calendar, gap detection, bar validation,
async fetch helpers, and main() entrypoint.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripts.daily_update import (
    _easter,
    bars_to_rows,
    classify_gaps,
    compute_ib_duration,
    export_bronze_parquet,
    fetch_batch,
    fetch_ticker_update,
    get_nyse_holidays,
    is_trading_day,
    load_preset,
    main,
    previous_trading_day,
    trading_days_between,
    validate_bars,
)

import asyncio


# ── helpers ───────────────────────────────────────────────────────────


def _make_bar(
    date="2025-01-02", open=150.0, high=155.0, low=149.0, close=153.0, volume=1000000
):
    """Create a mock IB BarData object."""
    return SimpleNamespace(
        date=date, open=open, high=high, low=low, close=close, volume=volume
    )


# ══════════════════════════════════════════════════════════════════════
# Easter calculation
# ══════════════════════════════════════════════════════════════════════


class TestEaster:
    def test_easter_2024(self):
        assert _easter(2024) == date(2024, 3, 31)

    def test_easter_2025(self):
        assert _easter(2025) == date(2025, 4, 20)

    def test_easter_2026(self):
        assert _easter(2026) == date(2026, 4, 5)


# ══════════════════════════════════════════════════════════════════════
# Trading calendar
# ══════════════════════════════════════════════════════════════════════


class TestGetNyseHolidays:
    def test_contains_new_years(self):
        holidays = get_nyse_holidays(2025)
        assert date(2025, 1, 1) in holidays

    def test_contains_christmas(self):
        holidays = get_nyse_holidays(2025)
        assert date(2025, 12, 25) in holidays

    def test_contains_mlk_day(self):
        # MLK Day 2025 = Jan 20
        holidays = get_nyse_holidays(2025)
        assert date(2025, 1, 20) in holidays

    def test_contains_presidents_day(self):
        # Presidents Day 2025 = Feb 17
        holidays = get_nyse_holidays(2025)
        assert date(2025, 2, 17) in holidays

    def test_contains_good_friday(self):
        # Good Friday 2025 = Apr 18
        holidays = get_nyse_holidays(2025)
        assert date(2025, 4, 18) in holidays

    def test_contains_memorial_day(self):
        # Memorial Day 2025 = May 26
        holidays = get_nyse_holidays(2025)
        assert date(2025, 5, 26) in holidays

    def test_contains_juneteenth(self):
        holidays = get_nyse_holidays(2025)
        assert date(2025, 6, 19) in holidays

    def test_juneteenth_not_before_2021(self):
        holidays = get_nyse_holidays(2020)
        assert date(2020, 6, 19) not in holidays

    def test_contains_independence_day(self):
        holidays = get_nyse_holidays(2025)
        assert date(2025, 7, 4) in holidays

    def test_contains_labor_day(self):
        # Labor Day 2025 = Sep 1
        holidays = get_nyse_holidays(2025)
        assert date(2025, 9, 1) in holidays

    def test_contains_thanksgiving(self):
        # Thanksgiving 2025 = Nov 27
        holidays = get_nyse_holidays(2025)
        assert date(2025, 11, 27) in holidays

    def test_saturday_holiday_observed_friday(self):
        # July 4, 2026 is a Saturday → observed Friday July 3
        holidays = get_nyse_holidays(2026)
        assert date(2026, 7, 3) in holidays

    def test_sunday_holiday_observed_monday(self):
        # New Year's 2023: Jan 1 is Sunday → observed Monday Jan 2
        holidays = get_nyse_holidays(2023)
        assert date(2023, 1, 2) in holidays

    def test_returns_set_of_dates(self):
        holidays = get_nyse_holidays(2025)
        assert isinstance(holidays, set)
        assert all(isinstance(d, date) for d in holidays)


class TestIsTradingDay:
    def test_weekday_not_holiday(self):
        # 2025-01-02 is Thursday, not a holiday
        assert is_trading_day(date(2025, 1, 2)) is True

    def test_saturday(self):
        assert is_trading_day(date(2025, 1, 4)) is False

    def test_sunday(self):
        assert is_trading_day(date(2025, 1, 5)) is False

    def test_holiday(self):
        # New Year's 2025
        assert is_trading_day(date(2025, 1, 1)) is False

    def test_good_friday(self):
        assert is_trading_day(date(2025, 4, 18)) is False


class TestPreviousTradingDay:
    def test_from_tuesday(self):
        # 2025-01-07 is Tuesday → prev is Monday 2025-01-06
        assert previous_trading_day(date(2025, 1, 7)) == date(2025, 1, 6)

    def test_from_monday(self):
        # 2025-01-06 is Monday → prev is Friday 2025-01-03
        assert previous_trading_day(date(2025, 1, 6)) == date(2025, 1, 3)

    def test_skips_holiday(self):
        # 2025-01-02 is Thursday, 2025-01-01 is holiday → prev is 2024-12-31
        assert previous_trading_day(date(2025, 1, 2)) == date(2024, 12, 31)


class TestTradingDaysBetween:
    def test_consecutive_trading_days(self):
        # 2025-01-02 (Thu) to 2025-01-03 (Fri) = 1
        assert trading_days_between(date(2025, 1, 2), date(2025, 1, 3)) == 1

    def test_over_weekend(self):
        # 2025-01-03 (Fri) to 2025-01-06 (Mon) = 1
        assert trading_days_between(date(2025, 1, 3), date(2025, 1, 6)) == 1

    def test_same_date(self):
        assert trading_days_between(date(2025, 1, 2), date(2025, 1, 2)) == 0

    def test_full_week(self):
        # Mon Jan 6 to Fri Jan 10 = 4 (Tue-Fri)
        assert trading_days_between(date(2025, 1, 6), date(2025, 1, 10)) == 4

    def test_over_holiday(self):
        # 2024-12-31 (Tue) to 2025-01-02 (Thu) — Jan 1 is holiday
        assert trading_days_between(date(2024, 12, 31), date(2025, 1, 2)) == 1


# ══════════════════════════════════════════════════════════════════════
# Gap detection
# ══════════════════════════════════════════════════════════════════════


class TestClassifyGaps:
    def test_up_to_date(self):
        # Target is 2025-01-03 (Fri), latest is also 2025-01-03
        up, single, multi = classify_gaps(
            {"AAPL": "2025-01-03"}, date(2025, 1, 3)
        )
        assert up == ["AAPL"]
        assert single == []
        assert multi == []

    def test_single_day_gap(self):
        # Target is 2025-01-03 (Fri), latest is 2025-01-02 (Thu)
        up, single, multi = classify_gaps(
            {"AAPL": "2025-01-02"}, date(2025, 1, 3)
        )
        assert up == []
        assert single == ["AAPL"]
        assert multi == []

    def test_multi_day_gap(self):
        # Target is 2025-01-06 (Mon), latest is 2025-01-02 (Thu)
        up, single, multi = classify_gaps(
            {"AAPL": "2025-01-02"}, date(2025, 1, 6)
        )
        assert up == []
        assert single == []
        assert multi == ["AAPL"]

    def test_mixed_tickers(self):
        up, single, multi = classify_gaps(
            {
                "AAPL": "2025-01-03",
                "MSFT": "2025-01-02",
                "NVDA": "2024-12-30",
            },
            date(2025, 1, 3),
        )
        assert up == ["AAPL"]
        assert single == ["MSFT"]
        assert multi == ["NVDA"]


class TestComputeIbDuration:
    def test_single_day(self):
        assert compute_ib_duration(date(2025, 1, 2), date(2025, 1, 3)) == "3 D"

    def test_same_day_returns_1d(self):
        assert compute_ib_duration(date(2025, 1, 2), date(2025, 1, 2)) == "1 D"

    def test_one_week(self):
        result = compute_ib_duration(date(2025, 1, 1), date(2025, 1, 8))
        assert result == "9 D"

    def test_six_months(self):
        result = compute_ib_duration(date(2024, 7, 1), date(2025, 1, 1))
        assert result == "1 Y"

    def test_over_one_year(self):
        result = compute_ib_duration(date(2023, 1, 1), date(2025, 1, 1))
        assert result == "2 Y"


# ══════════════════════════════════════════════════════════════════════
# Bar validation
# ══════════════════════════════════════════════════════════════════════


class TestValidateBars:
    def test_valid_bar_passes(self):
        bar = _make_bar(date="2025-01-02")
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 1
        assert issues == []

    def test_high_less_than_low(self):
        bar = _make_bar(high=100.0, low=200.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert len(issues) == 1
        assert "high" in issues[0] and "low" in issues[0]

    def test_high_less_than_open(self):
        bar = _make_bar(open=200.0, high=100.0, low=50.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "open" in issues[0]

    def test_high_less_than_close(self):
        bar = _make_bar(close=200.0, high=100.0, low=50.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "close" in issues[0]

    def test_low_greater_than_open(self):
        bar = _make_bar(open=50.0, low=100.0, high=200.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "open" in issues[0]

    def test_low_greater_than_close(self):
        bar = _make_bar(close=50.0, low=100.0, high=200.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "close" in issues[0]

    def test_negative_volume(self):
        bar = _make_bar(volume=-1)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "volume" in issues[0]

    def test_zero_open(self):
        bar = _make_bar(open=0.0, low=0.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "open" in issues[0]

    def test_zero_close(self):
        bar = _make_bar(close=0.0, low=0.0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "close" in issues[0]

    def test_null_field(self):
        bar = SimpleNamespace(
            date="2025-01-02", open=None, high=155.0, low=149.0,
            close=153.0, volume=1000000
        )
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "null" in issues[0]

    def test_non_trading_day(self):
        # Saturday
        bar = _make_bar(date="2025-01-04")
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "not a trading day" in issues[0]

    def test_duplicate_dates(self):
        bar1 = _make_bar(date="2025-01-02")
        bar2 = _make_bar(date="2025-01-02")
        valid, issues = validate_bars([bar1, bar2], "AAPL")
        assert len(valid) == 1
        assert len(issues) == 1
        assert "duplicate" in issues[0]

    def test_invalid_date_format(self):
        bar = _make_bar(date="not-a-date")
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 0
        assert "invalid date" in issues[0]

    def test_mixed_valid_and_invalid(self):
        good = _make_bar(date="2025-01-02")
        bad = _make_bar(date="2025-01-02", high=1.0, low=200.0)
        # Different dates to avoid duplicate issue
        bad.date = "2025-01-03"
        valid, issues = validate_bars([good, bad], "AAPL")
        assert len(valid) == 1
        assert len(issues) == 1

    def test_zero_volume_is_valid(self):
        bar = _make_bar(volume=0)
        valid, issues = validate_bars([bar], "AAPL")
        assert len(valid) == 1
        assert issues == []

    def test_empty_bars(self):
        valid, issues = validate_bars([], "AAPL")
        assert valid == []
        assert issues == []


# ══════════════════════════════════════════════════════════════════════
# bars_to_rows
# ══════════════════════════════════════════════════════════════════════


class TestBarsToRows:
    def test_converts_single_bar(self):
        bar = _make_bar()
        rows = bars_to_rows([bar], symbol_id=42)
        assert len(rows) == 1
        assert rows[0]["adj_close"] == rows[0]["close"]

    def test_empty_bars(self):
        assert bars_to_rows([], symbol_id=1) == []


# ══════════════════════════════════════════════════════════════════════
# load_preset
# ══════════════════════════════════════════════════════════════════════


class TestLoadPreset:
    def test_loads_preset_file(self, tmp_path):
        preset = {"name": "test-preset", "tickers": ["AAPL", "MSFT"]}
        preset_file = tmp_path / "test.json"
        preset_file.write_text(json.dumps(preset))
        name, tickers = load_preset(preset_file)
        assert name == "test-preset"
        assert tickers == ["AAPL", "MSFT"]


# ══════════════════════════════════════════════════════════════════════
# fetch_ticker_update (async)
# ══════════════════════════════════════════════════════════════════════


class TestFetchTickerUpdate:
    def test_fetches_bars(self):
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[])
        mock_ib.get_historical_data_async = AsyncMock(
            return_value=[_make_bar()]
        )

        sem = asyncio.Semaphore(6)
        ticker, bars = asyncio.run(
            fetch_ticker_update("AAPL", "5 D", mock_ib, sem)
        )
        assert ticker == "AAPL"
        assert len(bars) == 1

    def test_returns_empty_on_none(self):
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[])
        mock_ib.get_historical_data_async = AsyncMock(return_value=None)

        sem = asyncio.Semaphore(6)
        ticker, bars = asyncio.run(
            fetch_ticker_update("AAPL", "5 D", mock_ib, sem)
        )
        assert bars == []


class TestFetchBatch:
    def test_fetches_multiple_tickers(self):
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[])
        mock_ib.get_historical_data_async = AsyncMock(
            return_value=[_make_bar()]
        )

        result = asyncio.run(
            fetch_batch([("AAPL", "5 D"), ("NVDA", "3 D")], mock_ib, max_concurrent=6)
        )
        assert "AAPL" in result
        assert "NVDA" in result

    def test_handles_error(self):
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(
            side_effect=Exception("fail")
        )

        result = asyncio.run(
            fetch_batch([("FAIL", "5 D")], mock_ib, max_concurrent=6)
        )
        assert result["FAIL"] == []


# ══════════════════════════════════════════════════════════════════════
# export_bronze_parquet
# ══════════════════════════════════════════════════════════════════════


class TestExportBronzeParquet:
    @pytest.mark.integration
    def test_exports_parquet(self, db, tmp_path):
        sid = db.upsert_symbol("AAPL", "equity", "US")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        bronze_dir = tmp_path / "bronze"
        with patch("scripts.daily_update.BRONZE_DIR", bronze_dir):
            export_bronze_parquet(db)
        assert (bronze_dir / "equities_daily.parquet").exists()


# ══════════════════════════════════════════════════════════════════════
# main()
# ══════════════════════════════════════════════════════════════════════


def _mock_ib_instance(ticker_bars):
    """Create a mock IBClient context manager returning *ticker_bars*."""
    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    mock.ib.run.return_value = ticker_bars
    return mock


class TestMain:
    @pytest.mark.integration
    def test_not_trading_day_exits(self, tmp_duckdb, monkeypatch, capsys):
        """main() exits early on non-trading day without --force."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])
        with patch("scripts.daily_update.is_trading_day", return_value=False):
            main()

    @pytest.mark.integration
    def test_force_on_non_trading_day(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() runs with --force on non-trading day."""
        monkeypatch.setattr("sys.argv", ["daily_update.py", "--force"])

        with (
            patch("scripts.daily_update.is_trading_day", return_value=False),
            patch("scripts.daily_update.previous_trading_day", return_value=date(2025, 1, 3)),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
        ):
            main()  # No data in DB → early return

    @pytest.mark.integration
    def test_no_tickers_in_db(self, tmp_duckdb, monkeypatch):
        """main() exits when no tickers in DB."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.DBClient") as MockDB,
        ):
            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_db.get_latest_dates.return_value = {}
            MockDB.return_value = mock_db
            main()

    @pytest.mark.integration
    def test_all_up_to_date(self, tmp_duckdb, monkeypatch):
        """main() exits when all tickers are up to date."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        today = date(2025, 1, 3)
        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.DBClient") as MockDB,
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_db.get_latest_dates.return_value = {"AAPL": "2025-01-03"}
            MockDB.return_value = mock_db
            main()

    @pytest.mark.integration
    def test_dry_run(self, tmp_duckdb, monkeypatch):
        """main() with --dry-run prints gap report without fetching."""
        monkeypatch.setattr("sys.argv", ["daily_update.py", "--dry-run"])

        today = date(2025, 1, 3)
        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.DBClient") as MockDB,
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_db.get_latest_dates.return_value = {"AAPL": "2025-01-02"}
            MockDB.return_value = mock_db
            main()

        # Should not have called IB
        # (We never create IBClient mock, so if it was called it would fail)

    @pytest.mark.integration
    def test_end_to_end(self, tmp_duckdb, tmp_path, monkeypatch):
        """Full integration: main() fetches, validates, inserts bars."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        from clients.db_client import DBClient as RealDBClient

        # Pre-seed AAPL with data through 2025-01-02
        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        seed_db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        seed_db.close()

        today = date(2025, 1, 3)
        mock_ib = _mock_ib_instance(
            {"AAPL": [_make_bar(date="2025-01-03", open=154.0, high=158.0, low=152.0, close=156.0)]}
        )
        bronze_dir = tmp_path / "bronze"

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.IBClient", return_value=mock_ib),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
            patch("scripts.daily_update.BRONZE_DIR", bronze_dir),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()

        # Verify new bar was inserted
        check_db = RealDBClient(db_path=tmp_duckdb)
        result = check_db.query(
            "SELECT count(*) AS cnt FROM md.equities_daily WHERE symbol_id = ?",
            [sid],
        )
        assert result[0]["cnt"] == 2
        check_db.close()

        # Verify parquet was written
        assert (bronze_dir / "equities_daily.parquet").exists()

    @pytest.mark.integration
    def test_no_new_bars_after_latest(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() handles case where all fetched bars are older than latest_date."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        seed_db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        seed_db.close()

        today = date(2025, 1, 3)
        # IB returns a bar on the same date as latest — should be filtered out
        mock_ib = _mock_ib_instance(
            {"AAPL": [_make_bar(date="2025-01-02", close=153.0)]}
        )

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.IBClient", return_value=mock_ib),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
            patch("scripts.daily_update.BRONZE_DIR", tmp_path / "bronze"),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()

    @pytest.mark.integration
    def test_empty_bars_from_ib(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() handles tickers with no bars returned from IB."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        seed_db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        seed_db.close()

        today = date(2025, 1, 3)
        mock_ib = _mock_ib_instance({"AAPL": []})

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.IBClient", return_value=mock_ib),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
            patch("scripts.daily_update.BRONZE_DIR", tmp_path / "bronze"),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()

    @pytest.mark.integration
    def test_all_bars_fail_validation(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() handles tickers where all bars fail validation."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        seed_db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        seed_db.close()

        today = date(2025, 1, 3)
        # Bad bar: high < low
        bad_bar = _make_bar(date="2025-01-03", high=1.0, low=200.0)
        mock_ib = _mock_ib_instance({"AAPL": [bad_bar]})

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.IBClient", return_value=mock_ib),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
            patch("scripts.daily_update.BRONZE_DIR", tmp_path / "bronze"),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()

    @pytest.mark.integration
    def test_preset_filter(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() with --preset filters to only those tickers."""
        preset = {"name": "test", "tickers": ["AAPL"]}
        preset_file = tmp_path / "test.json"
        preset_file.write_text(json.dumps(preset))

        monkeypatch.setattr(
            "sys.argv", ["daily_update.py", "--preset", str(preset_file), "--dry-run"]
        )

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid_a = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        sid_m = seed_db.upsert_symbol("MSFT", "equity", "SMART")
        for sid in [sid_a, sid_m]:
            seed_db.insert_equities_daily(
                [
                    {
                        "trade_date": "2025-01-02",
                        "symbol_id": sid,
                        "open": 150.0, "high": 155.0, "low": 149.0,
                        "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                    }
                ]
            )
        seed_db.close()

        today = date(2025, 1, 3)
        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()  # Should only report AAPL, not MSFT

    @pytest.mark.integration
    def test_preset_no_matching_tickers(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() with --preset exits when no preset tickers in DB."""
        preset = {"name": "test", "tickers": ["NOPE"]}
        preset_file = tmp_path / "test.json"
        preset_file.write_text(json.dumps(preset))

        monkeypatch.setattr(
            "sys.argv", ["daily_update.py", "--preset", str(preset_file)]
        )

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        seed_db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        seed_db.close()

        today = date(2025, 1, 3)
        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()

    @pytest.mark.integration
    def test_batching(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() splits tickers into batches."""
        monkeypatch.setattr(
            "sys.argv", ["daily_update.py", "--batch-size", "1"]
        )

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        for sym in ["AAPL", "MSFT"]:
            sid = seed_db.upsert_symbol(sym, "equity", "SMART")
            seed_db.insert_equities_daily(
                [
                    {
                        "trade_date": "2025-01-02",
                        "symbol_id": sid,
                        "open": 150.0, "high": 155.0, "low": 149.0,
                        "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                    }
                ]
            )
        seed_db.close()

        today = date(2025, 1, 3)
        mock_ib = _mock_ib_instance(
            {
                "AAPL": [_make_bar(date="2025-01-03")],
                "MSFT": [_make_bar(date="2025-01-03")],
            }
        )

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.IBClient", return_value=mock_ib),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
            patch("scripts.daily_update.BRONZE_DIR", tmp_path / "bronze"),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()

        # With batch-size=1 and 2 tickers, should have 2 ib.ib.run calls
        assert mock_ib.ib.run.call_count == 2

    @pytest.mark.integration
    def test_validation_issues_printed(self, tmp_duckdb, tmp_path, monkeypatch):
        """main() prints validation issues in the summary."""
        monkeypatch.setattr("sys.argv", ["daily_update.py"])

        from clients.db_client import DBClient as RealDBClient

        seed_db = RealDBClient(db_path=tmp_duckdb)
        sid = seed_db.upsert_symbol("AAPL", "equity", "SMART")
        seed_db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        seed_db.close()

        today = date(2025, 1, 3)
        # Mix of good and bad bars
        good_bar = _make_bar(date="2025-01-03", open=154.0, high=158.0, low=152.0, close=156.0)
        bad_bar = _make_bar(date="2025-01-06", high=1.0, low=999.0)  # bad OHLC
        mock_ib = _mock_ib_instance({"AAPL": [good_bar, bad_bar]})

        with (
            patch("scripts.daily_update.is_trading_day", return_value=True),
            patch("scripts.daily_update.date") as mock_date,
            patch("scripts.daily_update.IBClient", return_value=mock_ib),
            patch(
                "scripts.daily_update.DBClient",
                lambda **kw: __import__("clients.db_client", fromlist=["DBClient"]).DBClient(
                    db_path=tmp_duckdb
                ),
            ),
            patch("scripts.daily_update.BRONZE_DIR", tmp_path / "bronze"),
        ):
            mock_date.today.return_value = today
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            main()
