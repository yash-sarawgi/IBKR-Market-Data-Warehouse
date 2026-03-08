# Market Data Warehouse

Local-first financial data warehouse for quantitative research. Parquet data lake as system of record, DuckDB for analytics, ClickHouse for production benchmarking.

## Project Layout

Two directory trees: this **git repo** and the **data warehouse** at `~/market-warehouse/`.

```
market-data-warehouse/              # Git repo
├── clients/
│   ├── __init__.py                 # Exports IBClient, DBClient
│   ├── ib_client.py                # Interactive Brokers API client (ib_insync)
│   ├── uw_client.py                # Unusual Whales REST API client (kept, not used for historical)
│   └── db_client.py                # DuckDB client for md.* schema
├── scripts/
│   ├── setup_market_warehouse.sh   # One-time system bootstrap
│   ├── fetch_ib_historical.py      # Bulk historical OHLCV ingestion from IB (supports --backfill)
│   ├── run_backfill_all.sh         # Auto-restarting runner for all presets
│   ├── daily_update.py             # Daily incremental update (scheduled)
│   ├── run_daily_update.sh         # Shell wrapper for launchd/cron
│   ├── com.market-warehouse.daily-update.plist  # macOS launchd config
│   └── pre-commit-secrets-scan.sh  # Pre-commit hook: secrets scanner
├── tests/
│   ├── conftest.py                 # Shared fixtures: tmp_duckdb, db
│   ├── test_uw_client.py           # Unit tests — HTTP mocked via `responses`
│   ├── test_db_client.py           # Integration tests — temp DuckDB per test
│   ├── test_fetch_ib_historical.py # Tests for IB fetch script
│   └── test_daily_update.py        # Tests for daily update script
├── pyproject.toml                  # pytest config, coverage enforcement
├── .env.example
└── README.md

~/market-warehouse/                 # Data warehouse (created by setup script)
├── .venv/                          # Python 3.12 venv
├── data-lake/
│   ├── bronze/asset_class=equity/  # Normalized Parquet (canonical)
│   ├── silver/                     # Cleaned / adjusted
│   └── gold/                       # Derived analytics / factor tables
├── duckdb/market.duckdb            # Analytical DB
├── clickhouse/                     # Optional ClickHouse data
├── scripts/                        # Bootstrap SQL, helper scripts
└── logs/
```

## Architecture

- **Parquet** is the system of record, not DuckDB
- **Data lake tiers**: bronze (normalized Parquet) -> silver (cleaned) -> gold (derived)
- **DuckDB** is the local query engine for research and backtesting
- **ClickHouse** is optional, for production-style benchmarking and concurrency testing
- **Python env** lives at `~/market-warehouse/.venv/` — activate with `source ~/market-warehouse/.venv/bin/activate`

## DuckDB Schema

Schema `md` with four tables:

- `md.symbols` — `symbol_id BIGINT PK`, `symbol`, `asset_class`, `venue`
- `md.equities_daily` — `trade_date DATE`, `symbol_id BIGINT`, OHLCV + `adj_close`; unique index on `(trade_date, symbol_id)` for dedup
- `md.futures_daily` — trade_date, contract_id, root_symbol, expiry, OHLCV + settlement + OI
- `md.options_daily` — trade_date, contract_id, underlier_id, expiry, strike, `option_right` (not `right` — reserved keyword), OHLCV + OI + implied_vol

ClickHouse mirrors the same schema with MergeTree engines partitioned by `toYYYYMM(trade_date)`.

## IB Gateway / IBC

IB Gateway is managed by **IBC** (IB Controller) for automated login, reconnection, and daily restarts.

- **IBC install**: `~/ibc-install/` (IBC.jar + scripts)
- **IBC config**: `~/ibc/config.ini` (credentials + settings)
- **IBC logs**: `~/ibc/logs/`
- **Start Gateway**: `~/ibc-install/gatewaystartmacos.sh`
- **Stop Gateway**: `~/ibc-install/stop.sh`
- **Reconnect data**: `~/ibc-install/reconnectdata.sh`
- **Command server**: port 7462
- **Gateway API port**: 4001
- **Auto-restart**: 11:58 PM daily, cold restart Sundays 07:05

## Data Ingestion

Data source: **Interactive Brokers** via `ib_insync`. Requires IB Gateway running on localhost (via IBC).

- `IBClient` wraps `ib_insync.IB` with connection management, historical data, and contract qualification
- `IBClient.get_historical_data()` fetches daily bars via `reqHistoricalData`
- `DBClient` wraps DuckDB: `upsert_symbol()` uses deterministic hash-based IDs, `insert_equities_daily()` deduplicates via unique constraint, `delete_equities_daily()` clears old data before re-insert, `get_latest_dates()` returns `{symbol: latest_trade_date}` for gap detection
- `adj_close` is set to `close` (IB TRADES data doesn't provide adjusted prices)

### IB BarData → DuckDB mapping

| IB BarData field | DuckDB column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from symbols) | `symbol_id` | Lookup/create in `md.symbols` |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `adj_close` | Same value |
| `bar.volume` | `volume` | `int(bar.volume)` |

### Running the pipeline

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/fetch_ib_historical.py                                  # Mag 7 default
python scripts/fetch_ib_historical.py --tickers AAPL NVDA              # Custom tickers
python scripts/fetch_ib_historical.py --preset presets/sp500.json      # From preset with cursor resume
python scripts/fetch_ib_historical.py --years 0 --skip-existing        # Inception, skip existing
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill  # Backfill older data
```

### Backfill mode

`--backfill` fetches only missing older data for tickers already in DB:
- Queries each ticker's oldest existing `trade_date`
- Fetches IB inception → oldest_date gap
- Inserts without deleting (dedup via unique constraint)
- Uses separate cursor: `cursor_backfill_{name}.json`
- Skips tickers not in DB (use normal fetch first)

### Auto-restarting runner

```bash
bash scripts/run_backfill_all.sh   # Runs all presets with stall detection + auto-restart
```

Output: DuckDB rows + bronze Parquet in `data-lake/bronze/`.

### Daily updates

`daily_update.py` is a lightweight script for daily scheduled runs (~2,500 tickers). It discovers tickers from DB, detects gaps vs the latest trading day, fetches only missing bars, validates OHLCV integrity, and inserts (no delete). Idempotent and safe for concurrent runs.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/daily_update.py                                  # Normal daily run
python scripts/daily_update.py --dry-run                        # Report gaps without fetching
python scripts/daily_update.py --force                          # Run on non-trading day
python scripts/daily_update.py --preset presets/sp500.json      # Limit to preset tickers
python scripts/daily_update.py --port 7497 --max-concurrent 4   # Custom IB config
python scripts/daily_update.py --batch-size 25                  # Custom batch size
```

**Scheduling with launchd** (macOS):
```bash
cp scripts/com.market-warehouse.daily-update.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
```
Runs at 21:05 UTC daily (4:05 PM EST / 5:05 PM EDT — always after market close). Non-trading days are harmless no-ops.

**Key design:**
- Discovers tickers from DB via `DBClient.get_latest_dates()` — no hardcoded lists
- Insert-only (dedup via unique constraint) — no delete, no data loss risk
- Bar validation: checks OHLCV relationships, positive prices, valid trading days, duplicate dates
- Pure-Python NYSE trading calendar — no new dependencies
- Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`

### Querying

```bash
duckdb ~/market-warehouse/duckdb/market.duckdb \
  "SELECT s.symbol, count(*) FROM md.symbols s JOIN md.equities_daily e ON s.symbol_id = e.symbol_id GROUP BY s.symbol"
```

## Testing

**All new code in `clients/` and `scripts/` must have tests. Coverage is enforced at 100%.**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v                                                        # Run all
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing  # With coverage
python -m pytest tests/ -v -m "not integration"                                   # Unit tests only
```

### Rules for new code

1. Add tests in `tests/test_<module>.py`
2. Mock all external I/O (IB connections via `MagicMock`, file paths via `patch`)
3. Use `tmp_duckdb` / `db` fixtures from `conftest.py` for DB tests
4. Mark DB tests with `@pytest.mark.integration`
5. Run coverage and confirm 100% before committing
6. `pyproject.toml` enforces `fail_under = 100`; `if __name__ == "__main__"` blocks are excluded
7. `clients/ib_client.py` is excluded from coverage (pre-existing, complex IB integration)

### Test deps

`pytest`, `pytest-cov`, `responses` (installed in `~/market-warehouse/.venv/`)

## Pre-commit Hook

A secrets scanner runs on every commit, checking staged files for API keys, passwords, private keys, tokens, and credentials. Install with:

```bash
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Catches: AWS keys, API key/secret/password assignments, private key headers, GitHub/Slack tokens, Google API keys, connection strings with credentials, hardcoded IB credentials, staged `.env` files. Allowlists test files, placeholders, comments, `os.environ` reads, and error messages to avoid false positives. Bypass with `git commit --no-verify` if needed.

## Key Implementation Details

- IB BarData provides native float/int types — no string parsing needed
- `symbol_id` is `abs(hash(symbol)) % 2^53` for deterministic IDs
- Fetch script does delete-then-insert per ticker: `delete_equities_daily()` then `insert_equities_daily()`
- Bronze Parquet is a single file at `data-lake/bronze/asset_class=equity/equities_daily.parquet` (full export on each run)
