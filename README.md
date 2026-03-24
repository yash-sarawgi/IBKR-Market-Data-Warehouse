# Market Data Warehouse

![Market Data Warehouse](https://raw.githubusercontent.com/joemccann/market-data-warehouse/main/.github/github-banner.png)

A **local-first financial data warehouse** for universe-scale market data.

---

## Overview

Market Data Warehouse is designed for storing and analyzing historical **OHLCV data across equities, futures, and volatility indices**, with a clear path from **daily bars → intraday data → production analytics**.

### Core Stack

* **Parquet data lake** → canonical storage
* **DuckDB** → local analytics, research, backtesting
* **ClickHouse (optional)** → large-scale aggregation & concurrency

### Current Capabilities

* Daily ingestion for:

  * **Equities (IB)**
  * **Futures (IB)**
  * **Volatility indices (CBOE API)**
* Per-ticker **bronze Parquet snapshots**
* **Atomic writes + validation**
* **Fallback recovery pipeline** for missing data
* On-demand **DuckDB rebuilds** from Parquet

> **In one sentence:**
> A local-first, production-ready market data warehouse for serious quantitative workflows.

---

## Goals

* ⚡ High-performance **local quant research environment**
* 🧱 Scalable **multi-asset data model**
* 🔁 Clean **local → production transition**
* 🌐 **Polyglot workflows** (Python, Rust, Node.js)

---

## Architecture

### Data Flow

```text
Raw → Bronze → Silver → Gold
```

* **Raw** → vendor data
* **Bronze** → canonical Parquet (primary ingestion layer)
* **Silver** → cleaned / adjusted datasets
* **Gold** → analytics, factors, derived tables

### Storage Strategy

* **System of record**: Parquet (`data-lake/`)
* **Local engine**: DuckDB
* **Warehouse (optional)**: ClickHouse

---

## Directory Structure

```text
~/market-warehouse/
├── data-lake/
│   ├── raw/
│   ├── bronze/
│   │   ├── asset_class=equity/symbol=AAPL/data.parquet
│   │   ├── asset_class=volatility/symbol=VIX/data.parquet
│   │   └── asset_class=futures/symbol=ES_202506/data.parquet
│   ├── silver/
│   └── gold/
├── duckdb/
│   └── market.duckdb
├── scripts/
├── logs/
└── .venv/
```

---

## Installation

### Requirements

* macOS (Apple Silicon recommended)
* Homebrew
* Python 3.12+
* Node.js 22+
* DuckDB
* Interactive Brokers account
* Docker (recommended for IB Gateway)
* ClickHouse (optional)

---

### Quick Start

```bash
chmod +x setup_market_warehouse.sh
./setup_market_warehouse.sh
```

### Full Bootstrap

```bash
./setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## Interactive Brokers Gateway

You need a running IB Gateway for ingestion.

---

### Option 1 (Recommended): Docker

Uses [`gnzsnz/ib-gateway-docker`](https://github.com/gnzsnz/ib-gateway-docker)

#### Quick Start

```bash
cd docker/ib-gateway
cp .env.example .env
mkdir -p secrets
echo "YOUR_IB_PASSWORD" > secrets/ib_password.txt
docker compose up -d
```

* Complete 2FA via:

  * VNC (`localhost:5900`)
  * IBKR mobile app

#### Ports

| Host | Purpose   |
| ---- | --------- |
| 4001 | Live API  |
| 4002 | Paper API |
| 5900 | VNC       |

---

### Option 2: Native macOS (IBC)

IBC provides:

* Auto login
* Session recovery
* Daily restarts

#### Install

```bash
python scripts/install_ibc_secure_service.py
```

#### Commands

```bash
~/ibc/bin/start-secure-ibc-service.sh
~/ibc/bin/stop-secure-ibc-service.sh
~/ibc/bin/status-secure-ibc-service.sh
```

> The IBC service is **machine-level**, not repo-scoped.

---

## Data Ingestion

### Prerequisites

* IB Gateway running (`127.0.0.1:4001` by default)
* Configurable via:

  * CLI flags (`--host`, `--port`)
  * Env vars (`MDW_IB_HOST`, `MDW_IB_PORT`)

---

### Fetch Historical Data

```bash
source ~/market-warehouse/.venv/bin/activate

# Default (Mag 7)
python scripts/fetch_ib_historical.py

# Specific tickers
python scripts/fetch_ib_historical.py --tickers AAPL NVDA

# Preset universe
python scripts/fetch_ib_historical.py --preset presets/sp500.json

# Futures
python scripts/fetch_ib_historical.py --preset presets/futures-index.json --asset-class futures

# Volatility (CBOE direct)
python scripts/fetch_cboe_volatility.py
```

---

### Backfill Missing Data

```bash
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill
```

* Fills only missing history
* Preserves existing data
* Independent cursor tracking

---

### Daily Updates

```bash
python scripts/daily_update.py
```

Common flags:

```bash
--dry-run
--force
--target-date YYYY-MM-DD
--preset presets/sp500.json
--asset-class {equity|volatility|futures}
```

#### Key Behavior

* Detects missing trading days
* Fetches only gaps
* Validates OHLCV
* Atomic snapshot updates
* Fallback recovery if IB fails

---

### Rebuild DuckDB

```bash
python scripts/rebuild_duckdb_from_parquet.py
```

---

## Scheduling

### macOS (`launchd`)

```bash
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
```

### Schedule

* **Daily sync**: 13:05 PT (4:05 PM ET)
* **Watchdog**: 18:30 PT

---

## Alerts & Monitoring

* Automatic retries
* Email alerts on failure
* Optional AI-generated summaries

### Setup

```bash
npm install
```

Example config:

```bash
MDW_ALERT_EMAIL_TO="you@example.com"
MDW_ALERT_SMTP_URL="smtp://user:pass@mail.example.com:587"
```


## Testing

### Run Tests

```bash
python -m pytest tests/ -v
```

### Coverage

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts
```

* ✅ **100% coverage enforced**

---

## Security

### Pre-commit Hook

```bash
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Detects:

* API keys
* credentials
* private keys
* `.env` leaks

---

## Data Model Notes

### Split-Adjusted Volume

All volume is **split-adjusted** to ensure consistency across time.

---

## ClickHouse (Optional)

Used for:

* Benchmarking
* Concurrency
* Production simulation

### Commands

```bash
~/market-warehouse/scripts/start_clickhouse.sh
~/market-warehouse/scripts/init_clickhouse.sh
~/market-warehouse/scripts/stop_clickhouse.sh
```

---

## Sample Data

```bash
python scripts/write_sample_parquet.py
```

---

## Recommended Workflow

1. Store all data in **Parquet (bronze)**
2. Use **DuckDB** for:

   * research
   * backtesting
3. Use **ClickHouse** for:

   * large-scale queries
   * production-like workloads

---

## Troubleshooting

### DuckDB Errors

* Use inline `PRIMARY KEY`
* Avoid reserved keywords (`right` → `option_right`)

### ClickHouse Issues

```bash
clickhouse-client --query "SELECT version()"
```

---

## Recommended Command

```bash
./setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## License

MIT
