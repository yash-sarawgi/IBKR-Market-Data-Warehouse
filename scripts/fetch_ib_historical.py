#!/usr/bin/env python3
"""Fetch historical daily OHLCV data from Interactive Brokers — inception to present.

Parallelises requests using ib_insync's async API with a semaphore to respect
IB's pacing limit (~6 concurrent historical-data requests).

Populates:
  - DuckDB md.symbols + md.equities_daily (delete + re-insert per ticker)
  - data-lake/bronze/asset_class=equity/ (normalized Parquet)

Requires IB Gateway or TWS running on localhost.

Usage:
    source ~/market-warehouse/.venv/bin/activate

    # Fetch Mag 7 (default, inception to present):
    python scripts/fetch_ib_historical.py

    # Custom tickers:
    python scripts/fetch_ib_historical.py --tickers AAPL NVDA

    # From a preset file (with cursor-based resume):
    python scripts/fetch_ib_historical.py --preset presets/sp500.json

    # Reset cursor and start fresh:
    python scripts/fetch_ib_historical.py --preset presets/sp500.json --reset

    # Custom batch size:
    python scripts/fetch_ib_historical.py --preset presets/sp500.json --batch-size 25

    # Custom IB Gateway port and concurrency:
    python scripts/fetch_ib_historical.py --port 7497 --max-concurrent 4

    # Backfill missing older data for tickers already in DB:
    python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from ib_insync import Stock
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

# Add project root to path so clients module is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.db_client import DBClient
from clients.ib_client import IBClient, IBError

# ── Config ─────────────────────────────────────────────────────────────

DATA_LAKE = Path.home() / "market-warehouse" / "data-lake"
BRONZE_DIR = DATA_LAKE / "bronze" / "asset_class=equity"
CURSOR_DIR = Path.home() / "market-warehouse" / "logs"

MAG7 = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

console = Console()

# ── Preset & cursor helpers ───────────────────────────────────────────


def load_preset(path: str | Path) -> tuple[str, list[str]]:
    """Read a preset JSON file and return ``(name, tickers)``."""
    p = Path(path)
    with p.open() as f:
        data = json.load(f)
    return (data["name"], data["tickers"])


def _cursor_path(name: str) -> Path:
    """Return the cursor file path for a given run name."""
    return CURSOR_DIR / f"cursor_{name}.json"


def load_cursor(name: str) -> set[str]:
    """Load completed tickers from cursor file. Returns empty set if none."""
    path = _cursor_path(name)
    if not path.exists():
        return set()
    with path.open() as f:
        data = json.load(f)
    return set(data.get("completed", []))


def save_cursor(name: str, completed: set[str], started_at: str) -> None:
    """Write cursor JSON atomically (write to tmp, then rename)."""
    path = _cursor_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    payload = {
        "completed": sorted(completed),
        "started_at": started_at,
        "updated_at": datetime.now().isoformat(),
    }
    with tmp.open("w") as f:
        json.dump(payload, f, indent=2)
    tmp.rename(path)


def clear_cursor(name: str) -> None:
    """Delete cursor file if it exists."""
    path = _cursor_path(name)
    if path.exists():
        path.unlink()


# ── Date windowing ────────────────────────────────────────────────────


def compute_date_windows(
    head_dt: datetime, end_dt: datetime
) -> list[tuple[str, str]]:
    """Generate ``("1 Y", end_datetime_str)`` tuples covering *head_dt* to *end_dt*.

    Walks backwards from *end_dt* in ~1-year steps.  Each window requests up to
    ``"1 Y"`` of data ending at the given date-time string.  The final (earliest)
    window is clamped so it doesn't extend before *head_dt*.

    Returns an empty list when *head_dt* >= *end_dt*.
    """
    if head_dt >= end_dt:
        return []

    windows: list[tuple[str, str]] = []
    cursor = end_dt

    while cursor > head_dt:
        end_str = cursor.strftime("%Y%m%d-%H:%M:%S")
        one_year_back = cursor - timedelta(days=365)

        if one_year_back <= head_dt:
            # Final window — remaining range fits in 1 Y
            windows.append(("1 Y", end_str))
            break
        else:
            windows.append(("1 Y", end_str))
            cursor = one_year_back

    return windows


# ── Transform ──────────────────────────────────────────────────────────


def bars_to_rows(bars: list, symbol_id: int) -> list[dict]:
    """Convert IB BarData objects to md.equities_daily row dicts.

    IB BarData fields: date, open, high, low, close, volume (native types).
    """
    rows = []
    for bar in bars:
        rows.append(
            {
                "trade_date": str(bar.date),
                "symbol_id": symbol_id,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "adj_close": float(bar.close),
                "volume": int(bar.volume),
            }
        )
    return rows


# ── Async fetching ────────────────────────────────────────────────────


async def fetch_ticker_bars(
    ticker: str, ib: IBClient, semaphore: asyncio.Semaphore,
    max_years: int = 0,
    end_dt_override: datetime | None = None,
) -> tuple[str, list]:
    """Fetch historical daily bars for *ticker*.

    When *max_years* > 0, caps lookback to that many years instead of inception.
    When *end_dt_override* is set, uses it as the end date and ignores *max_years*.
    Returns ``(ticker, bars)`` where bars are deduplicated IB BarData objects.
    """
    t0 = time.monotonic()
    contract = Stock(ticker, "SMART", "USD")
    await ib.ib.qualifyContractsAsync(contract)

    head_ts = await ib.get_head_timestamp_async(contract)
    if isinstance(head_ts, datetime):
        head_dt = head_ts.replace(tzinfo=None)
    else:
        head_str = str(head_ts)
        if not head_str or head_str == "[]":
            console.print(f"    [dim]{ticker}: no head timestamp — skipping[/dim]")
            return (ticker, [])
        head_dt = datetime.strptime(head_str, "%Y%m%d-%H:%M:%S")

    if end_dt_override is not None:
        end_dt = end_dt_override
    else:
        end_dt = datetime.now()

        # Cap lookback if max_years is set (only in normal mode)
        if max_years > 0:
            earliest_allowed = end_dt - timedelta(days=max_years * 365)
            if head_dt < earliest_allowed:
                head_dt = earliest_allowed

    windows = compute_date_windows(head_dt, end_dt)
    console.print(
        f"    [dim]{ticker}: history {head_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}"
        f" ({len(windows)} window{'s' if len(windows) != 1 else ''})[/dim]"
    )

    async def _fetch_chunk(duration: str, end_str: str) -> list:
        async with semaphore:
            return await ib.get_historical_data_async(
                contract,
                duration=duration,
                bar_size="1 day",
                what_to_show="TRADES",
                end_date=end_str,
            )

    chunk_results = await asyncio.gather(
        *[_fetch_chunk(dur, end_str) for dur, end_str in windows]
    )

    # Flatten and deduplicate by date
    seen_dates: set[str] = set()
    all_bars: list = []
    for chunk in chunk_results:
        if chunk:
            for bar in chunk:
                date_key = str(bar.date)
                if date_key not in seen_dates:
                    seen_dates.add(date_key)
                    all_bars.append(bar)

    # Sort by date
    all_bars.sort(key=lambda b: str(b.date))
    elapsed = time.monotonic() - t0
    console.print(
        f"    [cyan]{ticker}[/cyan]: fetched {len(all_bars)} bars in {elapsed:.1f}s"
    )
    return (ticker, all_bars)


async def fetch_all_tickers(
    tickers: list[str], ib: IBClient, max_concurrent: int = 6,
    max_years: int = 0,
    end_dt_overrides: dict[str, datetime] | None = None,
) -> dict[str, list]:
    """Fetch historical bars for all *tickers* concurrently.

    When *end_dt_overrides* is provided, each ticker uses its override as the
    end date (for backfill mode).

    Returns ``{ticker: bars}`` dict.  Per-ticker errors are logged and result
    in empty bar lists (the run continues for remaining tickers).
    """
    t0 = time.monotonic()
    mode_label = ", backfill" if end_dt_overrides else (f", {max_years}Y lookback" if max_years else ", inception")
    console.print(f"  [bold]Fetching {len(tickers)} tickers (max {max_concurrent} concurrent{mode_label})...[/bold]")
    semaphore = asyncio.Semaphore(max_concurrent)
    results: dict[str, list] = {}

    async def _safe_fetch(ticker: str) -> tuple[str, list]:
        try:
            edt = end_dt_overrides.get(ticker) if end_dt_overrides else None
            return await fetch_ticker_bars(ticker, ib, semaphore, max_years=max_years, end_dt_override=edt)
        except (IBError, Exception) as exc:
            console.print(f"    [red]{ticker}: {type(exc).__name__} — {exc}[/red]")
            return (ticker, [])

    gathered = await asyncio.gather(*[_safe_fetch(t) for t in tickers])
    for ticker, bars in gathered:
        results[ticker] = bars

    elapsed = time.monotonic() - t0
    ok = sum(1 for b in results.values() if b)
    fail = len(results) - ok
    console.print(
        f"  [bold]Fetch complete:[/bold] {ok} succeeded, {fail} failed/empty in {elapsed:.1f}s"
    )
    return results


# ── Per-ticker DB ops ─────────────────────────────────────────────────


def fetch_ticker(
    ticker: str,
    bars: list,
    db: DBClient,
) -> int:
    """Persist pre-fetched bars for *ticker* into DuckDB. Returns row count."""
    if not bars:
        console.print(f"  [yellow]No bar data for {ticker}[/yellow]")
        return 0

    symbol_id = db.upsert_symbol(ticker, "equity", "SMART")

    # Delete old data then insert fresh
    db.delete_equities_daily(symbol_id)
    rows = bars_to_rows(bars, symbol_id)
    inserted = db.insert_equities_daily(rows)

    return inserted


# ── DB helpers ────────────────────────────────────────────────────────


def get_existing_symbols(db: DBClient) -> set[str]:
    """Return the set of ticker symbols that already have equities_daily data."""
    rows = db.query(
        """
        SELECT DISTINCT s.symbol
        FROM md.symbols s
        JOIN md.equities_daily e ON s.symbol_id = e.symbol_id
        """
    )
    return {r["symbol"] for r in rows}


def get_oldest_dates(db: DBClient) -> dict[str, str]:
    """Return ``{symbol: oldest_trade_date_str}`` for each ticker with data."""
    rows = db.query(
        """
        SELECT s.symbol, MIN(e.trade_date) AS oldest
        FROM md.equities_daily e
        JOIN md.symbols s ON e.symbol_id = s.symbol_id
        GROUP BY s.symbol
        """
    )
    return {r["symbol"]: str(r["oldest"]) for r in rows}


def backfill_ticker(ticker: str, bars: list, db: DBClient) -> int:
    """Insert backfill bars for *ticker* without deleting existing data.

    Unlike ``fetch_ticker``, this skips the delete step. Dedup is handled
    by the unique constraint on ``(trade_date, symbol_id)``.
    Returns row count inserted.
    """
    if not bars:
        console.print(f"  [yellow]No backfill data for {ticker}[/yellow]")
        return 0

    symbol_id = db.upsert_symbol(ticker, "equity", "SMART")
    rows = bars_to_rows(bars, symbol_id)
    inserted = db.insert_equities_daily(rows)
    return inserted


# ── Parquet export ────────────────────────────────────────────────────


def export_bronze_parquet(db: DBClient) -> None:
    """Export all equities daily data to bronze Parquet."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = BRONZE_DIR / "equities_daily.parquet"
    db.export_to_parquet(
        """
        SELECT e.trade_date, s.symbol, e.open, e.high, e.low, e.close,
               e.adj_close, e.volume
        FROM md.equities_daily e
        JOIN md.symbols s ON e.symbol_id = s.symbol_id
        ORDER BY s.symbol, e.trade_date
        """,
        out_path,
    )
    console.print(f"  Bronze Parquet: [green]{out_path}[/green]")


# ── Main ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Fetch historical OHLCV from Interactive Brokers")
    ticker_group = parser.add_mutually_exclusive_group()
    ticker_group.add_argument(
        "--tickers",
        nargs="+",
        default=None,
        help=f"Tickers to fetch (default: {' '.join(MAG7)})",
    )
    ticker_group.add_argument(
        "--preset",
        type=str,
        default=None,
        help="Path to preset JSON file (reads .tickers array)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear existing cursor and start fresh",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers that already have data in DuckDB",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Max years of history to fetch (default: 10, 0=inception)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Tickers per async batch (default: 5)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=4001,
        help="IB Gateway port (default: 4001)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=6,
        help="Max concurrent IB historical requests (default: 6)",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill mode: fetch only missing older data for tickers already in DB",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    # ── Resolve tickers and cursor name ──────────────────────────────
    if args.preset:
        cursor_name, all_tickers = load_preset(args.preset)
        console.print(f"\n[bold]Preset:[/bold] {cursor_name} ({len(all_tickers)} tickers)")
    else:
        cursor_name = "custom"
        all_tickers = args.tickers if args.tickers else MAG7
        console.print(f"\n[bold]Tickers:[/bold] {' '.join(all_tickers)}")

    cursor_name_display = f"backfill_{cursor_name}" if args.backfill else cursor_name
    console.print(f"[bold]Cursor:[/bold]  {_cursor_path(cursor_name_display)}")
    years_label = f"{args.years}Y" if args.years else "inception"
    mode_label = "backfill" if args.backfill else "normal"
    console.print(
        f"[bold]Config:[/bold]  batch_size={args.batch_size}  max_concurrent={args.max_concurrent}"
        f"  port={args.port}  years={years_label}  skip_existing={args.skip_existing}"
        f"  mode={mode_label}"
    )

    # ── Cursor management ────────────────────────────────────────────
    effective_cursor = f"backfill_{cursor_name}" if args.backfill else cursor_name

    if args.reset:
        clear_cursor(effective_cursor)
        console.print("[yellow]Cursor reset.[/yellow]")

    completed = load_cursor(effective_cursor)
    remaining = [t for t in all_tickers if t not in completed]

    console.print(
        f"\n[bold]{len(remaining)} of {len(all_tickers)} tickers remaining"
        f" ({len(completed)} already completed via cursor)[/bold]"
    )

    if not remaining:
        console.print("[green bold]All tickers already completed. Use --reset to re-run.[/green bold]\n")
        return

    cursor_file = _cursor_path(effective_cursor)
    if completed and cursor_file.exists():
        with cursor_file.open() as f:
            started_at = json.load(f).get("started_at", datetime.now().isoformat())
    else:
        started_at = datetime.now().isoformat()

    # ── Skip existing (requires DB connection) ────────────────────────
    run_t0 = time.monotonic()

    with IBClient() as ib, DBClient() as db:
        ib.connect(port=args.port)

        if args.backfill:
            _run_backfill(args, ib, db, all_tickers, remaining, completed,
                          effective_cursor, started_at)
        else:
            _run_normal(args, ib, db, all_tickers, remaining, completed,
                        effective_cursor, started_at)

        run_elapsed = time.monotonic() - run_t0
        console.print(f"\n{'═' * 60}")
        console.print(f"[bold]Run elapsed:[/bold] {run_elapsed:.1f}s")

        # Export bronze Parquet
        console.print("\n[bold]Exporting bronze Parquet...[/bold]")
        export_bronze_parquet(db)

        # Summary
        summary = db.query(
            """
            SELECT s.symbol, count(*) as rows, min(e.trade_date) as earliest, max(e.trade_date) as latest
            FROM md.equities_daily e
            JOIN md.symbols s ON e.symbol_id = s.symbol_id
            GROUP BY s.symbol
            ORDER BY s.symbol
            """
        )
        if summary:
            console.print(f"\n[bold]Data summary ({len(summary)} symbols in DB):[/bold]")
            for row in summary:
                console.print(
                    f"  {row['symbol']:6s}  {row['rows']:>6,d} rows  "
                    f"{row['earliest']} → {row['latest']}"
                )

    console.print("\n[green bold]Done.[/green bold]\n")


def _run_backfill(args, ib, db, all_tickers, remaining, completed,
                  cursor_name, started_at):
    """Backfill mode: fetch only missing older data for tickers already in DB."""
    oldest_dates = get_oldest_dates(db)

    # Filter to tickers that exist in DB (have oldest dates)
    backfill_tickers = [t for t in remaining if t in oldest_dates]
    skipped_new = [t for t in remaining if t not in oldest_dates]
    if skipped_new:
        console.print(
            f"[cyan]Skipping {len(skipped_new)} tickers not yet in DB"
            f" (use normal fetch first):[/cyan] [dim]{' '.join(skipped_new)}[/dim]"
        )

    if not backfill_tickers:
        console.print("[green bold]No tickers to backfill.[/green bold]")
        return

    # Build end_dt_overrides: each ticker's oldest existing date
    end_dt_overrides: dict[str, datetime] = {}
    for ticker in backfill_tickers:
        end_dt_overrides[ticker] = datetime.strptime(oldest_dates[ticker], "%Y-%m-%d")

    console.print(f"[bold]{len(backfill_tickers)} tickers to backfill[/bold]")

    # Batch processing
    batches = [backfill_tickers[i:i + args.batch_size]
               for i in range(0, len(backfill_tickers), args.batch_size)]

    total_rows = 0
    total_ok = 0
    total_fail = 0

    for batch_idx, batch in enumerate(batches):
        batch_t0 = time.monotonic()
        console.print(
            f"\n{'─' * 60}\n"
            f"[bold]Backfill batch {batch_idx + 1}/{len(batches)}"
            f" ({len(batch)} tickers)[/bold]  "
            f"[dim]{' '.join(batch)}[/dim]"
        )

        batch_overrides = {t: end_dt_overrides[t] for t in batch}
        ticker_bars = ib.ib.run(
            fetch_all_tickers(batch, ib, max_concurrent=args.max_concurrent,
                              end_dt_overrides=batch_overrides)
        )

        batch_rows = 0
        batch_ok = 0
        batch_fail = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Backfilling...", total=len(batch))

            for ticker in batch:
                progress.update(task, description=f"Backfilling {ticker}...")
                bars = ticker_bars.get(ticker, [])
                count = backfill_ticker(ticker, bars, db)

                if count > 0:
                    completed.add(ticker)
                    save_cursor(cursor_name, completed, started_at)
                    console.print(f"  [green]{ticker}[/green]: {count:,} backfill rows inserted")
                    batch_ok += 1
                else:
                    console.print(f"  [yellow]{ticker}[/yellow]: 0 backfill rows (will retry next run)")
                    batch_fail += 1

                batch_rows += count
                progress.advance(task)

        batch_elapsed = time.monotonic() - batch_t0
        total_rows += batch_rows
        total_ok += batch_ok
        total_fail += batch_fail
        console.print(
            f"\n  [bold]Batch {batch_idx + 1} done:[/bold] "
            f"{batch_ok} ok, {batch_fail} failed, "
            f"{batch_rows:,} rows in {batch_elapsed:.1f}s  "
            f"[dim]({len(completed)}/{len(all_tickers)} total completed)[/dim]"
        )

    console.print(
        f"\n[bold]Backfill complete:[/bold] {total_ok} ok, {total_fail} failed, "
        f"{total_rows:,} rows"
    )
    console.print(f"[bold]Cursor:[/bold] {len(completed)}/{len(all_tickers)} tickers saved")


def _run_normal(args, ib, db, all_tickers, remaining, completed,
                cursor_name, started_at):
    """Normal fetch mode: delete + re-insert per ticker."""
    if args.skip_existing:
        existing = get_existing_symbols(db)
        skipped = [t for t in remaining if t in existing]
        if skipped:
            completed.update(skipped)
            save_cursor(cursor_name, completed, started_at)
            remaining = [t for t in remaining if t not in existing]
            console.print(
                f"[cyan]Skipped {len(skipped)} tickers already in DB:[/cyan] "
                f"[dim]{' '.join(skipped)}[/dim]"
            )
        console.print(f"[bold]{len(remaining)} tickers to fetch after skip-existing[/bold]")

    if not remaining:
        console.print("[green bold]All tickers already in DB. Use --reset to re-run.[/green bold]\n")
        return

    batches = [remaining[i:i + args.batch_size] for i in range(0, len(remaining), args.batch_size)]

    total_rows = 0
    total_ok = 0
    total_fail = 0

    for batch_idx, batch in enumerate(batches):
        batch_t0 = time.monotonic()
        console.print(
            f"\n{'─' * 60}\n"
            f"[bold]Batch {batch_idx + 1}/{len(batches)}"
            f" ({len(batch)} tickers)[/bold]  "
            f"[dim]{' '.join(batch)}[/dim]"
        )

        ticker_bars = ib.ib.run(
            fetch_all_tickers(batch, ib, max_concurrent=args.max_concurrent, max_years=args.years)
        )

        batch_rows = 0
        batch_ok = 0
        batch_fail = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Inserting...", total=len(batch))

            for ticker in batch:
                progress.update(task, description=f"Inserting {ticker}...")
                bars = ticker_bars.get(ticker, [])
                count = fetch_ticker(ticker, bars, db)

                if count > 0:
                    completed.add(ticker)
                    save_cursor(cursor_name, completed, started_at)
                    console.print(f"  [green]{ticker}[/green]: {count:,} rows inserted")
                    batch_ok += 1
                else:
                    console.print(f"  [yellow]{ticker}[/yellow]: 0 rows (will retry next run)")
                    batch_fail += 1

                batch_rows += count
                progress.advance(task)

        batch_elapsed = time.monotonic() - batch_t0
        total_rows += batch_rows
        total_ok += batch_ok
        total_fail += batch_fail
        console.print(
            f"\n  [bold]Batch {batch_idx + 1} done:[/bold] "
            f"{batch_ok} ok, {batch_fail} failed, "
            f"{batch_rows:,} rows in {batch_elapsed:.1f}s  "
            f"[dim]({len(completed)}/{len(all_tickers)} total completed)[/dim]"
        )

    console.print(
        f"\n[bold]Run complete:[/bold] {total_ok} ok, {total_fail} failed, "
        f"{total_rows:,} rows"
    )
    console.print(f"[bold]Cursor:[/bold] {len(completed)}/{len(all_tickers)} tickers saved")


if __name__ == "__main__":
    main()
