"""DuckDB client for the market data warehouse.

Wraps DuckDB with schema-aware methods for md.symbols and md.equities_daily.
Handles upserts, dedup, and Parquet export.

Usage:
    from clients import DBClient

    with DBClient() as db:
        sid = db.upsert_symbol("AAPL", "equity", "NASDAQ")
        db.insert_equities_daily([{"trade_date": "2024-01-02", "symbol_id": sid, ...}])
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import duckdb

log = logging.getLogger(__name__)

_DEFAULT_DB_PATH = Path.home() / "market-warehouse" / "duckdb" / "market.duckdb"


class DBClient:
    """Lean DuckDB client for the market data warehouse."""

    def __init__(self, db_path: Optional[str | Path] = None):
        self._db_path = str(db_path or _DEFAULT_DB_PATH)
        self._conn = duckdb.connect(self._db_path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create unique index for dedup if it doesn't exist."""
        try:
            self._conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_equities_daily_dedup "
                "ON md.equities_daily (trade_date, symbol_id)"
            )
        except duckdb.CatalogException:
            # Index already exists or table structure issue — safe to continue
            pass

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DBClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── Symbol management ──────────────────────────────────────────

    def upsert_symbol(self, symbol: str, asset_class: str, venue: str) -> int:
        """Insert or return existing symbol_id for a given symbol.

        Uses a hash-based ID derived from the symbol string for deterministic IDs.
        """
        # Check if symbol already exists
        result = self._conn.execute(
            "SELECT symbol_id FROM md.symbols WHERE symbol = ?", [symbol]
        ).fetchone()

        if result:
            return result[0]

        # Generate deterministic ID from symbol hash
        symbol_id = abs(hash(symbol)) % (2**53)

        self._conn.execute(
            "INSERT INTO md.symbols (symbol_id, symbol, asset_class, venue) VALUES (?, ?, ?, ?)",
            [symbol_id, symbol, asset_class, venue],
        )
        log.info("Inserted symbol %s (id=%d)", symbol, symbol_id)
        return symbol_id

    # ── Equities daily ─────────────────────────────────────────────

    def insert_equities_daily(self, rows: list[dict]) -> int:
        """Bulk insert OHLCV rows into md.equities_daily with dedup.

        Rows that conflict on (trade_date, symbol_id) are skipped.
        Returns the number of rows inserted.
        """
        if not rows:
            return 0

        inserted = 0
        for row in rows:
            try:
                self._conn.execute(
                    """
                    INSERT INTO md.equities_daily
                        (trade_date, symbol_id, open, high, low, close, adj_close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        row["trade_date"],
                        row["symbol_id"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["adj_close"],
                        row["volume"],
                    ],
                )
                inserted += 1
            except duckdb.ConstraintException:
                # Duplicate (trade_date, symbol_id) — skip
                continue

        log.info("Inserted %d/%d rows into md.equities_daily", inserted, len(rows))
        return inserted

    def delete_equities_daily(self, symbol_id: int) -> int:
        """Delete all equities_daily rows for a given symbol_id.

        Returns the number of rows deleted.
        """
        # DuckDB doesn't support changes_count directly; count first
        count = self._conn.execute(
            "SELECT count(*) FROM md.equities_daily WHERE symbol_id = ?",
            [symbol_id],
        ).fetchone()[0]
        if count:
            self._conn.execute(
                "DELETE FROM md.equities_daily WHERE symbol_id = ?",
                [symbol_id],
            )
            log.info("Deleted %d rows for symbol_id=%d", count, symbol_id)
        return count

    # ── Aggregation queries ─────────────────────────────────────────

    def get_latest_dates(self) -> dict[str, str]:
        """Return {symbol: latest_trade_date_str} for each ticker with data."""
        rows = self.query(
            """
            SELECT s.symbol, MAX(e.trade_date) AS latest
            FROM md.equities_daily e
            JOIN md.symbols s ON e.symbol_id = s.symbol_id
            GROUP BY s.symbol
            """
        )
        return {r["symbol"]: str(r["latest"]) for r in rows}

    # ── Query helpers ──────────────────────────────────────────────

    def query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        """Execute raw SQL and return results as list of dicts."""
        result = self._conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_equities_daily(
        self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> list[dict]:
        """Convenience query for equities daily data by symbol."""
        sql = """
            SELECT e.trade_date, s.symbol, e.open, e.high, e.low, e.close,
                   e.adj_close, e.volume
            FROM md.equities_daily e
            JOIN md.symbols s ON e.symbol_id = s.symbol_id
            WHERE s.symbol = ?
        """
        params: list[Any] = [symbol]

        if start_date:
            sql += " AND e.trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND e.trade_date <= ?"
            params.append(end_date)

        sql += " ORDER BY e.trade_date"
        return self.query(sql, params)

    # ── Parquet export ─────────────────────────────────────────────

    def export_to_parquet(self, sql: str, path: str | Path, params: Optional[list] = None) -> Path:
        """Export query results to a Parquet file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn.execute(
            f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)",
        )
        log.info("Exported to %s", path)
        return path
