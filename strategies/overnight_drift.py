"""Overnight Drift Backtesting Engine.

Vectorized backtest for the "Overnight Drift" anomaly:
  Buy SPY at close, sell at next open.
Optional VIX regime filter: only take overnight trades when VIX < 200-day MA.

SPY data comes from the bronze parquet lake via DuckDB.
VIX data comes from CBOE's public CSV endpoint (cached locally).

Note: adj_close == close in this warehouse (IB TRADES data, split-adjusted
but not dividend-adjusted). For overnight returns ln(Open_{t+1}/Close_t)
this is fine — the signal is close-to-open price movement. Buy-and-hold CAGR
will understate true total return by ~1.3%/yr due to missing dividends.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import duckdb
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from statsmodels.tsa.stattools import adfuller  # noqa: E402

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WAREHOUSE = Path.home() / "market-warehouse"
BRONZE_SPY = WAREHOUSE / "data-lake" / "bronze" / "asset_class=equity" / "symbol=SPY"
VIX_CACHE = WAREHOUSE / "data-lake" / "bronze" / "external" / "vix_cboe_history.csv"
VIX_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
VIX_STALE_SECONDS = 86400  # 24 hours

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

# IBKR Tiered fee model (US equities)
IBKR_PER_SHARE = 0.0035  # commission
IBKR_EXCHANGE_REG = 0.0030  # exchange + regulatory
IBKR_TOTAL_PER_SHARE = IBKR_PER_SHARE + IBKR_EXCHANGE_REG
IBKR_MIN_ORDER = 0.35
IBKR_MAX_PCT = 0.01  # 1% of trade value

DEFAULT_CAPITAL = 1_000_000.0
RISK_FREE_RATE = 0.04  # 4% annual


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_spy_from_parquet(parquet_path: Path = BRONZE_SPY) -> pd.DataFrame:
    """Load SPY daily bars from bronze parquet via DuckDB."""
    data_file = parquet_path / "data.parquet"
    if not data_file.exists():
        raise FileNotFoundError(f"SPY parquet not found: {data_file}")
    conn = duckdb.connect(":memory:")
    df = conn.execute(
        f"SELECT trade_date, open, high, low, close, volume "
        f"FROM read_parquet('{data_file}') ORDER BY trade_date"
    ).fetchdf()
    conn.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_vix_from_cboe(
    url: str = VIX_URL,
    cache_path: Path = VIX_CACHE,
    stale_seconds: int = VIX_STALE_SECONDS,
    _opener=None,
) -> pd.DataFrame:
    """Download/cache CBOE VIX CSV and return DataFrame.

    Re-downloads if cache is older than stale_seconds.
    """
    cache_path = Path(cache_path)
    need_download = True
    if cache_path.exists():
        age = time.time() - cache_path.stat().st_mtime
        if age < stale_seconds:
            need_download = False

    if need_download:
        import urllib.request

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        opener = _opener or urllib.request.urlopen
        resp = opener(url)
        data = resp.read()
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        cache_path.write_text(data)

    df = pd.read_csv(cache_path)
    # CBOE CSV has DATE, OPEN, HIGH, LOW, CLOSE columns
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={"date": "trade_date"})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Signal computation
# ---------------------------------------------------------------------------
def compute_overnight_returns(df: pd.DataFrame) -> pd.Series:
    """Compute overnight log returns: ln(Open_{t+1} / Close_t).

    Last row will be NaN (no next open).
    """
    next_open = df["open"].shift(-1)
    return np.log(next_open / df["close"])


def compute_vix_filter(vix_df: pd.DataFrame, lookback: int = 200) -> pd.DataFrame:
    """Add VIX MA and filter mask to VIX DataFrame.

    Returns DataFrame with trade_date, vix_close, vix_ma, vix_filter columns.
    vix_filter is True when VIX close < VIX MA (low-vol regime).
    """
    result = pd.DataFrame()
    result["trade_date"] = vix_df["trade_date"]
    result["vix_close"] = vix_df["close"].values
    result["vix_ma"] = vix_df["close"].rolling(window=lookback, min_periods=lookback).mean().values
    result["vix_filter"] = result["vix_close"] < result["vix_ma"]
    return result


# ---------------------------------------------------------------------------
# Fee model
# ---------------------------------------------------------------------------
def ibkr_roundtrip_cost(equity: float, price: float) -> float:
    """IBKR tiered round-trip cost for a fully-invested position.

    Returns total dollar cost for buy + sell.
    """
    shares = int(equity / price)
    if shares <= 0:
        return 0.0

    def one_side(n_shares: float, trade_value: float) -> float:
        raw = n_shares * IBKR_TOTAL_PER_SHARE
        raw = max(raw, IBKR_MIN_ORDER)
        raw = min(raw, IBKR_MAX_PCT * trade_value)
        return raw

    trade_value = shares * price
    return one_side(shares, trade_value) + one_side(shares, trade_value)


# ---------------------------------------------------------------------------
# Strategy simulation
# ---------------------------------------------------------------------------
def simulate_strategy(
    returns: np.ndarray,
    closes: np.ndarray,
    opens_next: np.ndarray,
    mask: np.ndarray,
    capital: float = DEFAULT_CAPITAL,
    fee_fn=ibkr_roundtrip_cost,
) -> np.ndarray:
    """Simulate overnight strategy with equity-tracking loop.

    Args:
        returns: overnight log returns array
        closes: close prices (buy price)
        opens_next: next-day open prices (sell price)
        mask: boolean array — True means take the trade
        capital: starting capital
        fee_fn: callable(equity, price) -> dollar cost

    Returns:
        equity curve array (length = len(returns) + 1, starting at capital)
    """
    n = len(returns)
    equity = np.empty(n + 1)
    equity[0] = capital
    current = capital

    for i in range(n):
        if mask[i] and np.isfinite(returns[i]):
            shares = int(current / closes[i])
            if shares > 0:
                cost = fee_fn(current, closes[i])
                pnl = shares * (opens_next[i] - closes[i])
                current = current + pnl - cost
            # else: no change
        equity[i + 1] = current

    return equity


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------
def cagr(equity: np.ndarray, years: float) -> float:
    """Compound annual growth rate."""
    if years <= 0 or equity[0] <= 0:
        return 0.0
    return (equity[-1] / equity[0]) ** (1.0 / years) - 1.0


def sharpe(returns: np.ndarray, rf: float = RISK_FREE_RATE) -> float:
    """Annualized Sharpe ratio from daily returns."""
    daily_rf = rf / 252
    excess = returns - daily_rf
    if len(excess) < 2 or np.std(excess) == 0:
        return 0.0
    return np.mean(excess) / np.std(excess, ddof=1) * np.sqrt(252)


def max_drawdown(equity: np.ndarray) -> float:
    """Maximum drawdown as a positive fraction."""
    peak = np.maximum.accumulate(equity)
    dd = (peak - equity) / peak
    return float(np.max(dd))


def var_95(returns: np.ndarray) -> float:
    """95% Value at Risk (historical, daily)."""
    return float(np.percentile(returns[np.isfinite(returns)], 5))


def adf_test(returns: np.ndarray) -> dict:
    """Augmented Dickey-Fuller test on returns series."""
    clean = returns[np.isfinite(returns)]
    stat, pvalue, *_ = adfuller(clean, maxlag=10, autolag="AIC")
    return {"adf_statistic": stat, "p_value": pvalue}


def annual_returns_table(
    equity: np.ndarray, dates: pd.Series, start_year: int = 2015
) -> pd.DataFrame:
    """Per-year returns from equity curve."""
    df = pd.DataFrame({"date": dates, "equity": equity[1:]})  # skip initial capital
    df["year"] = df["date"].dt.year
    df = df[df["year"] >= start_year]

    rows = []
    for year, grp in df.groupby("year"):
        first = grp["equity"].iloc[0]
        last = grp["equity"].iloc[-1]
        ret = (last / first) - 1.0
        rows.append({"year": int(year), "return": ret})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def plot_dashboard(
    results: dict,
    dates: pd.Series,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """Render equity curves and rolling volatility charts."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Panel 1: Equity curves
    fig, ax = plt.subplots(figsize=(14, 7))
    for name, data in results.items():
        ax.plot(dates, data["equity"][1:], label=name, linewidth=0.8)
    ax.set_title("Overnight Drift — Equity Curves")
    ax.set_ylabel("Portfolio Value ($)")
    ax.set_xlabel("Date")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_yscale("log")
    fig.tight_layout()
    fig.savefig(output_dir / "overnight_drift_equity.png", dpi=150)
    plt.close(fig)

    # Panel 2: Rolling 63-day volatility
    fig, ax = plt.subplots(figsize=(14, 5))
    for name, data in results.items():
        daily_ret = np.diff(data["equity"]) / data["equity"][:-1]
        rolling_vol = pd.Series(daily_ret).rolling(63).std() * np.sqrt(252)
        ax.plot(dates, rolling_vol.values, label=name, linewidth=0.8)
    ax.set_title("Overnight Drift — Rolling 63-day Annualized Volatility")
    ax.set_ylabel("Volatility")
    ax.set_xlabel("Date")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / "overnight_drift_volatility.png", dpi=150)
    plt.close(fig)

    log.info("Charts saved to %s", output_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Overnight Drift Backtest")
    parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, default=DEFAULT_CAPITAL, help="Starting capital")
    parser.add_argument("--no-vix-filter", action="store_true", help="Skip VIX-filtered strategy")
    parser.add_argument("--no-plots", action="store_true", help="Skip chart generation")
    parser.add_argument("--start-year-table", type=int, default=2015, help="Annual table start year")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Load data
    print("Loading SPY from bronze parquet...")
    spy = load_spy_from_parquet()
    print(f"  SPY: {len(spy):,} bars, {spy['trade_date'].min().date()} to {spy['trade_date'].max().date()}")

    include_vix = not args.no_vix_filter
    if include_vix:
        print("Loading VIX from CBOE...")
        vix_raw = load_vix_from_cboe()
        vix = compute_vix_filter(vix_raw)
        print(f"  VIX: {len(vix_raw):,} bars, {vix_raw['trade_date'].min().date()} to {vix_raw['trade_date'].max().date()}")

    # Date filtering
    if args.start_date:
        spy = spy[spy["trade_date"] >= pd.Timestamp(args.start_date)]
    if args.end_date:
        spy = spy[spy["trade_date"] <= pd.Timestamp(args.end_date)]
    spy = spy.reset_index(drop=True)

    # Compute overnight returns
    overnight = compute_overnight_returns(spy)
    spy["overnight_return"] = overnight.values
    spy["open_next"] = spy["open"].shift(-1)

    # Merge VIX filter if needed
    if include_vix:
        spy = spy.merge(vix[["trade_date", "vix_close", "vix_ma", "vix_filter"]], on="trade_date", how="left")
        # Days before VIX MA is available default to False (no trade)
        spy["vix_filter"] = spy["vix_filter"].fillna(False)
    else:
        spy["vix_filter"] = False

    # Drop last row (no next open)
    spy = spy.iloc[:-1].reset_index(drop=True)
    dates = spy["trade_date"]

    n = len(spy)
    ret = spy["overnight_return"].values
    closes = spy["close"].values
    opens_next = spy["open_next"].values

    # Define strategies
    strategies = {}

    # 1. Buy and Hold
    bh_equity = np.empty(n + 1)
    bh_equity[0] = args.capital
    shares_bh = int(args.capital / closes[0])
    cost_bh = ibkr_roundtrip_cost(args.capital, closes[0])
    for i in range(n):
        bh_equity[i + 1] = shares_bh * closes[i] + (args.capital - shares_bh * closes[0]) - cost_bh
    strategies["Buy & Hold"] = {"equity": bh_equity}

    # 2. Overnight (all days)
    mask_all = np.ones(n, dtype=bool)
    eq_all = simulate_strategy(ret, closes, opens_next, mask_all, args.capital)
    strategies["Overnight (All)"] = {"equity": eq_all}

    # 3. Overnight (VIX filtered)
    if include_vix:
        mask_vix = spy["vix_filter"].values.astype(bool)
        eq_vix = simulate_strategy(ret, closes, opens_next, mask_vix, args.capital)
        strategies["Overnight (VIX Filter)"] = {"equity": eq_vix}

    # Analytics
    years = (dates.iloc[-1] - dates.iloc[0]).days / 365.25

    print("\n" + "=" * 80)
    print("OVERNIGHT DRIFT BACKTEST RESULTS")
    print(f"Period: {dates.iloc[0].date()} to {dates.iloc[-1].date()} ({years:.1f} years)")
    print(f"Capital: ${args.capital:,.0f} | Fee model: IBKR Tiered")
    print("Note: adj_close == close (IB split-adj only); B&H CAGR understates by ~1.3%/yr")
    print("=" * 80)

    header = f"{'Strategy':<25} {'Final ($)':>14} {'CAGR':>8} {'Sharpe':>8} {'MaxDD':>8} {'VaR95':>8}"
    print(header)
    print("-" * len(header))

    for name, data in strategies.items():
        eq = data["equity"]
        daily_returns = np.diff(eq) / eq[:-1]
        c = cagr(eq, years)
        s = sharpe(daily_returns)
        md = max_drawdown(eq)
        v = var_95(daily_returns)
        print(f"{name:<25} {eq[-1]:>14,.0f} {c:>7.1%} {s:>8.2f} {md:>7.1%} {v:>8.4f}")

        # ADF test on overnight strategies
        if "Overnight" in name:
            adf = adf_test(daily_returns)
            print(f"  {'ADF stat':>23}: {adf['adf_statistic']:.4f}  p-value: {adf['p_value']:.6f}")

    # Annual returns table
    print(f"\nAnnual Returns (from {args.start_year_table}):")
    ann_header = f"{'Year':<6}"
    for name in strategies:
        ann_header += f" {name:>20}"
    print(ann_header)
    print("-" * len(ann_header))

    annual_tables = {}
    for name, data in strategies.items():
        annual_tables[name] = annual_returns_table(data["equity"], dates, args.start_year_table)

    # Collect all years
    all_years = set()
    for tbl in annual_tables.values():
        all_years.update(tbl["year"].tolist())

    for year in sorted(all_years):
        row = f"{year:<6}"
        for name in strategies:
            tbl = annual_tables[name]
            yr_row = tbl[tbl["year"] == year]
            if len(yr_row) > 0:
                row += f" {yr_row['return'].iloc[0]:>19.1%}"
            else:
                row += f" {'N/A':>20}"
        print(row)

    # Charts
    if not args.no_plots:
        print(f"\nGenerating charts to {OUTPUT_DIR}/...")
        plot_dashboard(results=strategies, dates=dates)
        print("Done.")

    # Trade-off commentary
    if include_vix and "Overnight (VIX Filter)" in strategies:
        eq_all_final = strategies["Overnight (All)"]["equity"]
        eq_vix_final = strategies["Overnight (VIX Filter)"]["equity"]
        dr_all = np.diff(eq_all_final) / eq_all_final[:-1]
        dr_vix = np.diff(eq_vix_final) / eq_vix_final[:-1]
        s_all = sharpe(dr_all)
        s_vix = sharpe(dr_vix)
        vix_days = int(spy["vix_filter"].sum())
        total_days = len(spy)
        print(f"\nVIX Filter traded {vix_days:,}/{total_days:,} days ({vix_days/total_days:.0%})")
        if s_vix > s_all:
            print(f"VIX filter improved Sharpe: {s_all:.2f} -> {s_vix:.2f} by avoiding high-vol gap risk")
        else:
            print(f"VIX filter Sharpe: {s_vix:.2f} vs unfiltered: {s_all:.2f}")


if __name__ == "__main__":
    main()
