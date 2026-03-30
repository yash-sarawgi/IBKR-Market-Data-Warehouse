"""Historical data provider abstraction.

Defines a clean interface for fetching IB historical data, with two
implementations:

- IBProvider: direct IB Gateway connection via ib_insync
- RadonApiProvider: HTTP calls to Radon FastAPI historical endpoints

Usage:
    provider = await create_provider(args)
    bars = await provider.get_historical_bars(spec, duration="1 Y")
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger("mdw.historical_provider")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class BarRecord:
    """OHLCV bar record. Date is ISO format: YYYY-MM-DD for daily bars."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


# ---------------------------------------------------------------------------
# Contract spec helpers
# ---------------------------------------------------------------------------

def ib_contract_to_spec(contract) -> dict:
    """Convert an ib_insync contract to a JSON-safe spec dict."""
    spec = {
        "sec_type": contract.secType or "STK",
        "symbol": contract.symbol,
        "exchange": contract.exchange or "SMART",
        "currency": contract.currency or "USD",
    }
    ltd = getattr(contract, "lastTradeDateOrContractMonth", "")
    if ltd:
        spec["last_trade_date"] = ltd
    return spec


def spec_to_ib_contract(spec: dict):
    """Convert a spec dict to an ib_insync contract."""
    from ib_insync import Stock, Future, Index

    sec_type = spec.get("sec_type", "STK")
    symbol = spec["symbol"]
    exchange = spec.get("exchange", "SMART")
    currency = spec.get("currency", "USD")

    if sec_type == "STK":
        return Stock(symbol, exchange, currency)
    elif sec_type == "FUT":
        return Future(symbol, spec.get("last_trade_date", ""), exchange, currency)
    elif sec_type == "IND":
        return Index(symbol, exchange, currency)
    raise ValueError(f"Unsupported sec_type: {sec_type}")


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class HistoricalProvider(ABC):
    """Interface for fetching IB historical data."""

    @abstractmethod
    async def qualify_contract(self, contract_spec: dict) -> dict:
        """Qualify a contract. Returns dict with conId and other fields."""

    @abstractmethod
    async def get_head_timestamp(
        self, contract_spec: dict, what_to_show: str = "TRADES", use_rth: bool = True
    ) -> Optional[str]:
        """Get earliest available data date. Returns ISO datetime string or None."""

    @abstractmethod
    async def get_historical_bars(
        self,
        contract_spec: dict,
        end_date_time: str = "",
        duration: str = "1 D",
        bar_size: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> List[BarRecord]:
        """Fetch historical OHLCV bars. Returns list of BarRecord with ISO dates."""

    @abstractmethod
    async def disconnect(self):
        """Clean up resources."""


# ---------------------------------------------------------------------------
# IBProvider — direct IB Gateway connection
# ---------------------------------------------------------------------------

class IBProvider(HistoricalProvider):
    """Fetches historical data via direct IB Gateway connection."""

    def __init__(self, host: str = "127.0.0.1", port: int = 4001):
        from clients.ib_client import IBClient
        self._client = IBClient()
        self._client.connect(host, port)
        self._host = host
        self._port = port

    async def qualify_contract(self, contract_spec: dict) -> dict:
        contract = spec_to_ib_contract(contract_spec)
        qualified = await asyncio.to_thread(
            self._client.qualify_contracts, contract
        )
        if qualified:
            c = qualified[0] if isinstance(qualified, list) else contract
            return {
                "conId": c.conId,
                "symbol": c.symbol,
                "secType": c.secType,
                "exchange": c.exchange,
                "currency": c.currency,
            }
        return contract_spec

    async def get_head_timestamp(self, contract_spec, what_to_show="TRADES", use_rth=True):
        contract = spec_to_ib_contract(contract_spec)
        await asyncio.to_thread(self._client.qualify_contracts, contract)
        ts = await self._client.get_head_timestamp_async(
            contract, what_to_show=what_to_show, use_rth=use_rth
        )
        if not ts:
            return None
        return str(ts)

    async def get_historical_bars(
        self, contract_spec, end_date_time="", duration="1 D",
        bar_size="1 day", what_to_show="TRADES", use_rth=True,
    ):
        contract = spec_to_ib_contract(contract_spec)
        await asyncio.to_thread(self._client.qualify_contracts, contract)
        bars = await self._client.get_historical_data_async(
            contract,
            end_date_time=end_date_time,
            duration=duration,
            bar_size=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        return [
            BarRecord(
                date=str(bar.date)[:10],  # Normalize to YYYY-MM-DD
                open=float(bar.open),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
                volume=int(bar.volume),
            )
            for bar in (bars or [])
        ]

    async def disconnect(self):
        await asyncio.to_thread(self._client.disconnect)


# ---------------------------------------------------------------------------
# RadonApiProvider — HTTP calls to Radon FastAPI
# ---------------------------------------------------------------------------

class RadonApiProvider(HistoricalProvider):
    """Fetches historical data via Radon FastAPI endpoints."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 120.0):
        import httpx
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key},
            timeout=timeout,
        )

    async def qualify_contract(self, contract_spec: dict) -> dict:
        resp = await asyncio.to_thread(
            self._client.post, "/contract/qualify",
            json={"contracts": [contract_spec]},
        )
        resp.raise_for_status()
        contracts = resp.json().get("contracts", [])
        return contracts[0] if contracts else contract_spec

    async def get_head_timestamp(self, contract_spec, what_to_show="TRADES", use_rth=True):
        resp = await asyncio.to_thread(
            self._client.post, "/historical/head-timestamp",
            json={
                "contract": contract_spec,
                "what_to_show": what_to_show,
                "use_rth": use_rth,
            },
        )
        resp.raise_for_status()
        return resp.json().get("timestamp")

    async def get_historical_bars(
        self, contract_spec, end_date_time="", duration="1 D",
        bar_size="1 day", what_to_show="TRADES", use_rth=True,
    ):
        resp = await asyncio.to_thread(
            self._client.post, "/historical/bars",
            json={
                "contract": contract_spec,
                "end_date_time": end_date_time,
                "duration": duration,
                "bar_size": bar_size,
                "what_to_show": what_to_show,
                "use_rth": use_rth,
            },
        )
        resp.raise_for_status()
        return [BarRecord(**b) for b in resp.json().get("bars", [])]

    async def disconnect(self):
        self._client.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# IBClientAdapter — makes RadonApiProvider quack like IBClient
# ---------------------------------------------------------------------------

class _FakeIB:
    """Minimal shim for ib.ib.qualifyContractsAsync() and ib.ib.run()."""

    def __init__(self, provider: RadonApiProvider):
        self._provider = provider

    async def qualifyContractsAsync(self, *contracts):
        for contract in contracts:
            spec = ib_contract_to_spec(contract)
            result = await self._provider.qualify_contract(spec)
            contract.conId = result.get("conId", 0)
            if result.get("exchange"):
                contract.exchange = result["exchange"]
        return list(contracts)

    def run(self, coro):
        """Run an async coroutine — replacement for ib_insync's event loop runner."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class IBClientAdapter:
    """Makes a RadonApiProvider look like IBClient for fetch scripts.

    Supports the subset of IBClient that fetch_ib_historical.py and
    daily_update.py actually use:
      - adapter.ib.qualifyContractsAsync(contract)
      - adapter.ib.run(coroutine)
      - adapter.get_head_timestamp_async(contract, ...)
      - adapter.get_historical_data_async(contract, ...)
      - adapter.connect() / adapter.disconnect() (no-ops)
    """

    def __init__(self, provider: RadonApiProvider):
        self._provider = provider
        self.ib = _FakeIB(provider)

    def connect(self, **kwargs):
        pass

    def disconnect(self):
        coro = self._provider.disconnect()
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            asyncio.run(coro)

    async def get_head_timestamp_async(self, contract, **kwargs):
        spec = ib_contract_to_spec(contract)
        ts = await self._provider.get_head_timestamp(
            spec,
            what_to_show=kwargs.get("what_to_show", "TRADES"),
            use_rth=kwargs.get("use_rth", True),
        )
        return ts

    async def get_historical_data_async(self, contract, **kwargs):
        spec = ib_contract_to_spec(contract)
        bars = await self._provider.get_historical_bars(
            spec,
            end_date_time=kwargs.get("end_date_time", kwargs.get("end_date", "")),
            duration=kwargs.get("duration", "1 D"),
            bar_size=kwargs.get("bar_size", "1 day"),
            what_to_show=kwargs.get("what_to_show", "TRADES"),
            use_rth=kwargs.get("use_rth", True),
        )
        return bars

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.disconnect()


def create_ib_client_or_adapter(
    host: str = "127.0.0.1",
    port: int = 4001,
) -> "IBClient | IBClientAdapter":
    """Create an IBClient or IBClientAdapter based on environment.

    If MDW_RADON_API_URL and MDW_API_KEY are set, returns an IBClientAdapter
    wrapping RadonApiProvider. Otherwise returns a real IBClient.

    The adapter is a drop-in replacement for IBClient in fetch scripts.
    """
    import httpx

    api_url = os.getenv("MDW_RADON_API_URL")
    api_key = os.getenv("MDW_API_KEY")

    if api_url and api_key:
        try:
            provider = RadonApiProvider(api_url, api_key)
            # Quick connectivity check (synchronous)
            resp = provider._client.post(
                "/contract/qualify",
                json={"contracts": [{"sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"}]},
            )
            resp.raise_for_status()
            logger.info("Using Radon API for IB data (%s)", api_url)
            return IBClientAdapter(provider)
        except (httpx.ConnectError, httpx.TimeoutException):
            logger.warning("Radon API unreachable/timeout, falling back to direct IB")
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403, 422):
                logger.error("Radon API rejected request (%d) — check MDW_API_KEY", e.response.status_code)
                raise
            if e.response.status_code >= 500:
                logger.warning("Radon API server error (%d), falling back to direct IB", e.response.status_code)
            else:
                raise

    from clients.ib_client import IBClient
    logger.info("Using direct IB connection (%s:%d)", host, port)
    return IBClient()


async def create_provider(
    host: str = "127.0.0.1",
    port: int = 4001,
) -> HistoricalProvider:
    """Create a HistoricalProvider based on environment configuration.

    If MDW_RADON_API_URL and MDW_API_KEY are set, uses RadonApiProvider.
    Falls back to IBProvider on connectivity/server errors.
    Fails fast on auth errors (401/403/422).
    """
    import httpx

    api_url = os.getenv("MDW_RADON_API_URL")
    api_key = os.getenv("MDW_API_KEY")

    if api_url and api_key:
        try:
            provider = RadonApiProvider(api_url, api_key)
            # Quick connectivity check
            await provider.qualify_contract({
                "sec_type": "STK", "symbol": "AAPL",
                "exchange": "SMART", "currency": "USD",
            })
            logger.info("Using Radon API for IB data (%s)", api_url)
            return provider
        except (httpx.ConnectError, httpx.TimeoutException):
            logger.warning("Radon API unreachable/timeout, falling back to direct IB")
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403, 422):
                logger.error(
                    "Radon API rejected request (%d) — check MDW_API_KEY",
                    e.response.status_code,
                )
                raise
            if e.response.status_code >= 500:
                logger.warning(
                    "Radon API server error (%d), falling back to direct IB",
                    e.response.status_code,
                )
            else:
                raise

    logger.info("Using direct IB connection (%s:%d)", host, port)
    return IBProvider(host, port)
