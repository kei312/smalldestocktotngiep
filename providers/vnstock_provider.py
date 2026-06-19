"""
VnstockProvider — wraps vnstock 4.x to conform to the DataProvider interface.

vnstock 4.x handles source fallback (VCI → TCBS → MSN) internally,
so this provider does NOT need to implement multi-source logic.
"""

import logging
from datetime import date
from typing import List

import pandas as pd

from .base import (
    DataProvider,
    ProviderError,
    ProviderRateLimitError,
    ProviderSchemaError,
    ProviderTimeoutError,
)

logger = logging.getLogger(__name__)

# Columns we expect from vnstock's Quote.history() response
_EXPECTED_COLUMNS = {"time", "open", "high", "low", "close", "volume"}

# Source tag written into the ``source`` column for audit trail
_SOURCE_TAG = "vnstock"


class VnstockProvider(DataProvider):
    """Concrete provider backed by the vnstock 4.x library."""

    # ------------------------------------------------------------------
    # Public interface (DataProvider contract)
    # ------------------------------------------------------------------

    def get_prices(
        self,
        symbols: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch OHLCV for stock symbols via vnstock Quote.history()."""
        frames: List[pd.DataFrame] = []
        for symbol in symbols:
            df = self._fetch_history(symbol, start, end)
            if df is not None and not df.empty:
                frames.append(df)
        if not frames:
            logger.warning("No data returned for symbols=%s", symbols)
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def get_index(
        self,
        indices: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch OHLCV for market indices (VNINDEX, VN30)."""
        frames: List[pd.DataFrame] = []
        for index_code in indices:
            df = self._fetch_history(index_code, start, end)
            if df is not None and not df.empty:
                frames.append(df)
        if not frames:
            logger.warning("No data returned for indices=%s", indices)
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def health_check(self) -> bool:
        """Quick connectivity test: fetch 1 day of VNM data."""
        try:
            from vnstock import Quote

            quote = Quote(symbol="VNM", source="VCI")
            df = quote.history(
                start="2024-01-02",
                end="2024-01-03",
                interval="1D",
            )
            ok = df is not None and not df.empty
            logger.info("Health check: %s", "OK" if ok else "EMPTY")
            return ok
        except Exception as e:
            logger.error("Health check failed: %s", str(e))
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_history(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Call vnstock and normalise the result to Bronze-compatible schema.

        Maps vnstock exceptions into our provider exception hierarchy so
        that the ingestion layer's retry decorator works uniformly.
        """
        try:
            from vnstock import Quote

            quote = Quote(symbol=symbol, source="VCI")
            df = quote.history(
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                interval="1D",
            )
            if df is None or df.empty:
                logger.info("Empty response for %s (%s → %s)", symbol, start, end)
                return pd.DataFrame()

            self._validate_schema(df, symbol)
            df = self._normalise(df, symbol)
            logger.info("Fetched %d rows for %s", len(df), symbol)
            return df

        except ProviderError:
            # Already mapped — re-raise as-is
            raise
        except ConnectionError as e:
            logger.error("Timeout / connection error for %s: %s", symbol, str(e))
            raise ProviderTimeoutError(str(e)) from e
        except Exception as e:
            error_msg = str(e).lower()
            if "429" in error_msg or "rate" in error_msg or "limit" in error_msg:
                logger.warning("Rate-limited for %s: %s", symbol, str(e))
                raise ProviderRateLimitError(str(e)) from e
            if "timeout" in error_msg or "timed out" in error_msg:
                logger.error("Timeout for %s: %s", symbol, str(e))
                raise ProviderTimeoutError(str(e)) from e
            logger.error("Provider error for %s: %s", symbol, str(e))
            raise ProviderError(str(e)) from e

    @staticmethod
    def _validate_schema(df: pd.DataFrame, symbol: str) -> None:
        """Ensure the upstream DataFrame has the columns we need."""
        missing = _EXPECTED_COLUMNS - set(df.columns)
        if missing:
            raise ProviderSchemaError(
                f"Schema drift for {symbol}: missing columns {missing}"
            )

    @staticmethod
    def _normalise(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Rename / add columns to match the Bronze input contract.

        Bronze expects: code, date, open, high, low, close, volume, source.
        vnstock returns: time, open, high, low, close, volume.
        """
        out = df.rename(columns={"time": "date"}).copy()
        out["code"] = symbol
        out["source"] = _SOURCE_TAG
        out["date"] = pd.to_datetime(out["date"]).dt.date
        return out[["code", "date", "open", "high", "low", "close", "volume", "source"]]
