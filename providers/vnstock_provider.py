"""
VnstockProvider — wraps vnstock 4.x to conform to the DataProvider interface.

Implements multi-source rotation and sequential fallback across available
Vietnamese stock quote sources (vci, kbs, msn) to maximize throughput while
respecting API rate limits.
"""

import logging
import time
import os
import threading
from datetime import date
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from vnstock import Quote, register_user
from vnstock.api.listing import Listing
from vnstock.config import Config

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

# Optimized global rate limit: 60 requests/minute is 1.0s. 
# We use 1.05s per request for safety margin.
_REQUEST_INTERVAL_SECONDS = 1.5

# Supported stock sources for history quotes in vnstock 4.x
_UNIQUE_SOURCES = ["kbs", "vci"]
_SOURCES_POOL = ["kbs", "kbs", "kbs", "kbs", "vci"]
_source_index = 0
_source_lock = threading.Lock() 

def _get_next_source() -> str:
    """Thread-safe round-robin source rotation from pool (4 kbs : 1 vci)."""
    global _source_index
    with _source_lock:
        src = _SOURCES_POOL[_source_index % len(_SOURCES_POOL)]
        _source_index += 1
        return src


class RateLimiter:
    def __init__(self, interval: float):
        self.interval = interval
        self.lock = threading.Lock()
        self.last_called = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_called
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_called = time.monotonic()


class VnstockProvider(DataProvider):
    """Concrete provider backed by the vnstock 4.x library."""

    def __init__(self):
        # vnstock 4.x enforces rate limits per API key globally, not per source.
        # We must use a single global rate limiter to prevent concurrent 429 errors.
        global_limiter = RateLimiter(_REQUEST_INTERVAL_SECONDS)
        self._rate_limiters = {
            src: global_limiter for src in _UNIQUE_SOURCES
        }
        
        # Thread-safe global pause event for rate-limiting
        self._pause_event = threading.Event()
        self._pause_event.set()  # Unpaused initially
        self._pause_lock = threading.Lock()

        # Enforce global network connection timeout
        Config.REQUEST_TIMEOUT = 15

        # Read API key if configured
        api_key = os.environ.get("VNSTOCK_API_KEY")
        if api_key:
            try:
                success = register_user(api_key)
                if success:
                    logger.info("vnstock user registered successfully with API key from environment")
                else:
                    logger.warning("vnstock register_user returned False")
            except Exception as e:
                logger.warning("Failed to register vnstock user API key: %s", str(e))

    def _get_rate_limiter(self, source: str) -> RateLimiter:
        """Get the rate limiter for a specific source (case-insensitive)."""
        src_lower = source.lower()
        if src_lower in self._rate_limiters:
            return self._rate_limiters[src_lower]
        # Fallback to the first available source rate limiter
        return self._rate_limiters[_UNIQUE_SOURCES[0]]

    def _wait_if_paused(self):
        """Block if we are currently in a rate limit cooldown pause."""
        self._pause_event.wait()

    def _trigger_pause(self, seconds: float):
        """Pause all provider threads by clearing the event and sleeping."""
        with self._pause_lock:
            if self._pause_event.is_set():
                self._pause_event.clear()
                logger.warning("Rate limit hit! Pausing all provider threads for %.1f seconds...", seconds)
                time.sleep(seconds)
                self._pause_event.set()
                logger.info("Provider threads resumed.")

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
        errors = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self._fetch_history, sym, start, end): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    df = future.result(timeout=60)
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as e:
                    logger.error("Failed to fetch prices for %s: %s", sym, str(e))
                    errors.append((sym, e))
                    
        # If all symbols failed and we got errors, propagate the first error
        if not frames and errors:
            raise errors[0][1]
        elif errors:
            logger.warning("Fetched prices with partial errors. Failed symbols: %s", [sym for sym, _ in errors])

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
        errors = []
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(self._fetch_history, idx_code, start, end): idx_code for idx_code in indices}
            for future in as_completed(futures):
                idx_code = futures[future]
                try:
                    df = future.result(timeout=60)
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as e:
                    logger.error("Failed to fetch index for %s: %s", idx_code, str(e))
                    errors.append((idx_code, e))

        # If all indices failed and we got errors, propagate the first error
        if not frames and errors:
            raise errors[0][1]
        elif errors:
            logger.warning("Fetched indices with partial errors. Failed: %s", [idx for idx, _ in errors])

        if not frames:
            logger.warning("No data returned for indices=%s", indices)
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def health_check(self) -> bool:
        """Quick connectivity test: check availability of both VCI and KBS sources.
        Returns True if at least one source is accessible (enabling fallback operation).
        """
        sources = ["vci", "kbs"]
        for src in sources:
            try:
                self._wait_if_paused()
                self._get_rate_limiter(src).wait()
                quote = Quote(symbol="VNM", source=src)
                df = quote.history(
                    start="2024-01-02",
                    end="2024-01-03",
                    interval="1D",
                )
                if df is not None and not df.empty:
                    logger.info("Health check OK for source=%s", src)
                    return True
            except Exception as e:
                logger.warning("Health check failed for source=%s: %s", src, str(e))
        logger.error("Health check failed for all sources.")
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

        Rotates sources and implements active fallback. Maps vnstock exceptions.
        """
        start_src = _get_next_source()
        
        # Build list trying start_src first, then fallback to other unique sources
        sources = [start_src]
        for src in _UNIQUE_SOURCES:
            if src != start_src:
                sources.append(src)

        last_error = None
        for src in sources:
            try:
                # Block if currently paused
                self._wait_if_paused()
                
                # Enforce rate limiting
                self._get_rate_limiter(src).wait()

                logger.debug("Fetching %s from %s", symbol, src)
                quote = Quote(symbol=symbol, source=src)
                df = quote.history(
                    start=start.strftime("%Y-%m-%d"),
                    end=end.strftime("%Y-%m-%d"),
                    interval="1D",
                )

                if df is None or df.empty:
                    logger.info("Empty response for %s from %s (%s → %s)", symbol, src, start, end)
                    # We treat empty as a valid response (no need to fallback to other sources)
                    return pd.DataFrame()

                self._validate_schema(df, symbol)
                df = self._normalise(df, symbol, src)
                logger.info("Fetched %d rows for %s from %s", len(df), symbol, src)
                return df

            except ProviderSchemaError:
                # Schema errors are fatal, don't fallback to other sources
                raise
            except SystemExit as e:
                logger.warning("Rate limit (SystemExit) hit for %s on source %s", symbol, src)
                self._trigger_pause(15.0)
                last_error = ProviderRateLimitError(f"vnstock rate limit (sys.exit) for {symbol}")
            except Exception as e:
                import requests
                
                # Unwrap tenacity.RetryError if present
                actual_err = e
                if type(e).__name__ == "RetryError":
                    try:
                        actual_err = e.last_attempt.exception()
                    except Exception:
                        pass
                
                if isinstance(actual_err, requests.exceptions.HTTPError) and actual_err.response.status_code == 429:
                    logger.warning("Rate-limited for %s on %s: %s", symbol, src, str(actual_err))
                    self._trigger_pause(10.0)
                    last_error = ProviderRateLimitError(str(actual_err))
                elif isinstance(actual_err, requests.exceptions.Timeout) or isinstance(actual_err, requests.exceptions.ConnectionError):
                    logger.warning("Timeout/ConnectionError for %s on %s: %s. Trying fallback source...", symbol, src, str(actual_err))
                    last_error = ProviderTimeoutError(str(actual_err))
                else:
                    # Fallback string matching for non-requests exceptions or wrapped ones
                    error_msg = str(actual_err).lower()
                    if "429" in error_msg or "rate" in error_msg or "limit" in error_msg:
                        logger.warning("Rate-limited (str match) for %s on %s: %s", symbol, src, str(actual_err))
                        self._trigger_pause(10.0)
                        last_error = ProviderRateLimitError(str(actual_err))
                    elif "timeout" in error_msg or "timed out" in error_msg or "connectionerror" in error_msg:
                        logger.warning("Timeout/ConnectionError (str match) for %s on %s: %s. Trying fallback source...", symbol, src, str(actual_err))
                        last_error = ProviderTimeoutError(str(actual_err))
                    else:
                        logger.warning("Provider error for %s on %s: %s. Trying fallback source...", symbol, src, str(actual_err))
                        last_error = ProviderError(str(actual_err))

        # If we exhausted all sources, raise the last mapped error
        if last_error:
            raise last_error
        raise ProviderError(f"Failed to fetch {symbol} from all sources.")

    @staticmethod
    def _validate_schema(df: pd.DataFrame, symbol: str) -> None:
        """Ensure the upstream DataFrame has the columns we need."""
        missing = _EXPECTED_COLUMNS - set(df.columns)
        if missing:
            raise ProviderSchemaError(
                f"Schema drift for {symbol}: missing columns {missing}"
            )

    @staticmethod
    def _normalise(df: pd.DataFrame, symbol: str, source_name: str) -> pd.DataFrame:
        """Rename / add columns to match the Bronze input contract.

        Bronze expects: code, date, open, high, low, close, volume, source.
        vnstock returns: time, open, high, low, close, volume.
        """
        out = df.rename(columns={"time": "date"}).copy()
        out["code"] = symbol
        out["source"] = f"{_SOURCE_TAG}_{source_name}"
        out["date"] = pd.to_datetime(out["date"]).dt.date
        out = out[["code", "date", "open", "high", "low", "close", "volume", "source"]]

        # Dedup tại nguồn: API có thể trả duplicate dates cho cùng 1 symbol
        before = len(out)
        out = out.drop_duplicates(subset=["date"], keep="last")
        if len(out) < before:
            logger.warning(
                "_normalise: dropped %d duplicate date rows for %s from %s",
                before - len(out), symbol, source_name,
            )

        return out

    def get_all_symbols(self) -> List[str]:
        """Fetch all stock symbols active on HOSE (HSX)."""
        try:
            self._wait_if_paused()
            self._get_rate_limiter("vci").wait()
            l = Listing(source='VCI')
            df = l.symbols_by_exchange()
            df_stocks = df[
                (df['type'] == 'STOCK') & 
                (df['exchange'] == 'HSX')
            ]
            return df_stocks['symbol'].tolist()
        except Exception as e:
            logger.error("Error fetching all HOSE symbols: %s", str(e))
            raise ProviderError(str(e)) from e

    def get_vn30_symbols(self) -> List[str]:
        """Fetch active VN30 stock symbols dynamically."""
        try:
            self._wait_if_paused()
            self._get_rate_limiter("vci").wait()
            l = Listing(source='VCI')
            series = l.symbols_by_group('VN30')
            return series.tolist()
        except Exception as e:
            logger.error("Error fetching VN30 symbols: %s", str(e))
            raise ProviderError(str(e)) from e
