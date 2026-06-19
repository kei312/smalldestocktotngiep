"""
ingestion/fetch_prices.py — Orchestrate price ingestion into the Bronze layer.

Entry points
------------
- ``run_prices(symbols, start, end)``  : Fetch + validate + save stock prices.
- ``run_index(indices, start, end)``   : Fetch + validate + save index prices.
- CLI: ``python -m ingestion.fetch_prices --start YYYY-MM-DD --end YYYY-MM-DD``

Design
------
- Provider is resolved from the PROVIDER env var via the registry.
- @retry wraps the provider call (transient errors only).
- validate_dataframe() enforces Bronze contract before any DB write.
- save_bronze_prices() performs ON CONFLICT DO UPDATE (upsert).
- All steps log row counts and timing for audit trail.
"""

import argparse
import logging
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from providers.registry import get_provider
from ingestion.config import VN30_SYMBOLS, INDEX_SYMBOLS
from ingestion.db import save_bronze_prices
from ingestion.utils import retry, setup_logging, validate_dataframe
from providers.base import ProviderRateLimitError, ProviderTimeoutError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core fetch functions (wrapped with retry)
# ---------------------------------------------------------------------------

@retry(max_attempts=3, backoff_base=2.0, jitter=1.0,
       retry_on=(ProviderRateLimitError, ProviderTimeoutError))
def _fetch_with_retry(provider, symbols: List[str], start: date, end: date, is_index: bool = False) -> pd.DataFrame:
    """Call provider with retry logic. Separated so @retry wraps only the I/O call."""
    if is_index:
        return provider.get_index(symbols, start, end)
    return provider.get_prices(symbols, start, end)


def run_prices(
    symbols: Optional[List[str]] = None,
    start: date = None,
    end: date = None,
) -> int:
    """
    Fetch, validate, and save daily OHLCV for stock symbols to bronze.bronze_prices.

    Parameters
    ----------
    symbols : list[str], optional
        Stock ticker codes. Defaults to the full VN30 basket from config.
    start : date
        First trading date (inclusive).
    end : date
        Last trading date (inclusive).

    Returns
    -------
    int
        Number of rows successfully inserted/upserted.
    """
    symbols = symbols or VN30_SYMBOLS
    t_start = datetime.utcnow()

    logger.info(
        "run_prices: start=%s end=%s symbols=%s (%d total)",
        start, end, symbols[:3], len(symbols)
    )

    provider = get_provider()

    # --- Fetch (with retry) ---
    df = _fetch_with_retry(provider, symbols, start, end, is_index=False)

    # --- Validate ---
    df['ingested_at'] = pd.Timestamp.utcnow()
    df = validate_dataframe(df, context=f"prices {start}→{end}")

    # --- Save ---
    save_bronze_prices(df, table="bronze.bronze_prices")

    elapsed = (datetime.utcnow() - t_start).total_seconds()
    logger.info(
        "run_prices: DONE — %d rows upserted in %.1fs", len(df), elapsed
    )
    return len(df)


def run_index(
    indices: Optional[List[str]] = None,
    start: date = None,
    end: date = None,
) -> int:
    """
    Fetch, validate, and save daily OHLCV for market indices to bronze.bronze_index.

    Parameters
    ----------
    indices : list[str], optional
        Index codes. Defaults to VNINDEX + VN30 from config.
    start : date
        First trading date (inclusive).
    end : date
        Last trading date (inclusive).

    Returns
    -------
    int
        Number of rows successfully inserted/upserted.
    """
    indices = indices or INDEX_SYMBOLS
    t_start = datetime.utcnow()

    logger.info("run_index: start=%s end=%s indices=%s", start, end, indices)

    provider = get_provider()

    # --- Fetch (with retry) ---
    df = _fetch_with_retry(provider, indices, start, end, is_index=True)

    # --- Validate ---
    df['ingested_at'] = pd.Timestamp.utcnow()
    df = validate_dataframe(df, context=f"index {start}→{end}")

    # --- Save ---
    save_bronze_prices(df, table="bronze.bronze_index")

    elapsed = (datetime.utcnow() - t_start).total_seconds()
    logger.info(
        "run_index: DONE — %d rows upserted in %.1fs", len(df), elapsed
    )
    return len(df)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch OHLCV data from provider and upsert into Bronze layer."
    )
    parser.add_argument(
        "--start", required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end", required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--symbols", nargs="*", default=None,
        help="Stock symbols (default: VN30 basket). E.g. --symbols VNM VCB",
    )
    parser.add_argument(
        "--indices", nargs="*", default=None,
        help="Index codes (default: VNINDEX VN30). E.g. --indices VNINDEX",
    )
    parser.add_argument(
        "--skip-prices", action="store_true",
        help="Skip stock price ingestion.",
    )
    parser.add_argument(
        "--skip-index", action="store_true",
        help="Skip index ingestion.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = _parse_args()

    start_dt = date.fromisoformat(args.start)
    end_dt = date.fromisoformat(args.end)

    total_rows = 0

    if not args.skip_prices:
        total_rows += run_prices(args.symbols, start_dt, end_dt)

    if not args.skip_index:
        total_rows += run_index(args.indices, start_dt, end_dt)

    logger.info("Total rows upserted: %d", total_rows)
