"""
ingestion/fetch_index.py — Orchestrate index price ingestion into the Bronze layer.

Entry points
------------
- ``run_index(indices, start, end)``   : Fetch + validate + save index prices.
- CLI: ``python -m ingestion.fetch_index --start YYYY-MM-DD --end YYYY-MM-DD``
"""


import argparse
import logging
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from providers.registry import get_provider
from ingestion.config import INDEX_SYMBOLS
from ingestion.db import save_bronze_prices
from ingestion.utils import retry, setup_logging, validate_dataframe
from providers.base import ProviderRateLimitError, ProviderTimeoutError

logger = logging.getLogger(__name__)


@retry(max_attempts=3, backoff_base=2.0, jitter=1.0,
       retry_on=(ProviderRateLimitError, ProviderTimeoutError))
def _fetch_with_retry(provider, symbols: List[str], start: date, end: date) -> pd.DataFrame:
    """Call provider with retry logic. Separated so @retry wraps only the I/O call."""
    return provider.get_index(symbols, start, end)


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
    df = _fetch_with_retry(provider, indices, start, end)

    # --- Validate & Save ---
    if df is not None and not df.empty:
        df['ingested_at'] = pd.Timestamp.utcnow()
        df = validate_dataframe(df, context=f"index {start}→{end}")
        save_bronze_prices(df, table="bronze.bronze_index")
        rows_saved = len(df)
    else:
        logger.warning("No index data returned from provider for indices=%s", indices)
        rows_saved = 0

    elapsed = (datetime.utcnow() - t_start).total_seconds()
    logger.info(
        "run_index: DONE — %d rows upserted in %.1fs", rows_saved, elapsed
    )
    return rows_saved


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch Index data from provider and upsert into Bronze layer."
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
        "--indices", nargs="*", default=None,
        help="Index codes (default: VNINDEX VN30). E.g. --indices VNINDEX",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = _parse_args()

    start_dt = date.fromisoformat(args.start)
    end_dt = date.fromisoformat(args.end)

    total_rows = run_index(args.indices, start_dt, end_dt)
    logger.info("Total index rows upserted: %d", total_rows)
