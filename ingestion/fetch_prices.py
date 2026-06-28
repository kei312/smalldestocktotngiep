"""
ingestion/fetch_prices.py — Orchestrate price ingestion into the Bronze layer.

Entry points
------------
- ``run_prices(symbols, start, end)``  : Fetch + validate + save stock prices.
- CLI: ``python -m ingestion.fetch_prices --start YYYY-MM-DD --end YYYY-MM-DD``
"""

import argparse
import logging
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from providers.registry import get_provider
from ingestion.config import VN30_SYMBOLS
from ingestion.db import save_bronze_prices
from ingestion.utils import retry, setup_logging, validate_dataframe
from providers.base import ProviderRateLimitError, ProviderTimeoutError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core fetch functions (wrapped with retry)
# ---------------------------------------------------------------------------

@retry(max_attempts=3, backoff_base=2.0, jitter=1.0,
       retry_on=(ProviderRateLimitError, ProviderTimeoutError))
def _fetch_with_retry(provider, symbols: List[str], start: date, end: date) -> pd.DataFrame:
    """Call provider with retry logic. Separated so @retry wraps only the I/O call."""
    return provider.get_prices(symbols, start, end)


def run_prices(
    symbols: Optional[List[str]] = None,
    start: date = None,
    end: date = None,
    mode: Optional[str] = None,
) -> int:
    """
    Fetch, validate, and save daily OHLCV for stock symbols to bronze.bronze_prices.

    Parameters
    ----------
    symbols : list[str], optional
        Stock ticker codes. If omitted, resolved dynamically via mode.
    start : date
        First trading date (inclusive).
    end : date
        Last trading date (inclusive).
    mode : str, optional
        Crawl mode: 'all' (all HSX stocks), 'vn30' (only VN30), 'others' (all HSX except VN30).

    Returns
    -------
    int
        Number of rows successfully inserted/upserted.
    """
    provider = get_provider()

    # Dynamic symbol resolution
    if not symbols:
        if mode == "vn30":
            symbols = provider.get_vn30_symbols()
            from ingestion.db import save_bronze_vn30_components
            save_bronze_vn30_components(symbols)
        elif mode == "others":
            all_symbols = provider.get_all_symbols()
            vn30_symbols = provider.get_vn30_symbols()
            symbols = list(set(all_symbols) - set(vn30_symbols))
        elif mode == "all":
            symbols = provider.get_all_symbols()
        else:
            # Fallback to VN30 pilot config for backwards compatibility
            symbols = VN30_SYMBOLS

    t_start = datetime.utcnow()

    logger.info(
        "run_prices: start=%s end=%s mode=%s symbols=%s (%d total)",
        start, end, mode, symbols[:3], len(symbols)
    )

    # --- Fetch (with retry) ---
    df = _fetch_with_retry(provider, symbols, start, end)

    # --- Validate ---
    if df is not None and not df.empty:
        df['ingested_at'] = pd.Timestamp.utcnow()
        df = validate_dataframe(df, context=f"prices {start}→{end}")
        # --- Save ---
        save_bronze_prices(df, table="bronze.bronze_prices")
        rows_saved = len(df)
    else:
        logger.warning("No price data returned from provider for symbols=%s", symbols[:5])
        rows_saved = 0

    elapsed = (datetime.utcnow() - t_start).total_seconds()
    logger.info(
        "run_prices: DONE — %d rows upserted in %.1fs", rows_saved, elapsed
    )
    return rows_saved



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
        help="Stock symbols (default: dynamic resolved by mode). E.g. --symbols VNM VCB",
    )
    parser.add_argument(
        "--mode", choices=["all", "vn30", "others"], default=None,
        help="Dynamic symbol resolution mode: 'all', 'vn30', 'others'.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = _parse_args()

    start_dt = date.fromisoformat(args.start)
    end_dt = date.fromisoformat(args.end)

    total_rows = run_prices(args.symbols, start_dt, end_dt, args.mode)
    logger.info("Total stock price rows upserted: %d", total_rows)

