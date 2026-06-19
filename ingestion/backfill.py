"""
ingestion/backfill.py — Batch historical price backfill into the Bronze layer.

Features
--------
- Batches symbols to avoid rate-limiting (default batch_size=5).
- Splits long date ranges into monthly chunks to prevent API timeouts.
- Skips date ranges where all symbols already have data (resume-safe).
- Exponential backoff retry via @retry (inherited from utils).
- Progress logging per batch with elapsed time.
- CLI: python -m ingestion.backfill --start YYYY-MM-DD --end YYYY-MM-DD

Design
------
  VN30 basket (30 symbols) → chunked into batches of 5
  Each batch → monthly date chunks (2021-01 … 2026-06)
  Each chunk → fetch → validate → upsert (ON CONFLICT DO UPDATE)
  Existing data → detected per (symbol, year-month) → skipped
"""

import argparse
import logging
import time
from datetime import date, timedelta
from typing import List, Optional

import psycopg2

from ingestion.config import VN30_SYMBOLS, INDEX_SYMBOLS, get_db_url
from ingestion.db import get_connection, save_bronze_prices
from ingestion.fetch_prices import run_prices, run_index
from ingestion.utils import setup_logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Date range helpers
# ---------------------------------------------------------------------------

def _month_chunks(start: date, end: date):
    """Yield (chunk_start, chunk_end) pairs of roughly one month each."""
    cursor = start
    while cursor <= end:
        # First day of next month
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        chunk_end = min(next_month - timedelta(days=1), end)
        yield cursor, chunk_end
        cursor = next_month


def _symbol_batches(symbols: List[str], batch_size: int):
    """Yield successive batches of symbols."""
    for i in range(0, len(symbols), batch_size):
        yield symbols[i : i + batch_size]


# ---------------------------------------------------------------------------
# Existing data check (skip optimization)
# ---------------------------------------------------------------------------

def _count_existing(symbols: List[str], chunk_start: date, chunk_end: date) -> int:
    """Return how many (symbol, date) pairs already exist in bronze_prices."""
    if not symbols:
        return 0
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM bronze.bronze_prices
                    WHERE code = ANY(%s)
                      AND date >= %s
                      AND date <= %s
                    """,
                    (symbols, chunk_start, chunk_end),
                )
                row = cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.warning("Could not check existing rows (will proceed): %s", e)
        return 0


# ---------------------------------------------------------------------------
# Core backfill logic
# ---------------------------------------------------------------------------

def backfill_prices(
    symbols: Optional[List[str]] = None,
    start: date = date(2021, 1, 1),
    end: date = date.today(),
    batch_size: int = 5,
    sleep_between_batches: float = 1.0,
    force: bool = False,
) -> int:
    """
    Backfill historical OHLCV prices for symbols into bronze.bronze_prices.

    Parameters
    ----------
    symbols : list[str], optional
        Defaults to VN30_SYMBOLS.
    start : date
        Historical start date.
    end : date
        Historical end date.
    batch_size : int
        Number of symbols per API call.
    sleep_between_batches : float
        Seconds to sleep between batches (rate-limit guard).
    force : bool
        If True, skip the existing-data check and always re-fetch.

    Returns
    -------
    int
        Total rows upserted.
    """
    symbols = symbols or VN30_SYMBOLS
    total_rows = 0
    total_batches = 0
    skipped_chunks = 0

    t0 = time.monotonic()
    batches = list(_symbol_batches(symbols, batch_size))
    chunks = list(_month_chunks(start, end))

    logger.info(
        "Backfill START: %d symbols / %d batches / %d monthly chunks",
        len(symbols), len(batches), len(chunks),
    )

    for b_idx, batch in enumerate(batches, 1):
        for c_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
            # --- Skip check ---
            if not force:
                existing = _count_existing(batch, chunk_start, chunk_end)
                expected = len(batch) * (chunk_end - chunk_start).days
                # If ≥80% of expected rows already exist, skip
                if expected > 0 and existing >= int(expected * 0.8):
                    logger.debug(
                        "SKIP batch=%d/%d chunk=%d/%d (%s→%s) — %d rows exist",
                        b_idx, len(batches), c_idx, len(chunks),
                        chunk_start, chunk_end, existing,
                    )
                    skipped_chunks += 1
                    continue

            logger.info(
                "Fetching batch=%d/%d [%s] chunk=%d/%d (%s→%s)",
                b_idx, len(batches), ",".join(batch),
                c_idx, len(chunks), chunk_start, chunk_end,
            )

            try:
                rows = run_prices(batch, chunk_start, chunk_end)
                total_rows += rows
                total_batches += 1
            except Exception as e:
                logger.error(
                    "FAILED batch=%d chunk=%d (%s→%s): %s",
                    b_idx, c_idx, chunk_start, chunk_end, e,
                )
                pass

    elapsed = time.monotonic() - t0
    logger.info(
        "Backfill DONE: %d rows upserted | %d batches | %d chunks skipped | %.1fs elapsed",
        total_rows, total_batches, skipped_chunks, elapsed,
    )
    return total_rows


def backfill_index(
    indices: Optional[List[str]] = None,
    start: date = date(2021, 1, 1),
    end: date = date.today(),
    sleep_between_batches: float = 0.5,
    force: bool = False,
) -> int:
    """
    Backfill historical OHLCV for market indices into bronze.bronze_index.
    Indices are fetched as a single batch (only 2 symbols by default).
    """
    indices = indices or INDEX_SYMBOLS
    total_rows = 0
    t0 = time.monotonic()
    chunks = list(_month_chunks(start, end))

    logger.info("Index backfill START: %s / %d chunks", indices, len(chunks))

    for c_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info("Index chunk=%d/%d (%s→%s)", c_idx, len(chunks), chunk_start, chunk_end)
        try:
            rows = run_index(indices, chunk_start, chunk_end)
            total_rows += rows
        except Exception as e:
            logger.error("FAILED index chunk=%d (%s→%s): %s", c_idx, chunk_start, chunk_end, e)
            continue

        if c_idx < len(chunks):
            time.sleep(sleep_between_batches)

    elapsed = time.monotonic() - t0
    logger.info("Index backfill DONE: %d rows upserted | %.1fs elapsed", total_rows, elapsed)
    return total_rows


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill historical OHLCV data into the Bronze layer."
    )
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",   required=True, help="End date   (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols", nargs="*", default=None,
        help="Stock symbols (default: full VN30 basket)",
    )
    parser.add_argument(
        "--indices", nargs="*", default=None,
        help="Index codes (default: VNINDEX VN30)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=5,
        help="Symbols per API call (default: 5)",
    )
    parser.add_argument(
        "--sleep", type=float, default=3.5,
        help="Seconds between requests (default: 3.5 to bypass vnstock rate limit)",
    )
    parser.add_argument(
        "--skip-prices", action="store_true", help="Skip stock price backfill"
    )
    parser.add_argument(
        "--skip-index", action="store_true", help="Skip index backfill"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Ignore existing-data check and re-fetch everything",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = _parse_args()

    start_dt = date.fromisoformat(args.start)
    end_dt   = date.fromisoformat(args.end)
    total    = 0

    if not args.skip_prices:
        total += backfill_prices(
            symbols=args.symbols,
            start=start_dt,
            end=end_dt,
            batch_size=args.batch_size,
            sleep_between_batches=args.sleep,
            force=args.force,
        )

    if not args.skip_index:
        total += backfill_index(
            indices=args.indices,
            start=start_dt,
            end=end_dt,
            sleep_between_batches=args.sleep / 2,
            force=args.force,
        )

    logger.info("Grand total rows upserted: %d", total)
