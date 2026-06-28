"""
ingestion/backfill.py — Batch historical price backfill into the Bronze layer.

Thiết kế
--------
  Mỗi lần gọi backfill_prices() / backfill_index() → fetch toàn bộ date range
  trong 1 lần gọi provider (không chia chunk theo tháng).
  VnstockProvider tự xử lý concurrency (ThreadPoolExecutor, max_workers=5)
  và rate limiting nội bộ — backfill.py không cần tầng batch.

  Skip check: nếu đã có ≥ 80% dòng kỳ vọng trong DB thì bỏ qua (resume-safe).
  Force: bỏ qua skip check, luôn re-fetch.
"""

import argparse
import logging
import time
from datetime import date, timedelta
from typing import List, Optional

from providers.registry import get_provider
from ingestion.config import VN30_SYMBOLS, INDEX_SYMBOLS
from ingestion.db import get_connection
from ingestion.fetch_prices import run_prices
from ingestion.fetch_index import run_index
from ingestion.utils import setup_logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lịch giao dịch VN — thay thế pd.bdate_range (lịch Mỹ)
# ---------------------------------------------------------------------------

# Ngày lễ cố định theo dương lịch (tháng, ngày)
# Tết Âm lịch không cố định → bỏ qua, threshold 80% vẫn bù được
_VN_FIXED_HOLIDAYS = frozenset({
    (1, 1),   # Tết Dương lịch
    (4, 30),  # Giải phóng miền Nam
    (5, 1),   # Quốc tế Lao động
    (9, 2),   # Quốc khánh
    (9, 3),   # Ngày nghỉ bù Quốc khánh (thường rơi vào đây)
})


def _count_vn_trading_days(start: date, end: date) -> int:
    """Đếm số ngày giao dịch VN trong khoảng [start, end] (T2–T6, trừ lễ cố định)."""
    count, cur = 0, start
    while cur <= end:
        if cur.weekday() < 5 and (cur.month, cur.day) not in _VN_FIXED_HOLIDAYS:
            count += 1
        cur += timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# Skip check — đọc DB 1 lần để quyết định có fetch không
# ---------------------------------------------------------------------------

def _count_existing(symbols: List[str], start: date, end: date) -> int:
    """Đếm số (symbol, date) đã tồn tại trong bronze_prices cho range này."""
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
                    (symbols, start, end),
                )
                row = cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.warning("Không thể kiểm tra existing rows (sẽ tiếp tục fetch): %s", e)
        return 0


# ---------------------------------------------------------------------------
# Core backfill — prices
# ---------------------------------------------------------------------------

def backfill_prices(
    symbols: Optional[List[str]] = None,
    start: date = date(2021, 1, 1),
    end: date = date.today(),
    mode: Optional[str] = None,
    force: bool = False,
) -> int:
    """
    Backfill giá lịch sử cho toàn bộ symbols vào bronze.bronze_prices.

    Gọi provider 1 lần với full date range.
    VnstockProvider xử lý concurrency (5 threads) và rate limiting nội bộ.

    Parameters
    ----------
    symbols : list[str], optional
        Danh sách mã cổ phiếu. Mặc định resolve động qua mode.
    start : date
        Ngày bắt đầu lịch sử.
    end : date
        Ngày kết thúc.
    mode : str, optional
        'vn30' | 'others' | 'all' — resolve symbols động từ provider.
    force : bool
        Bỏ qua skip check, luôn re-fetch.

    Returns
    -------
    int
        Tổng số dòng upserted.
    """
    provider = get_provider()

    # --- Resolve symbols ---
    if not symbols:
        if mode == "vn30":
            symbols = provider.get_vn30_symbols()
        elif mode == "others":
            all_syms = provider.get_all_symbols()
            vn30_syms = provider.get_vn30_symbols()
            symbols = sorted(set(all_syms) - set(vn30_syms))
        elif mode == "all":
            symbols = provider.get_all_symbols()
        else:
            symbols = list(VN30_SYMBOLS)

    trading_days = _count_vn_trading_days(start, end)
    expected_rows = len(symbols) * trading_days

    logger.info(
        "Backfill prices: %d symbols | %s → %s | ~%d dòng kỳ vọng | mode=%s | force=%s",
        len(symbols), start, end, expected_rows, mode, force,
    )

    # --- Skip check ---
    if not force and expected_rows > 0:
        existing = _count_existing(symbols, start, end)
        if existing >= int(expected_rows * 0.8):
            logger.info(
                "SKIP — %d/%d dòng đã có (≥80%%), bỏ qua fetch. Dùng --force để re-fetch.",
                existing, expected_rows,
            )
            return 0

    t0 = time.monotonic()
    total_rows = run_prices(symbols, start, end)
    elapsed = time.monotonic() - t0

    logger.info("Backfill prices DONE: %d dòng | %.1fs", total_rows, elapsed)
    return total_rows


# ---------------------------------------------------------------------------
# Core backfill — index
# ---------------------------------------------------------------------------

def backfill_index(
    indices: Optional[List[str]] = None,
    start: date = date(2021, 1, 1),
    end: date = date.today(),
    force: bool = False,
) -> int:
    """
    Backfill giá lịch sử cho market indices vào bronze.bronze_index.
    Index chỉ có 2 mã (VNINDEX, VN30).
    """
    indices = indices or list(INDEX_SYMBOLS)

    logger.info("Backfill index: %s | %s → %s | force=%s", indices, start, end, force)

    t0 = time.monotonic()
    total_rows = run_index(indices, start, end)
    elapsed = time.monotonic() - t0

    logger.info("Backfill index DONE: %d dòng | %.1fs", total_rows, elapsed)
    return total_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill historical OHLCV data into the Bronze layer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  # Backfill VN30 toàn bộ lịch sử
  python -m ingestion.backfill --start 2021-01-01 --end 2026-06-24 --mode vn30

  # Backfill tất cả HOSE
  python -m ingestion.backfill --start 2021-01-01 --end 2026-06-24 --mode all

  # Re-fetch toàn bộ bất kể đã có dữ liệu
  python -m ingestion.backfill --start 2021-01-01 --end 2026-06-24 --mode vn30 --force

  # Chỉ backfill index, bỏ qua prices
  python -m ingestion.backfill --start 2021-01-01 --end 2026-06-24 --skip-prices
        """,
    )
    parser.add_argument("--start",  required=True, help="Ngày bắt đầu (YYYY-MM-DD)")
    parser.add_argument("--end",    required=True, help="Ngày kết thúc (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols", nargs="*", default=None,
        help="Danh sách mã cụ thể (mặc định: resolve theo --mode)",
    )
    parser.add_argument(
        "--mode", choices=["all", "vn30", "others"], default=None,
        help="Chế độ resolve symbols: 'vn30' | 'others' | 'all'",
    )
    parser.add_argument(
        "--indices", nargs="*", default=None,
        help="Mã index (mặc định: VNINDEX VN30)",
    )
    parser.add_argument(
        "--skip-prices", action="store_true", help="Bỏ qua backfill cổ phiếu"
    )
    parser.add_argument(
        "--skip-index", action="store_true", help="Bỏ qua backfill index"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Bỏ qua skip check, re-fetch toàn bộ",
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
            mode=args.mode,
            force=args.force,
        )

    if not args.skip_index:
        total += backfill_index(
            indices=args.indices,
            start=start_dt,
            end=end_dt,
            force=args.force,
        )

    logger.info("Grand total rows upserted: %d", total)