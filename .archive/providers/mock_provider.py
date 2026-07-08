import os
import logging
from datetime import date
from typing import List

import pandas as pd

from .base import DataProvider

logger = logging.getLogger(__name__)

_SOURCE_TAG = "mock"

# ---------------------------------------------------------------------------
# Symbol lists — Mock basket (INTENTIONALLY khác VN30 thực tế)
# Mục đích: test pipeline flow, không cần mạng.
# Khớp với STOCK_PROFILES trong generate_mock.py — phải đồng bộ 2 file.
# Để thay đổi basket: sửa đồng thời cả 2 file rồi chạy lại generate_mock.py.
# ---------------------------------------------------------------------------

_VN30_SYMBOLS = [
    "ACB", "BCM", "BID", "BVH", "CTG",
    "FPT", "GAS", "GVR", "HDB", "HPG",
    "MBB", "MSN", "MWG", "NVL", "PDR",
    "PLX", "POW", "SAB", "SSI", "STB",
    "TCB", "TPB", "VCB", "VHM", "VIB",
    "VIC", "VJC", "VNM", "VPB", "VRE",
]

# Một số mã ngoài VN30 để simulate mode="all"
_EXTRA_HOSE_SYMBOLS = [
    "DXG", "DIG", "NLG", "KDH", "HDG",
    "REE", "PNJ", "EVF", "LPB", "SHB",
]

_ALL_HOSE_SYMBOLS = _VN30_SYMBOLS + _EXTRA_HOSE_SYMBOLS


class MockProvider(DataProvider):
    """
    Mock provider đọc từ CSV fixtures — KHÔNG gọi API thật.

    Mục đích:
      - Test pipeline Bronze/Silver/Gold mà không cần mạng (CI/CD, demo).
      - Test DQ gates (Silver flag, Gold filter) với mock_prices_dq.csv.
      - Test backfill flow với dữ liệu lịch sử (2021-2026) từ mock_prices.csv.

    Kích hoạt:
      .env: PROVIDER=mock
      Registry tự động chọn MockProvider, không ảnh hưởng PROVIDER=vnstock.

    Chuẩn bị fixtures trước khi dùng:
      python -m ingestion.generate_mock  # sinh mock_prices.csv + mock_prices_dq.csv

    Đơn vị dữ liệu (khớp với VnstockProvider):
      - Giá (open/high/low/close): NGHÌN VND (ví dụ: FPT=72.0 = 72,000 VND)
      - Volume: cổ phiếu
      - Source: 'mock'
    """

    def __init__(self, prices_csv: str = None, index_csv: str = None):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        self.prices_csv = prices_csv or os.path.join(base_dir, "tests", "fixtures", "mock_prices.csv")
        self.index_csv  = index_csv  or os.path.join(base_dir, "tests", "fixtures", "mock_index.csv")

    # ------------------------------------------------------------------
    # DataProvider interface
    # ------------------------------------------------------------------

    def get_prices(self, symbols: List[str], start: date, end: date) -> pd.DataFrame:
        return self._fetch_csv(self.prices_csv, symbols, start, end)

    def get_index(self, indices: List[str], start: date, end: date) -> pd.DataFrame:
        return self._fetch_csv(self.index_csv, indices, start, end)

    def health_check(self) -> bool:
        prices_ok = os.path.exists(self.prices_csv)
        index_ok  = os.path.exists(self.index_csv)
        if not prices_ok or not index_ok:
            logger.error(
                "MockProvider fixtures missing! "
                "Run: python -m ingestion.generate_mock\n"
                "  prices_csv exists: %s (%s)\n"
                "  index_csv  exists: %s (%s)",
                prices_ok, self.prices_csv,
                index_ok,  self.index_csv,
            )
        return prices_ok and index_ok

    def get_vn30_symbols(self) -> List[str]:
        return list(_VN30_SYMBOLS)

    def get_all_symbols(self) -> List[str]:
        return list(_ALL_HOSE_SYMBOLS)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_csv(self, file_path: str, codes: List[str], start: date, end: date) -> pd.DataFrame:
        if not os.path.exists(file_path):
            logger.error("Mock fixture missing: %s", file_path)
            return pd.DataFrame()

        df = pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["code"].isin(codes)]

        df_filtered = df[(df["date"] >= start) & (df["date"] <= end)].copy()

        if df_filtered.empty:
            if not df.empty:
                logger.info(
                    "No data in range [%s, %s] for %s. Applying fallback.",
                    start, end, codes,
                )
                fallback = df.sort_values("date").groupby("code").last().reset_index()
                fallback["date"]   = end
                fallback["source"] = _SOURCE_TAG
                return fallback[["code", "date", "open", "high", "low", "close", "volume", "source"]]
            return pd.DataFrame()

        df_filtered["source"] = _SOURCE_TAG
        return df_filtered[["code", "date", "open", "high", "low", "close", "volume", "source"]]