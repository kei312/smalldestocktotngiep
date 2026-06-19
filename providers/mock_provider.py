import os
import logging
from datetime import date
from typing import List

import pandas as pd

from .base import DataProvider

logger = logging.getLogger(__name__)

_SOURCE_TAG = "mock"

class MockProvider(DataProvider):
    """
    Mock provider that reads from CSV fixtures for testing.
    Proves provider-agnosticism and allows offline demos.
    """

    def __init__(self, prices_csv: str = None, index_csv: str = None):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        self.prices_csv = prices_csv or os.path.join(base_dir, "tests", "fixtures", "mock_prices.csv")
        self.index_csv = index_csv or os.path.join(base_dir, "tests", "fixtures", "mock_index.csv")

    def get_prices(
        self,
        symbols: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch OHLCV from mock_prices.csv."""
        return self._fetch_csv(self.prices_csv, symbols, start, end)

    def get_index(
        self,
        indices: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch OHLCV from mock_index.csv."""
        return self._fetch_csv(self.index_csv, indices, start, end)

    def health_check(self) -> bool:
        """Always return True for the mock provider if files exist."""
        return os.path.exists(self.prices_csv) and os.path.exists(self.index_csv)

    def _fetch_csv(self, file_path: str, codes: List[str], start: date, end: date) -> pd.DataFrame:
        if not os.path.exists(file_path):
            logger.error("Mock fixture missing: %s", file_path)
            return pd.DataFrame()
            
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Filter by codes
        df = df[df['code'].isin(codes)]
        
        # Filter by date range
        df = df[(df['date'] >= start) & (df['date'] <= end)]
        
        if df.empty:
            return pd.DataFrame()
            
        df['source'] = _SOURCE_TAG
        return df[["code", "date", "open", "high", "low", "close", "volume", "source"]]
