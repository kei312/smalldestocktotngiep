import pytest
from datetime import date
from unittest.mock import patch
import pandas as pd

from ingestion.fetch_prices import run_prices
from ingestion.fetch_index import run_index
from ingestion.utils import validate_dataframe

def test_validate_dataframe_success():
    df = pd.DataFrame({
        "code": ["VNM"],
        "date": [date(2024, 1, 1)],
        "open": [100], "high": [110], "low": [90], "close": [105],
        "volume": [1000], "source": ["mock"]
    })
    res = validate_dataframe(df)
    assert not res.empty

def test_validate_dataframe_missing_column():
    df = pd.DataFrame({
        "code": ["VNM"],
        "date": [date(2024, 1, 1)],
        "open": [100], "high": [110], "low": [90], "close": [105]
    })
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_dataframe(df)

def test_validate_dataframe_negative_price(caplog):
    df = pd.DataFrame({
        "code": ["VNM"],
        "date": [date(2024, 1, 1)],
        "open": [-10], "high": [110], "low": [90], "close": [105],
        "volume": [1000], "source": ["mock"]
    })
    # Nên trả về df bình thường (không văng lỗi), nhưng có ghi log warning
    res = validate_dataframe(df)
    assert not res.empty
    assert "non-positive value" in caplog.text

def test_validate_dataframe_null_pk(caplog):
    df = pd.DataFrame({
        "code": [None],
        "date": [date(2024, 1, 1)],
        "open": [100], "high": [110], "low": [90], "close": [105],
        "volume": [1000], "source": ["mock"]
    })
    # Nên trả về df bình thường (không văng lỗi), nhưng có ghi log warning
    res = validate_dataframe(df)
    assert not res.empty
    assert "Found NULL values in PK columns" in caplog.text
class DummyMockProvider:
    def get_prices(self, symbols, start, end):
        from datetime import timedelta
        data = []
        for sym in symbols:
            curr = start
            while curr <= end:
                data.append({
                    "code": sym,
                    "date": curr,
                    "open": 100.0,
                    "high": 105.0,
                    "low": 95.0,
                    "close": 101.0,
                    "volume": 1000,
                    "source": "mock"
                })
                curr += timedelta(days=1)
        return pd.DataFrame(data)

    def get_index(self, indices, start, end):
        return self.get_prices(indices, start, end)


@patch("ingestion.fetch_prices.get_provider")
@patch("ingestion.fetch_prices.save_bronze_prices")
def test_run_prices_success(mock_save, mock_get_provider):
    # Set up the mock provider
    mock_get_provider.return_value = DummyMockProvider()

    # Run prices
    start = date(2024, 1, 2)
    end = date(2024, 1, 3)
    rows = run_prices(["VNM", "VCB"], start, end)

    assert rows > 0
    mock_save.assert_called_once()
    saved_df = mock_save.call_args[0][0]
    assert len(saved_df) == rows
    assert "ingested_at" in saved_df.columns


@patch("ingestion.fetch_prices.get_provider")
@patch("ingestion.fetch_prices.save_bronze_prices")
def test_backfill_logic(mock_save, mock_get_provider):
    """Xác minh FR-08: test_backfill_logic (Khôi phục dữ liệu lịch sử)"""
    mock_get_provider.return_value = DummyMockProvider()

    # Backfill with a wider range
    start = date(2023, 1, 1)
    end = date(2024, 1, 5)
    rows = run_prices(["VNM"], start, end)

    assert rows > 0
    mock_save.assert_called_once()
    saved_df = mock_save.call_args[0][0]
    assert len(saved_df) == rows
    assert "ingested_at" in saved_df.columns


@patch("ingestion.fetch_index.get_provider")
@patch("ingestion.fetch_index.save_bronze_prices")
def test_run_index_success(mock_save, mock_get_provider):
    # Set up the mock provider
    mock_get_provider.return_value = DummyMockProvider()

    # Run index
    start = date(2024, 1, 2)
    end = date(2024, 1, 3)
    rows = run_index(["VNINDEX", "VN30"], start, end)

    assert rows > 0
    mock_save.assert_called_once()
    saved_df = mock_save.call_args[0][0]
    assert len(saved_df) == rows
    assert "ingested_at" in saved_df.columns
