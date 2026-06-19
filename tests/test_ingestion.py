import pytest
from datetime import date
from unittest.mock import patch
import pandas as pd

from ingestion.fetch_prices import run_prices, run_index
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

def test_validate_dataframe_negative_price():
    df = pd.DataFrame({
        "code": ["VNM"],
        "date": [date(2024, 1, 1)],
        "open": [-10], "high": [110], "low": [90], "close": [105],
        "volume": [1000], "source": ["mock"]
    })
    with pytest.raises(ValueError, match="non-positive value"):
        validate_dataframe(df)

@patch("ingestion.fetch_prices.get_provider")
@patch("ingestion.fetch_prices.save_bronze_prices")
def test_run_prices_success(mock_save, mock_get_provider):
    # Set up the mock provider
    from providers.mock_provider import MockProvider
    mock_get_provider.return_value = MockProvider()

    # Run prices
    start = date(2024, 1, 2)
    end = date(2024, 1, 3)
    rows = run_prices(["VNM", "VCB"], start, end)

    assert rows > 0
    mock_save.assert_called_once()
    saved_df = mock_save.call_args[0][0]
    assert len(saved_df) == rows
    assert "ingested_at" in saved_df.columns
