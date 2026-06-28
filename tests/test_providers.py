import os
import pytest
from datetime import date
import pandas as pd
from unittest.mock import patch

from providers.base import ProviderError, ProviderRateLimitError, ProviderSchemaError, ProviderTimeoutError
from providers.vnstock_provider import VnstockProvider
from providers.mock_provider import MockProvider
from providers.registry import get_provider

# P-01: VnstockProvider health_check returns True or False
def test_vnstock_health_check():
    provider = VnstockProvider()
    # It might fail if network is down, but it should return a bool
    result = provider.health_check()
    assert isinstance(result, bool)

# P-02: VnstockProvider handles RateLimit / Exceptions mapping
@patch('vnstock.Quote.history')
def test_vnstock_exception_mapping(mock_history):
    provider = VnstockProvider()
    # Simulate a rate limit error
    mock_history.side_effect = Exception("429 Too Many Requests")
    with pytest.raises(ProviderRateLimitError):
        provider.get_prices(["VNM"], date(2024, 1, 1), date(2024, 1, 2))

    # Simulate timeout
    mock_history.side_effect = Exception("Connection timed out")
    with pytest.raises(ProviderTimeoutError):
        provider.get_prices(["VNM"], date(2024, 1, 1), date(2024, 1, 2))

# P-03: MockProvider reads correct rows
def test_mock_provider_reads_correct_rows():
    provider = MockProvider()
    start = date(2024, 1, 3)
    end = date(2024, 1, 5)
    df = provider.get_prices(["VNM", "FPT"], start, end)
    
    assert not df.empty
    assert set(df['code'].unique()) == {"VNM", "FPT"}
    # 3 days for 2 stocks = 6 rows
    assert len(df) == 6
    assert df['date'].min() == start
    assert df['date'].max() == end

# P-04: MockProvider schema is correct
def test_mock_provider_schema():
    provider = MockProvider()
    df = provider.get_prices(["VCB"], date(2024, 1, 2), date(2024, 1, 2))
    expected_cols = {"code", "date", "open", "high", "low", "close", "volume", "source"}
    assert set(df.columns) == expected_cols
    assert df.iloc[0]['source'] == 'mock'

# P-05: Registry returns correct provider
def test_registry_returns_correct_provider():
    from ingestion.config import config
    with patch.object(config, 'provider', 'mock'):
        provider = get_provider()
        assert isinstance(provider, MockProvider)
    
    with patch.object(config, 'provider', 'vnstock'):
        provider = get_provider()
        assert isinstance(provider, VnstockProvider)

# P-06: MockProvider returns fallback row for future date
def test_mock_provider_future_date_fallback():
    provider = MockProvider()
    start = date(2030, 1, 1)
    end = date(2030, 1, 2)
    df = provider.get_prices(["VNM"], start, end)
    assert not df.empty
    assert df.iloc[0]['date'] == end
    assert df.iloc[0]['code'] == "VNM"

# P-07: VnstockProvider handles empty API response gracefully
@patch('vnstock.Quote.history')
def test_vnstock_empty_response(mock_history):
    provider = VnstockProvider()
    mock_history.return_value = pd.DataFrame()
    
    df = provider.get_prices(["VNM"], date(2024, 1, 1), date(2024, 1, 2))
    assert df.empty

# P-08: VnstockProvider handles invalid/NaN symbol gracefully
@patch('vnstock.Quote.history')
def test_vnstock_nan_symbol(mock_history):
    provider = VnstockProvider()
    mock_history.side_effect = Exception("Symbol not found")
    with pytest.raises(ProviderError) as exc_info:
        provider.get_prices(["NaN"], date(2024, 1, 1), date(2024, 1, 2))
    assert "Symbol not found" in str(exc_info.value)

