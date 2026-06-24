#!/usr/bin/env python
"""
Benchmark tool to verify the throughput performance of VnstockProvider.
Mocks the underlying vnstock API to isolate and measure rate-limiting overhead.
"""

import time
import logging
from datetime import date
from unittest.mock import patch
import pandas as pd

from providers.vnstock_provider import VnstockProvider

# Configure logging to see timestamps of requests
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
)

def mock_history(*args, **kwargs):
    # Simulate a fast response from the API (e.g. 10ms)
    time.sleep(0.01)
    return pd.DataFrame({
        "time": [pd.Timestamp("2024-01-02")],
        "open": [100.0],
        "high": [105.0],
        "low": [98.0],
        "close": [102.0],
        "volume": [1000000]
    })

@patch('vnstock.Quote.history', side_effect=mock_history)
def run_benchmark(mock_hist):
    provider = VnstockProvider()
    symbols = ["VNM", "VCB", "HPG", "FPT", "VIC", "MWG", "MSN", "CTG", "TCB", "MBB"]
    
    print("=" * 60)
    print(f"Starting throughput benchmark for {len(symbols)} symbols...")
    print(f"Sources: {provider._rate_limiters.keys()}")
    print(f"Rate limit interval: {1.05}s per source")
    print("=" * 60)
    
    start_time = time.time()
    
    # get_prices uses ThreadPoolExecutor internally with max_workers=5
    df = provider.get_prices(symbols, date(2024, 1, 2), date(2024, 1, 2))
    
    duration = time.time() - start_time
    print("=" * 60)
    print("BENCHMARK RESULTS:")
    print(f"Total symbols fetched: {len(symbols)}")
    print(f"Total execution time: {duration:.2f} seconds")
    print(f"DataFrame size: {df.shape}")
    
    # Calculate throughput
    req_per_minute = (len(symbols) / duration) * 60
    print(f"Measured Throughput: {req_per_minute:.2f} requests/minute")
    
    # Analytical verification
    # Old global lock: 10 requests * 1.05s = ~10.5s
    # New per-source locks: 10 requests shared between 2 sources = 5 requests per source
    # 5 requests per source with 1.05s interval = ~4.2s (with concurrency)
    print("\nThroughput Analysis:")
    print("- Under global lock: 10 requests * 1.05s ≈ 10.50 seconds")
    print("- Under per-source locks (expected): 5 requests/source * 1.05s ≈ 4.20 seconds")
    
    if duration < 7.0:
        print("\nSUCCESS: Per-source lock parallelized requests successfully!")
    else:
        print("\nWARNING: Execution time was slower than expected. Check rate limiters.")
    print("=" * 60)

if __name__ == "__main__":
    run_benchmark()
