import os
import pandas as pd
import numpy as np
from datetime import date

from ingestion.config import VN30_SYMBOLS, INDEX_SYMBOLS

def generate_mock_data(symbols, file_name):
    # 5 years, roughly 250 trading days/year = 1250 days
    dates = pd.date_range(start="2021-01-01", end="2026-06-18", freq="B")
    
    all_data = []
    for sym in symbols:
        # random walk
        base_price = np.random.randint(20000, 100000)
        returns = np.random.normal(0, 0.02, len(dates))
        prices = base_price * np.exp(np.cumsum(returns))
        
        df = pd.DataFrame({
            "code": sym,
            "date": dates.date,
            "open": prices,
            "high": prices * 1.01,
            "low": prices * 0.99,
            "close": prices * (1 + np.random.normal(0, 0.005, len(dates))),
            "volume": np.random.randint(100000, 5000000, len(dates))
        })
        all_data.append(df)
        
    final_df = pd.concat(all_data)
    final_df.to_csv(file_name, index=False)
    print(f"Generated {len(final_df)} rows for {file_name}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    generate_mock_data(VN30_SYMBOLS, os.path.join(base_dir, "tests", "fixtures", "mock_prices.csv"))
    generate_mock_data(INDEX_SYMBOLS, os.path.join(base_dir, "tests", "fixtures", "mock_index.csv"))
