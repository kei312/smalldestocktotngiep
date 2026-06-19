from vnstock import Quote
import pandas as pd
try:
    quote = Quote(symbol="VNM", source="VCI")
    df = quote.history(start="2024-01-01", end="2024-01-08", interval="1D")
    print(df.head())
except Exception as e:
    print(f"Error fetching data: {e}")
