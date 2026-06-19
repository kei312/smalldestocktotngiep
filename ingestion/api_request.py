from vnstock import Quote, Company
import requests
# # ==================== THÔNG TIN CÔNG TY ====================
# company = Company(symbol="FPT", source="KBS")   # hoặc 'VCI'
# print(company.overview())

# # ==================== GIÁ LỊCH SỬ ====================
# quote = Quote(symbol="FPT", source="KBS")

# # Lấy dữ liệu lịch sử
# df_history = quote.history(
#     start="2026-06-01", 
#     end="2026-06-15", 
#     interval="1D"          # 1D, 1H, 15m, ...
# )
# print(df_history.tail())

# # ==================== INTRADAY / REALTIME ====================
# intraday_df = quote.intraday(symbol="FPT", page_size=1000, show_log=False)
# print(intraday_df)

def mock_fetch_data():
    """
    Return a mock list of dictionaries resembling vnstock's output for testing.
    """
    data = [
        {"time": "2026-06-09 07:00:00", "open": 73.4, "high": 74.2, "low": 72.6, "close": 73.7, "volume": 7772300},
        {"time": "2026-06-10 07:00:00", "open": 73.5, "high": 74.5, "low": 73.5, "close": 74.2, "volume": 4736000},
        {"time": "2026-06-11 07:00:00", "open": 73.9, "high": 73.9, "low": 73.1, "close": 73.1, "volume": 3507300},
        {"time": "2026-06-12 07:00:00", "open": 73.6, "high": 74.3, "low": 73.5, "close": 73.5, "volume": 5688200},
        {"time": "2026-06-15 07:00:00", "open": 74.4, "high": 75.2, "low": 73.5, "close": 73.6, "volume": 6422700},
    ]
    return data

# print(mock_fetch_data())