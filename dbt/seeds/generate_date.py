import pandas as pd
import holidays

vn_holidays = holidays.VN(years=range(2020, 2027))
dates = pd.date_range(start='2020-01-01', end='2026-12-31')

is_trading_day = []
for date in dates:
    # Không phải T7, CN và không nằm trong danh sách ngày nghỉ lễ
    if date.dayofweek < 5 and date not in vn_holidays:
        is_trading_day.append(True)
    else:
        is_trading_day.append(False)

df = pd.DataFrame({
    'date': dates.strftime('%Y-%m-%d'),
    'day_of_week': dates.day_name(),
    'month': dates.month,
    'quarter': dates.quarter,
    'year': dates.year,
    'is_trading_day': is_trading_day
})
df.to_csv('seeds/dim_date.csv', index=False)
print('dim_date.csv created')

