import pandas as pd
dates = pd.date_range(start='2020-01-01', end='2026-12-31')
df = pd.DataFrame({
    'date': dates.strftime('%Y-%m-%d'),
    'day_of_week': dates.day_name(),
    'month': dates.month,
    'quarter': dates.quarter,
    'year': dates.year,
    'is_trading_day': dates.dayofweek < 5
})
df.to_csv('seeds/dim_date.csv', index=False)
print('dim_date.csv created')
