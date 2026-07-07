import pandas as pd
from datetime import datetime

# Generate dates from 2020-01-01 to 2050-12-31
date_range = pd.date_range(start='2020-01-01', end='2050-12-31')
df = pd.DataFrame({'date': date_range})

df['day_of_week'] = df['date'].dt.day_name()
df['day'] = df['date'].dt.day
df['month'] = df['date'].dt.month
df['quarter'] = df['date'].dt.quarter
df['year'] = df['date'].dt.year

# Assume trading day is just Monday-Friday for simplicity, matching common seed generation
df['is_trading_day'] = df['day_of_week'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])

df['date'] = df['date'].dt.strftime('%Y-%m-%d')

df.to_csv('dbt/seeds/dim_date.csv', index=False)
print("dim_date.csv generated up to 2050-12-31 successfully.")
