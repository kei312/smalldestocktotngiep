{{ config(
    materialized='table',
    unique_key=['symbol', 'trade_date'],
    indexes=[
      {'columns': ['symbol', 'trade_date'], 'unique': True},
      {'columns': ['symbol', 'row_num'], 'unique': True}
    ]
) }}

WITH base AS (
    SELECT
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        source,
        loaded_at AS silver_loaded_at,
        CURRENT_TIMESTAMP AS gold_loaded_at
    FROM {{ ref('silver_prices') }}
    WHERE is_valid = TRUE
),
calculated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS row_num,
        close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS daily_change
    FROM base
)
SELECT
    symbol,
    trade_date,
    row_num,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    source,
    silver_loaded_at,
    gold_loaded_at,
    GREATEST(COALESCE(daily_change, 0), 0) AS gain,
    ABS(LEAST(COALESCE(daily_change, 0), 0)) AS loss
FROM calculated
