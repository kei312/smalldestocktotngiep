{{ config(
    materialized='table',
    unique_key=['symbol', 'trade_date']
) }}

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
