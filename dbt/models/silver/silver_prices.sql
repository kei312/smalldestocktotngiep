{{ config(
    materialized='table',
    unique_key=['symbol', 'trade_date']
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_prices') }}
),
casted AS (
    SELECT
        code AS symbol,
        date AS trade_date,
        CAST(open AS DOUBLE PRECISION) AS open_price,
        CAST(high AS DOUBLE PRECISION) AS high_price,
        CAST(low AS DOUBLE PRECISION) AS low_price,
        CAST(close AS DOUBLE PRECISION) AS close_price,
        CAST(volume AS BIGINT) AS volume,
        source,
        ingested_at
    FROM source
),
index_prices AS (
    SELECT
        date AS trade_date,
        CAST(close AS DOUBLE PRECISION) AS index_close
    FROM {{ source('bronze', 'bronze_index') }}
    WHERE code = 'VNINDEX'
),
index_with_lag AS (
    SELECT
        trade_date,
        index_close,
        LAG(index_close) OVER (ORDER BY trade_date) AS prev_index_close
    FROM index_prices
),
index_change AS (
    SELECT
        trade_date,
        CASE
            WHEN prev_index_close IS NOT NULL AND prev_index_close > 0
            THEN ABS(index_close - prev_index_close) / prev_index_close
            ELSE 0
        END AS index_change_rate
    FROM index_with_lag
),
prices_with_lag AS (
    SELECT
        c.*,
        LAG(c.close_price) OVER (PARTITION BY c.symbol ORDER BY c.trade_date) AS prev_close_price
    FROM casted c
),
flagged AS (
    SELECT
        p.*,
        CASE
            WHEN p.close_price <= 0 THEN 'invalid_close_price'
            WHEN p.high_price < p.low_price THEN 'high_less_than_low'
            WHEN p.open_price <= 0 OR p.high_price <= 0 OR p.low_price <= 0 THEN 'invalid_ohlc'
            WHEN p.volume < 0 THEN 'negative_volume'
            WHEN p.prev_close_price IS NOT NULL AND p.prev_close_price > 0
                 AND ABS(p.close_price - p.prev_close_price) / p.prev_close_price > 0.15
                 AND COALESCE(i.index_change_rate, 0) < 0.03 THEN 'abnormal_price_gap'
            ELSE 'ok'
        END AS dq_flag,
        CURRENT_TIMESTAMP AS loaded_at
    FROM prices_with_lag p
    LEFT JOIN index_change i ON p.trade_date = i.trade_date
)

SELECT
    symbol,
    trade_date,
    CASE WHEN dq_flag = 'abnormal_price_gap' THEN prev_close_price ELSE open_price END AS open_price,
    CASE WHEN dq_flag = 'abnormal_price_gap' THEN prev_close_price ELSE high_price END AS high_price,
    CASE WHEN dq_flag = 'abnormal_price_gap' THEN prev_close_price ELSE low_price END AS low_price,
    CASE WHEN dq_flag = 'abnormal_price_gap' THEN prev_close_price ELSE close_price END AS close_price,
    CASE WHEN dq_flag = 'abnormal_price_gap' THEN 0 ELSE volume END AS volume,
    source,
    dq_flag,
    (dq_flag IN ('ok', 'abnormal_price_gap')) AS is_valid,
    loaded_at,
    ingested_at
FROM flagged
