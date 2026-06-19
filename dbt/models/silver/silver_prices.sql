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
flagged AS (
    SELECT
        *,
        CASE
            WHEN close_price <= 0 THEN 'invalid_close_price'
            WHEN high_price < low_price THEN 'high_less_than_low'
            WHEN open_price <= 0 OR high_price <= 0 OR low_price <= 0 THEN 'invalid_ohlc'
            WHEN volume < 0 THEN 'negative_volume'
            ELSE 'ok'
        END AS dq_flag,
        CURRENT_TIMESTAMP AS loaded_at
    FROM casted
)

SELECT
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    source,
    dq_flag,
    (dq_flag = 'ok') AS is_valid,
    loaded_at,
    ingested_at
FROM flagged
