{{ config(
    materialized = 'table',
    unique_key   = ['symbol', 'trade_date']
) }}
{#
    fact_stock_indicators — Gold layer final fact table
    Combines: MA50, MA200, Bollinger Bands (window functions)
              RSI14, MACD line/signal/histogram (from intermediate models)

    Materialization: table — full rebuild every dbt run.
    Ensures MA200 is always correct for all historical rows.
    row_num is taken from fact_stock_price (global sequence) to
    correctly guard MA/BB warm-up thresholds.
#}

WITH
source AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        row_num AS rn    -- global sequence from fact_stock_price (not recomputed here)
    FROM {{ ref('fact_stock_price') }}
),

-- MA5, MA20, Bollinger Bands — all window functions in a single pass
ma_bb AS (
    SELECT
        symbol,
        trade_date,
        close_price,

        -- MA50: NULL if fewer than 50 rows
        CASE
            WHEN rn >= 50
            THEN AVG(close_price) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS ma50,

        -- MA200: NULL if fewer than 200 rows
        CASE
            WHEN rn >= 200
            THEN AVG(close_price) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS ma200,

        -- Bollinger upper = BB(20) + 2 * STDDEV_POP(20)
        CASE
            WHEN rn >= 20
            THEN AVG(close_price) OVER (
                     PARTITION BY symbol ORDER BY trade_date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                 )
                 + 2.0 * STDDEV_POP(close_price) OVER (
                     PARTITION BY symbol ORDER BY trade_date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                 )
            ELSE NULL
        END AS bb_upper,

        -- Bollinger lower = BB(20) - 2 * STDDEV_POP(20)
        CASE
            WHEN rn >= 20
            THEN AVG(close_price) OVER (
                     PARTITION BY symbol ORDER BY trade_date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                 )
                 - 2.0 * STDDEV_POP(close_price) OVER (
                     PARTITION BY symbol ORDER BY trade_date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                 )
            ELSE NULL
        END AS bb_lower

    FROM source
),

-- RSI from intermediate model (avoids nested WITH RECURSIVE)
rsi AS (
    SELECT symbol, trade_date, rsi_14
    FROM {{ ref('int_rsi14') }}
),

-- MACD line + signal from intermediate models (true EMA9, not SMA approximation)
macd_full AS (
    SELECT
        ml.symbol,
        ml.trade_date,
        ml.macd_line,
        ms.macd_signal,
        ml.macd_line - ms.macd_signal AS macd_histogram
    FROM {{ ref('int_macd_line') }} ml
    LEFT JOIN {{ ref('int_macd_signal') }} ms
        ON ml.symbol = ms.symbol AND ml.trade_date = ms.trade_date
)

-- Final JOIN: LEFT JOIN preserves all rows from ma_bb even during warm-up periods (MA50/MA200/BB)
SELECT
    mb.symbol,
    mb.trade_date,
    mb.close_price,
    mb.ma50,
    mb.ma200,
    mb.bb_upper,
    mb.bb_lower,
    r.rsi_14,
    mf.macd_line,
    mf.macd_signal,
    mf.macd_histogram
FROM ma_bb mb
LEFT JOIN rsi       r  ON mb.symbol = r.symbol  AND mb.trade_date = r.trade_date
LEFT JOIN macd_full mf ON mb.symbol = mf.symbol AND mb.trade_date = mf.trade_date
