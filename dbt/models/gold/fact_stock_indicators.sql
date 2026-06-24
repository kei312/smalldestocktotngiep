{{ config(
    materialized          = 'incremental',
    unique_key            = ['symbol', 'trade_date'],
    incremental_strategy  = 'delete+insert'
) }}
{#
    fact_stock_indicators — Gold layer final fact table
    Combines: MA5, MA20, Bollinger Bands (window functions)
              RSI14, MACD line/signal/histogram (from intermediate models)

    Incremental strategy: delete+insert with 120-day lookback
    to ensure warm-up coverage for MACD Signal (needs 34 trading days).

    Optimization: single scan of fact_stock_price for all window-based
    indicators (MA + Bollinger) in one CTE pass.
#}

WITH
source AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS rn
    FROM {{ ref('fact_stock_price') }}

    {% if is_incremental() %}
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '120 days' FROM {{ this }})
    {% endif %}
),

-- MA5, MA20, Bollinger Bands — all window functions in a single pass
ma_bb AS (
    SELECT
        symbol,
        trade_date,
        close_price,

        -- MA5: NULL if fewer than 5 rows
        CASE
            WHEN rn >= 5
            THEN AVG(close_price) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS ma5,

        -- MA20: NULL if fewer than 20 rows
        CASE
            WHEN rn >= 20
            THEN AVG(close_price) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS ma20,

        -- Bollinger upper = MA20 + 2 * STDDEV_POP(20)
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

        -- Bollinger lower = MA20 - 2 * STDDEV_POP(20)
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

    {% if is_incremental() %}
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '120 days' FROM {{ this }})
    {% endif %}
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

    {% if is_incremental() %}
    WHERE ml.trade_date > (SELECT MAX(trade_date) - INTERVAL '120 days' FROM {{ this }})
    {% endif %}
)

-- Final JOIN: LEFT JOIN preserves all rows from ma_bb even during warm-up
SELECT
    mb.symbol,
    mb.trade_date,
    mb.close_price,
    mb.ma5,
    mb.ma20,
    mb.bb_upper,
    mb.bb_lower,
    r.rsi_14,
    mf.macd_line,
    mf.macd_signal,
    mf.macd_histogram
FROM ma_bb mb
LEFT JOIN rsi       r  ON mb.symbol = r.symbol  AND mb.trade_date = r.trade_date
LEFT JOIN macd_full mf ON mb.symbol = mf.symbol AND mb.trade_date = mf.trade_date
