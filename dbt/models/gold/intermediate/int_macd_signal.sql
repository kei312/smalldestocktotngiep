{{ config(materialized='table') }}

WITH signal AS (
    {{ calculate_ema(9, source_relation=ref('int_macd_line'), value_column='macd_line') }}
)

SELECT
    symbol,
    trade_date,
    ema_9   AS macd_signal
FROM signal
