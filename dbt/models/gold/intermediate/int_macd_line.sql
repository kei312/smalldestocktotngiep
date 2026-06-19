{{ config(materialized='table') }}

SELECT
    e12.symbol,
    e12.trade_date,
    e12.ema_12 - e26.ema_26                     AS macd_line
FROM {{ ref('int_ema12') }} e12
JOIN {{ ref('int_ema26') }} e26
    ON e12.symbol = e26.symbol AND e12.trade_date = e26.trade_date
WHERE e12.ema_12 IS NOT NULL
  AND e26.ema_26 IS NOT NULL
