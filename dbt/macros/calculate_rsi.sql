{% macro calculate_rsi(period) %}

WITH RECURSIVE
-- Seed SMA calculation using pre-calculated row_num, gain, and loss (indexed)
_rsi_seed AS (
    SELECT
        symbol,
        {{ period }} + 1          AS rn,
        AVG(gain)                 AS avg_gain,
        AVG(loss)                 AS avg_loss
    FROM {{ ref('fact_stock_price') }}
    WHERE row_num BETWEEN 2 AND {{ period }} + 1
    GROUP BY symbol
),

-- Recursive: Wilder smoothing = (prev * (period-1) + current) / period
-- Alpha = 1/period  (NOT 2/(period+1) — that's standard EMA)
_rsi_rec(symbol, rn, trade_date, avg_gain, avg_loss) AS (

    -- Base case: seed at row = period+1 (row 15 when period=14)
    SELECT b.symbol, b.row_num, b.trade_date, s.avg_gain, s.avg_loss
    FROM {{ ref('fact_stock_price') }} b
    JOIN _rsi_seed s ON b.symbol = s.symbol AND b.row_num = s.rn

    UNION ALL

    -- Recursive step: Wilder — α = 1/period
    -- Joins with the physical index on (symbol, row_num)
    SELECT
        b.symbol,
        b.row_num,
        b.trade_date,
        (r.avg_gain * ({{ period }} - 1) + b.gain) / {{ period }}::DOUBLE PRECISION,
        (r.avg_loss * ({{ period }} - 1) + b.loss) / {{ period }}::DOUBLE PRECISION
    FROM {{ ref('fact_stock_price') }} b
    JOIN _rsi_rec r ON b.symbol = r.symbol AND b.row_num = r.rn + 1
)

SELECT
    symbol,
    trade_date,
    CASE
        WHEN avg_loss = 0 AND avg_gain = 0 THEN NULL
        WHEN avg_loss = 0 THEN 100.0
        ELSE ROUND((100.0 - (100.0 / (1.0 + avg_gain / NULLIF(avg_loss, 0))))::NUMERIC, 4)
    END AS rsi_{{ period }}
FROM _rsi_rec

{% endmacro %}
