{% macro calculate_rsi(period) %}

WITH RECURSIVE
_rsi_base AS (
    SELECT
        symbol,
        trade_date,
        close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date)  AS daily_change,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date)                     AS rn
    FROM {{ ref('fact_stock_price') }}
),

_rsi_gl AS (
    SELECT
        symbol,
        trade_date,
        rn,
        GREATEST(COALESCE(daily_change, 0), 0)       AS gain,
        ABS(LEAST(COALESCE(daily_change, 0),    0))   AS loss
    FROM _rsi_base
),

-- SMA seed at row (period+1): uses period changes (rows 2..period+1)
-- rn=1 has no change (LAG is NULL), so changes start from rn=2
_rsi_seed AS (
    SELECT
        symbol,
        {{ period }} + 1          AS rn,
        AVG(gain)                 AS avg_gain,
        AVG(loss)                 AS avg_loss
    FROM _rsi_gl
    WHERE rn BETWEEN 2 AND {{ period }} + 1
    GROUP BY symbol
),

-- Recursive: Wilder smoothing = (prev * (period-1) + current) / period
-- Alpha = 1/period  (NOT 2/(period+1) — that's standard EMA)
_rsi_rec(symbol, rn, trade_date, avg_gain, avg_loss) AS (

    -- Base case: seed at row = period+1 (row 15 when period=14)
    SELECT gl.symbol, gl.rn, gl.trade_date, s.avg_gain, s.avg_loss
    FROM _rsi_gl gl
    JOIN _rsi_seed s ON gl.symbol = s.symbol AND gl.rn = s.rn

    UNION ALL

    -- Recursive step: Wilder — α = 1/period
    SELECT
        gl.symbol,
        gl.rn,
        gl.trade_date,
        (r.avg_gain * ({{ period }} - 1) + gl.gain) / {{ period }}::DOUBLE PRECISION,
        (r.avg_loss * ({{ period }} - 1) + gl.loss) / {{ period }}::DOUBLE PRECISION
    FROM _rsi_gl gl
    JOIN _rsi_rec r ON gl.symbol = r.symbol AND gl.rn = r.rn + 1
)

SELECT
    symbol,
    trade_date,
    CASE
        WHEN avg_loss  = 0 AND avg_gain  = 0 THEN NULL
        WHEN avg_loss  = 0                    THEN 100.0
        ELSE ROUND((100.0 - (100.0 / (1.0 + avg_gain / avg_loss)))::NUMERIC, 4)
    END AS rsi_{{ period }}
FROM _rsi_rec

{% endmacro %}
