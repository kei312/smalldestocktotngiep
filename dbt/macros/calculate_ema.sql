{% macro calculate_ema(period, source_relation=none, value_column='close_price') %}

{% if source_relation is none %}
    {% set source_relation = ref('fact_stock_price') %}
{% endif %}

WITH RECURSIVE
_ema_base AS (
    SELECT
        symbol,
        trade_date,
        {{ value_column }}                                                AS val,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date)        AS rn
    FROM {{ source_relation }}
),

-- SMA seed: average of first `period` rows, computed AT row number `period`
_ema_seed AS (
    SELECT
        symbol,
        {{ period }}            AS rn,
        AVG(val)                AS ema_val
    FROM _ema_base
    WHERE rn <= {{ period }}
    GROUP BY symbol
),

-- Recursive: each row = val * alpha + prev_ema * (1 - alpha)
-- Alpha = 2/(period+1)  — Standard EMA (NOT Wilder 1/period)
_ema_rec(symbol, rn, trade_date, ema_val) AS (

    -- Base case: seed at row = period
    SELECT b.symbol, b.rn, b.trade_date, s.ema_val
    FROM _ema_base b
    JOIN _ema_seed s ON b.symbol = s.symbol AND b.rn = s.rn

    UNION ALL

    -- Recursive step: alpha = 2/(period+1)
    SELECT
        b.symbol,
        b.rn,
        b.trade_date,
        b.val * (2.0 / ({{ period }} + 1.0))
            + e.ema_val * (1.0 - 2.0 / ({{ period }} + 1.0))
    FROM _ema_base b
    JOIN _ema_rec   e ON b.symbol = e.symbol AND b.rn = e.rn + 1
)

SELECT symbol, trade_date, ema_val AS ema_{{ period }}
FROM _ema_rec

{% endmacro %}
