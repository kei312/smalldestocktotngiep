{% macro calculate_ema(period, source_relation=none, value_column='close_price') %}

{% if source_relation is none %}
    {% set source_relation = ref('fact_stock_price') %}
{% endif %}

WITH RECURSIVE
-- Seed SMA calculation using pre-calculated row_num (indexed)
_ema_seed AS (
    SELECT
        symbol,
        {{ period }}            AS rn,
        AVG({{ value_column }}) AS ema_val
    FROM {{ source_relation }}
    WHERE row_num <= {{ period }}
    GROUP BY symbol
),

-- Recursive: each row = val * alpha + prev_ema * (1 - alpha)
-- Alpha = 2/(period+1)  — Standard EMA (NOT Wilder 1/period)
_ema_rec(symbol, rn, trade_date, ema_val) AS (

    -- Base case: seed at row = period
    SELECT b.symbol, b.row_num, b.trade_date, s.ema_val
    FROM {{ source_relation }} b
    JOIN _ema_seed s ON b.symbol = s.symbol AND b.row_num = s.rn

    UNION ALL

    -- Recursive step: alpha = 2/(period+1)
    -- Joins with the physical index on (symbol, row_num)
    SELECT
        b.symbol,
        b.row_num,
        b.trade_date,
        b.{{ value_column }} * (2.0 / ({{ period }} + 1.0))
            + e.ema_val * (1.0 - 2.0 / ({{ period }} + 1.0))
    FROM {{ source_relation }} b
    JOIN _ema_rec   e ON b.symbol = e.symbol AND b.row_num = e.rn + 1
)

SELECT symbol, trade_date, ema_val AS ema_{{ period }}
FROM _ema_rec

{% endmacro %}
