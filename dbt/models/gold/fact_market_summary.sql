{{ config(
    materialized='table'
) }}

WITH stock_stats AS (
    SELECT
        trade_date,
        SUM(CASE WHEN gain > 0 THEN 1 ELSE 0 END) AS gainers,
        SUM(CASE WHEN loss > 0 THEN 1 ELSE 0 END) AS losers,
        SUM(CASE WHEN gain = 0 AND loss = 0 THEN 1 ELSE 0 END) AS unchanged,
        SUM(volume) AS total_volume,
        COUNT(symbol) AS total_symbols
    FROM {{ ref('fact_stock_price') }}
    WHERE row_num > 1
    GROUP BY trade_date
),
vnindex AS (
    SELECT trade_date, close_price AS vnindex_close
    FROM {{ ref('silver_index') }}
    WHERE index_code = 'VNINDEX' AND is_valid = TRUE
),
vn30 AS (
    SELECT trade_date, close_price AS vn30_close
    FROM {{ ref('silver_index') }}
    WHERE index_code = 'VN30' AND is_valid = TRUE
)

SELECT
    s.trade_date,
    s.gainers,
    s.losers,
    s.unchanged,
    s.total_symbols,
    s.total_volume,
    vi.vnindex_close,
    v30.vn30_close
FROM stock_stats s
LEFT JOIN vnindex vi ON s.trade_date = vi.trade_date
LEFT JOIN vn30 v30 ON s.trade_date = v30.trade_date
