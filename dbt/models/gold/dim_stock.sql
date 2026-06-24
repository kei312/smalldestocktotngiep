{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT DISTINCT
        symbol,
        source AS data_source
    FROM {{ ref('silver_prices') }}
    WHERE is_valid = TRUE
),
vn30_list AS (
    -- Danh mục VN30 thực tế được nạp tự động qua task ingestion
    SELECT DISTINCT code AS symbol FROM {{ source('bronze', 'bronze_vn30_components') }}
),
grouped_source AS (
    SELECT
        symbol,
        MAX(data_source) AS exchange,
        'HOSE' AS default_exchange
    FROM source
    GROUP BY symbol
)

SELECT
    g.symbol,
    g.exchange,
    g.default_exchange,
    CASE WHEN v.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS is_vn30
FROM grouped_source g
LEFT JOIN vn30_list v ON g.symbol = v.symbol

