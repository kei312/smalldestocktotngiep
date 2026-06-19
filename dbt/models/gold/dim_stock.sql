{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT DISTINCT
        symbol,
        source AS data_source
    FROM {{ ref('silver_prices') }}
    WHERE is_valid = TRUE
)

SELECT
    symbol,
    MAX(data_source) AS exchange,
    'HOSE' AS default_exchange -- fallback info
FROM source
GROUP BY symbol
