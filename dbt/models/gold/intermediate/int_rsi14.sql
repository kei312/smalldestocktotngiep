{{ config(materialized='table') }}
{{ calculate_rsi(14) }}
