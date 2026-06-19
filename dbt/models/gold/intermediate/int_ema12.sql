{{ config(materialized='table') }}
{{ calculate_ema(12) }}
