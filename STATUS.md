# Project Status

## Current Phase: Phase 2 (dbt Transformations) - Gold Layer Completed

### Completed Modules
- **Ingestion**: VNStock API wrapper, PostgreSQL ingestion (Bronze layer). Script `backfill.py` is actively fetching 5 years of historical data.
- **dbt Silver**: Cleansing rules, source schema tests, index creation.
- **dbt Gold**: 
  - Base model: `fact_stock_price` (valid prices only).
  - Intermediate models: `int_rsi14`, `int_ema12`, `int_ema26`, `int_macd_line`, `int_macd_signal` (calculated using strict Wilder and EMA recursion logic).
  - Final incremental fact table: `fact_stock_indicators` (using idempotent `delete+insert` with a 60-day lookback).
- **Validation**: 
  - Incremental `delete+insert` mechanism verified against data duplication.
  - Schema tests (RSI ranges, BB bands logic) implemented at the model level to comply with dbt 1.10.x.
  - MACD calculations cross-checked perfectly against standard Python implementation (0.0000% error on HPG historical dataset).

### Next Steps (Phase 3)
- **Airflow Orchestration**: Wrap the ingestion scripts and dbt runs into daily automated Airflow DAGs.
- **Power BI Visualization**: Connect Power BI to the PostgreSQL `public_gold.fact_stock_indicators` table and build the financial dashboard.
