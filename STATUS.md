# Project Status

## Current Phase: Phase 2 (dbt Transformations) - Gold Layer Completed

### Completed Modules
- **Ingestion**: VNStock API wrapper, PostgreSQL ingestion (Bronze layer). Script `backfill.py` is actively fetching 5 years of historical data.
- **dbt Silver**: Cleansing rules, source schema tests, index creation.
- **dbt Gold**: 
  - Base model: `fact_stock_price` (valid prices only).
  - Intermediate models: `int_rsi14`, `int_ema12`, `int_ema26`, `int_macd_line`, `int_macd_signal` (calculated using strict Wilder and EMA recursion logic).
  - Final incremental fact table: `fact_stock_indicators` (using idempotent `delete+insert` with a 60-day lookback).
  - Market models: `dim_stock`, `fact_market_summary`, `dim_date`
- **Validation**: 
  - Incremental `delete+insert` mechanism verified against data duplication.
  - Schema tests (RSI ranges, BB bands logic) implemented at the model level to comply with dbt 1.10.x.
  - MACD calculations cross-checked perfectly against standard Python implementation (0.0000% error on HPG historical dataset).
- **Airflow Orchestration (Phase 3)**:
  - `docker-compose.yml` updated to mount `./` correctly and install `dbt-postgres==1.10.0` in the Airflow container.
  - `dag_daily.py` pipeline successfully orchestrated using `BashOperator` to execute ingestion and dbt incrementally.
  - Permission issues (`chmod`) and `PYTHONPATH` correctly resolved to run dbt directly via Airflow CLI.

### Next Steps
- **Power BI Visualization**: Connect Power BI to the PostgreSQL `public_gold` schema and build the financial dashboard.
