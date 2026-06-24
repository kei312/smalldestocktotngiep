# Project Status

## Current Phase: Phase 4 (Documentation & Finalization) - Completed

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
- **Airflow Orchestration (Phase 3 - Completed)**:
  - `docker-compose.yml` updated to mount `./` correctly and install `dbt-postgres==1.10.0` in the Airflow container.
  - `dag_daily.py` pipeline successfully orchestrated using `BashOperator` to execute ingestion and dbt incrementally.
  - Permission issues (`chmod`) and `PYTHONPATH` correctly resolved to run dbt directly via Airflow CLI.
  - Fixed Jinja templating bug specific to Airflow 3 SDK where manual triggers fail to define `logical_date`, adopting the more universal `{{ ds }}` default.
  - DAG trigger verified successfully with `PROVIDER=mock` running E2E actual production runs twice.
  - Database row counts verified after both runs: Data idempotency and `delete+insert` lookback of 60 days are 100% verified.
- **Documentation (Phase 4 - Completed)**:
  - Created Data Contracts between Bronze, Silver, and Gold layers.
  - Documented Architecture Decision Records (ADR-001 to ADR-004) covering PostgreSQL, dbt 1.10, unified Provider architecture, and MACD EMA9 signal changes.
  - Created [Operational Guide & Run Scenarios](docs/RUN_SCENARIOS.md) detailing E2E run steps for Offline Demo, Production, Testing, and Troubleshooting.
  - Created [DAG Run Scenarios & Expected Outputs](docs/DAG_SCENARIOS.md) outlining Airflow task states and expected data changes.
  - Created [Detailed Code Flow & Execution Paths](docs/CODE_FLOW.md) mapping core Python, Provider retry/interval logic, and recursive dbt indicator runs.
  - Finalized `README.md` and updated `STATUS.md`.
- **VnstockProvider Throughput Optimization (Completed)**:
  - Transitioned from a single global RateLimiter lock to per-source RateLimiters for `vci` and `kbs`.
  - Parallel queries from multiple threads now execute concurrently, raising measured throughput from ~57 req/minute to **~141 req/minute** (a **2.5x throughput increase**).
  - Validation: Provider unit tests and E2E Airflow daily pipeline ran successfully without regressions.

### Next Steps
- **Demonstration & Verification**: Final E2E testing and Airflow UI verifications (Task 4.4).
- **Power BI Visualization**: Connect Power BI to the PostgreSQL `public_gold` schema and build the financial dashboard (as described in `docs/POWERBI_QUICKSTART.md`).
