# Data Contracts

This document expands on Section 5 of PROJECT_RULES.md to provide detailed data contracts between layers in the Vietnam Stock Market Data Engineering Pipeline.

## 1. Bronze Layer Contract (Raw Ingestion)

**Source:** External APIs (vnstock / MockProvider) via Python Ingestion scripts
**Destination:** PostgreSQL `bronze` schema (`bronze_prices`, `bronze_index`, `bronze_vn30_components`)

### `bronze_prices`
- **code** (VARCHAR): Stock symbol (e.g., 'FPT', 'VCB'). Part of PK (partitioned).
- **date** (DATE): Trading date. Part of PK (partitioned).
- **open** (NUMERIC(18,4)): Open price. Nullable.
- **high** (NUMERIC(18,4)): High price. Nullable.
- **low** (NUMERIC(18,4)): Low price. Nullable.
- **close** (NUMERIC(18,4)): Close price. Nullable.
- **volume** (BIGINT): Trading volume. Nullable.
- **raw_json** (JSONB): Raw response from the provider.
- **source** (TEXT): Data source identifier (e.g., 'vnstock').
- **ingested_at** (TIMESTAMPTZ): Timestamp of ingestion.

**Constraints:** `(code, date)` must be unique (enforced per partition).

## 2. Silver Layer Contract (Cleansing & Validation)

**Source:** PostgreSQL `bronze` schema
**Destination:** PostgreSQL `public_silver` schema (`silver_prices`, `silver_index`) via dbt

### `silver_prices`
- **symbol** (VARCHAR): Cleaned stock symbol.
- **trade_date** (DATE): Trading date.
- **open_price**, **high_price**, **low_price**, **close_price** (DOUBLE PRECISION): Price columns.
- **volume** (BIGINT): Trading volume.
- **source** (TEXT): Data source identifier.
- **is_valid** (BOOLEAN): Data quality flag. `FALSE` if `close_price <= 0`, `high_price < low_price`, or `volume < 0`.
- **dq_flag** (TEXT): Reason for invalidity (e.g., 'invalid_close', 'high_lt_low', 'negative_volume', 'NULL').
- **loaded_at** (TIMESTAMPTZ): Timestamp of dbt run.

**Expectations:** The dbt model expects all Bronze columns. If any column is missing, the dbt pipeline should fail clearly.

## 3. Gold Layer Contract (Business Logic & Aggregation)

**Source:** PostgreSQL `public_silver` schema
**Destination:** PostgreSQL `public_gold` schema (Fact & Dimension tables) via dbt

### `fact_stock_price`
- Filtered subset of `silver_prices` where `is_valid = TRUE`.
- **Primary Key:** `(symbol, trade_date)`
- **symbol** (VARCHAR): Cleaned stock symbol.
- **trade_date** (DATE): Trading date.
- **row_num** (BIGINT): Sequential row number per symbol ordered by trade_date (B-Tree indexed).
- **open_price**, **high_price**, **low_price**, **close_price** (DOUBLE PRECISION): Price columns.
- **volume** (BIGINT): Trading volume.
- **source** (TEXT): Data source identifier.
- **silver_loaded_at** (TIMESTAMPTZ): Timestamp of Silver dbt run.
- **gold_loaded_at** (TIMESTAMPTZ): Timestamp of Gold dbt run.
- **gain** (DOUBLE PRECISION): Daily close price increase (for RSI calculation).
- **loss** (DOUBLE PRECISION): Daily close price decrease (for RSI calculation).

### `fact_stock_indicators`
- Calculated from `fact_stock_price`.
- **Primary Key:** `(symbol, trade_date)`
- Incremental model.
- Includes indicators: MA5, MA20, RSI14, MACD, MACD Signal, MACD Histogram, Bollinger Bands (Upper, Lower, Middle).

### `fact_market_summary`
- **Primary Key:** `(trade_date)`
- Aggregates daily market data: number of gainers, losers, unchanged stocks, total volume, and VN-Index / VN30 index values.

### `dim_stock`
- **Primary Key:** `(symbol)`
- Stock attributes: exchange, industry, company name.

### `dim_date`
- **Primary Key:** `(date_key)`
- Calendar dimensions and `is_trading_day` flag.

## 4. Pipeline Data Integrity & Idempotency
- **Idempotency Guarantee:** The pipeline must be strictly idempotent. Running the DAG or backfill script multiple times for the same date range must NOT duplicate data.
- **Bronze Layer Upsert:** Bronze ingestion enforces this via `INSERT ... ON CONFLICT (code, date) DO UPDATE`. It guarantees row-level uniqueness per stock symbol and trading date.
- **Data Preservation Rule:** The Bronze PostgreSQL database should NEVER be dropped or truncated before a daily DAG run. Bronze acts as the foundational raw history (acting like a data lake). Dropping it will destroy the historical window (e.g., 100+ days) required by Silver/Gold dbt layers to calculate rolling window functions like EMA26 and MACD. dbt incremental models are explicitly configured to handle new partial data without needing database wipes.
