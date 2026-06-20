# Data Contracts

This document expands on Section 5 of PROJECT_RULES.md to provide detailed data contracts between layers in the Vietnam Stock Market Data Engineering Pipeline.

## 1. Bronze Layer Contract (Raw Ingestion)

**Source:** External APIs (vnstock / MockProvider) via Python Ingestion scripts
**Destination:** PostgreSQL `bronze` schema (`bronze_stock_prices`, `bronze_market_index`)

### `bronze_stock_prices`
- **code** (VARCHAR): Stock symbol (e.g., 'FPT', 'VCB'). Part of PK.
- **date** (DATE): Trading date. Part of PK.
- **open** (NUMERIC(18,4)): Open price. Nullable.
- **high** (NUMERIC(18,4)): High price. Nullable.
- **low** (NUMERIC(18,4)): Low price. Nullable.
- **close** (NUMERIC(18,4)): Close price. Nullable.
- **volume** (BIGINT): Trading volume. Nullable.
- **raw_json** (JSONB): Raw response from the provider.
- **source** (TEXT): Data source identifier (e.g., 'vnstock').
- **ingested_at** (TIMESTAMPTZ): Timestamp of ingestion.

**Constraints:** `(code, date)` must be unique.

## 2. Silver Layer Contract (Cleansing & Validation)

**Source:** PostgreSQL `bronze` schema
**Destination:** PostgreSQL `silver` schema (`silver_stock_prices`) via dbt

### `silver_stock_prices`
- **symbol** (VARCHAR): Cleaned stock symbol.
- **trade_date** (DATE): Trading date.
- **open**, **high**, **low**, **close** (NUMERIC(18,4)): Price columns.
- **volume** (BIGINT): Trading volume.
- **source** (TEXT): Data source identifier.
- **is_valid** (BOOLEAN): Data quality flag. `FALSE` if `close <= 0`, `high < low`, or `volume < 0`.
- **dq_flag** (TEXT): Reason for invalidity (e.g., 'invalid_close', 'high_lt_low', 'negative_volume', 'NULL').
- **loaded_at** (TIMESTAMPTZ): Timestamp of dbt run.

**Expectations:** The dbt model expects all Bronze columns. If any column is missing, the dbt pipeline should fail clearly.

## 3. Gold Layer Contract (Business Logic & Aggregation)

**Source:** PostgreSQL `silver` schema
**Destination:** PostgreSQL `gold` schema (Fact & Dimension tables) via dbt

### `fact_stock_price`
- Filtered subset of `silver_stock_prices` where `is_valid = TRUE`.
- **Primary Key:** `(symbol, trade_date)`
- Contains clean OHLCV data ready for analysis.

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
