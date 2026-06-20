# Vietnam Stock Market Data Engineering Pipeline

A robust, daily-batch data engineering pipeline that extracts Vietnam stock market data, loads it into a PostgreSQL data warehouse via Apache Airflow, transforms it with dbt, and visualizes it in Power BI.

## Architecture Highlights
- **Bronze Layer**: Raw JSON ingestion from providers.
- **Silver Layer**: Data cleansing, quality checks, and deduplication.
- **Gold Layer**: Aggregations, dimensional models, and complex technical indicators (MA, RSI, MACD, Bollinger Bands).
- **Orchestration**: Apache Airflow.
- **Transformation**: dbt (data build tool).
- **Visualization**: Power BI.

## Setup Guide

### 1. Prerequisites
- Docker and Docker Compose
- Python 3.12+ (for local ingestion scripting/testing if needed)
- `dbt-postgres`

### 2. Clone and Configure
Clone the repository and set up the environment variables:
```bash
cp .env.example .env
```
Edit `.env` to set your PostgreSQL credentials and provider settings (e.g., `PROVIDER=vnstock` or `PROVIDER=mock`).

### 3. Start Infrastructure
Launch the PostgreSQL data warehouse and Apache Airflow using Docker Compose:
```bash
docker compose up -d
```
Wait for the containers to fully start. You can check the status with `docker ps`.

### 4. Initialize the Data Warehouse
Run the initial schema creation scripts:
```bash
docker exec postgres-container psql -U airflow -d stock_db -f /path/to/sql/init_schema.sql
```
*(Path depends on your volume mounts; check `docker-compose.yml` for exact locations)*

### 5. Backfill Data (Optional)
Run the backfill DAG in Airflow or trigger the ingestion script manually to populate the Bronze layer with historical data.
```bash
python scripts/run_pipeline.py --mode backfill --start 2024-01-01 --end 2024-06-01
```

### 6. Run dbt Transformations
Execute dbt to build the Silver and Gold models:
```bash
docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt build --profiles-dir ."
```

### 7. Power BI Integration
For connecting Power BI to the `gold` schema and building the dashboard, please refer to the detailed guide:
[Power BI Quickstart](docs/POWERBI_QUICKSTART.md)

## Documentation & ADRs
- [Context & Scope](docs/CONTEXT.md)
- [Project Rules](docs/PROJECT_RULES.md)
- [Data Contracts](docs/DATA_CONTRACTS.md)
- [Architecture Decision Records (ADRs)](docs/ADR/)
