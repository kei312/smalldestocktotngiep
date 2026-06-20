# ADR-001: Use PostgreSQL for Data Warehouse

## Status
Accepted

## Context
The project needs a robust database to act as the central data warehouse, handling layers of data from Bronze (raw JSON and semi-structured data) to Gold (aggregations and calculated indicators). We require strong SQL capabilities, JSON support for raw ingestion, and compatibility with dbt and Airflow.

## Decision
We decided to use PostgreSQL 17 as the core data warehouse.

## Consequences
- **Positive:** Excellent support for JSONB (crucial for our Bronze layer's `raw_json`), reliable performance, strong ecosystem compatibility (dbt-postgres, Airflow), and easy to set up locally via Docker.
- **Negative:** Not a distributed columnar data warehouse (like Snowflake or BigQuery), meaning analytical queries over massive datasets might eventually require indexing strategies, partitioning, or migration to a true OLAP database if scale grows significantly. However, for the scope of the Vietnam stock market (~1600 symbols), PostgreSQL is more than adequate.
