CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.bronze_prices (
    code VARCHAR(20),
    date DATE,
    open NUMERIC(18,4),
    high NUMERIC(18,4),
    low NUMERIC(18,4),
    close NUMERIC(18,4),
    volume BIGINT,
    raw_json JSONB,
    source TEXT,
    ingested_at TIMESTAMPTZ,
    PRIMARY KEY (code, date)
) PARTITION BY RANGE (date);

CREATE TABLE bronze.bronze_prices_2020 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE bronze.bronze_prices_2021 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE bronze.bronze_prices_2022 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE bronze.bronze_prices_2023 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE bronze.bronze_prices_2024 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE bronze.bronze_prices_2025 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE bronze.bronze_prices_2026 PARTITION OF bronze.bronze_prices FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS bronze.bronze_index (
    code VARCHAR(20),
    date DATE,
    open NUMERIC(18,4),
    high NUMERIC(18,4),
    low NUMERIC(18,4),
    close NUMERIC(18,4),
    volume BIGINT,
    raw_json JSONB,
    source TEXT,
    ingested_at TIMESTAMPTZ,
    PRIMARY KEY (code, date)
) PARTITION BY RANGE (date);

CREATE TABLE bronze.bronze_index_2020 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE bronze.bronze_index_2021 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE bronze.bronze_index_2022 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE bronze.bronze_index_2023 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE bronze.bronze_index_2024 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE bronze.bronze_index_2025 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE bronze.bronze_index_2026 PARTITION OF bronze.bronze_index FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS bronze.bronze_vn30_components (
    code VARCHAR(20) PRIMARY KEY,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

