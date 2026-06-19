# PROJECT CONTEXT

> **AI:** Đọc file này khi: session mới, câu hỏi về kiến trúc/scope, không chắc về stack.
> Không cần đọc lại nếu đã đọc trong cùng session.

## Dự án

Vietnam Stock Market Data Engineering Pipeline — đồ án tốt nghiệp.

## Stack (đã chốt, không thay đổi)

| Thành phần | Version | Ghi chú |
|---|---|---|
| Python | 3.12.x | Đã có trên WSL (3.12.3) |
| PostgreSQL | 17.x | JSONB, partition — Docker image `postgres:17` |
| Apache Airflow | 3.2.x | Docker, LocalExecutor |
| dbt-core | 1.10.x (pin `1.10.19`) | Bản ổn định, nhiều bản vá  |
| dbt-postgres | 1.10.0 | Khớp release track với dbt-core 1.10.x |
| vnstock | 4.x | Unified UI, fallback VCI/TCBS/MSN |
| Power BI | Desktop | DirectQuery hoặc Import từ Gold |

## Kiến trúc

```
Provider Layer (VnstockProvider / MockProvider)
  → Ingestion Layer (Python, retry, idempotent UPSERT)
    → Bronze (PostgreSQL, raw_json JSONB, PK(code,date), partition/năm)
      → Silver (dbt: clean, cast, is_valid, dq_flag)
        → Gold (dbt: star schema — facts + dims)
          → Power BI (4 dashboards, có Plan B HTML backup nếu cần — xem docs/POWERBI_QUICKSTART.md)
Airflow điều phối toàn bộ.
```

## Scope

- **Core:** Daily OHLCV (VN30 pilot → full), VNINDEX/VN30, MA5/MA20/RSI14/MACD/Bollinger, Airflow, Power BI
- **Mở rộng:** Fundamentals (PE/PB/ROE/ROA/EPS), full universe
- **Bỏ:** Intraday, streaming, trading

## Module layout

```
deproject/
├── providers/          # base.py, vnstock_provider.py, mock_provider.py, registry.py
├── ingestion/          # config.py, utils.py, fetch_prices.py, fetch_index.py, backfill.py, fetch_fundamentals.py
├── sql/                # init_schema.sql
├── dbt/
│   ├── macros/         # calculate_rsi.sql, calculate_ema.sql (generalized — xem SKILL_sql_indicators.md)
│   ├── models/silver/  # silver_prices.sql, silver_index.sql, schema.yml
│   ├── models/gold/
│   │   ├── intermediate/   # int_ema12, int_ema26, int_rsi14, int_macd_line, int_macd_signal (materialized='table')
│   │   ├── fact_stock_price.sql
│   │   ├── fact_stock_indicators.sql   # incremental, JOIN các intermediate ở trên
│   │   ├── fact_market_summary.sql
│   │   ├── dim_stock.sql
│   │   └── schema.yml
│   └── seeds/           # dim_date.csv
├── dags/                # dag_daily.py, dag_backfill.py
├── tests/               # fixtures/, test_providers.py, test_ingestion.py
├── scripts/             # verify_macd_g03.py
└── docs/                # CONTEXT.md (file này), PROJECT_RULES.md,
                          # SKILL_sql_indicators.md, SKILL_dbt_incremental.md,
                          # TEST_REPORTS.md, POWERBI_QUICKSTART.md
```

## Quyết định kiến trúc quan trọng

1. **1 VnstockProvider thay 4 provider viết tay** — vnstock 4.x tự fallback, không trùng lặp
2. **VNDirect loại khỏi production** — sự cố 3/2024, bị nhắc nhở 2025
3. **MockProvider cho test/CI** — chứng minh provider-agnostic, demo offline
4. **dbt-core 1.10.x** — bản ổn định lâu nhất hiện có (pin 1.10.19)
5. **Airflow 3.2.x** — kiến trúc khác 2.x, dùng docker-compose chính thức
6. **Pilot VN30 trước** — validate pipeline trước khi mở rộng
7. **Star schema ở Gold** — dim_stock + dim_date cho Power BI time-intelligence
8. **MACD Signal = EMA9 thật (không phải SMA xấp xỉ)** — SMA9 cho sai số 2–8% vs EMA9, vượt ngưỡng G-03 (<0.5%). Dùng `int_macd_signal.sql` qua macro `calculate_ema(9, ...)` đã generalize.
9. **Power BI giữ nguyên là deliverable chính** — thêm `POWERBI_QUICKSTART.md` hướng dẫn từng bước theo tên bảng Gold. Plan B HTML/Plotly nếu hết giờ.

## Idempotency contract

- Mọi fetch: `ON CONFLICT (code, date) DO UPDATE`
- Chạy 2 lần → không tăng dòng
- `ingested_at` UPDATE khi upsert → audit trail
- `source` ghi vci/tcbs/msn/mock → truy vết

## Bronze schema

```sql
bronze_prices: code VARCHAR(20), date DATE, open/high/low/close NUMERIC(18,4),
               volume BIGINT, raw_json JSONB, source TEXT, ingested_at TIMESTAMPTZ
               PK(code, date)
```

## Silver output

```
symbol, trade_date, open, high, low, close, volume, source, is_valid, dq_flag, loaded_at
```

## Gold tables

- `fact_stock_price` — OHLCV clean, grain (symbol, trade_date)
- `fact_stock_indicators` — MA5/MA20/RSI14/MACD/Bollinger, incremental
- `fact_market_summary` — gainers/losers/volume/vnindex/vn30, grain (trade_date)
- `dim_stock` — symbol, exchange, industry
- `dim_date` — calendar, is_trading_day
