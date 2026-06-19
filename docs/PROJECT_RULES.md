# PROJECT RULES & CONVENTIONS

> **AI:** Đọc file này khi sắp sinh code. Không cần đọc nếu chỉ hỏi về kiến trúc/scope.
> Mọi code phải tuân thủ Section 1, 3, 4. Không có ngoại lệ.

---

## 1. Naming Conventions

### Python
- **Style:** PEP 8
- **Files:** `snake_case.py` — `fetch_prices.py`, `vnstock_provider.py`
- **Classes:** `PascalCase` — `DataProvider`, `VnstockProvider`, `ProviderRateLimitError`
- **Functions/variables:** `snake_case` — `get_prices()`, `retry_count`
- **Constants:** `UPPER_SNAKE` — `MAX_RETRIES = 3`, `BATCH_SIZE = 50`
- **Private:** `_prefixed` — `_validate_schema()`

### SQL / PostgreSQL
- **Schema names:** `bronze`, `silver`, `gold`
- **Table names:** `snake_case` — `bronze_prices`, `fact_stock_indicators`
- **Column names:** `snake_case` — `trade_date`, `raw_json`, `ingested_at`
- **Prefix conventions:**
  - `fact_` cho fact tables
  - `dim_` cho dimension tables
  - `bronze_` / `silver_` cho staging tables

### dbt
- **Model files:** `snake_case.sql` — khớp tên table output
- **Schema files:** `schema.yml` cùng thư mục
- **Macro files:** `calculate_rsi.sql`, `calculate_ema.sql`
- **Test names:** mô tả rõ — `rsi_range_valid`, `high_gte_low`

### Environment
- **Env vars:** `UPPER_SNAKE` — `PROVIDER`, `DB_HOST`, `SYMBOLS_PILOT`
- **Config keys:** `snake_case` trong Python dataclass

---

## 2. Git Conventions

### Commit messages
```
<type>: <mô tả ngắn>

Ví dụ:
feat: add VnstockProvider with health_check
fix: handle null close in silver_prices
test: add idempotency test B-01
docs: update STATUS.md ngày 2
refactor: extract retry logic to utils.py
chore: pin airflow==3.2.x in requirements.txt
```

**Types:** `feat`, `fix`, `test`, `docs`, `refactor`, `chore`

### Branch strategy (đơn giản, 1 người)
- `main` — production-ready
- `dev` — working branch
- Merge `dev → main` cuối mỗi ngày khi gate pass

### Commit frequency
- **Ít nhất 2 commit/buổi** — hội đồng xem git log
- Commit sau mỗi module hoàn thành, không gom cuối ngày

---

## 3. Logging Conventions

```python
import logging
logger = logging.getLogger(__name__)

# Levels:
logger.info("Fetched %d rows for %s", count, symbol)     # Thao tác bình thường
logger.warning("Retry %d/%d for %s", attempt, max, url)   # Retry, degraded
logger.error("Provider failed: %s", str(e))                # Lỗi cần attention
logger.debug("Raw response: %s", response[:200])          # Debug only
```

**Quy tắc:**
- Không log secrets/passwords
- Không log toàn bộ DataFrame (chỉ log `.shape` hoặc `.head(3)`)
- Mọi `except` phải có `logger.error()` hoặc `logger.warning()` — không nuốt lỗi im lặng

---

## 4. Error Handling

```python
# Provider errors — dùng exception hierarchy
ProviderError              # base
├── ProviderRateLimitError # 429 → trigger retry
├── ProviderTimeoutError   # timeout → trigger retry  
└── ProviderSchemaError    # schema drift → fail ngay, không retry

# Retry chỉ cho RateLimit và Timeout
# SchemaError: fail ngay, log error, alert
```

**Nguyên tắc:**
- Không có lỗi âm thầm — mọi exception phải log
- Retry chỉ cho transient errors (429, timeout, connection reset)
- Non-transient errors (schema drift, auth fail) → fail fast
- Airflow `on_failure_callback` → alert khi task fail sau hết retry

---

## 5. Data Contracts (giữa các layer)

### Bronze output contract
```
Columns: code, date, open, high, low, close, volume, raw_json, source, ingested_at
Types:   VARCHAR, DATE, NUMERIC(18,4)×4, BIGINT, JSONB, TEXT, TIMESTAMPTZ
PK:      (code, date)
Nulls:   open/high/low/close/volume có thể null (raw data)
```

### Silver input expectation
```
Đọc từ Bronze. Expect columns ở trên.
Nếu thiếu column → dbt fail rõ ràng (không silent skip)
```

### Silver output contract
```
Columns: symbol, trade_date, open, high, low, close, volume, source, is_valid, dq_flag, loaded_at
Types:   VARCHAR, DATE, NUMERIC(18,4)×4, BIGINT, TEXT, BOOLEAN, TEXT, TIMESTAMPTZ
is_valid: FALSE nếu close<=0 OR high<low OR volume<0
dq_flag:  lý do reject (invalid_close / high_lt_low / negative_volume / NULL)
```

### Gold input expectation
```
Đọc từ Silver. fact_stock_price chỉ lấy WHERE is_valid = TRUE.
fact_stock_indicators tính trên fact_stock_price.
```

### Gold output contract
```
fact_stock_price:      (symbol, trade_date) PK, OHLCV clean
fact_stock_indicators: (symbol, trade_date) PK, MA5/MA20/RSI14/MACD/BB, incremental
fact_market_summary:   (trade_date) PK, gainers/losers/unchanged/volume/vnindex/vn30
dim_stock:             (symbol) PK, exchange, industry
dim_date:              (date_key) PK, calendar fields, is_trading_day
```

---

## 6. File Organization

- Mỗi module có **1 trách nhiệm** — không gộp fetch_prices + fetch_index
- Test fixtures trong `tests/fixtures/` — CSV, không hardcode trong test
- Config tập trung `ingestion/config.py` — không scatter `.env` reads
- SQL init tập trung `sql/init_schema.sql` — không tạo table trong Python

---

## 7. dbt Conventions

- Mỗi model có entry trong `schema.yml` cùng thư mục
- `sources.yml` ở root dbt/ — khai báo Bronze tables
- Silver models: `materialized='view'` hoặc `'table'`
- Gold fact_stock_indicators: `materialized='incremental'`, `unique_key=['symbol','trade_date']`
- Test: dùng built-in (`not_null`, `unique`, `accepted_values`) + `expression_is_true` cho business rules
