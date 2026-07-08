# BÁO CÁO THIẾT KẾ VÀ TRIỂN KHAI DATABASE
## Vietnam Stock Market Data Engineering Pipeline
### (Bronze · Silver · Gold Architecture + Data Contracts)

> Tài liệu này mô tả toàn bộ thiết kế database thực tế — từ DDL, cơ chế partitioning, materialization strategy, đến data contracts giữa các tầng. Mọi thông tin đều được xác minh trực tiếp từ codebase và database đang chạy.

---

## MỤC LỤC

1. [Tổng quan kiến trúc Medallion](#1-tổng-quan-kiến-trúc-medallion)
2. [Tầng Bronze — Thiết kế chi tiết](#2-tầng-bronze--thiết-kế-chi-tiết)
3. [Tầng Silver — Thiết kế chi tiết](#3-tầng-silver--thiết-kế-chi-tiết)
4. [Tầng Gold — Thiết kế chi tiết](#4-tầng-gold--thiết-kế-chi-tiết)
5. [Tầng Intermediate — Cầu nối tính toán EMA/RSI](#5-tầng-intermediate--cầu-nối-tính-toán-emasri)
6. [Data Contracts — Interface giữa các tầng](#6-data-contracts--interface-giữa-các-tầng)
7. [Airflow schema (Metadata)](#7-airflow-schema-metadata)
8. [ERD tổng quan](#8-erd-tổng-quan)

---

## 1. Tổng quan kiến trúc Medallion

Dự án áp dụng kiến trúc **Medallion (Bronze → Silver → Gold)** — mỗi tầng có mục đích rõ ràng:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER (Python + vnstock API)                                  │
│  ingestion/fetch_prices.py · ingestion/fetch_index.py                   │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ INSERT ON CONFLICT DO UPDATE
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  —  schema: bronze  (PostgreSQL)                           │
│  Dữ liệu thô, nguyên bản, không chỉnh sửa                               │
│  bronze_prices │ bronze_index │ bronze_vn30_components                   │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ dbt run --select models/silver
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  —  schema: public_silver  (PostgreSQL)                    │
│  Chuẩn hóa, đổi tên cột, kiểm định chất lượng dữ liệu                   │
│  silver_prices │ silver_index                                            │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ dbt run --select models/gold
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  INTERMEDIATE  —  schema: public_gold (bảng int_*)                       │
│  EMA12, EMA26, MACD Line/Signal, RSI14 (tính toán đệ quy SQL)           │
│  int_ema12 │ int_ema26 │ int_rsi14 │ int_macd_line │ int_macd_signal     │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ JOIN + aggregate
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  —  schema: public_gold  (PostgreSQL)                        │
│  Chỉ số kỹ thuật, dimension, thống kê thị trường — phục vụ dashboard    │
│  fact_stock_price │ fact_stock_indicators │ dim_stock │ fact_market_summary │
└─────────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
                    Power BI / Dashboard
```

### Thông số hiện tại (kiểm tra 2026-06-24)

| Tầng | Bảng chính | Số dòng thực | Ghi chú |
| :--- | :--- | ---: | :--- |
| Bronze | `bronze_prices` | 75,960 | Partitioned by year |
| Bronze | `bronze_index` | 8 | VNINDEX + VN30 index |
| Bronze | `bronze_vn30_components` | 30 | Danh sách VN30 động |
| Silver | `silver_prices` | 75,960 | 80 mã, từ 2020-06-11 |
| Silver | `silver_index` | 8 | |
| Gold | `fact_stock_price` | 75,960 | Chỉ is_valid=TRUE |
| Gold | `fact_stock_indicators` | 4,727 | Sau warm-up EMA/MACD |
| Gold | `dim_stock` | 80 | |
| Gold | `fact_market_summary` | 1,508 | |

---

## 2. Tầng Bronze — Thiết kế chi tiết

### Mục đích
> **Lưu trữ dữ liệu thô nguyên bản như một "data lake in-database"**. Không xóa, không sửa, không lọc. Đây là source of truth duy nhất.

### 2.1. DDL thực tế — `bronze.bronze_prices`

```sql
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.bronze_prices (
    code        VARCHAR(20)     NOT NULL,
    date        DATE            NOT NULL,
    open        NUMERIC(18,4),              -- Nullable: API có thể thiếu
    high        NUMERIC(18,4),
    low         NUMERIC(18,4),
    close       NUMERIC(18,4),
    volume      BIGINT,
    raw_json    JSONB,                      -- Toàn bộ response gốc từ API
    source      TEXT,                       -- 'vnstock_vci' hoặc 'vnstock_kbs'
    ingested_at TIMESTAMPTZ,               -- Thời điểm ghi vào DB
    PRIMARY KEY (code, date)
) PARTITION BY RANGE (date);              -- Partition theo năm

-- 7 partition hiện có (2020-2026)
CREATE TABLE bronze.bronze_prices_2020 PARTITION OF bronze.bronze_prices
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE bronze.bronze_prices_2021 PARTITION OF bronze.bronze_prices
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
-- ... (tương tự cho 2022, 2023, 2024, 2025, 2026)
CREATE TABLE bronze.bronze_prices_2026 PARTITION OF bronze.bronze_prices
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
```

#### Giải thích các lựa chọn thiết kế

| Quyết định thiết kế | Lý do |
| :--- | :--- |
| **Partition by RANGE(date)** | Query nhanh hơn khi lọc theo ngày (PostgreSQL chỉ scan partition cần thiết). Giảm kích thước index. |
| **PRIMARY KEY (code, date)** | Đảm bảo 1 mã chỉ có 1 dòng/ngày — không có duplicate. |
| **NUMERIC(18,4) cho giá** | Tránh lỗi làm tròn của FLOAT. 4 chữ số thập phân = đủ độ chính xác cho đơn vị nghìn VND. |
| **Cột raw_json JSONB** | Lưu toàn bộ response gốc từ API — cho phép re-parse nếu schema thay đổi sau này. |
| **Cột source TEXT** | Phân biệt nguồn dữ liệu: `vnstock_vci` hoặc `vnstock_kbs` — hỗ trợ audit và debugging. |
| **ingested_at TIMESTAMPTZ** | Không dùng DEFAULT để ingestion code có thể kiểm soát timestamp chính xác. |

### 2.2. Cơ chế ghi — Upsert idempotent

```sql
-- Câu lệnh được ingestion/fetch_prices.py thực thi
INSERT INTO bronze.bronze_prices
    (code, date, open, high, low, close, volume, raw_json, source, ingested_at)
VALUES
    (%(code)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
     %(volume)s, %(raw_json)s, %(source)s, CURRENT_TIMESTAMP)
ON CONFLICT (code, date)
    DO UPDATE SET
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        volume      = EXCLUDED.volume,
        raw_json    = EXCLUDED.raw_json,
        source      = EXCLUDED.source,
        ingested_at = CURRENT_TIMESTAMP;
```

**Ý nghĩa:** Chạy lại DAG cho cùng ngày → **không tạo duplicate**. Dữ liệu cũ được ghi đè bằng dữ liệu mới nhất từ API.

### 2.3. DDL — `bronze.bronze_index`

Cùng schema với `bronze_prices`, khóa chính `(code, date)`, cũng partitioned by year. Dùng để lưu VNINDEX, VN30 index.

```sql
CREATE TABLE IF NOT EXISTS bronze.bronze_index (
    code        VARCHAR(20)  NOT NULL,
    date        DATE         NOT NULL,
    open        NUMERIC(18,4),
    high        NUMERIC(18,4),
    low         NUMERIC(18,4),
    close       NUMERIC(18,4),
    volume      BIGINT,
    raw_json    JSONB,
    source      TEXT,
    ingested_at TIMESTAMPTZ,
    PRIMARY KEY (code, date)
) PARTITION BY RANGE (date);
```

### 2.4. DDL — `bronze.bronze_vn30_components`

```sql
CREATE TABLE IF NOT EXISTS bronze.bronze_vn30_components (
    code        VARCHAR(20)  PRIMARY KEY,
    ingested_at TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP
);
```

**Mục đích:** Lưu cache danh sách 30 mã VN30 động lấy từ API `Listing(source='VCI').symbols_by_group('VN30')`. Được cập nhật mỗi lần DAG chạy. Bảng `dim_stock` trong Gold đọc từ đây để flag `is_vn30`.

---

## 3. Tầng Silver — Thiết kế chi tiết

### Mục đích
> **Chuẩn hóa cấu trúc + kiểm định chất lượng dữ liệu**. Đổi tên cột, đổi kiểu dữ liệu, gắn nhãn `is_valid` cho mỗi bản ghi. Giữ lại bản ghi lỗi (không xóa) để audit.

### 3.1. DDL thực tế — `public_silver.silver_prices`

```sql
-- Được tạo tự động bởi dbt run --select models/silver
-- materialized='table' → DROP + CREATE TABLE mỗi lần chạy

CREATE TABLE public_silver.silver_prices (
    symbol      VARCHAR(20),
    trade_date  DATE,
    open_price  DOUBLE PRECISION,    -- đổi tên từ 'open'
    high_price  DOUBLE PRECISION,    -- đổi tên từ 'high'
    low_price   DOUBLE PRECISION,    -- đổi tên từ 'low'
    close_price DOUBLE PRECISION,    -- đổi tên từ 'close'
    volume      BIGINT,
    source      TEXT,
    dq_flag     TEXT,               -- Mô tả lý do lỗi (hoặc 'ok')
    is_valid    BOOLEAN,            -- TRUE = dữ liệu hợp lệ
    loaded_at   TIMESTAMPTZ,        -- Thời điểm dbt run
    ingested_at TIMESTAMPTZ         -- Kế thừa từ Bronze
);
```

### 3.2. Logic SQL của dbt model — `silver_prices.sql`

```sql
-- File: dbt/models/silver/silver_prices.sql
{{ config(materialized='table', unique_key=['symbol', 'trade_date']) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_prices') }}
),

-- Bước 1: Đổi tên cột + Cast kiểu dữ liệu
casted AS (
    SELECT
        code               AS symbol,
        date               AS trade_date,
        CAST(open  AS DOUBLE PRECISION) AS open_price,
        CAST(high  AS DOUBLE PRECISION) AS high_price,
        CAST(low   AS DOUBLE PRECISION) AS low_price,
        CAST(close AS DOUBLE PRECISION) AS close_price,
        CAST(volume AS BIGINT)          AS volume,
        source,
        ingested_at
    FROM source
),

-- Bước 2: Gắn nhãn chất lượng dữ liệu (Data Quality Flag)
flagged AS (
    SELECT
        *,
        CASE
            WHEN close_price <= 0                            THEN 'invalid_close_price'
            WHEN high_price  <  low_price                    THEN 'high_less_than_low'
            WHEN open_price  <= 0 OR high_price <= 0
                              OR low_price  <= 0             THEN 'invalid_ohlc'
            WHEN volume < 0                                  THEN 'negative_volume'
            ELSE 'ok'
        END AS dq_flag,
        CURRENT_TIMESTAMP AS loaded_at
    FROM casted
)

-- Bước 3: Output cuối — is_valid derived từ dq_flag
SELECT
    symbol, trade_date,
    open_price, high_price, low_price, close_price, volume,
    source, dq_flag,
    (dq_flag = 'ok') AS is_valid,    -- computed column
    loaded_at, ingested_at
FROM flagged
```

### 3.3. Bảng Data Quality Rules

| Quy tắc | Biểu thức SQL | `dq_flag` | `is_valid` |
| :--- | :--- | :--- | :--- |
| Giá đóng cửa phải dương | `close_price <= 0` | `'invalid_close_price'` | `FALSE` |
| Giá cao phải >= giá thấp | `high_price < low_price` | `'high_less_than_low'` | `FALSE` |
| OHLC không âm/bằng 0 | `open/high/low <= 0` | `'invalid_ohlc'` | `FALSE` |
| Volume không âm | `volume < 0` | `'negative_volume'` | `FALSE` |
| Không có lỗi nào | Tất cả OK | `'ok'` | `TRUE` |

**Lưu ý quan trọng:** NULL không bị coi là lỗi — dữ liệu thiếu một số cột từ API vẫn có thể hợp lệ (ví dụ: volume=NULL cho một số chứng khoán đặc biệt).

### 3.4. Materialization strategy của Silver

```
materialized='table'  →  mỗi lần dbt run: DROP TABLE + CREATE TABLE AS SELECT
```

**Lý do dùng `table` (không dùng `incremental`):**
- Silver cần nhìn thấy toàn bộ Bronze để kiểm định đúng
- Silver tương đối nhỏ (75k dòng, rebuild nhanh ~5 giây)
- Đảm bảo không có dữ liệu cũ bị "mắc kẹt" nếu Bronze bị sửa

### 3.5. Quy trình xử lý và Quality Gate (Run -> Test)

Quy trình dữ liệu từ Bronze sang Silver tuân thủ nghiêm ngặt mô hình **Run (Lưu) -> Test (Kiểm định) -> Run Gold (Publish)**. Mỗi bước đều có hành động, cơ chế phản hồi và kết quả dự tính rõ ràng:

1. **Bước 1: Thực thi biến đổi dữ liệu (dbt run)**
   - **Làm gì (`dbt run --select silver`)**: dbt biên dịch SQL để lấy dữ liệu thô từ Bronze, làm sạch, và tính toán logic nhằm gắn nhãn chất lượng (`is_valid`, `dq_flag`).
   - **Kết quả dự tính (Expected Outcome)**: 
     - Bảng vật lý (`silver_prices`, `silver_index`) được tạo/cập nhật thành công trong PostgreSQL.
     - Dữ liệu bẩn không bị xóa mà chỉ bị đánh dấu `is_valid = false` để làm log theo dõi.
     - Lệnh chạy luôn **PASS** (trừ trường hợp sai cú pháp SQL cứng).
     - **Ví dụ log dbt:**
       ```text
       14:20:10  1 of 2 START sql table model public_silver.silver_prices ........ [RUN]
       14:20:12  1 of 2 OK created sql table model public_silver.silver_prices ... [SELECT 403 in 2.1s]
       ```

2. **Bước 2: Kiểm định chất lượng (dbt test)**
   - **Làm gì (`dbt test --select silver`)**: Chạy các truy vấn SQL trực tiếp vào tầng Silver vừa tạo để kiểm tra sự vi phạm ràng buộc (Unique, Not Null).
   - **Lý do bắt buộc Run trước Test**: Phải có bảng vật lý được `run` tạo ra thì lệnh `test` mới có data để query.
   - **Kết quả dự tính & Phản ứng của Airflow**:
     - **Trường hợp PASS (Happy Path)**: Toàn bộ dữ liệu tuân thủ ràng buộc. Hệ thống tiếp tục chạy qua dbt run tầng Gold.
       ```text
       14:21:00  2 of 2 PASS unique_silver_prices_symbol_date .................... [PASS in 0.05s]
       14:21:00  Completed successfully
       ```
     - **Trường hợp FAIL (Bắt được dữ liệu hỏng)**: Bắt được dòng vi phạm ràng buộc.
       ```text
       14:22:15  1 of 2 FAIL 1 unique_silver_prices_symbol_date .................. [FAIL 1 in 0.06s]
       ```
       $\rightarrow$ **Phản ứng Airflow (Fail Fast)**: Task `dbt_test_silver` ngay lập tức báo **Đỏ (Failed)**. Các task hạ nguồn (`dbt_run_gold`) chuyển sang trạng thái **Cam (Upstream Failed)** và bị hủy bỏ. Airflow sẽ trigger callback `on_failure_callback` gửi thông báo cảnh báo qua Telegram/Slack. Dữ liệu lỗi bị chặn lại hoàn toàn tại Silver, bảo vệ Power BI khỏi số liệu sai lệch.

---

## 4. Tầng Gold — Thiết kế chi tiết

### 4.0. Ý nghĩa các bảng dữ liệu tầng Gold (Star Schema)

Tầng Gold được thiết kế theo mô hình hình sao (Star Schema) tối ưu cho việc truy vấn báo cáo trên Power BI, bao gồm các bảng Fact (Sự kiện) và Dimension (Chiều):

1. **`fact_stock_price` (Fact giá cổ phiếu)**:
   - *Ý nghĩa*: Lưu trữ dữ liệu giá giao dịch lịch sử hàng ngày (OHLCV) sạch của các cổ phiếu.
   - *Đặc điểm*: Chỉ nhận các bản ghi hợp lệ (`is_valid = TRUE`) từ tầng Silver. Đây là bảng dữ liệu gốc để vẽ các biểu đồ nến giá.
2. **`fact_stock_indicators` (Fact chỉ báo kỹ thuật)**:
   - *Ý nghĩa*: Lưu trữ các chỉ báo phân tích kỹ thuật được tính toán sẵn cho từng mã chứng khoán theo ngày (MA5, MA20, RSI14, MACD, Bollinger Bands).
   - *Đặc điểm*: Bảng này được cấu hình dạng `incremental` (chỉ tính toán và ghi đè phần dữ liệu mới nhất hàng ngày dựa trên khoá chính `symbol, trade_date`) để tăng tốc hiệu năng xử lý.
3. **`fact_market_summary` (Fact tổng hợp thị trường)**:
   - *Ý nghĩa*: Tổng hợp thông tin toàn cục của thị trường chứng khoán theo từng ngày giao dịch.
   - *Đặc điểm*: Chứa các chỉ số thống kê như số lượng mã tăng giá (gainers), số mã giảm giá (losers), đứng giá (unchanged), tổng khối lượng giao dịch toàn thị trường, và chỉ số VNINDEX, VN30.
4. **`dim_stock` (Dimension thông tin cổ phiếu)**:
   - *Ý nghĩa*: Chứa thông tin mô tả chi tiết của từng mã cổ phiếu.
   - *Đặc điểm*: Các thuộc tính bao gồm: mã cổ phiếu (symbol), tên sàn giao dịch (exchange: HOSE, HNX...), ngành công nghiệp (industry), và cờ phân loại nhóm VN30 (`is_vn30`).
5. **`dim_date` (Dimension thời gian)**:
   - *Ý nghĩa*: Bảng lịch chuẩn hóa hỗ trợ Power BI thực hiện các hàm phân tích theo chuỗi thời gian (Time Intelligence).
   - *Đặc điểm*: Chứa các thông tin về ngày, tháng, năm, quý, thứ, và cờ xác định ngày có giao dịch chứng khoán hay không (`is_trading_day`).


### Mục đích
> **Dữ liệu sẵn sàng cho dashboard**. Chỉ chứa bản ghi hợp lệ, đã tính toán chỉ số kỹ thuật. Phục vụ Power BI/visualization.

### 4.1. `public_gold.fact_stock_price`

```sql
-- File: dbt/models/gold/fact_stock_price.sql
-- Materialized: 'table' — rebuild toàn bộ, cấu hình unique indexes trên (symbol, trade_date) và (symbol, row_num)

WITH base AS (
    SELECT
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        source,
        loaded_at AS silver_loaded_at,
        CURRENT_TIMESTAMP AS gold_loaded_at
    FROM silver_prices
    WHERE is_valid = TRUE
),
calculated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS row_num,
        close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS daily_change
    FROM base
)
SELECT
    symbol,
    trade_date,
    row_num,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    source,
    silver_loaded_at,
    gold_loaded_at,
    GREATEST(COALESCE(daily_change, 0), 0) AS gain,
    ABS(LEAST(COALESCE(daily_change, 0), 0)) AS loss
FROM calculated
```

| Cột | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `symbol` | VARCHAR(20) | Mã cổ phiếu (viết hoa: VNM, ACB...) |
| `trade_date` | DATE | Ngày giao dịch |
| `row_num` | BIGINT | Số thứ tự dòng của mã chứng khoán (dùng làm index cho đệ quy) |
| `open/high/low/close_price` | DOUBLE PRECISION | Giá OHLC (đơn vị: nghìn VND) |
| `volume` | BIGINT | Khối lượng giao dịch (đơn vị: cổ phiếu) |
| `source` | TEXT | Nguồn dữ liệu gốc |
| `silver_loaded_at` | TIMESTAMPTZ | Thời điểm dbt Silver chạy |
| `gold_loaded_at` | TIMESTAMPTZ | Thời điểm dbt Gold chạy |
| `gain` | DOUBLE PRECISION | Giá trị tăng trong ngày so với hôm trước (phục vụ tính RSI) |
| `loss` | DOUBLE PRECISION | Giá trị giảm trong ngày so với hôm trước (phục vụ tính RSI) |

### 4.2. `public_gold.fact_stock_indicators`

**Bảng quan trọng nhất** — chứa toàn bộ chỉ báo kỹ thuật được tính bằng SQL đệ quy.

```sql
-- File: dbt/models/gold/fact_stock_indicators.sql
-- Materialized: 'incremental', strategy: 'delete+insert'
-- Lookback: 120 ngày từ MAX(trade_date)
```

| Cột | Kiểu | Mô tả | Warmup |
| :--- | :--- | :--- | :--- |
| `symbol` | VARCHAR(20) | Mã cổ phiếu | — |
| `trade_date` | DATE | Ngày giao dịch | — |
| `close_price` | DOUBLE PRECISION | Giá đóng cửa | — |
| `ma50` | DOUBLE PRECISION | Moving Average 50 ngày | 50 phiên |
| `ma200` | DOUBLE PRECISION | Moving Average 200 ngày | 200 phiên |
| `bb_upper` | DOUBLE PRECISION | Bollinger Band trên (MA200 + 2σ) | 200 phiên |
| `bb_lower` | DOUBLE PRECISION | Bollinger Band dưới (MA200 - 2σ) | 200 phiên |
| `rsi_14` | NUMERIC | RSI 14 phiên (Wilder smoothing) | 15 phiên |
| `macd_line` | DOUBLE PRECISION | EMA12 - EMA26 | 26 phiên |
| `macd_signal` | DOUBLE PRECISION | EMA9 của MACD Line | 35 phiên |
| `macd_histogram` | DOUBLE PRECISION | MACD Line - MACD Signal | 35 phiên |

**Incremental strategy — delete+insert:**
```sql
-- Bước 1: Xóa 360 ngày gần nhất để tính lại sạch
WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '360 days' FROM fact_stock_indicators)

-- Bước 2: Insert lại với dữ liệu mới nhất từ Silver
```

**Lý do lookback 120 ngày:** MACD Signal cần 35 phiên warmup (26 EMA + 9 EMA Signal). 120 ngày = 35 × 3.4 — margin an toàn đủ để không bị NULL ở phiên mới nhất.

### 4.3. `public_gold.dim_stock`

```sql
-- File: dbt/models/gold/dim_stock.sql
-- Materialized: 'table' — rebuild toàn bộ

SELECT
    g.symbol,
    g.exchange,        -- Nguồn dữ liệu (vnstock_vci/kbs)
    g.default_exchange,-- 'HOSE' cố định
    CASE WHEN v.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS is_vn30
FROM silver_prices g
LEFT JOIN bronze_vn30_components v ON g.symbol = v.symbol
WHERE g.is_valid = TRUE
```

| Cột | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `symbol` | VARCHAR(20) | Mã cổ phiếu (PK) |
| `exchange` | TEXT | Nguồn data: `vnstock_vci` hoặc `vnstock_kbs` |
| `default_exchange` | TEXT | `'HOSE'` — tất cả mã đều thuộc HOSE |
| `is_vn30` | BOOLEAN | `TRUE` nếu mã có trong `bronze_vn30_components` |

### 4.4. `public_gold.fact_market_summary`

```sql
-- Tổng hợp thị trường theo ngày giao dịch
```

| Cột | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `trade_date` | DATE | Ngày giao dịch (PK) |
| `gainers` | BIGINT | Số mã tăng giá so với hôm trước |
| `losers` | BIGINT | Số mã giảm giá |
| `unchanged` | BIGINT | Số mã đứng giá |
| `total_symbols` | BIGINT | Tổng số mã trong ngày |
| `total_volume` | NUMERIC | Tổng khối lượng toàn thị trường |
| `vnindex_close` | DOUBLE PRECISION | Giá đóng cửa VNINDEX |
| `vn30_close` | DOUBLE PRECISION | Giá đóng cửa VN30 index |

**Constraint dbt test:**
```yaml
- dbt_utils.expression_is_true:
    expression: "gainers + losers + unchanged = total_symbols"
```

### 4.5. Quy trình xử lý và Quality Gate (Run -> Test)

Giống như tầng Silver, tầng Gold cũng tuân thủ quy trình **Run -> Test**. Tuy nhiên, do tính chất dữ liệu ở tầng Gold phức tạp hơn (chứa các phép toán đệ quy tính RSI, MACD, MA), quy trình ở đây đóng vai trò kiểm soát độ chính xác của logic toán học nghiệp vụ:

1. **Bước 1: Thực thi tính toán chỉ số (dbt run)**
   - **Làm gì (`dbt run --select gold`)**: dbt thực thi chuỗi lệnh SQL phức tạp (qua các bảng Intermediate) để tính các chỉ báo kỹ thuật (RSI, MACD) và tổng hợp thị trường.
   - **Kết quả dự tính (Expected Outcome)**:
     - Bảng `fact_stock_indicators` được cập nhật theo cơ chế `incremental` (xóa 120 ngày cũ nhất và chèn dữ liệu mới tính toán).
     - Các bảng khác (`fact_market_summary`, `dim_stock`) được tạo thành công.
     - Lệnh chạy luôn **PASS** (miễn là SQL không lỗi cú pháp).
     - **Ví dụ log dbt:**
       ```text
       14:23:10  1 of 4 START sql incremental model public_gold.fact_stock_indicators ........ [RUN]
       14:23:15  1 of 4 OK created sql incremental model public_gold.fact_stock_indicators ... [INSERT 0 403 in 5.1s]
       ```

2. **Bước 2: Kiểm định logic toán học (dbt test)**
   - **Làm gì (`dbt test --select gold`)**: dbt chạy truy vấn để verify các ràng buộc gắt gao của nghiệp vụ tài chính. (Ví dụ: `rsi_14` phải nằm trong khoảng [0, 100], `bb_upper >= bb_lower`).
   - **Kết quả dự tính & Cách xử lý của Airflow**:
     - **Trường hợp PASS (Happy Path)**: Công thức toán học tính đúng 100%. Task `dbt_test_gold` báo xanh, Airflow chạy tiếp sang bước `notify_success` để gửi thông báo Pipeline hoàn tất tốt đẹp.
       ```text
       14:24:00  1 of 4 PASS dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_... [PASS in 0.08s]
       14:24:01  Completed successfully
       ```
     - **Trường hợp FAIL (Sai số toán học)**: Một chỉ số bị tính sai (Ví dụ: code SQL đệ quy làm sai lệch khiến RSI vọt lên 105).
       ```text
       14:24:15  1 of 4 FAIL 1 dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_... [FAIL 1 in 0.09s]
       ```
       $\rightarrow$ **Vấn đề lọt dữ liệu lỗi**: Vì lệnh `run` chạy trước `test`, dữ liệu sai (RSI=105) **đã bị ghi đè thành công vào Database**. Khác với Silver (dữ liệu sai bị chặn và đánh cờ `is_valid=false`), dữ liệu sai ở Gold sẽ hiển thị lên báo cáo nếu Power BI cập nhật ngay lúc đó.
       $\rightarrow$ **Cơ chế xử lý (Idempotent Healing)**: 
       1. Airflow đánh dấu luồng chạy là **Đỏ (Failed)** và **không kích hoạt** lệnh tự động làm mới Power BI. Kỹ sư nhận được cảnh báo.
       2. Kỹ sư tiến hành fix lại công thức SQL (VD: sửa hàm chia cho 0).
       3. Chạy lại DAG. Vì Gold dùng chiến lược `incremental` (delete+insert window 120 ngày), `dbt run` ở lần chạy lại sẽ tự động xóa sạch dòng dữ liệu lỗi ban nãy (của 120 ngày qua) và chèn lại dữ liệu mới đã tính đúng, tự động phục hồi tính đúng đắn của Database mà không cần rollback thủ công.

---

## 5. Tầng Intermediate — Cầu nối tính toán EMA/RSI

### Lý do tồn tại
PostgreSQL không hỗ trợ `WITH RECURSIVE` lồng bên trong CTE thông thường. Việc tính EMA (cần đệ quy) và sau đó dùng kết quả đó trong cùng 1 câu SQL sẽ báo lỗi. Các bảng intermediate giải quyết vấn đề này bằng cách tính toán trước và persist kết quả.

### 5.1. Sơ đồ dependency

```
fact_stock_price
    ├── int_ema12  ─────────┐
    │   (EMA period=12)     ├── int_macd_line ─────┐
    ├── int_ema26  ─────────┘   (EMA12 - EMA26)     ├── fact_stock_indicators
    │   (EMA period=26)                              │
    └── int_rsi14 ──────────────────────────────────┘
        (Wilder RSI 14)
                                         int_macd_signal ──┘
                                         (EMA9 of macd_line)
```

### 5.2. Macro `calculate_ema(period)` — EMA đệ quy SQL

```sql
-- File: dbt/macros/calculate_ema.sql
-- Được gọi bởi: int_ema12.sql, int_ema26.sql, int_macd_signal.sql

WITH RECURSIVE
-- Bước 1: SMA seed — giá trị khởi đầu tại row=period (sử dụng cột row_num vật lý đã được đánh index)
_ema_seed AS (
    SELECT symbol, period AS rn, AVG(close_price) AS ema_val
    FROM fact_stock_price
    WHERE row_num <= period
    GROUP BY symbol
),

-- Bước 2: Đệ quy — tính EMA từ row period+1 đến cuối (join trực tiếp với bảng vật lý có index)
_ema_rec(symbol, rn, trade_date, ema_val) AS (
    -- Base case: seed
    SELECT b.symbol, b.row_num, b.trade_date, s.ema_val
    FROM fact_stock_price b 
    JOIN _ema_seed s ON b.symbol = s.symbol AND b.row_num = s.rn

    UNION ALL

    -- Recursive: EMA(t) = Close(t) * α + EMA(t-1) * (1 - α)
    --   α = 2 / (period + 1)  — Standard EMA (khác Wilder 1/period)
    SELECT b.symbol, b.row_num, b.trade_date,
           b.close_price * (2.0 / (period + 1.0))
           + e.ema_val * (1.0 - 2.0 / (period + 1.0))
    FROM fact_stock_price b 
    JOIN _ema_rec e ON b.symbol = e.symbol AND b.row_num = e.rn + 1
)

SELECT symbol, trade_date, ema_val AS ema_{period} FROM _ema_rec
```

**Hai loại smoothing được dùng trong dự án:**

| Chỉ số | Công thức α | Ghi chú |
| :--- | :--- | :--- |
| **EMA12, EMA26** (dùng cho MACD) | α = 2/(period+1) | Standard EMA — Bloomberg/TradingView standard |
| **RSI14** | α = 1/period | Wilder smoothing — định nghĩa gốc của J. Welles Wilder |

### 5.3. Macro `calculate_rsi(period)` — RSI Wilder đệ quy SQL

```sql
-- File: dbt/macros/calculate_rsi.sql
-- RSI = 100 - (100 / (1 + RS))
-- RS = avg_gain / avg_loss (Wilder smoothing)

WITH RECURSIVE
-- Bước 1: Tính daily_change = close - LAG(close)
_rsi_base AS (...),

-- Bước 2: Tách gain/loss từ daily_change
_rsi_gl AS (
    gain = GREATEST(daily_change, 0)
    loss = ABS(LEAST(daily_change, 0))
),

-- Bước 3: SMA seed tại row=period+1 (lấy period=14 row đầu)
_rsi_seed AS (AVG(gain/loss) WHERE rn BETWEEN 2 AND period+1),

-- Bước 4: Wilder smoothing đệ quy
-- avg_gain(t) = (avg_gain(t-1) * (period-1) + gain(t)) / period
_rsi_rec AS (...)

SELECT symbol, trade_date,
    CASE
        WHEN avg_loss = 0 AND avg_gain = 0 THEN NULL  -- Không đổi
        WHEN avg_loss = 0                  THEN 100   -- Toàn tăng
        ELSE ROUND(100 - (100 / (1 + avg_gain/avg_loss)), 4)
    END AS rsi_14
```

### 5.4. Các bảng intermediate và schema của chúng

| Bảng | Schema | Mô tả | Warmup |
| :--- | :--- | :--- | :--- |
| `int_ema12` | `(symbol, trade_date, ema_12)` | EMA 12 phiên | 12 phiên |
| `int_ema26` | `(symbol, trade_date, ema_26)` | EMA 26 phiên | 26 phiên |
| `int_macd_line` | `(symbol, trade_date, macd_line)` | EMA12 - EMA26 | 26 phiên |
| `int_macd_signal` | `(symbol, trade_date, macd_signal)` | EMA9 của macd_line | 35 phiên |
| `int_rsi14` | `(symbol, trade_date, rsi_14)` | RSI Wilder 14 | 15 phiên |

---

## 6. Data Contracts — Interface giữa các tầng

### 6.1. Nguyên tắc Data Contract

> **Data Contract = Thỏa thuận về cấu trúc dữ liệu giữa producer và consumer của mỗi tầng.** Nếu producer thay đổi schema, consumer phải được cập nhật đồng thời.

### 6.2. Contract Bronze → Silver

| Cột Bronze (producer) | Kiểu | → | Cột Silver (consumer) | Kiểu | Biến đổi |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `code` | VARCHAR(20) | → | `symbol` | VARCHAR(20) | Đổi tên |
| `date` | DATE | → | `trade_date` | DATE | Đổi tên |
| `open` | NUMERIC(18,4) | → | `open_price` | DOUBLE PRECISION | Đổi tên + Cast |
| `high` | NUMERIC(18,4) | → | `high_price` | DOUBLE PRECISION | Đổi tên + Cast |
| `low` | NUMERIC(18,4) | → | `low_price` | DOUBLE PRECISION | Đổi tên + Cast |
| `close` | NUMERIC(18,4) | → | `close_price` | DOUBLE PRECISION | Đổi tên + Cast |
| `volume` | BIGINT | → | `volume` | BIGINT | Giữ nguyên |
| `source` | TEXT | → | `source` | TEXT | Giữ nguyên |
| `ingested_at` | TIMESTAMPTZ | → | `ingested_at` | TIMESTAMPTZ | Giữ nguyên |
| — | — | ✨ | `dq_flag` | TEXT | **Tạo mới (computed)** |
| — | — | ✨ | `is_valid` | BOOLEAN | **Tạo mới (derived từ dq_flag)** |
| — | — | ✨ | `loaded_at` | TIMESTAMPTZ | **Tạo mới (CURRENT_TIMESTAMP)** |

**Quy tắc bất biến (KHÔNG ĐƯỢC vi phạm):**
- Bronze KHÔNG ĐƯỢC xóa cột `code`, `date`, `open`, `high`, `low`, `close`, `volume` — Silver sẽ vỡ
- Silver KHÔNG ĐƯỢC xóa cột `is_valid`, `symbol`, `trade_date`, `close_price` — Gold sẽ vỡ

### 6.3. Contract Silver → Gold

| Điều kiện lọc | Mô tả |
| :--- | :--- |
| `WHERE is_valid = TRUE` | Gold chỉ nhận bản ghi hợp lệ từ Silver |

| Cột Silver (producer) | → | Cột Gold (consumer) | Bảng đích |
| :--- | :---: | :--- | :--- |
| `symbol`, `trade_date` | → | `symbol`, `trade_date` | Tất cả bảng Gold |
| `close_price` | → | `close_price` | `fact_stock_price`, `fact_stock_indicators` |
| `open/high/low_price`, `volume` | → | Same names | `fact_stock_price` |
| `loaded_at` | → | `silver_loaded_at` | `fact_stock_price` |

**Cột mới được tính toán bởi Gold (không từ Silver):**

| Cột Gold | Nguồn tính | Công thức |
| :--- | :--- | :--- |
| `ma50` | `fact_stock_price.close_price` | `AVG(close_price) OVER (50 PRECEDING)` |
| `ma200` | `fact_stock_price.close_price` | `AVG(close_price) OVER (200 PRECEDING)` |
| `bb_upper/lower` | `fact_stock_price.close_price` | `MA200 ± 2 × STDDEV_POP(200)` |
| `rsi_14` | `int_rsi14.rsi_14` | Wilder EMA đệ quy |
| `macd_line` | `int_ema12 - int_ema26` | EMA12 - EMA26 |
| `macd_signal` | `int_macd_signal` | EMA9 của macd_line |

### 6.4. Contract dbt Tests (Automated Validation)

```yaml
# File: dbt/models/gold/schema.yml

# 1. RSI phải nằm trong [0, 100]
- dbt_utils.expression_is_true:
    expression: "rsi_14 >= 0 AND rsi_14 <= 100"
    config:
      where: "rsi_14 IS NOT NULL"    # Bỏ qua warmup NULL

# 2. MA200 phải dương
- dbt_utils.expression_is_true:
    expression: "ma200 > 0"
    config:
      where: "ma200 IS NOT NULL"

# 3. Bollinger Band trên phải >= Band dưới
- dbt_utils.expression_is_true:
    expression: "bb_upper >= bb_lower"
    config:
      where: "bb_upper IS NOT NULL AND bb_lower IS NOT NULL"

# 4. Thống kê thị trường phải nhất quán
- dbt_utils.expression_is_true:
    expression: "gainers + losers + unchanged = total_symbols"

# 5. dim_stock: symbol phải unique và không NULL
- unique
- not_null
```

---

## 7. Airflow schema (Metadata)

Schema `public` trong `stock_db` cũng chứa toàn bộ bảng metadata của **Apache Airflow** (không phải bảng dữ liệu chứng khoán). Chúng share cùng một PostgreSQL instance để đơn giản hóa setup.

| Nhóm bảng Airflow | Ví dụ | Mục đích |
| :--- | :--- | :--- |
| DAG metadata | `dag`, `dag_run`, `dag_version` | Quản lý DAG definition |
| Task state | `task_instance`, `task_instance_history` | Trạng thái từng task |
| Auth | `ab_user`, `ab_role` | Quản lý user/quyền Web UI |
| Asset/Data | `asset`, `asset_event` | Dataset awareness Airflow 3.x |
| Connection | `connection`, `variable` | Cấu hình kết nối |

**Lưu ý:** Trong môi trường production thực tế, nên tách Airflow DB và Data DB ra 2 PostgreSQL instance riêng để tránh tranh chấp resource.

---

## 8. ERD tổng quan

```
BRONZE LAYER
┌──────────────────────┐   ┌─────────────────────┐   ┌──────────────────┐
│ bronze.bronze_prices │   │ bronze.bronze_index  │   │ bronze.bronze_   │
│ (code, date) PK      │   │ (code, date) PK      │   │ vn30_components  │
│ Partitioned by year  │   │ Partitioned by year  │   │ (code) PK        │
└──────────┬───────────┘   └──────────┬───────────┘   └───────┬──────────┘
           │ dbt silver                │ dbt silver             │ dbt gold
           ▼                           ▼                        │
SILVER LAYER                                                    │
┌──────────────────────┐   ┌─────────────────────┐             │
│ public_silver.       │   │ public_silver.      │             │
│ silver_prices        │   │ silver_index        │             │
│ + is_valid, dq_flag  │   │ + is_valid, dq_flag │             │
└──────────┬───────────┘   └──────────┬───────────┘            │
           │ WHERE is_valid=TRUE       │                        │
           │ dbt gold                  │ dbt gold               │
           ▼                           ▼                        ▼
GOLD LAYER
┌──────────────────┐   ┌────────────────────┐   ┌──────────────┐
│ fact_stock_price │   │ fact_market_summary│   │  dim_stock   │
│ (symbol,         │   │ (trade_date) PK    │   │ (symbol) PK  │
│  trade_date) PK  │   │ gainers, losers,   │   │ is_vn30 flag │
│ OHLCV sạch       │   │ vnindex_close...   │   └──────────────┘
└────────┬─────────┘   └────────────────────┘
         │ Window functions + JOIN intermediates
         ▼
┌────────────────────────┐
│ fact_stock_indicators  │
│ (symbol, trade_date) PK│
│ ma50, ma200, bb, rsi14 │
│ macd_line, signal, hist│
└────────────────────────┘

INTERMEDIATE (public_gold schema, computed by macros)
int_ema12 → int_macd_line ─┐
int_ema26 ─────────────────┤
int_rsi14 ─────────────────┼──→ fact_stock_indicators
int_macd_signal ───────────┘
```

---

## PHỤ LỤC: Lệnh kiểm tra schema nhanh

```bash
# Xem DDL đầy đủ của bất kỳ bảng nào
docker exec postgres-container psql -U airflow -d stock_db -c "\d bronze.bronze_prices"
docker exec postgres-container psql -U airflow -d stock_db -c "\d public_silver.silver_prices"
docker exec postgres-container psql -U airflow -d stock_db -c "\d public_gold.fact_stock_indicators"

# Liệt kê tất cả bảng trong một schema
docker exec postgres-container psql -U airflow -d stock_db -c "\dt bronze.*"
docker exec postgres-container psql -U airflow -d stock_db -c "\dt public_silver.*"
docker exec postgres-container psql -U airflow -d stock_db -c "\dt public_gold.*"

# Xem constraint (PK, FK, Index)
docker exec postgres-container psql -U airflow -d stock_db -c "\d+ bronze.bronze_prices"

# Xem kích thước thực tế của từng bảng
docker exec postgres-container psql -U airflow -d stock_db -c "
  SELECT schemaname, tablename,
         pg_size_pretty(pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename))) AS size
  FROM pg_tables
  WHERE schemaname IN ('bronze','public_silver','public_gold')
  ORDER BY pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename)) DESC;"

# Xem các partition của bronze_prices
docker exec postgres-container psql -U airflow -d stock_db -c "\d+ bronze.bronze_prices"
```
