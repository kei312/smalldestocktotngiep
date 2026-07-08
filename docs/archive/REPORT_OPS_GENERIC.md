# BÁO CÁO VẬN HÀNH THỦ CÔNG — HƯỚNG DẪN TOÀN DỰ ÁN
## (Manual Operations Guide — Generic Reference)

> Tài liệu này trả lời các câu hỏi vận hành cốt lõi: **Dữ liệu được lấy về, lưu trữ và xử lý thế nào?**
> Khả năng chịu lỗi với data không chính xác ở các tầng là gì?
> Cách kiểm tra thủ công toàn dự án bằng Terminal + Database?

---

## MỤC LỤC

1. [Cách chạy thủ công toàn bộ pipeline qua Terminal](#1-cách-chạy-thủ-công-toàn-bộ-pipeline-qua-terminal)
2. [Dữ liệu được lấy về, lưu trữ và xử lý thế nào?](#2-dữ-liệu-được-lấy-về-lưu-trữ-và-xử-lý-thế-nào)
3. [Kiểm tra database: số dòng và bản ghi chi tiết](#3-kiểm-tra-database-số-dòng-và-bản-ghi-chi-tiết)
4. [Khả năng chịu lỗi với data không chính xác](#4-khả-năng-chịu-lỗi-với-data-không-chính-xác)
5. [Kịch bản DAG chi tiết từng trường hợp cụ thể](#5-kịch-bản-dag-chi-tiết-từng-trường-hợp-cụ-thể)

---

## 1. Cách chạy thủ công toàn bộ pipeline qua Terminal

### 1.1. Kiểm tra hạ tầng trước khi bắt đầu (Pre-flight Check)

**Bước 1 — Kiểm tra container Docker đang chạy:**
```bash
docker ps --filter "name=postgres" --filter "name=airflow" --format "{{.Names}} {{.Status}}"
```
Kết quả thực tế (đã kiểm tra):
```
airflow-container Up 7 hours
postgres-container Up 7 hours
```

**Bước 2 — Kiểm tra kết nối database:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c "SELECT 1;"
```
Kết quả mong đợi:
```
 ?column?
----------
        1
```

**Bước 3 — Kiểm tra kết nối dbt:**
```bash
docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt debug --profiles-dir ." 2>&1 | tail -5
```
Kết quả mong đợi: dòng cuối là `All checks passed!`

---

### 1.2. Bước 1: Cào dữ liệu thô (Bronze Layer Ingestion)

Pipeline có 2 lựa chọn khi cào dữ liệu:

**Chỉ VN30 (30 mã, nhanh ~5 phút):**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project && \
   PYTHONPATH=/opt/airflow/project \
   python3 -m ingestion.fetch_prices --mode vn30 --start 2026-06-24 --end 2026-06-24"
```

**Toàn bộ HOSE (~403 mã, mất ~20–40 phút tuỳ rate limit):**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project && \
   PYTHONPATH=/opt/airflow/project \
   python3 -m ingestion.fetch_prices --mode all --start 2026-06-24 --end 2026-06-24"
```

**Cào 1 nhóm mã cụ thể (ví dụ 3 mã bất kỳ):**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project && \
   PYTHONPATH=/opt/airflow/project \
   python3 -m ingestion.fetch_prices --symbols ACB,HPG,VNM --start 2026-06-01 --end 2026-06-24"
```

**Cào chỉ số thị trường (VNINDEX, VN30 index):**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project && \
   PYTHONPATH=/opt/airflow/project \
   python3 -m ingestion.fetch_index --start 2026-06-24 --end 2026-06-24"
```

**Backfill dữ liệu lịch sử dài hạn:**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project && \
   PYTHONPATH=/opt/airflow/project \
   python3 -m ingestion.backfill --start 2021-01-01 --end 2025-12-31"
```
> ⚠️ Lệnh trên sẽ mất nhiều giờ cho toàn bộ HOSE. Airflow DAG Backfill ưu tiên hơn vì có cơ chế retry tự động.

---

### 1.3. Bước 2: Chuẩn hóa dữ liệu (Silver Layer — dbt)

```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt run --select models/silver --profiles-dir ."
```

Chạy kèm kiểm định chất lượng dữ liệu Silver:
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --select models/silver --profiles-dir ."
```

---

### 1.4. Bước 3: Tính chỉ số tài chính (Gold Layer — dbt)

```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt run --select models/gold --profiles-dir ."
```

Kiểm định ràng buộc dữ liệu tầng Gold:
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --select models/gold --profiles-dir ."
```

**Chạy toàn bộ pipeline Silver + Gold trong 1 lệnh:**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt build --profiles-dir ."
```

---

### 1.5. Tổng hợp — Copy-paste 1 lần chạy E2E thủ công hoàn chỉnh

```bash
# --- Bước 0: Pre-flight ---
docker ps --filter "name=postgres" --filter "name=airflow" --format "{{.Names}} {{.Status}}"
docker exec postgres-container psql -U airflow -d stock_db -c "SELECT 1;"

# --- Bước 1: Cào dữ liệu ---
docker exec airflow-container bash -c "cd /opt/airflow/project && PYTHONPATH=/opt/airflow/project python3 -m ingestion.fetch_prices --mode vn30 --start $(date +%Y-%m-%d) --end $(date +%Y-%m-%d)"
docker exec airflow-container bash -c "cd /opt/airflow/project && PYTHONPATH=/opt/airflow/project python3 -m ingestion.fetch_index --start $(date +%Y-%m-%d) --end $(date +%Y-%m-%d)"

# --- Bước 2 + 3: dbt Silver → Gold ---
docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt build --profiles-dir ."

# --- Bước 4: Verify ---
docker exec postgres-container psql -U airflow -d stock_db -c "
  SELECT 'Bronze' AS layer, COUNT(*) FROM bronze.bronze_prices
  UNION ALL SELECT 'Silver', COUNT(*) FROM public_silver.silver_prices
  UNION ALL SELECT 'Gold FSP', COUNT(*) FROM public_gold.fact_stock_price
  UNION ALL SELECT 'Gold FSI', COUNT(*) FROM public_gold.fact_stock_indicators;"
```

---

## 2. Dữ liệu được lấy về, lưu trữ và xử lý thế nào?

### 2.1. Luồng dữ liệu tổng quan

```
[Vnstock API ]
    ↓ HTTP request → DataFrame Python
[ingestion/fetch_prices.py]
    ↓ INSERT ON CONFLICT (code, date) DO UPDATE
[bronze.bronze_prices]        ← Dữ liệu thô, nguyên bản
    ↓ dbt run silver
[public_silver.silver_prices] ← Chuẩn hóa, gắn nhãn chất lượng
    ↓ dbt run gold
[public_gold.fact_stock_price]      ← Chỉ bản ghi is_valid=TRUE
[public_gold.fact_stock_indicators] ← MA5/MA20/RSI14/MACD/Bollinger
[public_gold.dim_stock]             ← Dimension thông tin cổ phiếu
[public_gold.fact_market_summary]   ← Thống kê thị trường theo ngày
```

### 2.2. Tầng Bronze — Lưu trữ thô, nguyên bản (Raw Lake)

| Thuộc tính | Chi tiết |
| :--- | :--- |
| **Schema DB** | `bronze` |
| **Bảng chính** | `bronze_prices`, `bronze_index`, `bronze_vn30_components` |
| **Số dòng hiện tại** | 75,960 bản ghi giá + 8 bản ghi index |
| **Đơn vị giá** | Nghìn VND (ví dụ: 22.35 = 22,350 VND) |
| **Cơ chế ghi** | `INSERT ... ON CONFLICT (code, date) DO UPDATE SET ...` |
| **Idempotency** | Chạy lại nhiều lần cho cùng ngày → không duplicate |
| **Partitioning** | Có partition theo năm: `bronze_prices_2020`, `...2021` ... `...2026` |

**Trích xuất ví dụ dữ liệu thô tầng Bronze (bất kỳ mã nào):**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, open, high, low, close, volume, source
   FROM bronze.bronze_prices
   WHERE code = '<SYMBOL>'
   ORDER BY date DESC LIMIT 5;"

docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, open, high, low, close, volume, source
   FROM bronze.bronze_prices
   WHERE code = 'FPT'
   ORDER BY date DESC LIMIT 100;"
```
Thay `<SYMBOL>` bằng mã bất kỳ: `ACB`, `VNM`, `HPG`...

Kết quả ví dụ với mã ACB (dữ liệu thực tế 2026-06-24):
```
 code |    date    |  open   |  high   |   low   |  close  |  volume  |   source
------+------------+---------+---------+---------+---------+----------+-------------
 ACB  | 2026-06-24 | 22.3500 | 22.7000 | 22.3000 | 22.5000 | 10833000 | vnstock_vci
 ACB  | 2026-06-23 | 22.0000 | 22.8500 | 21.9000 | 22.3500 | 22629615 | vnstock_vci
 ACB  | 2026-06-22 | 22.2000 | 22.3000 | 21.9500 | 22.0000 |  9958486 | vnstock_vci
```

### 2.3. Tầng Silver — Chuẩn hóa và kiểm định (Cleansed)

| Thuộc tính | Chi tiết |
| :--- | :--- |
| **Schema DB** | `public_silver` |
| **Bảng chính** | `silver_prices`, `silver_index` |
| **Số dòng hiện tại** | 75,960 bản ghi (toàn bộ giữ nguyên, kể cả dòng lỗi) |
| **Phạm vi dữ liệu** | 80 mã cổ phiếu, từ `2020-06-11` đến `2026-06-24` |
| **Materialization** | `table` — DROP + CREATE mỗi lần dbt run |
| **Cột kiểm định** | `is_valid` (BOOLEAN) + `dq_flag` (TEXT — mô tả lý do lỗi) |
| **Quy tắc lọc lỗi** | `close <= 0`, `high < low`, `volume < 0`, bất kỳ cột NULL |

**Xem dữ liệu Silver bất kỳ mã:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume, is_valid, dq_flag
   FROM public_silver.silver_prices
   WHERE symbol = '<SYMBOL>'
   ORDER BY trade_date DESC LIMIT 5;"
```

**Xem các bản ghi lỗi trong Silver:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price, dq_flag
   FROM public_silver.silver_prices
   WHERE is_valid = false
   ORDER BY trade_date DESC LIMIT 10;"
```

Kết quả kiểm tra thực tế: **0 bản ghi lỗi** — dữ liệu từ vnstock_vci hiện đang rất sạch.

### 2.4. Tầng Gold — Chỉ số kỹ thuật (Business Intelligence)

| Thuộc tính | Chi tiết |
| :--- | :--- |
| **Schema DB** | `public_gold` |
| **Bảng fact** | `fact_stock_price`, `fact_stock_indicators`, `fact_market_summary` |
| **Bảng dim** | `dim_stock` |
| **Số dòng hiện tại** | `fact_stock_price`: 75,960 · `fact_stock_indicators`: 4,727 · `dim_stock`: 80 |
| **Materialization** | Incremental (delete+insert lookback 120 ngày) cho indicators |
| **Chỉ số tính toán** | MA5, MA20, RSI14, MACD Line, MACD Signal, MACD Histogram, Bollinger Bands |

**Xem chỉ số kỹ thuật của bất kỳ mã:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price, ma5, ma20, rsi_14, macd_line, macd_signal, macd_histogram
   FROM public_gold.fact_stock_indicators
   WHERE symbol = '<SYMBOL>'
   ORDER BY trade_date DESC LIMIT 10;"
```

---

## 3. Kiểm tra database: số dòng và bản ghi chi tiết

### 3.1. Lệnh đếm số dòng toàn bộ các tầng (1 lệnh)

```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT 'Bronze Prices' AS layer, COUNT(*) FROM bronze.bronze_prices
   UNION ALL SELECT 'Bronze Index', COUNT(*) FROM bronze.bronze_index
   UNION ALL SELECT 'Bronze VN30 Components', COUNT(*) FROM bronze.bronze_vn30_components
   UNION ALL SELECT 'Silver Prices', COUNT(*) FROM public_silver.silver_prices
   UNION ALL SELECT 'Silver Index', COUNT(*) FROM public_silver.silver_index
   UNION ALL SELECT 'Gold dim_stock', COUNT(*) FROM public_gold.dim_stock
   UNION ALL SELECT 'Gold fact_stock_price', COUNT(*) FROM public_gold.fact_stock_price
   UNION ALL SELECT 'Gold fact_indicators', COUNT(*) FROM public_gold.fact_stock_indicators
   UNION ALL SELECT 'Gold market_summary', COUNT(*) FROM public_gold.fact_market_summary;"
```

Kết quả thực tế (kiểm tra lúc 2026-06-24, sau backfill đầy đủ HOSE):

| Layer | Schema | Bảng | Số dòng |
| :--- | :--- | :--- | ---: |
| Bronze | `bronze` | `bronze_prices` | **514,942** |
| Bronze | `bronze` | `bronze_index` | 2,792 |
| Bronze | `bronze` | `bronze_vn30_components` | 30 |
| Silver | `public_silver` | `silver_prices` | **514,942** |
| Silver | `public_silver` | `silver_index` | 2,792 |
| Gold | `public_gold` | `dim_stock` | 401 |
| Gold | `public_gold` | `fact_stock_price` | **514,942** |
| Gold | `public_gold` | `fact_stock_indicators` | **4,727** |
| Gold | `public_gold` | `fact_market_summary` | 1,607 |

> **Nhận xét:** Bronze → Silver → Gold `fact_stock_price` đều có 514,942 dòng — dữ liệu đồng nhất 100%, không bị rơi rụng. `fact_stock_indicators` có ít hơn do cần warmup ~35 ngày lịch sử trước khi EMA26/MACD có thể tính.

### 3.2. Kiểm tra phạm vi ngày và số mã trong Bronze

```bash
# Bronze: phạm vi ngày và tổng số mã
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT COUNT(DISTINCT code) AS so_ma,
          MIN(date) AS ngay_bat_dau,
          MAX(date) AS ngay_cuoi_cung
   FROM bronze.bronze_prices;"
```

Kết quả thực tế: **401 mã**, từ `2020-01-14` đến `2026-06-24`.

### 3.3. Kiểm tra phạm vi ngày và số mã trong Silver

```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT COUNT(DISTINCT symbol) AS so_ma,
          MIN(trade_date) AS ngay_bat_dau,
          MAX(trade_date) AS ngay_cuoi_cung
   FROM public_silver.silver_prices;"
```
Kết quả: 401 mã, từ 2020-01-14 đến 2026-06-24.

### 3.4. Xem top 10 mã có nhiều phiên giao dịch nhất (Bronze)

```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, COUNT(*) AS so_phien,
          MIN(date) AS tu_ngay, MAX(date) AS den_ngay
   FROM bronze.bronze_prices
   GROUP BY code ORDER BY so_phien DESC LIMIT 10;"
```

### 3.5. Lấy bản ghi theo mã cổ phiếu cụ thể (Bronze)

```bash
# N ngày gần nhất của 1 mã
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, open, high, low, close, volume, source
   FROM bronze.bronze_prices
   WHERE code = 'VNM'
   ORDER BY date DESC LIMIT 200;"

docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT *
   FROM bronze.bronze_prices
   WHERE code = 'VNM'
   ORDER BY date DESC LIMIT 10;"

```

```bash
# Lấy trong khoảng ngày cụ thể
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, close, volume
   FROM bronze.bronze_prices
   WHERE code = 'HPG'
     AND date BETWEEN '2026-01-01' AND '2026-06-24'
   ORDER BY date;"
```

```bash
# Lấy nhiều mã cùng lúc
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, close, volume
   FROM bronze.bronze_prices
   WHERE code IN ('VNM', 'VCB', 'HPG', 'FPT')
     AND date = '2026-06-24'
   ORDER BY code;"
```

### 3.6. Lấy bản ghi Silver (đã chuẩn hóa) theo mã

```bash
# Silver với cột is_valid và dq_flag
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, open_price, high_price, low_price, close_price,
          volume, is_valid, dq_flag
   FROM public_silver.silver_prices
   WHERE symbol = 'ACB'
   ORDER BY trade_date DESC LIMIT 10;"

docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT *
   FROM public_silver.silver_prices
   WHERE symbol = 'VNM'
   ORDER BY trade_date DESC LIMIT 10;"



```

```bash
# Chỉ các bản ghi hợp lệ (is_valid = true)
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price
   FROM public_silver.silver_prices
   WHERE symbol = 'ACB' AND is_valid = true
   ORDER BY trade_date DESC LIMIT 10;"
```

```bash
# Xem bản ghi lỗi (nếu có)
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price, dq_flag
   FROM public_silver.silver_prices
   WHERE is_valid = false
   ORDER BY trade_date DESC LIMIT 10;"
```

### 3.7. Lấy bản ghi Gold — Giá và chỉ báo kỹ thuật

```bash
# fact_stock_price: N ngày gần nhất
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume
   FROM public_gold.fact_stock_price
   WHERE symbol = 'VNM'
   ORDER BY trade_date DESC LIMIT 10;"

docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT *
   FROM public_gold.fact_stock_price
   WHERE symbol = 'FPT'
   ORDER BY trade_date DESC LIMIT 10;"
```

```bash
# fact_stock_indicators: xem đầy đủ chỉ báo kỹ thuật (RSI, EMA, MACD, BB)
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price,
          ma5, ma20, rsi_14,
          ema_12, ema_26, macd_line, macd_signal, macd_histogram,
          bb_upper, bb_middle, bb_lower
   FROM public_gold.fact_stock_indicators
   WHERE symbol = 'HPG'
   ORDER BY trade_date DESC LIMIT 15;"

docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT *
   FROM public_gold.fact_stock_indicators
   WHERE symbol = 'VNM'
   ORDER BY trade_date DESC LIMIT 10;"
```

```bash
# fact_market_summary: thống kê thị trường theo ngày
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT trade_date, total_symbols, gainers, losers, unchanged,
          avg_close_price, total_volume
   FROM public_gold.fact_market_summary
   ORDER BY trade_date DESC LIMIT 10;"
```

```bash
# dim_stock: thông tin dimension của tất cả mã cổ phiếu
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, exchange, is_vn30
   FROM public_gold.dim_stock
   ORDER BY is_vn30 DESC, symbol
   LIMIT 30;"
```

### 3.8. Xem chỉ số thị trường (Silver/Bronze Index)

```bash
# Silver index (VNINDEX, VN30)
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, trade_date, open_price, close_price, volume
   FROM public_silver.silver_index
   ORDER BY trade_date DESC LIMIT 10;"
```

```bash
# Bronze index gốc (raw)
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, open, high, low, close, volume
   FROM bronze.bronze_index
   ORDER BY date DESC LIMIT 10;"
```

### 3.9. Xem toàn bộ lược đồ (schema) các bảng chính

```bash
# Xem cấu trúc cột bảng Bronze
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT column_name, data_type FROM information_schema.columns
   WHERE table_schema='bronze' AND table_name='bronze_prices'
   ORDER BY ordinal_position;"

# Xem cấu trúc cột bảng Silver
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT column_name, data_type FROM information_schema.columns
   WHERE table_schema='public_silver' AND table_name='silver_prices'
   ORDER BY ordinal_position;"

# Xem cấu trúc cột bảng Gold indicators
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT column_name, data_type FROM information_schema.columns
   WHERE table_schema='public_gold' AND table_name='fact_stock_indicators'
   ORDER BY ordinal_position;"
```

---

## 4. Khả năng chịu lỗi với data không chính xác

### 4.1. Cơ chế phòng thủ 3 tầng

```
Bronze (Lưu thô — không lọc, không từ chối)
  ↓
Silver (Gắn nhãn — is_valid=FALSE + dq_flag, giữ lại để audit)
  ↓
Gold (Lọc — chỉ nhận is_valid=TRUE từ Silver)
```

**Triết lý thiết kế:** Không xóa bản ghi lỗi khỏi Bronze/Silver — giữ lại để audit. Chỉ ngăn không cho bản ghi lỗi đi tiếp lên Gold phục vụ dashboard.

### 4.2. Các quy tắc kiểm định tầng Silver

| Quy tắc | Điều kiện gây lỗi | `dq_flag` |
| :--- | :--- | :--- |
| Giá đóng cửa không âm | `close_price <= 0` | `'invalid_close_price'` |
| Giá cao >= Giá thấp | `high_price < low_price` | `'high_lt_low'` |
| Volume không âm | `volume < 0` | `'negative_volume'` |
| Không có giá trị NULL | Bất kỳ cột OHLCV NULL | `'NULL_fields'` |

### 4.3. Ví dụ thực hành: Chèn bản ghi lỗi và quan sát cơ chế phòng thủ

**Bước 1 — Chèn bản ghi lỗi vào Bronze:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "INSERT INTO bronze.bronze_prices (code, date, open, high, low, close, volume, source, ingested_at)
   VALUES ('HPG', '2099-01-06', 70.0, 71.0, 69.0, -100.0, 500000, 'manual_test', now())
   ON CONFLICT (code, date) DO NOTHING;"
```

**Bước 2 — Chạy lại pipeline dbt để cập nhật Silver & Gold:**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt build --profiles-dir ."
```

**Bước 3 — Kiểm tra kết quả tại Silver:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price, is_valid, dq_flag
   FROM public_silver.silver_prices
   WHERE symbol = 'HPG' AND trade_date = '2099-01-06';"
```
Kết quả mong đợi: `is_valid = false`, `dq_flag = 'invalid_close_price'`

**Bước 4 — Xác nhận bản ghi lỗi không xuất hiện ở Gold:**
```bash
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT COUNT(*) FROM public_gold.fact_stock_price
   WHERE symbol = 'HPG' AND trade_date = '2099-01-06';"
```
Kết quả mong đợi: `0` — bản ghi lỗi bị chặn hoàn toàn.

### 4.4. Khả năng chịu lỗi tầng API (Rate Limit / Timeout)

| Tình huống | Cơ chế xử lý |
| :--- | :--- |
| **HTTP 429 Rate Limit** | Per-source rate limiter: mỗi source (vci/kbs) tự giới hạn 57 req/phút. Khi nhận 429 → pause 62 giây rồi retry |
| **Connection Timeout** | Provider retry 3 lần với exponential backoff |
| **Source vci lỗi** | Tự động rotate sang source kbs |
| **Cả 2 sources lỗi** | Ghi log lỗi, Airflow task FAIL, DAG retry 3 lần |
| **API trả về 0 bản ghi** | Pipeline ghi log `"0 rows fetched, skipping write."` — không INSERT gì vào Bronze |

---

## 5. Kịch bản DAG chi tiết từng trường hợp cụ thể

### 5.1. Cấu trúc 2 DAG chính

**DAG 1: `daily_stock_pipeline`** — Cấu hình lịch chạy tự động
```
Lịch: 0 11 * * 1-5 (UTC) = 18:00 Thứ 2 - Thứ 6 (giờ VN)
Tham số: run_vn30_only (boolean, mặc định False)
```

Luồng task:
```
health_check
  ├── fetch_prices_vn30 (LUÔN chạy — VN30 ưu tiên)
  │     └── fetch_prices_others (Bỏ qua nếu run_vn30_only=True)
  └── fetch_index (chạy song song)
        ↓ (Chờ tất cả nhánh cào dữ liệu hoàn thành)
dbt_run_silver
  └── dbt_test_silver
        └── dbt_run_gold
              └── dbt_test_gold
                    └── notify_success
```

**DAG 2: `manual_backfill_pipeline`** — Trigger thủ công
```
Tham số trigger (JSON):
{
  "start_date": "2021-01-01",
  "end_date": "2025-12-31",
  "run_vn30_only": false
}
```

### 5.2. Ma trận kết quả theo từng kịch bản

| # | Kịch bản | Trạng thái Task | Số dòng INSERT Bronze | Silver | Gold |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Daily, Thứ 3, `run_vn30_only=False` | ✅ Xanh toàn bộ | +~403 dòng (1 mã/ngày) | +403 dòng (`is_valid=t`) | +403 `fact_stock_price` + cập nhật indicators 120 ngày |
| 2 | Daily, Thứ 4, `run_vn30_only=True` | ✅ Xanh, `fetch_prices_others` SKIP | +30 dòng (VN30 only) | +30 dòng | +30 `fact_stock_price` |
| 3 | Daily, Thứ 7 (cuối tuần) | ✅ Xanh (thị trường đóng) | +0 dòng | Không thay đổi | Không thay đổi |
| 4 | Daily, API trả về dữ liệu bẩn | ✅ Xanh toàn bộ | +N dòng (kể cả dòng lỗi) | N dòng, bản ghi lỗi `is_valid=f` | Chỉ ghi bản ghi `is_valid=t` |
| 5 | Daily, API Timeout / 429 | ❌ `fetch_prices` ĐỎ, dbt tasks CAM (Upstream Failed) | 0 dòng (giữ nguyên cũ) | Không thay đổi | Không thay đổi |
| 6 | Daily, `dbt_test_gold` FAIL | ✅→❌ (`dbt_test_gold` ĐỎ) | +N dòng ghi vào | +N dòng | Đã ghi nhưng pipeline DỪNG, báo lỗi |
| 7 | Backfill `2021–2025`, `run_vn30_only=False` | ✅ Xanh | +lịch sử 403×D ngày (VN30 trước) | +(403×D) dòng | Tính toán lại indicators |
| 8 | Backfill `run_vn30_only=True` | ✅ Xanh, `backfill_others` SKIP | +30×D dòng | +30×D dòng | Indicators chỉ cho VN30 |
| 9 | Backfill khoảng toàn cuối tuần | ✅ Xanh (skip logic) | +0 dòng | Không thay đổi | Không thay đổi |

> **D = Số ngày giao dịch** trong khoảng `start_date` đến `end_date` (ngày T2–T6, trừ ngày nghỉ lễ)

### 5.3. Trigger thủ công DAG qua Terminal (Airflow CLI)

**Trigger Daily DAG với VN30 only:**
```bash
docker exec airflow-container airflow dags trigger daily_stock_pipeline \
  --conf '{"run_vn30_only": true}'
```

**Trigger Backfill DAG với khoảng thời gian:**
```bash
docker exec airflow-container airflow dags trigger manual_backfill_pipeline \
  --conf '{"start_date": "2026-06-01", "end_date": "2026-06-24", "run_vn30_only": false}'
```

**Xem trạng thái DAG run gần nhất:**
```bash
docker exec airflow-container airflow dags list-runs -d daily_stock_pipeline --limit 5
```

**Xem log của một task cụ thể:**
```bash
docker exec airflow-container airflow tasks logs daily_stock_pipeline fetch_prices_vn30 <RUN_ID>
```

### 5.4. Theo dõi tiến trình qua Web UI Airflow

URL truy cập: **http://localhost:8080** (username: `admin` / password: `admin`)

| Màu task | Ý nghĩa |
| :--- | :--- |
| 🟢 Xanh lá (Success) | Thực thi thành công |
| 🔴 Đỏ (Failed) | Thất bại — xem log để debug |
| 🟠 Cam nhạt (Upstream Failed) | Task phía trước fail nên không chạy được |
| ⚪ Xám (Skipped) | Bị bỏ qua theo điều kiện logic (ví dụ: `run_vn30_only=True` → `fetch_prices_others` skip) |
| 🟡 Vàng (Running) | Đang chạy |

---

## PHỤ LỤC: LỆNH KIỂM TRA NHANH (QUICK REFERENCE)

```bash
# === KIỂM TRA HẠ TẦNG ===
docker ps --filter "name=postgres" --filter "name=airflow" --format "{{.Names}} {{.Status}}"
docker exec postgres-container psql -U airflow -d stock_db -c "SELECT 1;"
docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt debug --profiles-dir ." 2>&1 | tail -3

# === ĐẾM DÒNG CÁC TẦNG ===
docker exec postgres-container psql -U airflow -d stock_db -c "
  SELECT 'Bronze' AS t, COUNT(*) FROM bronze.bronze_prices UNION ALL
  SELECT 'Silver', COUNT(*) FROM public_silver.silver_prices UNION ALL
  SELECT 'Gold FSP', COUNT(*) FROM public_gold.fact_stock_price UNION ALL
  SELECT 'Gold FSI', COUNT(*) FROM public_gold.fact_stock_indicators;"

# === XEM DỮ LIỆU CHI TIẾT MỘT MÃ ===
# Thay <SYMBOL> bằng mã cần xem: ACB, HPG, VNM...
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT code, date, close FROM bronze.bronze_prices WHERE code='<SYMBOL>' ORDER BY date DESC LIMIT 5;"
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price, is_valid, dq_flag FROM public_silver.silver_prices WHERE symbol='<SYMBOL>' ORDER BY trade_date DESC LIMIT 5;"
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, close_price, rsi_14, macd_line FROM public_gold.fact_stock_indicators WHERE symbol='<SYMBOL>' ORDER BY trade_date DESC LIMIT 5;"

# === KIỂM TRA LỖI DỮ LIỆU ===
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, dq_flag FROM public_silver.silver_prices WHERE is_valid=false LIMIT 20;"

# === THỐNG KÊ THỊ TRƯỜNG ===
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT trade_date, gainers, losers, unchanged FROM public_gold.fact_market_summary ORDER BY trade_date DESC LIMIT 5;"
```
