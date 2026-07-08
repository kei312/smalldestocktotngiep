# BÁO CÁO BẰNG CHỨNG KIỂM THỬ DAG VÀ DATA QUALITY (MẪU)
**Ngày thực hiện:** 2026-06-26
**Môi trường:** Real / Demo
**Người thực hiện:** QA / Data Engineer

---

## 1. Bằng chứng Kịch bản Thành công (Happy Path - Dữ liệu thật)

*Mục tiêu: Đảm bảo dữ liệu chạy xuyên suốt 3 tầng (Bronze -> Silver -> Gold) không bị rớt dòng và logic tính toán chỉ số (dbt test) đều hợp lệ trên dữ liệu thật.*

### 1.1 Kiểm tra dòng chảy dữ liệu (Row Count Cross-check)

**Lệnh thực thi:**
```bash
docker exec -it postgres-container psql -U airflow -d stock_db -c "
SELECT '1. Bronze' as layer, COUNT(*) as row_count FROM bronze.bronze_prices
UNION ALL
SELECT '2. Silver (Hợp lệ)', COUNT(*) FROM public_silver.silver_prices WHERE is_valid=true
UNION ALL
SELECT '3. Gold', COUNT(*) FROM public_gold.fact_stock_price
ORDER BY layer;
"
```

**Output mẫu (Bằng chứng):**
```text
        layer        | row_count 
---------------------+-----------
 1. Bronze           |     87421
 2. Silver (Hợp lệ)  |     87421
 3. Gold             |     87421
(3 rows)
```
**Kết luận:** Số lượng dòng khớp 100% giữa các tầng, không có dữ liệu nào bị thất thoát.

### 1.2 Kiểm tra logic và ràng buộc (dbt Data Tests)

**Lệnh thực thi:**
```bash
docker exec -it airflow-container bash -c "cd /opt/airflow/project/dbt && dbt test --profiles-dir ."
```

**Output mẫu (Bằng chứng):**
```text
11:32:15  Running with dbt=1.7.13
11:32:15  Registered adapter: postgres=1.7.13
11:32:15  Found 5 models, 10 tests, 0 snapshots, 0 analyses, 305 macros, 0 operations, 0 seed files, 1 source, 0 exposures, 0 metrics
11:32:15  
11:32:16  Concurrency: 4 threads (target='dev')
11:32:16  
11:32:16  1 of 10 START test dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_... [RUN]
11:32:16  2 of 10 START test dbt_utils_expression_is_true_fact_stock_indicators_ma200... [RUN]
11:32:16  3 of 10 START test dbt_utils_expression_is_true_fact_stock_indicators_bb... [RUN]
...
11:32:20  10 of 10 PASS unique_dim_stock_symbol .................................... [PASS in 0.08s]
11:32:20  
11:32:20  Finished running 10 tests in 4.82s.
11:32:20  
11:32:20  Completed successfully
11:32:20  
11:32:20  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```
**Kết luận:** Mọi logic toán học (RSI từ 0-100, MA200 > 0) và tính toàn vẹn (Unique, Not Null) đều đạt chuẩn.

---

## 2. Bằng chứng Kịch bản Lỗi Data Quality (Môi trường Demo)

*Mục tiêu: Đảm bảo Data Quality Gate ở tầng Silver hoạt động đúng, bắt được dòng dữ liệu bẩn và gắn cờ `is_valid=false` thay vì báo lỗi hệ thống.*

**Bước 1: Chèn dữ liệu bẩn (Giá đóng cửa âm -5000)**
```bash
docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "
INSERT INTO bronze.bronze_prices (code, date, open, high, low, close, volume, source, ingested_at) 
VALUES ('FPT', '2026-06-26', 100, 102, 99, -5000, 10000, 'mock', now());
"
```

**Bước 2: Kích hoạt Data Quality Gate (dbt run Silver)**
```bash
docker exec -it airflow-container bash -c "cd /opt/airflow/project/dbt && dbt run --models silver --profiles-dir ."
```

**Bước 3: Lấy bằng chứng từ Database (Kiểm tra Flag)**
```bash
docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "
SELECT symbol, trade_date, close_price, is_valid, dq_flag 
FROM public_silver.silver_prices WHERE close_price < 0;
"
```

**Output mẫu (Bằng chứng):**
```text
 symbol | trade_date | close_price | is_valid | dq_flag 
--------+------------+-------------+----------+---------------------
 FPT    | 2026-06-26 |       -5000 | f        | invalid_close_price
(1 row)
```
**Kết luận:** Gate tầng Silver đã hoạt động chính xác, tự động nhận diện giá trị `-5000` là lỗi và đánh cờ `invalid_close_price`. Dòng này sẽ bị loại bỏ ở tầng Gold, bảo vệ an toàn cho dữ liệu Report.
