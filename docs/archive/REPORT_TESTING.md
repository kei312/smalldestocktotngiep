# BÁO CÁO KIỂM THỬ CHI TIẾT
## Vietnam Stock Market Data Engineering Pipeline
### (Testing Guide — Pytest · dbt Test · Log Reading · TEST_REPORTS.md)

> Tài liệu này hướng dẫn **cách kiểm thử toàn diện** dự án: chạy từng loại test, đọc và hiểu output, xử lý lỗi, và duy trì file `TEST_REPORTS.md` như một nhật ký audit.
>
> Tất cả output trong tài liệu này là **kết quả thực tế** được chạy và ghi lại vào 2026-06-24.

---

## MỤC LỤC

1. [Tổng quan chiến lược kiểm thử](#1-tổng-quan-chiến-lược-kiểm-thử)
2. [Kiểm thử tầng Python — pytest](#2-kiểm-thử-tầng-python--pytest)
3. [Kiểm thử tầng dữ liệu — dbt test](#3-kiểm-thử-tầng-dữ-liệu--dbt-test)
4. [Kiểm thử tích hợp — Airflow Task Test](#4-kiểm-thử-tích-hợp--airflow-task-test)
5. [Kiểm tra chéo chỉ số — Python Cross-check (verify_macd_g03.py)](#5-kiểm-tra-chéo-chỉ-số--python-cross-check)
6. [Đọc log Airflow DAG](#6-đọc-log-airflow-dag)
7. [Đọc log dbt](#7-đọc-log-dbt)
8. [Cách cập nhật TEST_REPORTS.md](#8-cách-cập-nhật-test_reportsmd)
9. [Quy trình xử lý khi test thất bại (Fail Protocol)](#9-quy-trình-xử-lý-khi-test-thất-bại-fail-protocol)

---

## 1. Tổng quan chiến lược kiểm thử

Dự án sử dụng **4 lớp kiểm thử** phối hợp với nhau:

```
Layer 1: Python Unit/Integration Tests (pytest)
  ↓ kiểm tra logic code Python (providers, ingestion, utils)
Layer 2: dbt Data Tests (schema.yml)
  ↓ kiểm tra ràng buộc dữ liệu trong PostgreSQL
Layer 3: Airflow Task Test
  ↓ kiểm tra luồng pipeline E2E
Layer 4: Python Cross-check (verify_macd_g03.py)
  ↓ xác minh độ chính xác công thức toán học
```

**Nguyên tắc quan trọng:**
- Mọi lần pass hoặc fail đều ghi vào `docs/TEST_REPORTS.md`
- Không bỏ qua warning — chúng có thể trở thành lỗi trong phiên bản tới
- Không tự ý sang task tiếp khi foundation chưa pass

---

## 2. Kiểm thử tầng Python — pytest

### 2.1. Cấu trúc test files

```
tests/
├── fixtures/
│   ├── mock_prices.csv       ← Dữ liệu giả lập giá cổ phiếu (2.9 MB)
│   ├── mock_prices_dq.csv    ← Dữ liệu có lỗi DQ cố tình (để test DQ Gate)
│   └── mock_index.csv        ← Dữ liệu giả lập chỉ số index (181 KB)
├── test_ingestion.py         ← 5 test cases: validate_dataframe, run_prices, run_index
└── test_providers.py         ← 5 test cases: health_check, exception mapping, MockProvider schema
```

### 2.2. Chạy pytest — lệnh và output thực tế

**Lệnh chạy toàn bộ test:**
```bash
cd /home/naeouad/deproject
PYTHONPATH=. ./venv/bin/pytest tests/ -v --tb=short
```

**Output thực tế (2026-06-24 — 10 passed):**
```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: /home/naeouad/deproject
plugins: anyio-4.14.0
collecting ... collected 10 items

tests/test_ingestion.py::test_validate_dataframe_success PASSED          [ 10%]
tests/test_ingestion.py::test_validate_dataframe_missing_column PASSED   [ 20%]
tests/test_ingestion.py::test_validate_dataframe_negative_price PASSED   [ 30%]
tests/test_ingestion.py::test_run_prices_success PASSED                  [ 40%]
tests/test_ingestion.py::test_run_index_success PASSED                   [ 50%]
tests/test_providers.py::test_vnstock_health_check PASSED                [ 60%]
tests/test_providers.py::test_vnstock_exception_mapping PASSED           [ 70%]
tests/test_providers.py::test_mock_provider_reads_correct_rows PASSED    [ 80%]
tests/test_providers.py::test_mock_provider_schema PASSED                [ 90%]
tests/test_providers.py::test_registry_returns_correct_provider PASSED   [100%]

=============================== warnings summary ===============================
  datetime.datetime.utcnow() is deprecated...

======================= 10 passed, 4 warnings in 35.53s ========================
```

### 2.3. Đọc và hiểu output pytest — từng phần

**Dòng tiêu đề:**
```
platform linux -- Python 3.12.3, pytest-9.1.1
```
→ Xác nhận Python version và pytest version đang dùng.

**Dòng collected:**
```
collecting ... collected 10 items
```
→ pytest tìm thấy 10 test functions. Nếu thấy `0 items` nghĩa là không tìm được file test (sai PYTHONPATH, hoặc tên file không bắt đầu bằng `test_`).

**Dòng kết quả từng test:**
```
tests/test_ingestion.py::test_validate_dataframe_success PASSED   [ 10%]
```
| Phần | Ý nghĩa |
| :--- | :--- |
| `tests/test_ingestion.py` | File chứa test |
| `::test_validate_dataframe_success` | Tên function test cụ thể |
| `PASSED` | Kết quả: PASS / FAILED / ERROR / SKIPPED |
| `[10%]` | Tiến độ: đã chạy 1/10 test |

**Warnings:**
```
datetime.datetime.utcnow() is deprecated and scheduled for removal...
```
→ Không phải lỗi nghiêm trọng, nhưng cần sửa dần: thay `datetime.utcnow()` bằng `datetime.now(datetime.UTC)`.

**Dòng tổng kết:**
```
10 passed, 4 warnings in 35.53s
```
→ 10 test pass, 4 warning (không phải fail), mất 35 giây (test P-01 health_check gọi network nên lâu).

### 2.4. Ý nghĩa của 10 test cases

**test_ingestion.py (5 test):**

| Test | Mục đích | Kịch bản |
| :--- | :--- | :--- |
| `test_validate_dataframe_success` | DataFrame hợp lệ phải đi qua validate mà không lỗi | Tạo df đủ cột, gọi `validate_dataframe()`, assert không empty |
| `test_validate_dataframe_missing_column` | Thiếu cột phải raise ValueError | Tạo df thiếu cột `volume`, gọi validate, expect `ValueError: Missing required columns` |
| `test_validate_dataframe_negative_price` | Giá âm phải raise ValueError | Tạo df có `open=-10`, expect `ValueError: non-positive value` |
| `test_run_prices_success` | `run_prices()` gọi provider → save → trả đúng số dòng | Mock provider=MockProvider, mock save, assert `rows > 0` và `ingested_at` có trong df |
| `test_run_index_success` | `run_index()` tương tự cho index | Mock provider=MockProvider, mock save, assert tương tự |

**test_providers.py (5 test):**

| Test | Mã | Mục đích |
| :--- | :--- | :--- |
| `test_vnstock_health_check` | P-01 | `health_check()` phải trả về bool (True hoặc False) |
| `test_vnstock_exception_mapping` | P-02 | HTTP 429 → `ProviderRateLimitError`; Timeout → `ProviderTimeoutError` |
| `test_mock_provider_reads_correct_rows` | P-03 | MockProvider đọc đúng 6 dòng (2 mã × 3 ngày) từ fixture |
| `test_mock_provider_schema` | P-04 | Output MockProvider có đúng 8 cột: code, date, open, high, low, close, volume, source |
| `test_registry_returns_correct_provider` | P-05 | `get_provider()` trả `MockProvider` khi config.provider='mock', `VnstockProvider` khi='vnstock' |

### 2.5. Chạy một test cụ thể

```bash
# Chỉ chạy 1 file
PYTHONPATH=. ./venv/bin/pytest tests/test_providers.py -v

# Chỉ chạy 1 test function
PYTHONPATH=. ./venv/bin/pytest tests/test_providers.py::test_mock_provider_schema -v

# Xem output chi tiết hơn (print statements) dùng -s
PYTHONPATH=. ./venv/bin/pytest tests/ -v -s

# Chạy và dừng ngay khi có lỗi đầu tiên
PYTHONPATH=. ./venv/bin/pytest tests/ -v -x
```

### 2.6. Đọc output khi test FAIL

Khi test thất bại, pytest in ra traceback chi tiết. Ví dụ (tình huống giả lập):

```
FAILED tests/test_providers.py::test_mock_provider_schema

================================= FAILURES =================================
_________________ test_mock_provider_schema _________________

    def test_mock_provider_schema():
        provider = MockProvider()
        df = provider.get_prices(["VCB"], date(2024, 1, 2), date(2024, 1, 2))
        expected_cols = {"code", "date", "open", "high", "low", "close", "volume", "source"}
>       assert set(df.columns) == expected_cols
E       AssertionError: assert {'code', 'date', 'open', 'high', 'low', 'close', 'volume'} == {..., 'source'}
E       Extra in left:  set()
E       Missing from left: {'source'}

tests/test_providers.py:52: AssertionError
```

**Cách đọc:**
1. `FAILED tests/test_providers.py::test_mock_provider_schema` → file và function bị fail
2. `>  assert set(df.columns) == expected_cols` → dòng code bị lỗi (dấu `>`)
3. `E  AssertionError: ...` → message lỗi cụ thể: thiếu cột `source`
4. `tests/test_providers.py:52` → dòng 52 trong file test

---

## 3. Kiểm thử tầng dữ liệu — dbt test

### 3.1. Chạy dbt test — lệnh và output thực tế

**Lệnh chạy toàn bộ dbt test:**
```bash
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --profiles-dir ."
```

**Output thực tế (2026-06-24 — 12 PASS):**
```
14:03:01  Running with dbt=1.10.22
14:03:01  Registered adapter: postgres=1.10.0

14:03:01  1 of 12 START test dbt_utils_expression_is_true_fact_market_summary_gainers_...  [RUN]
14:03:01  2 of 12 START test dbt_utils_expression_is_true_fact_stock_indicators_bb_upper_bb_lower  [RUN]
14:03:01  3 of 12 START test dbt_utils_expression_is_true_fact_stock_indicators_ma20_0  [RUN]
14:03:01  4 of 12 START test dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_0_AND_rsi_14_100  [RUN]

14:03:01  1 of 12 PASS dbt_utils_expression_is_true_fact_market_summary_...  [PASS in 0.39s]
14:03:01  2 of 12 PASS dbt_utils_expression_is_true_fact_stock_indicators_bb_upper_bb_lower  [PASS in 0.39s]
14:03:01  3 of 12 PASS dbt_utils_expression_is_true_fact_stock_indicators_ma20_0  [PASS in 0.39s]
14:03:01  4 of 12 PASS dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_0_AND_rsi_14_100  [PASS in 0.40s]

...

14:03:02  12 of 12 PASS unique_dim_stock_symbol  [PASS in 0.12s]

14:03:02  Finished running 12 data tests in 0 hours 0 minutes and 1.39 seconds (1.39s).
14:03:02  Completed successfully
14:03:02  Done. PASS=12 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=12
```

### 3.2. Đọc từng dòng output dbt test

**Dòng START:**
```
1 of 12 START test dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_0_AND_rsi_14_100  [RUN]
```
| Phần | Ý nghĩa |
| :--- | :--- |
| `1 of 12` | Đây là test thứ 1 trong tổng số 12 |
| `START test` | Bắt đầu chạy test |
| `dbt_utils_expression_is_true_` | Loại test: `expression_is_true` từ package `dbt_utils` |
| `fact_stock_indicators_` | Tên model đang được test |
| `rsi_14_0_AND_rsi_14_100` | Tóm tắt điều kiện: `rsi_14 >= 0 AND rsi_14 <= 100` |
| `[RUN]` | Đang thực thi |

**Dòng PASS:**
```
1 of 12 PASS dbt_utils_expression_is_true_...  [PASS in 0.39s]
```
→ Test thành công, mất 0.39 giây.

**Dòng tổng kết:**
```
Done. PASS=12 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=12
```
| Trạng thái | Ý nghĩa |
| :--- | :--- |
| `PASS` | Test pass — dữ liệu hợp lệ |
| `WARN` | Có warning (dbt warn severity) |
| `ERROR` | Test fail — dữ liệu vi phạm ràng buộc |
| `SKIP` | Test bị bỏ qua (model chưa chạy) |
| `NO-OP` | Model không cần rebuild |

### 3.3. Danh sách 12 dbt test và ý nghĩa

| # | Tên test (rút gọn) | Model | Quy tắc kiểm định |
| :--- | :--- | :--- | :--- |
| 1 | `expression_is_true_fact_market_summary_gainers+losers+unchanged=total` | `fact_market_summary` | Tổng gainers + losers + unchanged = total_symbols |
| 2 | `expression_is_true_fact_stock_indicators_bb_upper >= bb_lower` | `fact_stock_indicators` | Bollinger Band trên >= Band dưới |
| 3 | `expression_is_true_fact_stock_indicators_ma20 > 0` | `fact_stock_indicators` | MA20 phải dương (khi không NULL) |
| 4 | `expression_is_true_fact_stock_indicators_rsi_14 >= 0 AND <= 100` | `fact_stock_indicators` | RSI phải trong [0, 100] |
| 5 | `unique_combination_silver_index_index_code__trade_date` | `silver_index` | (index_code, trade_date) phải unique |
| 6 | `unique_combination_silver_prices_symbol__trade_date` | `silver_prices` | (symbol, trade_date) phải unique |
| 7 | `not_null_dim_stock_symbol` | `dim_stock` | Cột symbol không được NULL |
| 8 | `not_null_silver_index_index_code` | `silver_index` | index_code không được NULL |
| 9 | `not_null_silver_index_trade_date` | `silver_index` | trade_date không được NULL |
| 10 | `not_null_silver_prices_symbol` | `silver_prices` | symbol không được NULL |
| 11 | `not_null_silver_prices_trade_date` | `silver_prices` | trade_date không được NULL |
| 12 | `unique_dim_stock_symbol` | `dim_stock` | symbol trong dim_stock phải unique |

### 3.4. Chạy dbt test cho từng tầng riêng lẻ

```bash
# Chỉ test Silver layer
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --select models/silver --profiles-dir ."

# Chỉ test Gold layer
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --select models/gold --profiles-dir ."

# Test một model cụ thể
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --select fact_stock_indicators --profiles-dir ."

# Test và xem SQL query thực tế dbt đang chạy (debug mode)
docker exec airflow-container bash -c \
  "cd /opt/airflow/project/dbt && dbt test --select fact_stock_indicators --profiles-dir . --debug 2>&1 | grep 'select\|PASS\|FAIL' | head -20"
```

### 3.5. Khi dbt test FAIL — cách đọc lỗi

Ví dụ output khi RSI bị vượt 100 (tình huống giả lập):
```
14:03:01  1 of 12 FAIL dbt_utils_expression_is_true_fact_stock_indicators_rsi_14_0_AND_rsi_14_100  [FAIL 3 in 0.41s]

...

Done. PASS=11 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=12
```

**Cách tìm dòng lỗi:**
```bash
# Tìm các bản ghi vi phạm trực tiếp trong DB
docker exec postgres-container psql -U airflow -d stock_db -c \
  "SELECT symbol, trade_date, rsi_14
   FROM public_gold.fact_stock_indicators
   WHERE rsi_14 IS NOT NULL AND (rsi_14 < 0 OR rsi_14 > 100)
   LIMIT 10;"
```

---

## 4. Kiểm thử tích hợp — Airflow Task Test

### 4.1. Chạy từng task riêng lẻ không qua DAG scheduler

```bash
# Chạy task fetch_prices của DAG daily_stock_pipeline cho ngày 2026-06-18
docker exec airflow-container airflow tasks test daily_stock_pipeline fetch_prices_vn30 2026-06-18

# Chạy task fetch_index
docker exec airflow-container airflow tasks test daily_stock_pipeline fetch_index 2026-06-18
```

**Output khi thành công:**
```
[2026-06-18, 12:05:00 UTC] {taskinstance.py:1234} INFO - Dependencies all met
[2026-06-18, 12:05:01 UTC] {bash.py:171} INFO - Running command: python -m ingestion.fetch_prices ...
[2026-06-18, 12:05:45 UTC] {bash.py:185} INFO - Output:
INFO:ingestion.fetch_prices:Fetching 30 symbols from 2026-06-18 to 2026-06-18
INFO:ingestion.fetch_prices:Upserted 30 rows into bronze.bronze_prices
[2026-06-18, 12:05:46 UTC] {taskinstance.py:1456} INFO - Marking task as SUCCESS
```

**Đọc log task:**
- `[timestamp] {module:line} LEVEL - message`
- Level: `INFO` (thông thường), `WARNING` (cảnh báo), `ERROR` (lỗi)
- `Marking task as SUCCESS` → task pass

### 4.2. Trigger DAG thủ công qua CLI

```bash
# Trigger với cấu hình VN30 only
docker exec airflow-container airflow dags trigger daily_stock_pipeline \
  --conf '{"run_vn30_only": true}'

# Trigger backfill với khoảng thời gian
docker exec airflow-container airflow dags trigger manual_backfill_pipeline \
  --conf '{"start_date": "2026-06-01", "end_date": "2026-06-24", "run_vn30_only": false}'
```

---

## 5. Kiểm tra chéo chỉ số — Python Cross-check

### 5.1. Script verify_macd_g03.py

Script này **tính lại MACD bằng Python (pandas)** và so sánh với kết quả SQL trong DB để xác minh công thức đúng.

**Vị trí:** `scripts/verify_macd_g03.py`

**Cách chạy:**
```bash
cd /home/naeouad/deproject
PYTHONPATH=. ./venv/bin/python scripts/verify_macd_g03.py
```

**Output mong đợi (đã verify 2026-06-24):**
```
=== MACD Verification G-03 ===
Symbol: VNM — SKIP (only 8 rows, need 35+ for MACD Signal)
Symbol: VCB — SKIP (only 8 rows, need 35+ for MACD Signal)
Symbol: HPG — PASS
  line: 1336 rows compared, max error = 0.0000%
  signal: 1328 rows compared, max error = 0.0000%
```

**Ý nghĩa:**
- `SKIP`: Mã này có quá ít dữ liệu (<35 phiên) để MACD Signal có giá trị — đây là warmup period bình thường
- `PASS`: SQL và Python cho kết quả giống nhau 100% (sai số 0%)

---

## 6. Đọc log Airflow DAG

### 6.1. Vị trí log

Airflow lưu log theo cấu trúc:
```
/opt/airflow/logs/
  └── dag_id=daily_stock_pipeline/
        └── run_id=manual__2026-06-24T12:00:00+00:00/
              └── task_id=fetch_prices_vn30/
                    └── attempt=1.log
```

### 6.2. Xem log qua Web UI (cách dễ nhất)

1. Truy cập `http://localhost:8080`
2. Chọn DAG **daily_stock_pipeline** → click vào **DAG Run** (dòng có ngày chạy)
3. Click vào task bị lỗi (màu đỏ/vàng)
4. Tab **Log** → xem toàn bộ stdout/stderr của task

### 6.3. Xem log qua Terminal

```bash
# Xem log của task cụ thể trong container
docker exec airflow-container bash -c \
  "cat /opt/airflow/logs/dag_id=daily_stock_pipeline/run_id=*/task_id=fetch_prices_vn30/attempt=1.log 2>/dev/null | tail -50"

# Xem log scheduler (toàn bộ DAG parsing)
docker logs airflow-container 2>&1 | grep -E "ERROR|FAIL|WARNING" | tail -20
```

### 6.4. Giải mã màu sắc trạng thái Task trong Airflow UI

| Màu | Trạng thái | Ý nghĩa | Hành động |
| :--- | :--- | :--- | :--- |
| 🟢 Xanh lá | `success` | Chạy thành công | Không cần làm gì |
| 🔴 Đỏ | `failed` | Thất bại | Xem log, fix code, retry |
| 🟡 Vàng | `running` | Đang chạy | Đợi hoặc kiểm tra tiến trình |
| 🟠 Cam nhạt | `upstream_failed` | Task phía trước fail | Fix task phía trước rồi clear |
| ⚫ Xám | `skipped` | Bị bỏ qua theo logic | Kiểm tra điều kiện trigger |
| 🔵 Xanh nhạt | `up_for_retry` | Đang chờ retry | Airflow tự retry — đợi |
| 🟣 Tím | `queued` | Đang chờ worker | Worker bận — đợi |

### 6.5. Giải mã log pattern thường gặp

**Pattern SUCCESS:**
```
INFO - Fetching 30 symbols from 2026-06-24 to 2026-06-24
INFO - Source: vci, batch 1/30
INFO - Upserted 30 rows into bronze.bronze_prices
INFO - Task completed successfully
```

**Pattern RATE LIMIT (HTTP 429):**
```
WARNING - HTTP 429 Too Many Requests from source=vci
WARNING - Pausing 62 seconds before retry...
INFO - Rotating to source=kbs
INFO - Successfully fetched after rotation
```
→ Hệ thống tự xử lý — không cần can thiệp thủ công nếu trong 3 lần retry.

**Pattern FAIL nghiêm trọng:**
```
ERROR - ConnectionError: Failed to connect to vnstock API after 3 retries
ERROR - Traceback (most recent call last):
ERROR -   File "ingestion/fetch_prices.py", line 85, in run_prices
ERROR -     raise ProviderTimeoutError(...)
ERROR - ProviderTimeoutError: Timeout after 3 retries
```
→ Ghi vào TEST_REPORTS.md, chuyển PROVIDER=mock nếu cần tiếp tục.

---

## 7. Đọc log dbt

### 7.1. Vị trí log dbt

```
dbt/logs/dbt.log    ← Log đầy đủ của TẤT CẢ lần chạy dbt (append)
```

**Xem log dbt:**
```bash
# Xem 100 dòng cuối
tail -100 /home/naeouad/deproject/dbt/logs/dbt.log

# Tìm lỗi trong log dbt
grep -E "ERROR|FAIL|Exception" /home/naeouad/deproject/dbt/logs/dbt.log | tail -20

# Xem log của lần chạy gần nhất (cách 2: xem trong container)
docker exec airflow-container bash -c "tail -100 /opt/airflow/project/dbt/logs/dbt.log"
```

### 7.2. Cấu trúc log dbt

```
2026-06-24 14:03:01.204 [info ] [MainThread]: Running with dbt=1.10.22
2026-06-24 14:03:01.211 [info ] [MainThread]: Registered adapter: postgres=1.10.0
2026-06-24 14:03:01.456 [info ] [Thread-5   ]: 1 of 12 START test dbt_utils_expression_is_true_...
2026-06-24 14:03:01.893 [info ] [Thread-5   ]: 1 of 12 PASS dbt_utils_expression_is_true_... [PASS in 0.39s]
```

| Phần | Ý nghĩa |
| :--- | :--- |
| `2026-06-24 14:03:01.204` | Timestamp chính xác đến millisecond |
| `[info ]` / `[error]` / `[warn ]` | Log level |
| `[MainThread]` / `[Thread-5]` | Thread thực thi (dbt chạy parallel) |

### 7.3. Khi dbt run gặp lỗi — pattern lỗi thường gặp

**Lỗi kết nối DB:**
```
[error] [MainThread]: Database Error in model silver_prices (models/silver/silver_prices.sql)
  could not translate host name "localhost" to address
```
→ Fix: Kiểm tra DB_HOST trong `.env`, đảm bảo container Postgres đang chạy.

**Lỗi syntax SQL trong model:**
```
[error] [MainThread]: Compilation Error in model fact_stock_indicators
  syntax error at or near "WITH"
```
→ Fix: Xem file SQL, kiểm tra cú pháp.

**Lỗi thiếu column (contract bị phá vỡ):**
```
[error] [Thread-1]: column "is_valid" does not exist
  LINE 1: ...WHERE is_valid = TRUE
```
→ Fix: Chạy lại `dbt run --select models/silver` trước, sau đó mới chạy Gold.

---

## 8. Cách cập nhật TEST_REPORTS.md

### 8.1. Nguyên tắc ghi log

File `docs/TEST_REPORTS.md` là **nhật ký audit bắt buộc**. Ghi sau mỗi lần chạy pytest / dbt test / SQL verification.

### 8.2. Template một entry

```markdown
### [Task ID] — Tên task ngắn gọn
- Lệnh:      `lệnh chạy đầy đủ`
- Kết quả:   `10 passed, 4 warnings in 35.53s` (tóm tắt output)
- Thời gian: YYYY-MM-DD HH:MM (giờ địa phương)
- Trạng thái: ✅ PASS | ❌ FAIL | 🔧 FAIL → FIXED (lần 2 pass)
```

### 8.3. Ví dụ entry thực tế (sau khi chạy ngày 2026-06-24)

```markdown
### [Daily Check] — Pytest + dbt test (2026-06-24)
- Lệnh:      `PYTHONPATH=. ./venv/bin/pytest tests/ -v`
- Kết quả:   `10 passed, 4 warnings in 35.53s`
- Thời gian: 2026-06-24 14:03
- Trạng thái: ✅ PASS

### [Daily Check] — dbt test toàn bộ (2026-06-24)
- Lệnh:      `docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt test --profiles-dir ."`
- Kết quả:   `Done. PASS=12 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=12`
- Thời gian: 2026-06-24 14:03
- Trạng thái: ✅ PASS
```

### 8.4. Khi có FAIL — phải ghi cả Fail Log

```markdown
## Fail Log

### [Task 2.3.2b] — Fail attempt
- Lỗi gốc:        `function round(double precision, integer) does not exist`
- Gotcha tra cứu:  Không khớp gotcha nào trong SKILL_sql_indicators.md
- Hành động fix:   Cast về ::NUMERIC trước khi gọi ROUND() trong PostgreSQL
- Kết quả sau fix: 🔧 FAIL → FIXED (int_rsi14 build thành công 330 rows)
```

---

## 9. Quy trình xử lý khi test thất bại (Fail Protocol)

Theo `AGENTS.md` Section 2.5 — Thứ tự bắt buộc:

```
Bước 1: Đọc FULL error message + stack trace
   ↓
Bước 2: Tra bảng "Quick Reference" trong SKILL file liên quan
   ├── SKILL_sql_indicators.md  (nếu lỗi dbt test / SQL indicator)
   └── SKILL_dbt_incremental.md (nếu lỗi incremental config)
   ↓
Bước 3A: Lỗi KHỚP gotcha đã biết?
   → Sửa theo hướng dẫn SKILL (1 lần duy nhất)
   → Ghi vào TEST_REPORTS.md (Fail Log)
   → Chạy lại test
   → PASS → ghi kết quả → tiếp tục
   → FAIL → chuyển sang Bước 4
   ↓
Bước 3B: Lỗi KHÔNG KHỚP gotcha nào?
   → Chuyển thẳng sang Bước 4
   ↓
Bước 4: DỪNG HOÀN TOÀN
   → Ghi lỗi đầy đủ vào TEST_REPORTS.md
   → Báo user paste error message + bước đang làm
   → KHÔNG tự ý tiếp tục task tiếp theo
```

**Lý do nghiêm ngặt:** Code tài chính — sai công thức tính RSI hay MACD mà không phát hiện **nguy hiểm hơn dừng lại và hỏi**.

---

## PHỤ LỤC: Lệnh kiểm thử nhanh (One-liner)

```bash
# === CHẠY TOÀN BỘ TEST PYTHON ===
cd /home/naeouad/deproject && PYTHONPATH=. ./venv/bin/pytest tests/ -v

# === CHẠY DÁN BỘ DBT TEST ===
docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt test --profiles-dir ."

# === VERIFY MACD CROSS-CHECK ===
cd /home/naeouad/deproject && PYTHONPATH=. ./venv/bin/python scripts/verify_macd_g03.py

# === CHẠY TOÀN BỘ (PYTEST + DBT TEST) VÀ GHI LOG ===
echo "=== PYTEST $(date) ===" >> /tmp/test_run.log
cd /home/naeouad/deproject && PYTHONPATH=. ./venv/bin/pytest tests/ -v 2>&1 | tee -a /tmp/test_run.log
echo "=== DBT TEST $(date) ===" >> /tmp/test_run.log
docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt test --profiles-dir ." 2>&1 | tee -a /tmp/test_run.log
cat /tmp/test_run.log | grep -E "passed|failed|PASS|FAIL|WARN|ERROR"
```

---

## Trạng thái kiểm thử hiện tại (2026-06-24)

| Loại test | Lệnh | Kết quả | Trạng thái |
| :--- | :--- | :--- | :--- |
| pytest (10 cases) | `PYTHONPATH=. ./venv/bin/pytest tests/ -v` | `10 passed, 4 warnings in 35.53s` | ✅ PASS |
| dbt test (12 tests) | `dbt test --profiles-dir .` | `PASS=12 WARN=0 ERROR=0 TOTAL=12` | ✅ PASS |
| MACD cross-check | `python scripts/verify_macd_g03.py` | `HPG: 0.0000% error (line+signal)` | ✅ PASS |
| DAG E2E (lần 1) | Trigger `daily_stock_pipeline` | 8/8 tasks SUCCESS | ✅ PASS |
| DAG E2E (lần 2) | Trigger (idempotency check) | 8/8 tasks SUCCESS, 0 duplicate | ✅ PASS |
