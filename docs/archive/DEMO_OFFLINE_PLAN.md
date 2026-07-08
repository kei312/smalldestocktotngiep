# Kế hoạch Demo Sản phẩm (Chế độ Offline & Cô lập Database)

Tài liệu này hướng dẫn chi tiết cách chạy demo sản phẩm E2E pipeline dữ liệu chứng khoán trong môi trường **hoàn toàn không có mạng Internet (offline)**, trình diễn rõ ràng **Input/Output của từng layer (Bronze -> Silver -> Gold)** và đảm bảo **tuyệt đối không ảnh hưởng đến database phát triển hiện tại**.

Đặc biệt, dự án cung cấp bộ công cụ tự động hóa **[demo_helper.py](file://wsl.localhost/Ubuntu/home/naeouad/deproject/scripts/demo_helper.py)** để chuyển đổi môi trường và làm sạch dữ liệu nhanh chóng chỉ với 1 câu lệnh.

---

## 1. Nguyên tắc Demo & Cơ chế hoạt động

Để bảo vệ và trình diễn dự án một cách an toàn nhất, kế hoạch demo dựa trên hai trụ cột:
1. **MockProvider (Offline Mode):** Khi cấu hình biến môi trường `PROVIDER=mock`, hệ thống sẽ tự động chuyển sang đọc dữ liệu mẫu từ các file CSV fixture có sẵn trong máy (`mock_prices.csv` và `mock_index.csv`), giả lập y hệt hành vi cào dữ liệu từ API Vnstock mà không cần kết nối mạng.
2. **Database-level Isolation (Cô lập Database):** Tạo một database demo riêng biệt là `stock_db_demo` để chạy toàn bộ pipeline. Điều này giúp:
   - Giữ nguyên dữ liệu thật và lịch sử chạy cũ của bạn trong database `stock_db`.
   - Tạo một giao diện Airflow và Database sạch sẽ, dễ theo dõi cho hội đồng chấm.
   - Dễ dàng làm sạch/reset dữ liệu để chạy lại demo nhiều lần mà không cần xóa database hay restart container.

---

## 2. Chuẩn bị môi trường Demo (Nhanh & Tự động)

Thực hiện các bước sau trước khi hội đồng vào đánh giá:

### Bước 2.1: Tạo Database Demo (Chỉ cần chạy 1 lần duy nhất)
Kết nối vào Postgres container và tạo database `stock_db_demo`:
```bash
docker exec -it postgres-container psql -U airflow -d stock_db -c "CREATE DATABASE stock_db_demo;"
```

### Bước 2.2: Chuyển cấu hình sang môi trường Demo
Chạy công cụ helper để tự động sửa đổi cấu hình trong file `.env` (chuyển sang `stock_db_demo` và `PROVIDER=mock`):
```bash
python scripts/demo_helper.py switch-demo
```

### Bước 2.3: Khởi động lại Docker Compose để áp dụng cấu hình
```bash
docker compose down
docker compose up -d
```
*Đợi khoảng 30 - 45 giây để Airflow container khởi động hoàn tất và tự tạo tài khoản admin trên database demo.*

### Bước 2.4: Khởi tạo schema và Reset dữ liệu rỗng
Làm sạch database demo và nạp lại cấu hình schema Bronze chuẩn:
```bash
python scripts/demo_helper.py reset
```

---

## 3. Kịch bản Demo từng phần (Show Input & Output)

Trình bày cho hội đồng theo luồng đi tuần tự của dữ liệu. Bạn có thể mở 2 cửa sổ terminal: một bên chạy lệnh và một bên chạy câu lệnh kiểm tra trạng thái dữ liệu.

### 🔍 Kiểm tra trạng thái rỗng trước khi chạy
Chạy lệnh hiển thị thống kê dữ liệu hiện tại (tất cả các bảng đều phải rỗng hoặc chưa tạo):
```bash
python scripts/demo_helper.py status
```

---

### 🌟 PHẦN 1: Ingestion Layer (Bronze Layer - Dữ liệu thô)

* **Input (Dữ liệu fixture):** 
  - Chỉ ra file CSV fixture chứa dữ liệu thô tại: `tests/fixtures/mock_prices.csv`
  - Show 5 dòng đầu của file CSV này cho hội đồng xem bằng lệnh:
    ```bash
    head -n 5 tests/fixtures/mock_prices.csv
    ```

* **Thực thi Ingestion (Chạy thủ công):**
  Chạy script python để cào dữ liệu cho 2 mã cổ phiếu (ví dụ: `VNM`, `ACB`) từ ngày `2024-01-01` đến `2024-01-05`:
  ```bash
  docker exec -it airflow-container python -m ingestion.fetch_prices --start 2024-01-01 --end 2024-01-05 --symbols VNM ACB
  ```

* **Output (Bronze Database):**
  Truy vấn dữ liệu thô vừa chèn vào:
  ```bash
  docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "SELECT code, date, open, high, low, close, volume, source, ingested_at FROM bronze.bronze_prices LIMIT 5;"
  ```
  👉 **Điểm nhấn cần giải thích với Hội đồng:**
  - Cột `source` hiển thị giá trị là `mock` (thể hiện đang chạy offline).
  - Có cột `raw_json` lưu trữ định dạng JSONB nguyên bản từ API để phục vụ đối soát.
  - Dữ liệu được ghi vào các bảng phân vùng vật lý (Partition) theo năm (ví dụ: `bronze.bronze_prices_2024`).

---

### 🌟 PHẦN 2: Clean & Validate Layer (Silver Layer - Chuẩn hóa dữ liệu)

* **Input:** Bảng dữ liệu thô `bronze.bronze_prices`.
* **Thực thi biến đổi (dbt Run):**
  Chạy dbt để chuẩn hóa kiểu dữ liệu, viết hoa mã chứng khoán và kiểm tra chất lượng:
  ```bash
  docker exec -it airflow-container bash -c "cd /opt/airflow/project/dbt && dbt run --select models/silver --profiles-dir ."
  ```

* **Output (Silver Database):**
  Kiểm tra dữ liệu đã chuẩn hóa trong schema `public_silver`:
  ```bash
  docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "SELECT symbol, trade_date, open_price, close_price, is_valid, dq_flag FROM public_silver.silver_prices LIMIT 5;"
  ```

* **🔥 Kịch bản Demo Data Quality (Điểm cộng cực lớn):**
  Chứng minh hệ thống tự động phát hiện và đánh dấu dữ liệu lỗi:
  1. Chèn thủ công một dòng dữ liệu lỗi (giá đóng cửa âm) vào Bronze:
     ```bash
     docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "INSERT INTO bronze.bronze_prices (code, date, open, high, low, close, volume, source, ingested_at) VALUES ('VNM', '2024-01-06', 70000, 71000, 69000, -100, 500000, 'mock', now());"
     ```
  2. Chạy lại dbt model silver:
     ```bash
     docker exec -it airflow-container bash -c "cd /opt/airflow/project/dbt && dbt run --select models/silver --profiles-dir ."
     ```
  3. Query dòng dữ liệu lỗi đó ở Silver:
     ```bash
     docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "SELECT symbol, trade_date, close_price, is_valid, dq_flag FROM public_silver.silver_prices WHERE trade_date = '2024-01-06';"
     ```
     *Kết quả mong đợi:* Dòng này có cột `is_valid = false` và cột `dq_flag = 'invalid_close_price'`. Điều này chứng tỏ dữ liệu lỗi đã bị gắn cờ và loại khỏi bảng Gold.

---

### 🌟 PHẦN 3: Business & Indicator Layer (Gold Layer - Thứ cấp & Chỉ số)

* **Input:** View `public_silver.silver_prices` (chỉ lấy các dòng có `is_valid = true`).
* **Thực thi dbt Run:**
  Chạy dbt để build các Dimension, Fact tables và tính toán các chỉ số phân tích kỹ thuật:
  ```bash
  docker exec -it airflow-container bash -c "cd /opt/airflow/project/dbt && dbt run --select models/gold --profiles-dir ."
  ```

* **Output (Gold Database):**
  1. **Bảng Fact giá cổ phiếu sạch** (Loại hoàn toàn bản ghi lỗi ở ngày 2024-01-06 vừa test):
     ```bash
     docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "SELECT * FROM public_gold.fact_stock_price WHERE symbol = 'VNM' AND trade_date >= '2024-01-05';"
     ```
     *Kết quả mong đợi:* Chỉ hiển thị ngày `2024-01-05` (hợp lệ), ngày `2024-01-06` (lỗi) đã bị lọc sạch.
     
  2. **Bảng Fact chỉ số kỹ thuật** (MA5, MA20, RSI, MACD, Bollinger Bands):
     ```bash
     docker exec -it postgres-container psql -U airflow -d stock_db_demo -c "SELECT symbol, trade_date, close_price, ma5, rsi14, macd_line, macd_signal FROM public_gold.fact_stock_indicators LIMIT 5;"
     ```
     👉 **Điểm nhấn cần giải thích với Hội đồng:**
     - Chỉ số kỹ thuật được tính toán tự động qua dbt macros.
     - Phép toán MACD Signal sử dụng EMA9 chuẩn xác (đệ quy thực thụ) chứ không dùng SMA9 xấp xỉ sai số lớn.

---

### 🌟 PHẦN 4: Điều phối tự động (Airflow E2E Orchestration)

Trình diễn chạy tự động toàn bộ luồng trên giao diện đồ họa:

1. Mở trình duyệt và truy cập Airflow UI tại: `http://localhost:8080` (Tài khoản: `admin` / Mật khẩu: `admin`).
2. Tìm DAG có tên `dag_daily`.
3. Nhấp vào nút **Trigger DAG** để bắt đầu chạy pipeline.
4. Chuyển sang tab **Graph** hoặc **Grid View** để chỉ cho hội đồng thấy các task chạy tuần tự từ cào đến kiểm thử và tổng hợp chỉ số.

---

## 4. Reset và Chạy lại Demo nhiều lần

Nếu bạn cần demo cho nhiều nhóm chấm khác nhau hoặc chạy thử lại nhiều lần:
**KHÔNG cần xóa database hay restart container docker.**
Bạn chỉ cần chạy duy nhất lệnh sau để làm sạch toàn bộ dữ liệu về trạng thái ban đầu:
```bash
python scripts/demo_helper.py reset
```
Hệ thống sẽ dọn sạch 3 schema dữ liệu và tái tạo cấu trúc rỗng trong vòng chưa đầy 1 giây! Bạn có thể dùng `python scripts/demo_helper.py status` để kiểm tra kết quả reset.

---

## 5. Dọn dẹp & Khôi phục sau Demo

Sau khi hoàn thành xuất sắc các buổi demo, hãy khôi phục lại database phát triển:

### Bước 5.1: Khôi phục cấu hình môi trường Thật
Chạy lệnh helper để khôi phục cấu hình trong file `.env`:
```bash
python scripts/demo_helper.py switch-real
```

### Bước 5.2: Khởi động lại Docker Compose
```bash
docker compose down
docker compose up -d
```

### Bước 5.3: Xóa database demo để giải phóng bộ nhớ
```bash
docker exec -it postgres-container psql -U airflow -d stock_db -c "DROP DATABASE stock_db_demo;"
```

Hệ thống đã quay trở lại trạng thái phát triển bình thường với toàn bộ dữ liệu lịch sử và lịch sử chạy Airflow nguyên vẹn!
