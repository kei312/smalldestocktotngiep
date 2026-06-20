# QA & Knowledge Log — Quá Trình Gỡ Lỗi và Nghiệm Thu Hệ Thống

> **Mục đích:** File này lưu trữ chi tiết các lỗi, quá trình tư duy (troubleshooting), và cách giải quyết trong suốt quá trình xây dựng pipeline. Dữ liệu trong file này sẽ được dùng trực tiếp để viết phần **"Đánh giá", "Khó khăn gặp phải" và "Kinh nghiệm rút ra"** trong báo cáo Đồ án tốt nghiệp / Báo cáo môn học.

---

## 1. Vấn đề Tích hợp Airflow và dbt trong Docker (Task 3.2)

### 1.1 Bối cảnh (Context)
Theo thiết kế ban đầu, hệ thống sử dụng `PythonOperator` để gọi các module Python trong thư mục `ingestion/` và sau đó sử dụng thư viện `dbt` để transform dữ liệu. Tuy nhiên, khi triển khai thực tế trên Docker Container của Airflow, một loạt các lỗi chí mạng đã xảy ra khiến DAG bị crash hoàn toàn.

### 1.2 Các lỗi kỹ thuật gặp phải (Errors/Bugs)

1. **Lỗi thiếu module Python (`ModuleNotFoundError`)**:
   - *Triệu chứng:* Khi Airflow kích hoạt `PythonOperator`, hệ thống báo lỗi không tìm thấy module `ingestion`.
   - *Nguyên nhân:* File `docker-compose.yml` gốc chỉ thực hiện mount thư mục `./dags` vào trong container, dẫn đến container hoàn toàn "mù" và không có quyền truy cập vào các thư mục chứa logic chính của dự án như `ingestion/` hay `providers/`.

2. **Lỗi thiếu dependency dbt (`CommandNotFound` / `ImportError`)**:
   - *Triệu chứng:* Các task dbt fail với thông báo không tìm thấy lệnh `dbt`.
   - *Nguyên nhân:* Môi trường Airflow base image (`apache/airflow:3.2.2`) không cài sẵn dbt. Biến `_PIP_ADDITIONAL_REQUIREMENTS` trong file cấu hình cũng chưa liệt kê `dbt-postgres`.

3. **Lỗi phân quyền thư mục dbt (`PermissionError: [Errno 13] Permission denied`)**:
   - *Triệu chứng:* Ngay cả khi dbt đã được cài, khi gọi `dbt run`, hệ thống báo lỗi không thể ghi file vào `/opt/airflow/project/dbt/logs` và `/opt/airflow/project/dbt/target`.
   - *Nguyên nhân:* dbt cần ghi log và build target file lúc runtime. Tuy nhiên, do quá trình mount file từ Host (WSL/Windows) vào Container, User `airflow` trong container không có đủ quyền Write (Ghi) lên các thư mục này, dẫn đến crash quá trình parse project.

### 1.3 Cách giải quyết và Bài học kinh nghiệm (Solutions & Knowledge)

Để khắc phục triệt để và đảm bảo độ ổn định cho production, hệ thống đã được tái cấu trúc kiến trúc Orchestration như sau:

1. **Mount toàn bộ Project Workspace:** 
   - Sửa cấu hình `docker-compose.yml` từ `volumes: - ./dags:/opt/airflow/dags` thành `volumes: - ./:/opt/airflow/project`. 
   - Nhờ vậy, container có thể nhìn thấy toàn bộ cấu trúc dự án. Thư mục DAGs được config trỏ vào `/opt/airflow/project/dags`.

2. **Chuyển đổi Operator (`PythonOperator` ➔ `BashOperator`):**
   - Thay vì dùng `PythonOperator` (dễ gây conflict về global scope và Python path bên trong Celery worker), hệ thống chuyển sang dùng `BashOperator`. 
   - *Bài học:* `BashOperator` giúp mô phỏng chính xác cách một kỹ sư chạy lệnh CLI. Chúng ta sử dụng biến môi trường tạm thời: `export PYTHONPATH=/opt/airflow/project && python -m ingestion.fetch_prices`. Điều này giúp module resolver của Python định vị đúng package `ingestion`.

3. **Cài đặt Dependency & Xử lý Quyền:**
   - Khai báo cứng phiên bản `dbt-core==1.10.22` và `dbt-postgres==1.10.0` vào `_PIP_ADDITIONAL_REQUIREMENTS` để container tự động cài khi khởi động.
   - Xử lý quyền truy cập bằng cách thiết lập `chmod -R 777` cho các thư mục sinh ra lúc runtime (`dbt/logs`, `dbt/target`) ở máy Host, từ đó cho phép user trong container thoải mái ghi log mà không bị chặn.

### 1.4 Kết quả kiểm thử (Test Pass)
- Airflow UI đã load thành công DAG `daily_stock_pipeline`.
- Kích hoạt DAG thủ công (Manual Trigger) và toàn bộ các node (từ `health_check` ➔ `fetch_prices` ➔ `dbt_silver` ➔ `dbt_gold`) đều trả về trạng thái **Success (Màu xanh)**.
- Query kiểm tra dưới database xác nhận số liệu đã đổ vào Gold Schema (`fact_stock_indicators` có hơn 14,600 dòng, `fact_stock_price` có hơn 40,000 dòng).

---

## 2. Vấn đề API VNStock & Cơ chế Fallback (Task 1.3.6 / 3.2.6)

### 2.1 Bối cảnh
API VNStock phụ thuộc vào đường truyền mạng và server bên thứ 3. Trong quá trình test Airflow, task `fetch_prices` thường xuyên bị đẩy vào trạng thái Retry.

### 2.2 Kiến trúc bảo vệ dữ liệu (Data Protection Architecture)
- Thiết lập cơ chế **Exponential Backoff Retry**: Airflow được cấu hình để retry task 3 lần, mỗi lần cách nhau lâu hơn (jitter) để tránh spam API.
- Cấu hình Trigger Rule: Task dbt (`dbt_silver`) yêu cầu tất cả các task upstream (fetch data) phải `success`. Do đó, khi API sập hoàn toàn, `fetch_prices` chuyển sang `Failed`, kéo theo toàn bộ luồng dbt bị ngừng lại ở trạng thái `Upstream Failed`.
- *Bài học:* Đây là hành vi mong muốn. Thà pipeline dừng lại còn hơn là dbt chạy trên dữ liệu trống/lỗi, gây sai lệch nghiêm trọng cho các chỉ số tài chính (MACD, RSI).

### 2.3 Giải pháp Mock Data
Để đảm bảo tiến độ mô phỏng hệ thống cho Đồ án, hệ thống cung cấp lớp `MockProvider` (thực thi pattern Factory trong `registry.py`). Bằng cách thay đổi biến môi trường `PROVIDER=mock` trong `.env`, hệ thống sẽ đọc dữ liệu từ file csv fixture (tĩnh) mà không cần mạng Internet.

---

## 3. Sự cố phân mảnh Module Ingestion và Tái cấu trúc Cấu hình (Task 1.3)

### 3.1 Bối cảnh
Trong quá trình nghiệm thu chất lượng mã nguồn ở Task 1.3, mặc dù hệ thống đã nạp đầy đủ dữ liệu nhưng phát hiện ra kiến trúc code gặp 2 vấn đề lớn:
1. Logic nạp dữ liệu Index bị gộp chung vào file `fetch_prices.py` thay vì có file `fetch_index.py` riêng biệt theo đúng layout đặc tả.
2. File cấu hình `config.py` chỉ chứa các biến phẳng, thiếu cấu trúc `@dataclass` cấu hình tập trung và việc đọc biến môi trường bị phân tán (file `providers/registry.py` gọi trực tiếp `os.getenv("PROVIDER")`).

### 3.2 Các lỗi và Vi phạm quy tắc
- **Vi phạm quy tắc đơn nhiệm (Single Responsibility Principle):** Việc gộp fetch_prices và fetch_index vi phạm quy định *"Mỗi module có 1 trách nhiệm"* trong PROJECT_RULES.md Section 6.
- **Vi phạm quy tắc tập trung hóa cấu hình:** Đọc env scatter tại `registry.py` vi phạm quy tắc *"Config tập trung ingestion/config.py — không scatter .env reads"*.

### 3.3 Giải pháp khắc phục
1. **Tái cấu trúc cấu hình bằng dataclass:** Viết lại [ingestion/config.py](file://wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/config.py) định nghĩa `@dataclass IngestionConfig` làm Single Source of Truth đọc env. Các biến được export ra module-level để làm alias đảm bảo tương thích ngược.
2. **Tập trung hóa registry:** Cập nhật [providers/registry.py](file://wsl.localhost/Ubuntu/home/naeouad/deproject/providers/registry.py) import trực tiếp từ `config.provider`.
3. **Tách module nạp:** Di chuyển logic `run_index` từ `fetch_prices.py` sang file mới tạo [ingestion/fetch_index.py](file://wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_index.py), đồng thời cập nhật imports ở `backfill.py` và `__init__.py`.
4. **Cập nhật Airflow daily DAG:** Điều chỉnh task `fetch_index` trong [dags/dag_daily.py](file://wsl.localhost/Ubuntu/home/naeouad/deproject/dags/dag_daily.py) để chạy trực tiếp script độc lập thay vì thông qua các cờ skip của script prices.
5. **Cập nhật và chạy Unit Test:** Sửa file test [tests/test_ingestion.py](file://wsl.localhost/Ubuntu/home/naeouad/deproject/tests/test_ingestion.py), thêm test case `test_run_index_success`.

### 3.4 Kết quả kiểm thử
- 5/5 bài kiểm thử unit test đã **PASSED**.
- Chạy thử nghiệm thành công với `MockProvider` nạp dữ liệu hoàn hảo không sinh lỗi thô vào bảng `bronze.bronze_prices` (30 dòng) và `bronze.bronze_index` (2 dòng).

---
### 3.5 Nghiệm thu E2E DAG & Xác minh tính Idempotency trên Production
- **Kích hoạt thực tế:** Trigger thành công DAG `daily_stock_pipeline` trong môi trường Airflow Docker Container thực tế qua CLI. Cả 2 lần chạy liên tiếp (`12:33:32` và `12:36:41` UTC) đều đạt trạng thái **Success (Màu xanh)** trên toàn bộ 8 tasks:
  `health_check` ➔ `fetch_prices` & `fetch_index` ➔ `dbt_run_silver` ➔ `dbt_test_silver` ➔ `dbt_run_gold` ➔ `dbt_test_gold` ➔ `notify_success`.
- **Kiểm toán dữ liệu (Data Auditing):**
  - Thực hiện query đếm số dòng ở cả 3 layer sau 2 lần chạy liên tiếp:
    - Bảng Bronze: `bronze_prices` (40,659 dòng), `bronze_index` (2,724 dòng).
    - Bảng Silver: `silver_prices` (40,659 dòng), `silver_index` (2,724 dòng).
  - **Kết luận:** Số lượng dòng hoàn toàn giữ nguyên, không có bất kỳ dòng trùng lặp nào được sinh ra ở cả Bronze (idempotent upsert), Silver, và Gold (cơ chế dbt incremental delete+insert lookback 60 days). Hệ thống vận hành chính xác theo đúng đặc tả kiến trúc.

---

## 4. So Sánh Thiết Kế Giữa Daily DAG và Backfill DAG

### 4.1 Bối cảnh
Khi xây dựng và vận hành hệ thống, có hai nhu cầu khác biệt: chạy tự động hàng ngày thu thập dữ liệu mới (`daily_stock_pipeline`) và chạy lại toàn bộ dữ liệu lịch sử trong quá khứ khi khởi tạo hoặc sửa đổi chỉ báo (`manual_backfill_pipeline`). Thiết kế của hai DAG này có độ phức tạp chênh lệch lớn vì các lý do kỹ thuật dưới đây.

### 4.2 Điểm khác biệt kỹ thuật chi tiết

1. **Khả năng chịu lỗi và tự phục hồi (Resilience & Retries):**
   - **Daily DAG:** Chạy hoàn toàn tự động không có sự giám sát trực tiếp. Môi trường mạng Internet và API bên thứ ba (TCBS/SSI) thường phát sinh lỗi transient (lỗi mạng tạm thời, nghẽn băng thông, rate-limit). Vì vậy, DAG Daily bắt buộc phải cấu hình `retries=3`, `retry_delay=timedelta(minutes=2)` kết hợp `retry_exponential_backoff=True` để tự động khôi phục luồng dữ liệu mà không cần gọi Data Engineer lúc nửa đêm.
   - **Backfill DAG:** Chạy manual (thủ công) dưới sự kiểm soát trực tiếp của kỹ sư. Nếu gặp lỗi sập API hoặc rate limit kéo dài, kỹ sư sẽ tự động dừng tiến trình, điều chỉnh tham số hoặc cooldown rồi trigger lại, nên không cần đặt cơ chế retry tự động.

2. **Chế độ Fail-Fast (Bảo vệ dữ liệu hạ nguồn):**
   - **Daily DAG:** Tích hợp task `health_check` ở đầu pipeline để kiểm tra kết nối API. Nếu API sập hoàn toàn từ đầu, pipeline sẽ dừng ngay lập tức tại đây. Điều này ngăn chặn việc chạy các task dbt rỗng làm ô nhiễm dữ liệu hạ nguồn và kích hoạt cảnh báo sớm.
   - **Backfill DAG:** Không có task `health_check` vì kỹ sư dữ liệu là người kích hoạt và đã xác nhận sự sẵn sàng của hệ thống.

3. **Cơ chế Templating động vs. Tham số tĩnh (Airflow Jinja Templating):**
   - **Daily DAG:** Sử dụng các macro động của Airflow như `{{ dag_run.logical_date.strftime('%Y-%m-%d') }}` để tự động xác định ngày chạy hiện tại (Mon-Fri) mà không cần can thiệp thủ công. Cơ chế này cũng đảm bảo tính Idempotency khi Airflow thực hiện catchup tự động.
   - **Backfill DAG:** Sử dụng tham số tĩnh (`start_date`, `end_date`) được khai báo sẵn hoặc nhập trực tiếp từ UI thông qua `airflow.models.param.Param` để phục vụ nhu cầu backfill khoảng thời gian tùy ý.

4. **Tách biệt vai trò (Task Isolation):**
   - **Daily DAG:** Chia nhỏ việc nạp giá cổ phiếu (`fetch_prices`) và giá chỉ số (`fetch_index`) thành 2 task song song. Lỗi cào chỉ số index sẽ không gây ảnh hưởng hay làm gián đoạn việc cập nhật giá cổ phiếu chính.
   - **Backfill DAG:** Gộp chung các logic xử lý qua CLI `backfill.py` để tối giản hóa đồ họa DAG, do kỹ sư chỉ quan tâm đến kết quả cuối cùng của lịch sử.

5. **Hệ thống Cảnh báo và Giám sát (Alerting & Notification):**
   - **Daily DAG:** Cấu hình `on_failure_callback` để log lỗi và bắn cảnh báo (có thể tích hợp Webhook Slack/Telegram ở production), kết thúc bằng task `notify_success` để xác nhận hệ thống vận hành hoàn hảo.
   - **Backfill DAG:** Không cấu hình alert do log được hiển thị trực tiếp và theo dõi thời gian thực bởi kỹ sư chạy lệnh.

---
*(Sẽ tiếp tục cập nhật các kiến thức từ quá trình kết nối Power BI trong Phase 4)*

