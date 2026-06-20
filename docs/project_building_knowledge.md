# SỔ TAY KIẾN TRÚC & TRIỂN KHAI CHUYÊN SÂU (PROJECT KNOWLEDGE BASE)

Bản báo cáo này hệ thống hóa toàn bộ các quyết định mang tính chiến lược, những kinh nghiệm xương máu khi xử lý các vấn đề hạ tầng, và kỹ thuật tinh chỉnh thuật toán ở quy mô production trong khuôn khổ dự án.

## 1. Hạ tầng và Orchestration (Airflow + Docker)

### Vấn đề cách ly hệ sinh thái (The Isolation Dilemma)
**Ngữ cảnh:** Hệ thống vận hành trên 2 môi trường: 
1. **WSL Ubuntu (Host):** Chứa mã nguồn `ingestion/`, cấu hình `dbt`, và thư viện `venv`.
2. **Debian Docker Container:** Nơi Airflow thực thi các tiến trình tự động hóa.

**Sự cố phát sinh:** 
- Kế hoạch ban đầu chỉ mount thư mục `dags/` vào container. Điều này khiến `PythonOperator` của Airflow bị mù hoàn toàn (lỗi `ModuleNotFoundError`) vì không tìm thấy mã nguồn `ingestion`.
- Chia sẻ `venv` từ Host vào Container là một "Anti-pattern" nguy hiểm do sự khác biệt về thư viện C cốt lõi (glibc), có thể dẫn đến Segment Fault khi import các thư viện nặng như `psycopg2` hay `pandas`.
- Khi Docker mount một volume mới chưa tồn tại, nó tự chiếm quyền `root`. AI Agent chạy bằng user bình thường bị từ chối quyền ghi file (Access is denied), cản trở việc tự động sinh code.

**Quyết định kiến trúc:**
1. **Sử dụng BashOperator:** Bắt Airflow mô phỏng các lệnh terminal thay vì gọi hàm Python trực tiếp.
2. **Container tự cung tự cấp:** Khai báo biến môi trường `_PIP_ADDITIONAL_REQUIREMENTS: vnstock requests pandas psycopg2-binary dbt-postgres==1.10.0` để container tự tải "vũ khí" của riêng nó, vứt bỏ sự phụ thuộc vào `venv` của Host.
3. **Mount toàn bộ Workspace:** Bê nguyên thư mục dự án vào `/opt/airflow/project` thay vì chỉ khoét lỗ cho thư mục `dags/`.
4. **Mở khóa bằng CLI:** Yêu cầu User can thiệp bằng lệnh `sudo chown -R $USER:$USER` để giải phóng quyền thao tác.

---

## 2. Ingestion & Giới hạn API (Rate Limits)

### Idempotency (Hợp đồng không lặp lặp)
Cơ sở dữ liệu PostgreSQL sử dụng lệnh `ON CONFLICT (code, date) DO UPDATE`.
**Hiệu quả:** Khi kịch bản Backfill chạy đè lần thứ hai lên dữ liệu cũ, hệ thống báo cáo `0 rows upserted | 6 chunks skipped`. Idempotency được bảo vệ hoàn hảo, không sinh rác dữ liệu ở quy mô hàng chục nghìn dòng.

### Nghệ thuật tối ưu giới hạn API (Rate-limit Hacking)
**Vấn đề:** 
- API vnstock (guest tier) chỉ cho phép **20 requests / phút**. Hệ thống đã phòng ngự bằng lệnh `time.sleep(3.1)`.
- Kịch bản cũ băm dữ liệu theo từng tháng. 5 năm x 32 mã = 1,920 requests. Tổng thời gian chạy ngầm tốn 2 tiếng cho 40,000 dòng.

**Chiến lược tối ưu:**
- Máy chủ SSI/TCBS đủ mạnh để trả về lượng dữ liệu "Daily" (1D) lên tới 5 năm trong 1 request duy nhất (vì 5 năm chỉ có 1,250 dòng).
- Khi đẩy khối lượng băm (chunk size) từ 1 tháng lên 5 năm, số lượng request rớt thẳng đứng từ 1,920 xuống chỉ còn **32 requests**.
- Tổng thời gian Backfill 5 năm lập tức giảm 60 lần, từ 2 tiếng xuống còn **1.5 phút**, trong khi vẫn tuyệt đối tuân thủ khoảng nghỉ 3.1 giây giữa các request để vô hình dưới radar kiểm duyệt.

---

## 3. Data Transformation (dbt Silver & Gold)

### Tách bạch Môi trường Kiểm thử (Mock vs Real)
Database PostgreSQL (`bronze_prices`) chỉ lưu trữ dữ liệu có `source = vnstock` (hiện tại là 40,629 dòng dữ liệu thật). Các bài kiểm thử Unit Test sử dụng `MockProvider` nạp dữ liệu ảo (với giá VNM 60k, HPG 25k) lên thẳng RAM để verify luồng schema, không làm ô nhiễm kho dữ liệu lõi. Lớp Silver làm nhiệm vụ làm sạch các chỉ số `null` hoặc `< 0`.

### Sai số kinh điển của Chỉ báo MACD
**Triệu chứng:** Khi đối chiếu MACD Signal tính bằng SQL so với thư viện Python chuẩn, sai số văng xa ngưỡng 0.5% cho phép.
**Chuẩn đoán:** Các lập trình viên thường lấy Simple Moving Average (SMA) để xấp xỉ Exponential Moving Average (EMA) trong giai đoạn khởi chạy (warm-up) của MACD. Cách xấp xỉ này mang lại tốc độ nhưng phá vỡ tính chính xác trong toán học tài chính.
**Giải pháp phẫu thuật:**
- Chuyển toàn bộ các biểu thức tính MA sang hàm đệ quy (Recursive CTE).
- Rút lõi logic tính EMA ra thành một macro tổng quát: `calculate_ema(period, source_relation, value_column)`.
- Ép tính đường tín hiệu MACD bằng **EMA9 thật** thông qua hai bảng trung gian (`int_macd_line` và `int_macd_signal`).
- Kết quả: Sai số hạ cánh an toàn ở mức **0.0000%**.

---

## 4. Kiểm toán và Quản trị Rủi Ro (Auditing & Protocols)

### Quản trị rủi ro "Cảm tính"
Luật kiểm toán nội bộ: AI tuyệt đối không phán đoán mù mờ (hallucination) về trạng thái dự án. Khi User nghi ngờ tính đầy đủ của Cụm 2.1 (Backfill), AI bắt buộc phải:
1. Gọi Pytest kiểm tra trạng thái 9 unit test.
2. Viết Python script truy vấn PostgreSQL đếm phân bố năm (Yearly Distribution).
3. Đưa ra bằng chứng rành mạch bằng chuỗi số log trước khi tick checkmark trong `task3.md`.

### Hệ thống Log Lỗi (TEST_REPORTS.md)
Tất cả các lỗi kinh điển phát sinh trong quá trình build dự án đã được lưu vết vĩnh viễn thành tài sản tri thức:
- **Lỗi Numpy int64 (1.3.6):** psycopg2 không hiểu kiểu int64 của Pandas/Numpy. Xử lý bằng cách `item()` casting.
- **Lỗi DNS Postgres (2.2.8):** dbt gọi `localhost` sẽ thất bại trong mạng nội bộ docker. Phải cấu hình host là `localhost` khi chạy trên WSL, và host là tên container `postgres` nếu chạy trong Docker.
- **Lỗi chia cho 0 và ép kiểu NUMERIC (2.3.2):** Hàm `ROUND` trong PostgreSQL yêu cầu kiểu `NUMERIC`, nếu truyền `DOUBLE PRECISION` sẽ bị từ chối. Sửa bằng cách ép kiểu `::NUMERIC`.
- **Lỗi OSError (3.1.4):** Đường dẫn thư mục dbt seed bị sai cấu hình, yêu cầu chỉnh sửa trong file `dbt_project.yml` thành `data-paths: ["data"]`.

---

## 5. Docker, Airflow và Sự cố Dependency Hell

### Xung đột phiên bản thư viện cốt lõi (Alpha vs Stable)
**Triệu chứng:** Khi chạy `docker compose up` để cài đặt `dbt-postgres==1.10.0` qua biến `_PIP_ADDITIONAL_REQUIREMENTS`, `pip` đã tự động giải quyết phụ thuộc (dependency resolution) và kéo về bản `dbt-core 2.0.0-alpha.2`. Bản alpha này không tương thích với dự án đang chạy ổn định ở `1.10.22` trên Host.
**Giải pháp phẫu thuật:** 
- Phải dùng kỹ thuật "Pinning" (ghim chặt): `_PIP_ADDITIONAL_REQUIREMENTS: vnstock requests pandas psycopg2-binary dbt-core==1.10.22 dbt-postgres==1.10.0 python-dotenv`. 
- Bằng cách ép cả thư viện cha lẫn thư viện con, Container buộc phải tải đúng hệ sinh thái `1.10.x`, chặn đứng rủi ro lỗi không tương thích.

### Quyền thực thi lệnh bên trong Container
**Triệu chứng:** Cố gắng chạy `pip list` hoặc `pip show` bên trong container bằng lệnh `docker exec` bị chặn với lỗi `bash: /root/bin/pip: Permission denied`.
**Căn nguyên:** Lệnh `docker exec` chạy dưới quyền của user `airflow` mặc định, trong khi `pip` được cài bởi `root` trong quá trình khởi tạo container.
**Chiến lược Né tránh:** Thay vì vật lộn với phân quyền OS, tôi chuyển sang truy vấn trực tiếp qua binary của dbt: `dbt --version`. Điều này chứng minh ứng dụng thực sự hoạt động độc lập với quyền sở hữu file quản lý gói.
