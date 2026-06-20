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

### Vai trò và Mục tiêu của dbt run & dbt test ở hai Layer Silver và Gold
Việc phân chia vai trò cụ thể giữa `run` (chuyển đổi và lưu trữ) và `test` (chốt chặn chất lượng dữ liệu) ở hai tầng giúp hệ thống vận hành đúng chuẩn Medallion và bảo vệ sự toàn vẹn của báo cáo:
1.  **dbt run (Transform & Materialize):**
    *   **Silver Layer:** Chuyển đổi dữ liệu JSONB thô từ Bronze thành cấu trúc bảng phẳng, thực hiện ép kiểu nghiêm ngặt (như casting các cột giá thành `numeric`, ngày thành `date`), xử lý lọc bỏ các dữ liệu không hợp lệ và gắn cờ báo lỗi (`dq_flag`, `is_valid`) để phục vụ kiểm toán sau này.
    *   **Gold Layer:** Đọc từ Silver để tiến hành tính toán các chỉ báo tài chính theo công thức chuẩn thông qua các intermediate models vật lý (Wilder RSI 14, EMA 12/26 đệ quy, MACD Line, MACD Signal, Bollinger Bands) và cấu trúc lại dữ liệu theo mô hình hình sao (Star Schema) tối ưu hóa tốc độ truy vấn của Power BI.
2.  **dbt test (Data Quality Gates):**
    *   **Silver Layer:** Chốt chặn kiểm định kỹ thuật (Technical Quality). Đảm bảo các cột khóa chính (`symbol`, `trade_date`) không bị NULL và là duy nhất (Unique), đồng thời xác thực các giá trị Open/High/Low/Close không bị âm.
    *   **Gold Layer:** Chốt chặn kiểm định nghiệp vụ (Business Logic Quality). Đảm bảo các giá trị tính toán hợp lý về mặt nghiệp vụ tài chính (ví dụ: RSI luôn nằm trong phạm vi `[0, 100]`, dải Upper Bollinger Bands phải lớn hơn hoặc bằng Lower Bollinger Bands, tổng số lượng mã tăng/giảm/không đổi của thị trường khớp với tổng số mã đang theo dõi). Nếu phát hiện lỗi logic, Airflow sẽ ngay lập tức ngắt pipeline (Fail Fast) để tránh cập nhật dữ liệu sai lệch lên Dashboard Power BI.

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

---

## 6. Vấn đề Tích hợp Airflow và dbt trong Docker (Task 3.2)

### 6.1 Sự cố Cách ly Môi trường
**Triệu chứng:** Khi Airflow kích hoạt `PythonOperator`, hệ thống báo lỗi không tìm thấy module `ingestion`. Tương tự, các task dbt fail với thông báo không tìm thấy lệnh `dbt`. Khi cài dbt xong, lại dính lỗi `PermissionError: [Errno 13] Permission denied` khi ghi log.
**Căn nguyên:**
1. File `docker-compose.yml` gốc chỉ mount thư mục `./dags` vào trong container, dẫn đến container hoàn toàn "mù" và không có quyền truy cập vào mã nguồn cốt lõi `ingestion/`.
2. Môi trường Airflow base image không cài sẵn thư viện dbt.
3. User `airflow` trong container không có đủ quyền Write (Ghi) lên các thư mục sinh ra lúc runtime (`dbt/logs` và `dbt/target`) nằm trên máy Host.

**Giải pháp Kiến trúc:**
1. **Mount toàn bộ Project Workspace:** Đổi cấu hình thành `volumes: - ./:/opt/airflow/project` để container có thể nhìn thấy toàn bộ cấu trúc dự án.
2. **Chuyển đổi sang BashOperator:** Sử dụng `BashOperator` thay vì `PythonOperator`. Cấu hình `export PYTHONPATH=/opt/airflow/project` để giả lập môi trường terminal nguyên thủy, giúp Python định vị đúng package `ingestion`.
3. **Cài đặt Dependency & Xử lý Quyền:** Khai báo cứng `dbt-core==1.10.22` và `dbt-postgres==1.10.0` vào biến khởi tạo của Airflow. Đồng thời, chạy `chmod 777 -R dbt` trên Host để cấp quyền rỗng cho container thoải mái ghi file log.
**Hiệu quả:** Pipeline chạy trơn tru end-to-end từ fetch API ➔ dbt Silver ➔ dbt Gold trong container Docker khép kín.

### 6.3. Cạm bẫy Jinja Template (`logical_date` vs `ds`) trong Airflow 3
- **Vấn đề**: Trong Airflow 2, khi manual trigger một DAG, biến `logical_date` vẫn được truyền vào template, hoặc có thể fallback bằng `(logical_date or run_after)`. Tuy nhiên, với cấu trúc Airflow 3 SDK mới, việc gọi `{{ (logical_date or run_after).strftime('%Y-%m-%d') }}` sẽ văng lỗi Jinja `UndefinedError: 'logical_date' is undefined`.
- **Hệ quả**: Task `fetch_prices` ngầm bị crash mà không chạy bất cứ script Python nào.
- **Giải pháp**: Xóa bỏ các template tự chế và quay về sử dụng biến built-in mặc định an toàn nhất của Airflow là `{{ ds }}` (date string chuẩn YYYY-MM-DD), nó luôn tồn tại trên mọi phiên bản dù là trigger tự động hay thủ công.

### 6.2 Kiến trúc bảo vệ dữ liệu bằng Upstream Failed
**Vấn đề:** Khi API vnstock sập hoặc trả về timeout, nếu dbt vẫn tiếp tục chạy sẽ ghi đè lên dữ liệu sai, phá hỏng các chỉ số kỹ thuật ở bảng Gold.
**Thiết kế bảo vệ:** Cài đặt 3x Exponential Backoff Retry. Nếu `fetch_prices` thất bại hoàn toàn (Failed), các task dbt phía sau sẽ tự động nhảy sang màu cam (`Upstream Failed`) chặn đứng luồng ETL, bảo vệ tính toàn vẹn tuyệt đối của Data Warehouse. Cùng với cấu hình `PROVIDER=mock`, hệ thống có đường lui an toàn để mô phỏng dữ liệu.

## 7. Thiết kế Database và Bản chất Dữ liệu thực tế

### 7.1 Cấu trúc Medallion (Bronze - Silver - Gold)
Hệ thống sử dụng mô hình Medallion 3 lớp để chia tách trách nhiệm và đảm bảo tính chuẩn xác của dữ liệu phân tích:
- **Tầng Bronze (Raw Data)**: 
  - Lưu trữ dữ liệu thô (raw JSON) thu được từ API. Sử dụng kiểu dữ liệu `JSONB` của PostgreSQL để tối ưu hóa hiệu năng đọc/ghi dữ liệu phi cấu trúc.
  - Phân vùng (Partition) dữ liệu theo mốc thời gian từng năm (từ 2020 đến 2026) dựa trên cơ chế `PARTITION BY RANGE (date)`. Điều này giúp tránh suy giảm hiệu năng khi cơ sở dữ liệu phình to theo thời gian.
  - Được tạo lập bằng mã SQL thuần trong file [init_schema.sql](file:///home/naeouad/deproject/sql/init_schema.sql) chạy trong quá trình khởi tạo container cơ sở dữ liệu.
- **Tầng Silver (Staged & Cleaned Data)**:
  - Loại bỏ các giá trị bị trùng lặp, xử lý các bản ghi lỗi hoặc thiếu giá trị, kiểm tra tính hợp lệ (`is_valid`) và đánh dấu lỗi chất lượng (`dq_flag`).
  - Được dbt tự động biên dịch và tạo lập dưới dạng các Views/Tables từ các model khai báo trong thư mục [dbt/models/silver/](file:///home/naeouad/deproject/dbt/models/silver/).
- **Tầng Gold (Analytical Star Schema)**:
  - Tổ chức dữ liệu theo mô hình hình sao (Star Schema) phục vụ tối ưu cho Power BI gồm các bảng Fact (Sự kiện giao dịch, chỉ số kỹ thuật) và các bảng Dim (Mã chứng khoán, lịch giao dịch). Các chỉ báo tài chính phức tạp như RSI, EMA, Bollinger Bands được tính toán tập trung ở đây.
  - Được dbt tự động biên dịch và tạo lập từ các model trong [dbt/models/gold/](file:///home/naeouad/deproject/dbt/models/gold/).

### 7.2 Tính xác thực của dữ liệu (Mock vs Real)
- Khi biến cấu hình `PROVIDER` trong file `.env` được chuyển thành `vnstock`, toàn bộ dữ liệu được hệ thống cào về thông qua các task Airflow (`fetch_prices`, `fetch_index`) là **dữ liệu thật 100%** từ thị trường chứng khoán Việt Nam (vnstock lấy nguồn từ SSI/TCBS).
- Tuy nhiên, do quá trình phát triển hệ thống trước đó có chạy thử nghiệm bằng `MockProvider` (`PROVIDER=mock`), cơ sở dữ liệu có thể chứa lẫn lộn cả dữ liệu mock cũ và dữ liệu thật mới cào về nếu database chưa từng được xóa sạch. Để đảm bảo tính thuần khiết của dữ liệu thật cho buổi bảo vệ, cần chạy dọn dẹp (Truncate/Drop) database và chạy lại pipeline với nguồn `vnstock`.

### 7.3 So sánh chi tiết Mock vs Real (vnstock) trên các phương diện
*   **Tính chất vận hành**:
    *   `vnstock`: Kéo dữ liệu thực tế từ thị trường. Hoạt động phụ thuộc vào Internet và sự ổn định của API nguồn (TCBS/SSI/VCI/MSN). Phù hợp cho môi trường Production và báo cáo đồ án chính thức.
    *   `mock`: Sinh dữ liệu giả định cục bộ. Hoạt động 100% offline, không cần mạng, không bị rate limit hay timeout. Phù hợp cho Unit Test tự động (CI/CD) và demo báo cáo khi mất mạng.
*   **Dữ liệu ở 3 tầng**:
    *   *Bronze Layer*: `vnstock` lưu raw_json thực, cột `source` ghi rõ nguồn API thực tế (như tcbs, vci...). `mock` lưu dữ liệu mock với cột `source = 'mock'`.
    *   *Silver Layer*: `vnstock` phản ánh các lỗi thực tế từ thị trường (giá close rỗng, `high < low`...). `mock` dữ liệu mặc định sạch sẽ (trừ các case test lỗi cố ý).
    *   *Gold Layer*: `vnstock` tính chỉ báo (RSI, MACD...) phản ánh chính xác biến động thực tế phục vụ phân tích. `mock` chỉ mang tính chất minh họa về mặt toán học.
*   **Airflow DAG**:
    *   `vnstock`: DAG daily chạy lúc 18:00 tối. Tác vụ cào có thể failed/retry nếu API lỗi. DAG backfill chạy lâu vì phải giữ delay (~3.5s) tránh khóa IP.
    *   `mock`: DAG chạy cực nhanh, luôn thành công. Có cơ chế tự động bù dữ liệu (fallback dòng cuối làm ngày tương lai) để tránh crash DAG khi trigger thủ công vào ngày nghỉ.
*   **Power BI Dashboard**:
    *   `vnstock`: Biểu đồ trực quan hóa chính xác xu hướng giá thực tế.
    *   `mock`: Biểu đồ hiển thị các đường biến động tuần hoàn đơn giản, chỉ có giá trị minh họa chức năng hiển thị (Visual Validation).

### 7.4 Hệ quả nghiêm trọng của việc chạy lẫn lộn dữ liệu (Data Pollution)
Nếu hệ thống vận hành đan xen giữa `mock` và `vnstock` mà không dọn dẹp DB trước đó, sẽ gây ra các lỗi hệ quả nặng nề:
1.  **Ghi đè dữ liệu ở Bronze**: 
    Cơ chế UPSERT (`ON CONFLICT (code, date) DO UPDATE`) sẽ hoạt động. Nếu chạy `mock` sau `vnstock` trên cùng 1 ngày giao dịch, dữ liệu thật của ngày đó sẽ bị ghi đè bởi dữ liệu mock (cột `source` chuyển thành `'mock'`), hoặc ngược lại.
2.  **Đứt gãy chuỗi thời gian ở Gold (Lỗi tính toán chỉ báo kỹ thuật)**: 
    Các chỉ báo như **RSI, MACD, MA, Bollinger Bands** dựa trên các thuật toán tính toán lũy kế hoặc đệ quy qua các ngày giao dịch liên tiếp. Việc lẫn lộn dữ liệu (ví dụ giá cổ phiếu FPT thật đang 130,000đ bỗng dưng nhảy về giá mock 70,000đ do chuyển đổi provider) sẽ tạo ra các điểm nhảy vọt (spikes/gaps) vô lý. Điều này làm **méo mó hoàn toàn kết quả tính toán chỉ báo**, khiến đồ thị kỹ thuật bị sai lệch hoàn toàn.
3.  **Lỗi kiểm toán dbt (dbt test failed)**: 
    Các bài test nghiệp vụ dbt ở tầng Gold (như kiểm tra khoảng giá trị hợp lệ, kiểm tra biên Bollinger Bands trên > Bollinger Bands dưới) có thể bị fail do dữ liệu bị nhảy vọt phi thực tế.
4.  **Power BI hiển thị dị dạng**: 
    Dashboard sẽ vẽ ra các đường biểu đồ đứt gãy hoặc có những đỉnh/đáy cực kỳ vô lý, làm mất đi tính chuyên nghiệp khi trình bày đồ án.
*   **Giải pháp khắc phục**: Trước khi đổi chế độ `PROVIDER` trong `.env` để cào dữ liệu mới, bắt buộc phải chạy lệnh SQL dọn sạch Database cũ:
    `TRUNCATE TABLE bronze.bronze_prices CASCADE;`
    `TRUNCATE TABLE bronze.bronze_index CASCADE;`
    Sau đó khởi động lại Airflow và trigger lại các DAG để nạp lại dữ liệu đồng bộ.

## 8. Chi tiết Kịch bản Điều phối DAG (Airflow Orchestration)

Dự án sử dụng Apache Airflow làm bộ não điều phối dữ liệu (Orchestrator). Có hai kịch bản (DAG) chính được thiết kế để giải quyết hai nhu cầu khác nhau của luồng dữ liệu:

### 8.1 DAG Daily (`daily_stock_pipeline`)
* **Mục đích**: Vận hành tự động hóa hàng ngày để cập nhật dữ liệu mới nhất của thị trường sau khi phiên giao dịch kết thúc.
* **Tần suất chạy (Schedule)**: `0 11 * * 1-5` (11:00 UTC hàng ngày từ Thứ Hai đến Thứ Sáu, tương đương **18:00 tối giờ Việt Nam**). Giờ này được chọn vì sàn chứng khoán đóng cửa lúc 15:00, đến 18:00 dữ liệu giao dịch trên các hệ thống nguồn đã khớp hoàn toàn và ổn định.
* **Cơ chế truyền ngày tự động**: Sử dụng biến Jinja template `{{ ds }}` của Airflow (chuỗi ký tự ngày định dạng `YYYY-MM-DD` tại thời điểm chạy logic).
* **Luồng chạy chi tiết**:
  1. `health_check`: Kiểm tra kết nối tới nhà cung cấp dữ liệu (vnstock hoặc mock).
  2. `fetch_prices` và `fetch_index` (Chạy song song): Tải dữ liệu giá cổ phiếu VN30 và chỉ số VNINDEX/VN30 của ngày hôm nay ghi vào tầng Bronze. Cấu hình **Retry 3 lần** kèm **Exponential Backoff** (thời gian chờ tăng dần giữa các lần thử) để tự phục hồi khi gặp lỗi nghẽn mạng nhất thời.
  3. `dbt_run_silver` & `dbt_test_silver` (Tuần tự): Chạy dbt để làm sạch dữ liệu thô vừa cào và chạy các test kiểm tra chất lượng dữ liệu ở tầng Silver.
  4. `dbt_run_gold` & `dbt_test_gold` (Tuần tự): Tính toán các chỉ báo kỹ thuật tài chính phức tạp và kiểm tra giá trị đầu ra (như RSI nằm trong khoảng 0-100, Bollinger Bands trên lớn hơn Bollinger Bands dưới).
  5. `notify_success`: Ghi nhận log hoàn tất chu trình thành công.

### 8.2 DAG Backfill (`manual_backfill_pipeline`)
* **Mục đích**: Chạy thủ công để nạp lại dữ liệu lịch sử trên diện rộng (ví dụ cào lại dữ liệu 5 năm từ 2021 đến 2026).
* **Tần suất chạy (Schedule)**: `None` (Chỉ kích hoạt bằng tay từ giao diện Airflow UI hoặc qua CLI).
* **Cấu hình tham số (Params)**: Thiết kế sẵn form nhập ngày tháng dạng lịch trên Airflow UI:
  - `start_date`: Mặc định là `2021-01-01`
  - `end_date`: Mặc định là `2026-06-18`
* **Luồng chạy chi tiết**:
  1. `backfill_data`: Thực thi script [ingestion/backfill.py](file:///home/naeouad/deproject/ingestion/backfill.py) để tải dữ liệu lịch sử lớn và nạp vào tầng Bronze.
  2. `dbt_run_silver` ➔ `dbt_test_silver` ➔ `dbt_run_gold` ➔ `dbt_test_gold` (Chạy tuần tự tuyến tính): Thực thi tính toán lại toàn bộ dữ liệu lịch sử từ Bronze qua các tầng Silver và Gold để cập nhật kho dữ liệu.
* **Đặc trưng thiết kế**: Do khối lượng dữ liệu xử lý lịch sử lớn, DAG này được nâng giới hạn thời gian chờ (`execution_timeout`) lên tới **2 tiếng** đối với các tác vụ chạy dbt và backfill để chống tình trạng Airflow ngắt tác vụ giữa chừng (timeout).

## 9. Hướng dẫn Sử dụng Chi tiết theo Kịch bản & Cách thức Đóng gói

Hệ thống được thiết kế để vận hành linh hoạt trên môi trường thực tế (production) cũng như phục vụ việc báo cáo/demo offline. Dưới đây là tài liệu chi tiết hướng dẫn vận hành theo từng trường hợp cụ thể và kiến trúc đóng gói của dự án.

### 9.1 Hướng dẫn vận hành theo các kịch bản thực tế (Operational Scenarios)

#### **Trường hợp 1: Thiết lập mới toàn bộ hệ thống (Greenfield Setup)**
Áp dụng khi triển khai hệ thống lần đầu tiên trên một máy tính mới:
1. Sao chép file cấu hình môi trường: Sao chép `.env.example` thành `.env` và điều chỉnh các biến kết nối nếu cần.
2. Khởi động hạ tầng Docker: Chạy lệnh `docker compose up -d` ở thư mục gốc của dự án. Lệnh này sẽ khởi động Postgres DB và cụm dịch vụ Airflow (Webserver, Triggerer, Scheduler).
3. Kiểm tra kết nối và cấp quyền:
   - Đảm bảo các container đang chạy: `docker ps`
   - Cấp quyền đọc/ghi cho thư mục để dbt trong container ghi log về máy host: `chmod 777 -R dbt/`
4. Nạp dữ liệu danh mục tĩnh (Seed data): Chạy dbt seed để nạp bảng ngày tháng và danh sách mã cổ phiếu nhóm VN30:
   `docker exec -it airflow-container bash -c "cd /opt/airflow/project/dbt && dbt seed --profiles-dir ."`

#### **Trường hợp 2: Chạy kiểm thử offline không cần mạng (Offline / Mock Mode)**
Áp dụng khi chuẩn bị thuyết trình báo cáo ở nơi không có kết nối internet ổn định, hoặc khi API vnstock bị lỗi chặn request:
1. Mở file `.env` chỉnh sửa biến: `PROVIDER=mock`
2. Khởi động lại dịch vụ Airflow để cập nhật cấu hình: `docker compose restart airflow-webserver airflow-scheduler`
3. Trigger chạy thử các DAG trên giao diện Airflow UI. Hệ thống sẽ tự động dùng `MockProvider` sinh dữ liệu giả lập chuẩn chỉnh để thông luồng pipeline mà không báo lỗi mạng.

#### **Trường hợp 3: Nạp dữ liệu lịch sử (Historical Backfill)**
Áp dụng khi cần tải một khối lượng lớn dữ liệu giá lịch sử (ví dụ 3-5 năm) về để dựng biểu đồ:
1. Truy cập Airflow UI tại `http://localhost:8080` (Tài khoản: `admin` / `admin`).
2. Tìm DAG `manual_backfill_pipeline` và chọn **Trigger DAG w/ config**.
3. Nhập ngày bắt đầu (`start_date`) và ngày kết thúc (`end_date`) mong muốn, sau đó bấm nút **Trigger**.
4. Theo dõi log tiến trình chạy. Toàn bộ dữ liệu lịch sử sẽ được kéo về và tính toán lại các chỉ số kỹ thuật tương ứng.

#### **Trường hợp 4: Vận hành cào tự động hàng ngày (Production Daily Run)**
Áp dụng khi hệ thống đi vào vận hành thực tế ổn định:
1. Đảm bảo biến môi trường thiết lập `PROVIDER=vnstock` trong file `.env`.
2. Trên Airflow UI, gạt nút công tắc bên cạnh `daily_stock_pipeline` sang trạng thái **ON**.
3. Hệ thống sẽ tự động kích hoạt vào lúc 18:00 các ngày từ Thứ Hai đến Thứ Sáu để nạp dữ liệu phiên giao dịch mới nhất và cập nhật Dashboard.

---

### 9.2 Cách thức Đóng gói và Phân phối hệ thống (Packaging & Delivery)

Dự án được thiết kế để đóng gói gọn nhẹ, dễ dàng di chuyển sang bất kỳ máy tính nào có cài sẵn Docker:

1. **Đóng gói Hạ tầng (Infrastructure as Code)**:
   - Toàn bộ cơ sở dữ liệu Postgres 17 và môi trường điều phối Apache Airflow 3.2 được định nghĩa và đóng gói thông qua file `docker-compose.yml`. Người dùng cuối không cần tự tay cài đặt Postgres hay cấu hình biến môi trường cục bộ trên hệ điều hành.

2. **Đóng gói Thư viện và Môi trường (Dependency Packaging)**:
   - Các thư viện Python phục vụ cào dữ liệu (`vnstock`, `pandas`, `requests`, `psycopg2-binary`) và các thư viện dbt (`dbt-core`, `dbt-postgres`) được đóng gói động thông qua khai báo trong thuộc tính `_PIP_ADDITIONAL_REQUIREMENTS` của file `docker-compose.yml`. Khi container khởi động lần đầu, nó sẽ tự xây dựng môi trường Python khép kín phù hợp, tránh hiện tượng xung đột thư viện với hệ điều hành Host.

3. **Đóng gói Mã nguồn và Báo cáo (Application & Deliverables)**:
   - Mã nguồn cào dữ liệu và cấu hình dbt được đóng gói trong một cấu trúc thư mục chuẩn mực.
   - Bản thiết kế trực quan báo cáo được đóng gói dưới dạng tệp tin định dạng `reports/stock_dashboard.pbix` (Power BI Desktop). Người dùng chỉ cần mở file này và bấm Refresh để cập nhật số liệu mới từ database.

---

## 10. Bản đồ Phân bổ File và Luồng Kéo Dữ liệu (Data Ingestion Map)

Để phục vụ công tác phát triển, kiểm thử, và vận hành, cấu trúc các file liên quan đến kéo dữ liệu (Data Ingestion) được phân bổ theo 3 tầng kiến trúc MECE rõ ràng:

### 10.1 Lớp kết nối dữ liệu — Provider Layer (Thư mục `providers/`)
Lớp này đóng vai trò giao tiếp trực tiếp với nguồn cấp dữ liệu bên ngoài, che giấu sự phức tạp của API:
- [base.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/providers/base.py): Định nghĩa lớp cơ sở `DataProvider` làm tiêu chuẩn cho mọi provider.
- [vnstock_provider.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/providers/vnstock_provider.py): Dùng thư viện `vnstock` cào dữ liệu thật từ thị trường Việt Nam (tự động fallback qua các nguồn TCBS, VCI, MSN...).
- [mock_provider.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/providers/mock_provider.py): Sinh dữ liệu giả lập (mock data) để chạy Unit Test offline, đảm bảo không phụ thuộc vào internet hoặc khi API thật bị chặn.
- [registry.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/providers/registry.py): Quản lý đăng ký provider và cung cấp cơ chế chuyển đổi động giữa dữ liệu thật và dữ liệu mock.

### 10.2 Lớp thực thi kéo dữ liệu — Ingestion Layer (Thư mục `ingestion/`)
Lớp này nhận Provider được cấu hình để tải dữ liệu, xử lý nghiệp vụ lưu trữ đảm bảo tính Idempotency:
- [config.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/config.py): Nơi tập trung đọc tất cả biến môi trường từ tệp `.env`.
- [utils.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/utils.py): Cung cấp các hàm bổ trợ như kết nối DB, ghi Log, và Exponential Backoff Retry cho các transient errors.
- [fetch_prices.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_prices.py): Kéo dữ liệu giá giao dịch cổ phiếu hàng ngày (OHLCV) nạp vào bảng thô `bronze_prices` theo phương thức idempotent UPSERT.
- [fetch_index.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_index.py): Kéo dữ liệu chỉ số thị trường (VNINDEX, VN30).
- [fetch_fundamentals.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_fundamentals.py): Kéo dữ liệu chỉ số tài chính cơ bản phục vụ phân tích (PE, PB, ROE, ROA, EPS).
- [backfill.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/backfill.py): Kịch bản nạp bù dữ liệu lịch sử số lượng lớn cho khoảng thời gian dài trước đó.

### 10.3 Lớp điều phối tiến trình — Orchestration Layer (Thư mục `dags/`)
Sử dụng Apache Airflow làm bộ não lập lịch chạy định kỳ:
- [dag_daily.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/dags/dag_daily.py): Lập lịch tự động chạy vào lúc 18:00 tối hàng ngày để kéo dữ liệu phiên giao dịch mới, làm sạch, tính chỉ báo và cập nhật Data Warehouse.
- [dag_backfill.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/dags/dag_backfill.py): DAG chạy thủ công để bù dữ liệu lịch sử diện rộng trên giao diện Airflow Web UI.

---

## 11. Tái Cấu Trúc Chuẩn Hóa Tầng Ingestion (Task 1.3 Alignment)

### Vấn đề thiết kế phi tập trung (Decentralized Configuration & Monolithic Fetcher)
**Ngữ cảnh:** Trong đợt rà soát chất lượng kỹ thuật, hai lỗ hổng kiến trúc lớn đã được phát hiện trong cách thức tổ chức code của Ingestion Layer:
1. **Gộp module (Violating SRP):** Logic kéo dữ liệu chỉ số thị trường (`run_index`) bị gộp chung vào file [fetch_prices.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_prices.py) thay vì có file [fetch_index.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_index.py) riêng biệt. Điều này vi phạm nguyên tắc đơn nhiệm ("Mỗi module có 1 trách nhiệm — không gộp fetch_prices + fetch_index" trong PROJECT_RULES.md).
2. **Scatter Environment Reads:** File [config.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/config.py) chỉ định nghĩa các biến thô đơn giản, không sử dụng `@dataclass` như đặc tả. Hơn nữa, module `providers/registry.py` tự gọi `os.getenv("PROVIDER")` trực tiếp từ môi trường thay vì đi qua file cấu hình tập trung.

**Quyết định kiến trúc & Sửa đổi:**
1. **Thiết lập `@dataclass IngestionConfig`:** Xây dựng lại cấu trúc cấu hình tập trung trong [ingestion/config.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/config.py) để làm Single Source of Truth đọc env. Các biến được export ra module-level để làm alias đảm bảo tương thích ngược cho toàn bộ project.
2. **Giải phóng scatter reads:** Sửa đổi [providers/registry.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/providers/registry.py) import trực tiếp từ `config.provider`.
3. **Phân rã module kéo chỉ số:** Tạo mới file [ingestion/fetch_index.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_index.py) chứa hàm `run_index()` và CLI độc lập. Xóa bỏ hoàn toàn phần logic này khỏi [ingestion/fetch_prices.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/ingestion/fetch_prices.py) để đảm bảo tính module hóa.
4. **Cập nhật luồng điều phối Airflow:** Chỉnh sửa task `fetch_index` trong file [dags/dag_daily.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/dags/dag_daily.py) trỏ trực tiếp sang `python -m ingestion.fetch_index` thay vì lạm dụng các cờ skip của prices script.
5. **Bổ sung Unit Test tự động:** Cập nhật [tests/test_ingestion.py](file:///wsl.localhost/Ubuntu/home/naeouad/deproject/tests/test_ingestion.py), thêm test case `test_run_index_success` sử dụng MockProvider để kiểm toán độc lập cho index ingestion.

**Kết quả Nghiệm thu:**
- Toàn bộ 5 bài unit test đều đã **PASSED 100%** (`tests/test_ingestion.py::test_run_index_success PASSED [100%]`).
- Chạy thử nghiệm thủ công với `MockProvider` nạp dữ liệu hoàn hảo: 30 dòng prices và 2 dòng index được upsert thành công vào database mà không gặp bất kỳ lỗi logic nào.

