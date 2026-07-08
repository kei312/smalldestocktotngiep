# Kế Hoạch Demo Đồ Án Tốt Nghiệp: Vietnam Stock Market Data Engineering Pipeline

Bản kế hoạch này hướng dẫn bạn cách trình bày dự án một cách chuyên nghiệp, đi sâu vào kỹ thuật và **đặc biệt là show được sự biến đổi của dữ liệu qua từng tầng (Data Lineage)**.

---

## 1. Mục Tiêu Buổi Demo
- Chứng minh hệ thống chạy luồng dữ liệu thực tế: Từ thu thập $\rightarrow$ Lưu trữ $\rightarrow$ Làm sạch $\rightarrow$ Tính toán chỉ số $\rightarrow$ Trực quan hoá.
- Trình bày mạch lạc để hội đồng thấy bạn nắm rõ 100% cách dữ liệu chảy trong hệ thống.
- Thời lượng: 15 - 20 phút.

---

## 2. Kịch Bản Demo: Truy vết dữ liệu (Data Tracing Demo)

Hãy chọn **1 mã cổ phiếu (ví dụ: FPT)** và **1 ngày cụ thể (ví dụ ngày gần nhất)** làm sợi chỉ đỏ xuyên suốt buổi demo.

### Bước 1: Data Ingestion (Airflow & Python)
- **Hành động**: Mở Airflow UI, show DAG `dag_daily`.
- **Trình bày**: 
  - "Hệ thống của em dùng Airflow để lập lịch chạy mỗi ngày. Code Python sẽ gọi API Vnstock để kéo dữ liệu về."
  - "Điểm đặc biệt trong code Python là em dùng cơ chế **Idempotent** (Upsert: `ON CONFLICT DO UPDATE`). Nếu hôm nay em lỡ chạy DAG 3 lần, dữ liệu trong Database vẫn không bị nhân lên gấp 3, đảm bảo tính vẹn toàn dữ liệu."

### Bước 2: Dữ liệu thô - Tầng Bronze (PostgreSQL)
- **Hành động**: Mở psql hoặc DBeaver, chạy query:
  ```sql
  SELECT code, date, open, close, volume FROM bronze.bronze_prices 
  WHERE code = 'FPT' ORDER BY date DESC LIMIT 5;
  ```
- **Trình bày**: 
  - "Đây là dữ liệu thô vừa lấy từ API về. Đặc điểm của tầng Bronze là giữ nguyên bản dữ liệu gốc nhất có thể. Em dùng tính năng **Partitioning theo năm** của Postgres để chia nhỏ bảng này ra, giúp sau này select dữ liệu 10 năm vẫn cực kỳ nhanh."

### Bước 3: Dữ liệu sạch - Tầng Silver (dbt)
- **Hành động**: Chuyển sang thư mục `dbt/models/silver/` trong VS Code, show file `silver_prices.sql`. Sau đó query DB:
  ```sql
  SELECT symbol, trade_date, close, is_valid, dq_flag 
  FROM public_silver.silver_prices WHERE symbol = 'FPT' ORDER BY trade_date DESC LIMIT 5;
  ```
- **Trình bày**: 
  - "Dữ liệu Bronze chưa tin cậy để dùng ngay. Em dùng công cụ **dbt** để chuyển nó sang tầng Silver."
  - "Ở tầng này, em đổi tên cột (`code` $\rightarrow$ `symbol`), ép kiểu dữ liệu chuẩn xác, và quan trọng nhất là thêm cột kiểm tra chất lượng `is_valid` và `dq_flag` (Data Quality). Ví dụ, nếu volume âm hoặc giá đóng cửa rỗng, nó sẽ bị cắm cờ lỗi ở đây thay vì làm hỏng biểu đồ."

### Bước 4: Xử lý logic phức tạp - Tầng Gold Intermediate (dbt)
- **Hành động**: Show file macro `dbt/macros/calculate_ema.sql` và bảng intermediate. Mở query DB:
  ```sql
  SELECT symbol, trade_date, close, ema_12 
  FROM public_gold.int_ema12 WHERE symbol = 'FPT' ORDER BY trade_date DESC LIMIT 5;
  ```
- **Trình bày**: 
  - "Để tính được chỉ báo kỹ thuật như MACD, em không tính nhồi nhét vào 1 file. Em tách ra các bảng trung gian (intermediate) như bảng tính riêng EMA 12 ngày, EMA 26 ngày."
  - "Việc tách nhỏ này giúp em dễ debug. Nếu công thức MACD cuối cùng bị sai, em có thể query vào bảng `int_ema12` để kiểm tra lỗi do đâu."

### Bước 5: Dữ liệu sẵn sàng cho Business - Tầng Gold Fact (dbt)
- **Hành động**: Show model `fact_stock_indicators.sql`. Truy vấn DB:
  ```sql
  SELECT symbol, trade_date, close, ema_12, ema_26, macd_line, macd_signal 
  FROM public_gold.fact_stock_indicators WHERE symbol = 'FPT' ORDER BY trade_date DESC LIMIT 5;
  ```
- **Trình bày**: 
  - "Đây là bảng Fact cuối cùng. Nó join tất cả các chỉ số đã tính toán lại với nhau."
  - "Điểm mạnh kỹ thuật: Bảng này em cấu hình chạy kiểu **Incremental**. Mỗi ngày dbt chỉ tính toán và append dữ liệu của ngày mới, chứ không tính lại toàn bộ dữ liệu lịch sử từ đầu. Điều này tối ưu tài nguyên server rất lớn."

### Bước 6: Trực quan hóa (Power BI)
- **Hành động**: Mở Power BI. Show tính năng Model View.
- **Trình bày**: 
  - "Mô hình em thiết kế theo chuẩn **Star Schema** gồm các bảng Fact (Fact Indicators, Fact Summary) bao quanh bởi các bảng Dim (Dim Stock, Dim Date)."
  - Bấm chọn mã 'FPT' trên biểu đồ để show sự biến đổi của giá và đường MACD. Mọi thứ mượt mà vì dữ liệu đã được tính sẵn dưới DB.

### Bước 7: Kịch bản rủi ro (Mock Provider)
- **Hành động**: Mở file `providers/mock_provider.py` và file `.env`.
- **Trình bày**:
  - "Nếu API cấp dữ liệu bị chết, hệ thống của em không bị sập. Em áp dụng Design Pattern (Strategy/Interface) để tạo ra `MockProvider`."
  - Đổi biến môi trường sang `mock`, chạy lại Airflow và show cho hội đồng thấy quy trình vẫn hoàn tất 100% bằng dữ liệu giả lập.

---

## 3. Checklist Chuẩn Bị
1. [ ] Chạy sẵn Docker (Postgres, Airflow).
2. [ ] Test kết nối Power BI tới Database (cổng 5432).
3. [ ] Lưu sẵn các câu lệnh query ở trên vào một file SQL trên máy hoặc DataGrip/DBeaver để lúc demo bấm Run (F5) cho nhanh, không phải gõ tay.
4. [ ] Mở sẵn VS Code ở chế độ Presentation (Ctrl + + để font chữ to ra).
