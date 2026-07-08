# HƯỚNG DẪN QUẢN LÝ BẢNG BIỂU VÀ HÌNH ẢNH

Tài liệu này lưu trữ danh sách, vị trí và thông tin chi tiết của toàn bộ bảng biểu và hình ảnh sử dụng trong `BAOCAO_DATN.md`. 
Nó đóng vai trò như một "kho lưu trữ" để bạn có thể xem lại hoặc vẽ lại (chèn ảnh thực tế) sau này khi cần thiết.

---

## 1. Danh sách Hình ảnh (Figures)

Hiện tại trong file `BAOCAO_DATN.md`, toàn bộ các hình ảnh đã được **mô phỏng lại bằng Text/ASCII** để có thể đọc hiểu ngay lập tức. Sau này khi bạn chụp ảnh từ phần mềm, bạn chỉ cần xóa khối code `text` / `mermaid` đó đi và thay bằng đường link ảnh thật.

| Hình | Nội dung / Ý nghĩa | Loại Text hiện tại | Vị trí (Mục) |
|---|---|---|---|
| **Hình 3.1** | Sơ đồ kiến trúc Data Flow tổng thể | Mã **Mermaid** có thể render trực tiếp ra biểu đồ chuẩn | Mục 3.1 |
| **Hình 4.1** | Airflow UI DAG Graph View - luồng thực thi các task | **ASCII Text** thể hiện các ô check_postgres -> fetch... | Mục 4.4 |
| **Hình 4.2** | Airflow UI DAG Runs history - lịch sử chạy daily | **ASCII Text** mô phỏng lưới Grid với biểu tượng [O] (Xanh/Success) | Mục 4.4 |
| **Hình 4.3** | Power BI Dashboard: Market Overview | **ASCII Text** mô tả các thành phần: Filter, KPI Volume, Biểu đồ VNINDEX | Mục 4.6.1 |
| **Hình 4.4** | Power BI Dashboard: Stock Analysis | **ASCII Text** mô tả biểu đồ Nến Nhật, RSI, MACD, Volume | Mục 4.6.1 |

*(Code Mermaid của Hình 3.1 đã được nhúng thẳng vào file `BAOCAO_DATN.md` ở Mục 3.1, bạn chỉ cần dùng các Markdown viewer hỗ trợ Mermaid là hình sẽ tự hiện ra, không cần vẽ tay lại).*

---

## 2. Danh sách Bảng biểu (Tables)

Tất cả 17 bảng biểu dưới đây đều đã được thiết kế hoàn thiện bằng định dạng Markdown Table ngay trong file `BAOCAO_DATN.md`. Nội dung của chúng là cố định và không cần chèn thêm ảnh:

- **Bảng 1.1** — Tóm tắt kết quả Core Scope đã đạt được (Liệt kê số liệu toàn sàn HoSE, số lượng request)
- **Bảng 1.2** — Phân định phạm vi đồ án (Chia thành CORE, MỞ RỘNG, STRETCH, LOẠI BỎ)
- **Bảng 3.1** — Yêu cầu chức năng (Functional Requirements)
- **Bảng 3.2** — Yêu cầu phi chức năng (Non-Functional Requirements)
- **Bảng 3.3** — Cấu trúc đầu ra tầng Silver (`silver_prices`)
- **Bảng 3.4** — Mô tả các bảng Gold (Liệt kê 4 bảng: `fact_stock_price`, `fact_stock_indicators`, `dim_stock`, `dim_date`)
- **Bảng 3.5** — Hợp đồng dữ liệu giữa các tầng (Giải thích quy tắc Data Contract)
- **Bảng 3.6** — So sánh thiết kế Daily DAG và Backfill DAG
- **Bảng 4.1** — Hiệu quả tối ưu hóa throughput thu thập dữ liệu (Thống kê req/phút của vnstock)
- **Bảng 4.2** — Số liệu backfill thực nghiệm theo phạm vi (VN30 vs Toàn sàn HoSE ~ 8-9 phút)
- **Bảng 4.3** — Warm-up Reference — số phiên tối thiểu cần thiết cho từng chỉ báo
- **Bảng 4.4** — Minh họa Toán học về sự hội tụ sai số của EMA26 theo độ dài Lookback
- **Bảng 4.5** — Bốn gotcha của dbt-postgres 1.10.x khi triển khai Incremental Model
- **Bảng 5.1** — Các lớp kiểm thử áp dụng trong hệ thống (Unit test, Data test, DAG test)
- **Bảng 5.2** — Kết quả kiểm thử G-03 (So sánh sai số MACD giữa SQL và Python = 0.0000%)
- **Bảng 5.3** — Kiểm toán số dòng dữ liệu sau 2 lần chạy DAG liên tiếp (Chứng minh tính Idempotent)
- **Bảng 5.4** — Tổng hợp các chỉ số hiệu năng chính
- **Bảng 5.5** — Tổng hợp lỗi kỹ thuật điển hình và giải pháp khắc phục (Troubleshooting)

*(Chi tiết các trường dữ liệu cụ thể (Data Dictionary) của từng bảng Database được lưu ở **Phụ lục B** của file báo cáo).*
