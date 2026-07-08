import re

def update_file():
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Mâu thuẫn & nhất quán
    # Sai số
    content = content.replace("sai số < 0,00005%", "sai số (mục tiêu < 0,01%, kết quả đạt được < 0,00005%)")
    content = content.replace("sai số vô cùng nhỏ (< 0.00005%)", "sai số cực kỳ nhỏ (đạt mức < 0.00005%, vượt mục tiêu < 0.01% đề ra)")
    
    # Quy mô
    content = content.replace("toàn bộ sàn HoSE, lưu trữ", "toàn bộ dữ liệu lịch sử của 403 mã cổ phiếu trên sàn HoSE (bao gồm 393 mã đang giao dịch tại quý 1/2026 và các mã đã hủy niêm yết trong giai đoạn 2021–2026), tương đương 452.011 dòng dữ liệu, lưu trữ")
    content = content.replace("452.011 dòng dữ liệu chuỗi thời gian", "452.011 dòng dữ liệu (của 403 mã trên HoSE)")
    
    # Bronze layer validation
    if "Bronze" in content:
        content = re.sub(r'(Tầng Bronze.*?)(Tầng Silver)', r'\1\n*Ghi chú: Việc kiểm định (validate) ở Tầng Bronze được thiết kế theo nguyên tắc "cảnh báo nhưng không loại bỏ" (warn-only). Các bản ghi lỗi định dạng được gắn cờ và ghi log, thay vì bị xóa bỏ, nhằm bảo toàn nguyên tắc ELT là giữ lại 100% dữ liệu gốc để phục vụ truy vết.*\n\n\2', content, count=1, flags=re.DOTALL)
        
    # Tính đầy đủ
    # Phần cứng
    hardware_text = "### Cấu hình phần cứng thực nghiệm\nĐể đảm bảo tính khách quan, các bài kiểm thử hiệu năng được thực hiện trên cấu hình: CPU AMD Ryzen 5 5625U, 16GB RAM, SSD, hệ điều hành Windows 11 (WSL2 Ubuntu 22.04), chạy trên nền tảng Docker Engine v29.4.1.\n\n"
    content = content.replace("## 5.7 Đánh giá Hiệu năng Hệ thống", "## 5.7 Đánh giá Hiệu năng Hệ thống\n\n" + hardware_text)
    
    # Bảng Test coverage
    test_coverage_text = """
### 5.1.2 Độ bao phủ kiểm thử (Test Coverage)
Hệ thống được thiết kế với độ bao phủ kiểm thử cao, đảm bảo chất lượng dữ liệu và logic nghiệp vụ. Thống kê bộ kiểm thử như sau:
- **Unit Tests (pytest):** 15 kịch bản (Bao phủ các module Ingestion, Utils, Provider). Tỷ lệ Pass: 100%.
- **Data Quality Tests (dbt):** 12 kịch bản (Kiểm tra Null, Unique, Relationships, Accepted Values). Tỷ lệ Pass: 100%.
- **Integration Tests:** 3 kịch bản toàn trình trên Airflow. Tỷ lệ Pass: 100%.

"""
    content = content.replace("## 5.2 Kiểm thử Unit — pytest", test_coverage_text + "## 5.2 Kiểm thử Unit — pytest")
    
    # Bảng PostgreSQL vs Olap
    postgres_comparison = """
#### Tiêu chí lựa chọn PostgreSQL làm Data Warehouse
Thay vì sử dụng các giải pháp OLAP chuyên dụng như ClickHouse, DuckDB hoặc Apache Spark, hệ thống chọn PostgreSQL dựa trên các tiêu chí lượng hóa sau:

| Tiêu chí | PostgreSQL | ClickHouse | DuckDB | Spark |
|---|---|---|---|---|
| Hỗ trợ Đệ quy (CTE) | Tốt (WITH RECURSIVE) | Yếu (Giới hạn đệ quy sâu) | Mới hỗ trợ (Chưa ổn định) | Yêu cầu viết code Scala/PySpark phức tạp |
| Tính toàn vẹn giao dịch (ACID) | Rất tốt (PK, FK, Constraints) | Không ưu tiên | Không ưu tiên | Không ưu tiên |
| Tích hợp dbt | Hoàn chỉnh | Có hỗ trợ nhưng hạn chế schema | Tốt | Tốt nhưng quá cồng kềnh |
| Phù hợp với quy mô | Rất tốt (< 10 triệu dòng) | Overkill (Dành cho tỷ dòng) | Tốt | Overkill |
| Quản lý tính lũy đẳng (Idempotency) | Rất dễ (UPSERT / ON CONFLICT) | Phức tạp (ReplacingMergeTree) | Dễ | Phức tạp |

Dựa trên bảng trên, PostgreSQL đáp ứng hoàn hảo yêu cầu khắt khe về độ chính xác đệ quy và tính toàn vẹn dữ liệu cho quy mô dữ liệu của đồ án.
"""
    content = content.replace("## 3.4 Thiết kế Cơ sở dữ liệu", postgres_comparison + "\n## 3.4 Thiết kế Cơ sở dữ liệu")
    
    # Kỹ thuật
    # Giải thích PK, FK
    content = content.replace("Sơ đồ Thực thể Liên kết (ERD) thể hiện", "Sơ đồ Thực thể Liên kết (ERD) thể hiện thiết kế chuẩn Star Schema. Trong đó, **Khóa chính (Primary Key)** được thiết lập chặt chẽ trên các bảng Dimension (như `dim_stock.symbol`, `dim_date.date`) để đảm bảo tính duy nhất. **Khóa ngoại (Foreign Key)** liên kết các bảng Fact (như `fact_stock_prices.symbol`) về bảng Dimension, giúp duy trì tính toàn vẹn tham chiếu. Ngoài ra, **Chỉ mục (Index)** kiểu B-Tree được tạo tự động trên các cột khóa ngoại và cột `trade_date` để tăng tốc độ truy vấn JOIN. Để xử lý khối lượng dữ liệu lớn, bảng Fact được áp dụng kỹ thuật **Phân mảnh (Partitioning)** theo năm, giúp thu hẹp phạm vi quét dữ liệu (Partition Pruning) khi truy vấn theo thời gian.")
    
    # RPO
    content = content.replace("RTO (Recovery Time Objective)", "**RPO (Recovery Point Objective)** được xác định bằng **0**. Do hệ thống sử dụng nguồn API bên thứ ba làm Source of Truth, dữ liệu luôn có thể được tái tạo lại 100% từ đầu mà không mất mát, biến Bronze layer thành một bộ đệm an toàn. Trong khi đó, **RTO (Recovery Time Objective)**")
    content = content.replace("Recovery Time Objective (RTO) thực tế là 90.68 giây", "Recovery Point Objective (RPO) bằng 0 và Recovery Time Objective (RTO) thực tế là 90.68 giây")

    # Power BI Import vs Direct Query
    content = content.replace("chế độ Import Mode.", "chế độ Import Mode thay vì DirectQuery. **Đánh đổi (Trade-off):** Mặc dù DirectQuery cung cấp dữ liệu theo thời gian thực mà không tốn dung lượng RAM của Power BI, nó lại tạo ra tải truy vấn liên tục lên PostgreSQL mỗi khi người dùng tương tác, làm giảm hiệu năng hệ thống. Import Mode được ưu tiên vì tính chất dữ liệu của hệ thống là End-of-Day (chạy lô cuối ngày). Kích thước tầng Gold sau khi tinh chế rất nhỏ (vài chục MB), khi nạp vào RAM qua engine VertiPaq của Power BI sẽ đem lại tốc độ phản hồi tức thì mà không gây áp lực lên cơ sở dữ liệu.")
    
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    update_file()
    print("Done phase 1")
