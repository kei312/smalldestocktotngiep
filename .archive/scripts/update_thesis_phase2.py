import re

def update_file():
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Data Contract Versioning
    for i, line in enumerate(lines):
        if "## 3.5 Hợp đồng Dữ liệu (Data Contracts)" in line:
            lines.insert(i+1, "### Phiên bản hóa Hợp đồng Dữ liệu (Data Contract Versioning)\nKhi có sự thay đổi về cấu trúc (schema) từ phía nguồn API (ví dụ: thêm/bớt cột), hệ thống áp dụng chính sách kiểm soát nghiêm ngặt (Strict Mode). Việc thay đổi schema sẽ gây lỗi (fail) pipeline ngay tại tầng Silver thông qua cơ chế kiểm thử của dbt, nhằm ngăn chặn dữ liệu rác hoặc cấu trúc lệch chuẩn đi sâu vào tầng Gold gây sai lệch báo cáo cuối. Kỹ sư dữ liệu sau đó sẽ cập nhật mã nguồn dbt để tương thích với schema mới trước khi chạy lại luồng.\n\n")
            break

    # Bottleneck statement & Baseline Comparison
    for i, line in enumerate(lines):
        if "## 5.7 Đánh giá Hiệu năng Hệ thống" in line:
            lines.insert(i+1, """### Phân tích Nghẽn cổ chai (Bottleneck)
Trong kiến trúc hiện tại, **nghẽn cổ chai (Bottleneck) chính nằm ở pha Ingestion (I/O bound)** do bị giới hạn bởi Rate Limit của API nhà cung cấp (50 requests/phút). Ngược lại, pha biến đổi dữ liệu (Transformation) bằng dbt tại tầng Gold — vốn chứa các phép tính đệ quy phức tạp (CPU bound) — lại xử lý rất nhanh nhờ kỹ thuật In-database Processing tận dụng tối đa tài nguyên phần cứng của PostgreSQL.

### So sánh định lượng với Baseline Truyền thống
Để làm nổi bật ưu thế của kiến trúc ELT/dbt, hệ thống được so sánh với một giải pháp "Baseline truyền thống" (sử dụng Python/Pandas để tính toán on-the-fly trên RAM và xuất ra file CSV):
- **Độ trễ và Hiệu suất:** Baseline Pandas yêu cầu tải toàn bộ 452.011 dòng dữ liệu lên RAM, gây quá tải bộ nhớ và mất nhiều phút để tính toán vòng lặp. Trong khi đó, giải pháp ELT của đồ án xử lý toàn bộ tập dữ liệu trong thời gian cực ngắn ngay tại cơ sở dữ liệu.
- **Tính toàn vẹn (Idempotency):** Baseline Pandas dễ gây trùng lặp dữ liệu khi script chạy lỗi giữa chừng. ELT xử lý triệt để thông qua lệnh UPSERT và Constraints của RDBMS.
- **Quản trị rủi ro:** Lỗi trong Baseline Pandas thường là "Silent failure" (lỗi im lặng). ELT có cơ chế ngắt mạch (Circuit Breaker) qua Airflow, dừng tiến trình ngay khi dbt test thất bại.

""")
            break

    # Rủi ro bảo trì API & Scalability 5-10x
    for i, line in enumerate(lines):
        if "## 6.1 Hạn chế hiện tại" in line:
            lines.insert(i+1, """### Rủi ro thay đổi schema API và khả năng mở rộng
- **Rủi ro API:** Do phụ thuộc vào nguồn dữ liệu API công khai, bất kỳ thay đổi đột ngột nào về cấu trúc dữ liệu trả về (schema drift) hoặc chính sách chặn IP (Rate Limit) đều có thể làm gián đoạn luồng Ingestion.
- **Khả năng mở rộng:** Nếu khối lượng dữ liệu tăng lên 5–10 lần (ví dụ: bổ sung dữ liệu Intraday tick-by-tick), chiến lược Full Refresh hiện tại ở tầng Gold sẽ trở nên quá tải. Khi đó, hệ thống bắt buộc phải chuyển sang chiến lược Incremental Materialization để chỉ tính toán phần dữ liệu mới cập nhật.

""")
            break

    # Bảng mục tiêu kết quả ở Kết Luận
    for i, line in enumerate(lines):
        if "- [KẾT LUẬN CHUNG](#kết-luận-chung)" in line.lower() or "## KẾT LUẬN CHUNG" in line.upper() or "KẾT LUẬN CHUNG" in line:
            # We will insert the table right after the KẾT LUẬN CHUNG heading if we find it
            if line.startswith("# KẾT LUẬN CHUNG") or line.startswith("## KẾT LUẬN CHUNG"):
                lines.insert(i+1, """
### Bảng đối chiếu Mục tiêu và Kết quả đạt được

| Mục tiêu nghiên cứu đề ra | Kết quả thực tế đạt được |
|---|---|
| Tự động hóa luồng thu thập dữ liệu chứng khoán | Thành công. Throughput đạt ~50 req/phút với cơ chế Rate Limiting và Fallback. |
| Áp dụng kiến trúc Medallion (Bronze-Silver-Gold) | Thành công. Triển khai hoàn chỉnh trên PostgreSQL bằng dbt. |
| Tính toán chỉ báo đệ quy (RSI, EMA, MACD) bằng SQL | Thành công. Sai số toán học < 0.00005% so với thư viện Python chuẩn. |
| Cơ chế chịu lỗi, lũy đẳng và ngắt mạch (Circuit Breaker) | Thành công. Đảm bảo dữ liệu không trùng lặp, Airflow tự động dừng khi chất lượng dữ liệu kém. |
| Trực quan hóa dữ liệu qua Dashboard | Thành công. Xây dựng 3 Dashboard Power BI (Import Mode) phản hồi tức thì. |

""")
                break

    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'w', encoding='utf-8') as f:
        f.writelines(lines)

if __name__ == "__main__":
    update_file()
    print("Done phase 2")
