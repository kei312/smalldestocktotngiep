import re

def get_section(lines, start_heading):
    start_idx = -1
    end_idx = -1
    for i, line in enumerate(lines):
        if line.strip().startswith(start_heading):
            start_idx = i
            break
            
    if start_idx != -1:
        # Find the next heading of same or higher level (## or #)
        for i in range(start_idx + 1, len(lines)):
            if lines[i].strip().startswith("## ") or lines[i].strip().startswith("# "):
                end_idx = i
                break
        if end_idx == -1:
            end_idx = len(lines)
            
    return start_idx, end_idx

def update_file():
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Move 2.8 and 2.9 to Chapter 3
    s_28, e_28 = get_section(lines, "## 2.8")
    s_29, e_29 = get_section(lines, "## 2.9")
    
    # Check if they were found
    if s_28 != -1 and s_29 != -1:
        # Extract them
        # 2.8 and 2.9 are consecutive, so we can extract from s_28 to e_29
        scd_elt_content = lines[s_28:e_29]
        
        # We need to change the heading numbers from 2.8, 2.9 to 3.x
        for i in range(len(scd_elt_content)):
            if scd_elt_content[i].startswith("## 2.8"):
                scd_elt_content[i] = scd_elt_content[i].replace("2.8", "3.12")
            if scd_elt_content[i].startswith("## 2.9"):
                scd_elt_content[i] = scd_elt_content[i].replace("2.9", "3.13")
                
        # Find where to insert in Chapter 3. We'll insert before Chapter 4.
        ch4_idx, _ = get_section(lines, "# CHƯƠNG 4")
        
        # Now we need to delete from the original list (delete in reverse order)
        del lines[s_28:e_29]
        
        # Recalculate ch4_idx because we deleted lines before it
        ch4_idx, _ = get_section(lines, "# CHƯƠNG 4")
        
        # Insert before chapter 4
        lines = lines[:ch4_idx] + scd_elt_content + lines[ch4_idx:]

    # Merge 3.10 and 3.11
    # First find 3.10 and 3.11
    s_310, e_310 = get_section(lines, "## 3.10")
    s_311, e_311 = get_section(lines, "## 3.11")
    
    if s_310 != -1 and s_311 != -1:
        # We'll merge them. Basically just change the title of 3.10 and remove title 3.11
        for i in range(s_310, e_310):
            if lines[i].startswith("## 3.10"):
                lines[i] = "## 3.10 Phân tích Đánh đổi và Thảo luận Chuyên sâu về Quyết định Kiến trúc\n"
                break
                
        for i in range(s_311, e_311):
            if lines[i].startswith("## 3.11"):
                lines[i] = "### Thảo luận Chuyên sâu (Architecture Discussion)\n"
                break

    # Đóng góp khoa học -> Gom vào 1.5
    s_15, e_15 = get_section(lines, "## 1.5")
    if s_15 != -1:
        for i in range(s_15, e_15):
            if lines[i].startswith("## 1.5"):
                lines[i] = "## 1.5 Ý nghĩa và Đóng góp khoa học thực tiễn\n"
                break
                
        lines.insert(s_15 + 1, "Đồ án có các đóng góp khoa học chính như sau:\n- Ứng dụng kỹ thuật In-database Processing bằng dbt và SQL đệ quy để giải quyết bài toán tính toán chỉ báo tài chính ở quy mô lớn trực tiếp tại PostgreSQL, giảm tải I/O và RAM.\n- Áp dụng nguyên lý thiết kế Idempotency trong pipeline để ngăn ngừa dữ liệu trùng lặp khi chạy lại.\n- Xây dựng thành công kiến trúc Data Lakehouse (Medallion) hoàn chỉnh, từ Data Ingestion, Quality Gating, tới Visualization.\n\n")

    # Add Threats to Validity to 6.1
    s_61, e_61 = get_section(lines, "## 6.1")
    if s_61 != -1:
        lines.insert(e_61, "\n### Các yếu tố rủi ro ảnh hưởng đến tính đúng đắn (Threats to Validity)\nKết quả thực nghiệm có thể bị ảnh hưởng bởi một số rủi ro (Threats to Validity) bao gồm: Sự khác biệt về cách làm tròn số (floating-point arithmetic) giữa thư viện Python `ta` và PostgreSQL có thể dẫn đến sai số dư ở thập phân; Dữ liệu nguồn API không nhất quán giữa các ngày có thể làm lệch chỉ báo đệ quy (do tính phụ thuộc vào lịch sử dài). Mặc dù sai số đã đạt mức cực thấp (< 0.00005%), trong các chiến lược giao dịch High-Frequency Trading (HFT) đòi hỏi độ chính xác tuyệt đối, những rủi ro này cần được đánh giá nghiêm ngặt hơn.\n\n")

    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'w', encoding='utf-8') as f:
        f.writelines(lines)

if __name__ == "__main__":
    update_file()
    print("Done phase 3")
