import re

def slugify(text):
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text).strip('-')
    return text

def update_file():
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    toc_start = -1
    toc_end = -1
    for i, line in enumerate(lines):
        if line.strip() == "## MỤC LỤC":
            toc_start = i
        if toc_start != -1 and line.strip() == "# CHƯƠNG 1: GIỚI THIỆU":
            toc_end = i
            break
            
    # Extract actual headings
    new_toc = ["\n"]
    for line in lines[toc_end:]:
        if line.startswith("# CHƯƠNG") or line.startswith("## ") or line.startswith("# KẾT LUẬN") or line.startswith("# PHỤ LỤC"):
            # skip ## MỤC LỤC or anything like that if it appears again
            if "## MỤC LỤC" in line or "## DANH MỤC" in line:
                continue
                
            level = 1 if line.startswith("# ") else 2
            heading_text = line.strip().lstrip("#").strip()
            slug = slugify(heading_text)
            
            indent = "" if level == 1 else "  "
            new_toc.append(f"{indent}- [{heading_text}](#{slug})\n")

    # This auto-generation might miss some custom slugs or manual entries like "LỜI CAM ĐOAN". 
    # Since I only moved a few sections, I'll just do text replacement in the existing TOC instead of full rebuild.
    pass

def simple_toc_update():
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Remove 2.8 and 2.9 from TOC
    content = re.sub(r'\s*- \[2\.8 Khái niệm Biến đổi Dữ liệu.*?\]\(.*?\)\n(.*?\n)?', '\n', content)
    content = re.sub(r'\s*- \[2\.9 Sự chuyển dịch từ ETL.*?\]\(.*?\)\n(.*?\n)?', '\n', content)
    
    # Merge 3.10 and 3.11 in TOC
    content = re.sub(r'- \[3\.10 Phân tích Đánh đổi Kiến trúc\].*?\n\s*- \[3\.11 Thảo luận Chuyên sâu về Quyết định Kiến trúc \(Architecture Discussion\)\].*?\n', 
                     '  - [3.10 Phân tích Đánh đổi và Thảo luận Chuyên sâu về Quyết định Kiến trúc](#310-phân-tích-đánh-đổi-và-thảo-luận-chuyên-sâu-về-quyết-định-kiến-trúc)\n', content)
                     
    # Add 3.12 and 3.13
    # Find chapter 4 in TOC and insert before it
    ch4_toc = r'- \[CHƯƠNG 4: TRIỂN KHAI HỆ THỐNG\]'
    new_items = "  - [3.12 Khái niệm Biến đổi Dữ liệu Lịch sử (Slowly Changing Dimensions - SCD)](#312-khái-niệm-biến-đổi-dữ-liệu-lịch-sử-slowly-changing-dimensions---scd)\n  - [3.13 Sự chuyển dịch từ ETL sang ELT trong Modern Data Stack](#313-sự-chuyển-dịch-từ-etl-sang-elt-trong-modern-data-stack)\n"
    content = re.sub(f'({ch4_toc})', f'{new_items}\\1', content)

    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    simple_toc_update()
    print("Done TOC update")
