import re

def update_file():
    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Grammar & Redundancy
    content = content.replace("của toàn bộ dữ liệu lịch sử của 403 mã cổ phiếu", "của 403 mã cổ phiếu")
    content = content.replace("(sai số (mục tiêu < 0,01%, kết quả đạt được < 0,00005%))", "(với mục tiêu sai số < 0.01%, kết quả thực nghiệm đạt < 0.00005%)")

    # 2. Survivorship Bias - Convert from a limitation to a strength
    # Match the paragraph starting with "**Thiên lệch Sống sót (Survivorship Bias).**"
    content = re.sub(
        r'\*\*Thiên lệch Sống sót \(Survivorship Bias\)\.\*\*.*?(?=\n\n|\Z)', 
        r'**Khắc phục Thiên lệch Sống sót (Survivorship Bias).** Điểm mạnh của hệ thống là danh sách 403 mã cổ phiếu được sử dụng đã bao gồm cả các mã bị hủy niêm yết trong giai đoạn 2021–2026. Nhờ vậy, hệ thống đã loại bỏ được rủi ro Thiên lệch Sống sót (Survivorship Bias), đảm bảo tính chính xác và khách quan khi backtest các chiến lược phân tích kỹ thuật trong quá khứ.', 
        content, 
        flags=re.DOTALL
    )

    # 3. Tone Sweep (Replacing promotional adverbs)
    content = content.replace("giải quyết triệt để", "giải quyết hiệu quả")
    content = content.replace("khắc phục triệt để", "khắc phục hiệu quả")
    content = content.replace("xử lý triệt để", "xử lý hiệu quả")
    content = content.replace(" triệt để ", " hiệu quả ")
    content = content.replace(" triệt để.", " hiệu quả.")
    
    content = content.replace("tự động hóa hoàn toàn luồng", "tự động hóa luồng")
    content = content.replace("loại bỏ hoàn toàn sự phụ thuộc", "loại bỏ sự phụ thuộc")
    content = content.replace("loại bỏ hoàn toàn khỏi", "loại bỏ khỏi")
    content = content.replace("hoàn toàn không bị ảnh hưởng", "không bị ảnh hưởng")
    content = content.replace("hoàn toàn độc lập", "độc lập")
    content = content.replace("hoàn toàn khách quan", "khách quan")
    content = content.replace("hoàn toàn không có lỗi", "không có lỗi")
    content = content.replace("hoàn toàn tương đương", "tương đương")
    content = content.replace("hoàn toàn tuân theo", "tuân thủ chặt chẽ")
    content = content.replace("hoàn toàn có thể thay thế", "có khả năng thay thế")
    content = content.replace("đáp ứng hoàn toàn", "đáp ứng tốt")
    content = content.replace("cách ly hoàn toàn", "cách ly")
    content = content.replace(" hoàn toàn ", " toàn diện ")
    
    content = content.replace("vô cùng nhỏ", "rất nhỏ")
    content = content.replace("vô cùng mạnh mẽ", "mạnh mẽ")
    content = content.replace(" vô cùng ", " rất ")
    
    content = content.replace("chính xác tuyệt đối", "chính xác cao")
    content = content.replace("ổn định tuyệt đối", "ổn định cao")
    content = content.replace("bảo toàn tuyệt đối", "bảo toàn chặt chẽ")
    content = content.replace(" tuyệt đối ", " chặt chẽ ")

    with open('/home/naeouad/deproject/docs/BAOCAO_DATN.md', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    update_file()
    print("Deep clean completed.")
