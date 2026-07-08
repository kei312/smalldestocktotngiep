# Hướng Dẫn Cấu Hình Tự Động Publish Dashboard Lên GitHub Pages

Tài liệu này hướng dẫn chi tiết cách thiết lập để Airflow tự động cập nhật dữ liệu chứng khoán vào dashboard HTML và đẩy lên GitHub Pages lúc **18h20** hàng ngày.

---

## Bước 1: Tạo GitHub Personal Access Token (PAT)
Bạn cần một token có quyền ghi để Docker Airflow có thể tự động đẩy code lên repository của bạn.

1. Truy cập GitHub của bạn, nhấp vào ảnh đại diện góc trên cùng bên phải -> chọn **Settings** (Cài đặt).
2. Ở cột menu bên trái, cuộn xuống dưới cùng -> chọn **Developer settings**.
3. Chọn **Personal access tokens** -> chọn **Tokens (classic)**.
4. Chọn **Generate new token** -> chọn **Generate new token (classic)**.
5. Cấu hình các mục sau:
   - **Note**: `Airflow Dashboard Publisher`
   - **Expiration**: Chọn thời hạn hiệu lực (nên chọn `No expiration` hoặc `90 days`).
   - **Select scopes**: Tích chọn quyền **`repo`** (quyền duy nhất cần thiết để push code).
6. Cuộn xuống dưới cùng và nhấn nút màu xanh **Generate token**.
7. **Copy mã token** (có dạng `ghp_...`) và lưu lại ở một nơi an toàn. *Lưu ý: Mã này chỉ hiển thị duy nhất một lần.*

---

## Bước 2: Cấu Hình Biến Môi Trường
Mở file `.env` ở thư mục gốc của dự án trên máy tính của bạn và cập nhật thông tin:

```env
# GitHub configuration for dashboard auto-publishing
GITHUB_PAT=ghp_mã_token_bạn_vừa_tạo_ở_bước_1
GITHUB_REPO=github.com/kei312/smalldestocktotngiep.git
```

Sau khi sửa file `.env`, khởi động lại container Airflow để áp dụng cấu hình:
```bash
docker compose up -d
```

---

## Bước 3: Kích Hoạt GitHub Pages trên Repository
1. Vào repository của bạn trên GitHub: [smalldestocktotngiep](https://github.com/kei312/smalldestocktotngiep)
2. Chọn tab **Settings** (Cài đặt).
3. Chọn mục **Pages** ở danh sách menu bên trái.
4. Tại phần **Build and deployment**:
   - **Source**: Chọn `Deploy from a branch`.
   - **Branch**: Chọn nhánh **`gh-pages`** (Nhánh này sẽ tự động được tạo ra sau lần chạy đầu tiên của Airflow DAG).
   - **Folder**: Chọn thư mục **`/ (root)`**.
   - Nhấn nút **Save**.
5. Đợi 1-2 phút, GitHub sẽ xuất bản trang web của bạn tại địa chỉ:
   👉 **`https://kei312.github.io/smalldestocktotngiep/`**

---

## Bước 4: Kiểm Tra Hoạt Động
Bạn có thể kiểm tra xem hệ thống tự động đẩy dữ liệu hoạt động chính xác chưa bằng cách chạy lệnh trigger thủ công:
```bash
docker exec airflow-container airflow dags trigger publish_dashboard_pipeline
```
Sau đó kiểm tra lịch sử commit trên GitHub xem có commit tự động từ `Airflow Bot` hay không.
