#!/bin/bash

# =============================================
# API Request Script - Chứng khoán Việt Nam
# =============================================

# Cấu hình
PROJECT_DIR="/home/naeouad/deproject/ingestion"
SCRIPT_NAME="api_request.py"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/ingestion_$(date +%Y-%m-%d).log"

# Tạo thư mục logs nếu chưa có
mkdir -p "$LOG_DIR"

# Hàm ghi log
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=== BẮT ĐẦU INGESTION ==="

# Chuyển đến thư mục project
cd "$PROJECT_DIR" || {
    log "❌ LỖI: Không tìm thấy thư mục $PROJECT_DIR"
    exit 1
}

# Kiểm tra venv tồn tại
if [ ! -d "venv" ]; then
    log "❌ LỖI: Không tìm thấy venv. Vui lòng tạo venv trước."
    exit 1
fi

# Chạy script Python bằng Python trong venv
log "Đang chạy $SCRIPT_NAME..."

./venv/bin/python "$SCRIPT_NAME"

# Kiểm tra kết quả chạy
if [ $? -eq 0 ]; then
    log "✅ HOÀN THÀNH thành công!"
else
    log "❌ CÓ LỖI khi chạy script!"
    exit 1
fi

log "=== KẾT THÚC INGESTION ===\n"
