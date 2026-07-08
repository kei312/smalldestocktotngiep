#!/usr/bin/env python
"""
scripts/demo_helper.py
Hỗ trợ tự động hóa cấu hình, reset dữ liệu demo và kiểm tra trạng thái cho các buổi demo sản phẩm.
Cách dùng:
  python scripts/demo_helper.py switch-demo  : Đổi .env sang Database Demo và Mock Provider
  python scripts/demo_helper.py switch-real  : Đổi .env sang Database Thật và Vnstock Provider
  python scripts/demo_helper.py reset        : Xóa sạch dữ liệu trong database demo và tái tạo schema rỗng
  python scripts/demo_helper.py status       : Kiểm tra cấu hình .env và thống kê số dòng trong database
"""

import os
import sys
import argparse
import logging

# Cấu hình encoding utf-8 cho stdout/stderr để tránh lỗi UnicodeEncodeError trên Windows console
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python cũ hơn 3.7
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Thiết lập log cơ bản
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ENV_PATH = ".env"

def read_env():
    """Đọc file .env thành dictionary."""
    if not os.path.exists(ENV_PATH):
        logger.error("Không tìm thấy file .env ở thư mục gốc!")
        sys.exit(1)
    
    env_vars = {}
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            env_vars[key.strip()] = val.strip()
    return env_vars

def write_env(updates):
    """Cập nhật các biến trong file .env."""
    if not os.path.exists(ENV_PATH):
        logger.error("Không tìm thấy file .env ở thư mục gốc!")
        sys.exit(1)
        
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    new_lines = []
    updated_keys = set()
    
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _ = stripped.split("=", 1)
            key = key.strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}\n")
                updated_keys.add(key)
                continue
        new_lines.append(line)
        
    # Thêm các key mới nếu chưa có
    for key, val in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={val}\n")
            
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

def get_db_connection():
    """Tạo kết nối tới postgres sử dụng biến môi trường thực tế."""
    import psycopg2
    # Đọc từ biến môi trường thực tế (đã được load bởi Docker hoặc shell)
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "stock_db")
    user = os.getenv("DB_USER", "airflow")
    password = os.getenv("DB_PASS", "airflow")
    
    # Kiểm tra xem có đang chạy trong Docker container hay không
    in_docker = os.path.exists("/.dockerenv")
    if not in_docker and host == "db":
        host = "localhost"
        
    logger.info("Kết nối database: host=%s, port=%s, dbname=%s, user=%s", host, port, dbname, user)
    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password
    )

def handle_switch_demo():
    """Chuyển sang môi trường Demo offline."""
    updates = {
        "DB_NAME": "stock_db_demo",
        "PROVIDER": "mock"
    }
    write_env(updates)
    print("\n" + "="*70)
    print(">>> ĐÃ CHUYỂN CẤU HÌNH SANG MÔI TRƯỜNG DEMO OFFLINE")
    print("   - Database: stock_db_demo")
    print("   - Provider: mock")
    print("="*70)
    print("[YÊU CẦU BẮT BUỘC]:")
    print("   Hãy chạy lệnh sau để Docker Compose reload lại biến môi trường:")
    print("   docker compose up -d")
    print("="*70 + "\n")

def handle_switch_real():
    """Chuyển sang môi trường Thật online."""
    updates = {
        "DB_NAME": "stock_db",
        "PROVIDER": "vnstock"
    }
    write_env(updates)
    print("\n" + "="*70)
    print(">>> ĐÃ CHUYỂN CẤU HÌNH SANG MÔI TRƯỜNG THẬT / PHÁT TRIỂN")
    print("   - Database: stock_db")
    print("   - Provider: vnstock")
    print("="*70)
    print("[YÊU CẦU BẮT BUỘC]:")
    print("   Hãy chạy lệnh sau để Docker Compose reload lại biến môi trường:")
    print("   docker compose up -d")
    print("="*70 + "\n")

def handle_reset():
    """Xóa sạch dữ liệu demo và tái tạo schema rỗng."""
    import psycopg2
    # Load .env để chắc chắn biến môi trường được cập nhật nếu chạy độc lập
    from dotenv import load_dotenv
    load_dotenv()
    
    db_name = os.getenv("DB_NAME", "")
    
    if "demo" not in db_name.lower():
        print("\n[LỖI NGUY HIỂM]: Database hiện tại là '{}' (Không phải database demo!).".format(db_name))
        print("   Script này chỉ được phép reset các database có tên chứa chữ 'demo' (ví dụ: 'stock_db_demo')")
        print("   để tránh xóa nhầm dữ liệu phát triển thật của bạn.")
        print("   Vui lòng chạy 'python scripts/demo_helper.py switch-demo' trước.\n")
        sys.exit(1)
        
    print("[RESET] Đang kết nối tới database demo '{}' để dọn dẹp dữ liệu...".format(db_name))
    
    try:
        conn = get_db_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Drop các schema để xóa sạch hoàn toàn
            print("   - Đang xóa các schema cũ (bronze, public_silver, public_gold)...")
            cur.execute("DROP SCHEMA IF EXISTS bronze CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS public_silver CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS public_gold CASCADE;")
            
            # Đọc file sql/init_schema.sql để tái tạo cấu trúc Bronze
            schema_sql_path = "sql/init_schema.sql"
            if not os.path.exists(schema_sql_path):
                logger.error("Không tìm thấy file sql/init_schema.sql!")
                sys.exit(1)
                
            print("   - Đang tái tạo cấu trúc Bronze từ sql/init_schema.sql...")
            with open(schema_sql_path, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            cur.execute(schema_sql)
            
        conn.close()
        print("\n" + "="*70)
        print("[SUCCESS] RESET DATABASE DEMO THÀNH CÔNG!")
        print("   - Toàn bộ dữ liệu cũ đã bị xóa sạch.")
        print("   - Bảng `bronze` rỗng và các bảng partition đã được tái tạo.")
        print("   - Các schema silver và gold đã được xóa sạch (sẽ tự động tạo lại khi chạy dbt).")
        print("   - Sẵn sàng chạy lượt demo mới từ đầu (Input rỗng)!")
        print("="*70 + "\n")
        
    except Exception as e:
        logger.error("Không thể kết nối hoặc reset database: %s", e)
        print("\n[LƯU Ý] Hãy chắc chắn rằng:")
        print("   1. Docker postgres container đang hoạt động.")
        print("   2. Bạn đã chạy lệnh 'CREATE DATABASE {};' trước đó.".format(db_name))
        sys.exit(1)

def handle_status():
    """Kiểm tra cấu hình hiện tại và thống kê số dòng."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
    
    db_name = os.getenv("DB_NAME", "")
    provider = os.getenv("PROVIDER", "")
    host = os.getenv("DB_HOST", "")
    user = os.getenv("DB_USER", "")
    
    print("\n" + "="*70)
    print("[STATUS] TRẠNG THÁI CẤU HÌNH HIỆN TẠI (trong hệ thống)")
    print("   - Database: {}".format(db_name))
    print("   - Provider: {}".format(provider))
    print("   - Host/User: {} / {}".format(host, user))
    print("="*70)
    
    print("[STATUS] Đang kết nối tới database và thống kê số lượng dữ liệu...")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            tables = [
                ("Bronze Prices", "bronze.bronze_prices"),
                ("Bronze Index", "bronze.bronze_index"),
                ("Silver Prices", "public_silver.silver_prices"),
                ("Gold Fact Prices", "public_gold.fact_stock_price"),
                ("Gold Fact Indicators", "public_gold.fact_stock_indicators"),
                ("Gold Market Summary", "public_gold.fact_market_summary")
            ]
            
            print("\n   {:<25} | {:<25} | {:<10}".format("Tên Tầng", "Schema.Table", "Số Dòng"))
            print("   " + "-"*68)
            
            for name, table in tables:
                try:
                    cur.execute("SELECT COUNT(*) FROM {};".format(table))
                    count = cur.fetchone()[0]
                    print("   {:<25} | {:<25} | {:<10}".format(name, table, count))
                except psycopg2.Error:
                    # Nếu bảng chưa tồn tại (chưa chạy dbt run)
                    conn.rollback()
                    print("   {:<25} | {:<25} | {:<10}".format(name, table, "Chưa tạo (Rỗng)"))
        conn.close()
        print("="*70 + "\n")
    except Exception as e:
        print("[ERROR] Không thể kết nối tới Database để lấy số liệu: {}\n".format(e))

def main():
    parser = argparse.ArgumentParser(description="Công cụ hỗ trợ Demo Offline & Quản lý database.")
    parser.add_argument("action", choices=["switch-demo", "switch-real", "reset", "status"],
                        help="Hành động cần thực hiện")
    
    args = parser.parse_args()
    
    if args.action == "switch-demo":
        handle_switch_demo()
    elif args.action == "switch-real":
        handle_switch_real()
    elif args.action == "reset":
        handle_reset()
    elif args.action == "status":
        handle_status()

if __name__ == "__main__":
    main()
