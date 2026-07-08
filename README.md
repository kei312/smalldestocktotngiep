# Vietnam Stock Market Data Engineering Pipeline

Một hệ thống tự động thu thập, xử lý và trực quan hóa dữ liệu thị trường chứng khoán Việt Nam hàng ngày theo kiến trúc **Medallion (Bronze -> Silver -> Gold)**, được điều phối bởi **Apache Airflow**, biến đổi dữ liệu bằng **dbt (PostgreSQL)**, và kết nối báo cáo qua **Power BI** & **HTML Dashboard**.

---

## 🏗️ 1. Giới thiệu & Kiến trúc hệ thống (Architecture)

```mermaid
graph TD
    %% Providers & Ingestion
    subgraph Ingestion_Layer ["Ingestion Layer (Python)"]
        subgraph Providers ["Provider Layer (Vnstock)"]
            VCI["Nguồn VCI"]
            KBSV["Nguồn KBSV"]
        end
        RL["Rate Limiter"]
        TP["Thread Pool (5 workers)"]
        FP["fetch_prices.py"]
        
        Providers --> RL
        RL --> TP
        TP --> FP
    end

    %% Orchestrator
    Airflow((Apache Airflow<br/>Orchestrator))

    %% Bronze Layer
    subgraph Bronze_Layer ["Bronze Layer (PostgreSQL)"]
        B_Prices[("bronze_prices<br/>(JSONB, Partitioned)")]
        B_Index[("bronze_index")]
        B_VN30[("bronze_vn30_components")]
    end

    FP -->|UPSERT Idempotent| B_Prices

    %% Silver Layer
    subgraph Silver_Layer ["Silver Layer (dbt)"]
        S_Prices["silver_prices<br/>(Kiểm định chất lượng:<br/>Clean, Cast, Flag is_valid)"]
    end

    B_Prices -->|Clean, Cast, Flag is_valid| S_Prices

    %% Gold Layer
    subgraph Gold_Layer ["Gold Layer (dbt - Star Schema)"]
        F_Price["fact_stock_price"]
        IM["Intermediate Models<br/>(EMA, RSI, MACD)"]
        F_Indicators["fact_stock_indicators Table"]
        D_Stock["dim_stock"]
        D_Date["dim_date"]
        
        F_Price --> IM
        IM --> F_Indicators
    end

    S_Prices -->|Lọc is_valid = TRUE| F_Price

    %% Presentation
    subgraph Presentation_Layer ["Presentation Layer"]
        PBI["Power BI Dashboards<br/>(Import Mode)"]
        HTML_Dash["HTML Dashboard<br/>(GitHub Pages)"]
    end

    F_Price --> PBI
    F_Indicators --> PBI
    D_Stock --> PBI
    D_Date --> PBI
    
    F_Price --> HTML_Dash
    F_Indicators --> HTML_Dash

    %% Airflow Orchestration Links (Dotted)
    Airflow -.->|Điều phối| Ingestion_Layer
    Airflow -.->|Điều phối| Silver_Layer
    Airflow -.->|Điều phối| Gold_Layer

    %% Styling
    style Airflow fill:#f3e5f5,stroke:#ab47bc,stroke-width:2px,color:#4a148c
    style B_Prices fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    style B_Index fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    style B_VN30 fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    style S_Prices fill:#eceff1,stroke:#b0bec5,stroke-width:1px
    style F_Price fill:#fffde7,stroke:#fff176,stroke-width:1px
    style F_Indicators fill:#fffde7,stroke:#fff176,stroke-width:1px
    style D_Stock fill:#fffde7,stroke:#fff176,stroke-width:1px
    style D_Date fill:#fffde7,stroke:#fff176,stroke-width:1px
    style PBI fill:#fffde7,stroke:#fbc02d,stroke-width:2px,color:#333
    style HTML_Dash fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1
```

* **Bronze Layer**: Lưu trữ dữ liệu thô (raw JSON) thu thập từ các nguồn (KBS, VCI...) thông qua thư viện Vnstock.
* **Silver Layer**: Làm sạch dữ liệu, kiểm tra chất lượng (Data Quality Gates) và loại bỏ trùng lặp.
* **Gold Layer**: Tính toán các chỉ báo tài chính (EMA, RSI, MACD, Bollinger Bands) theo mô hình hình sao (Star Schema).

---

## 🚀 2. Hướng dẫn khởi chạy nhanh (Quick Start)

Nhờ cấu hình Docker tự động hóa, bạn chỉ cần thực hiện 2 bước đơn giản sau tại thư mục gốc của dự án:

### Bước 1: Thiết lập tệp cấu hình môi trường
```bash
cp .env.example .env
```
*(Mở file `.env` vừa tạo ra và điền khóa `VNSTOCK_API_KEY` của bạn).*

### Bước 2: Khởi chạy hạ tầng container
```bash
docker compose up -d
```
> [!NOTE]
> Lệnh này dựng toàn bộ hạ tầng (Postgres, Airflow, dbt) và tự động khởi tạo cấu trúc cơ sở dữ liệu, phân vùng bảng, và tải sẵn các package dbt cần thiết.

---

## ⚙️ 3. Quản lý vận hành & Dashboard Báo cáo

### A. Vận hành & Giám sát Data Pipeline (Operations)
* **Airflow Web UI (Điều phối daily)**:
  * Địa chỉ: `http://localhost:8080` (Tài khoản mặc định: `admin` / `admin`).
  * Sử dụng: Bật/Trigger DAG `daily_stock_pipeline` để bắt đầu cào và xử lý dữ liệu tự động.
* **Biến đổi dữ liệu thủ công (dbt CLI)**:
  Nếu muốn chạy trực tiếp dbt để test hoặc tạo lại bảng Silver/Gold:
  ```bash
  docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt build --profiles-dir ."
  ```

### B. Trực quan hóa & Phân tích số liệu (Dashboards)

Hệ thống cung cấp hai phương án trực quan hóa dữ liệu trực tuyến (online) phục vụ báo cáo và phân tích kỹ thuật:

| Dashboard | Trạng thái / Link Online | Mô tả vai trò | Hướng dẫn sử dụng |
| :--- | :--- | :--- | :--- |
| **Power BI Web Report** | [![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://app.powerbi.com/view?r=your_actual_powerbi_share_link) | **Báo cáo phân tích chuyên sâu (Primary)**: Kết nối database Gold, chứa các phân tích đa chiều nâng cao, biểu đồ phân tích kỹ thuật tổng hợp, và tính năng time-intelligence. | Click vào badge để xem trực tuyến trên trình duyệt (hoặc tải file offline `.pbix` trong thư mục `reports/`). |
| **HTML Dashboard** | [![GitHub Pages](https://img.shields.io/badge/GitHub_Pages-222222?style=for-the-badge&logo=githubpages&logoColor=white)](https://kei312.github.io/smalldestocktotngiep/) | **Trang web truy cập nhanh & Dự phòng (Web/Offline Backup)**: Chạy serverless trên GitHub Pages, tự động cập nhật lúc 18h20, nhẹ nhàng, xem được trên mọi thiết bị và hoạt động offline không cần mạng. | Click vào badge để xem trực tuyến.<br>Để mở file local từ terminal:<br>• **WSL**: `powershell.exe -c "Start-Process '$(wslpath -w docs/index.html)'"`<br>• **Linux**: `xdg-open docs/index.html`<br>• **macOS**: `open docs/index.html` |

> [!TIP]
> - **Cấu hình tự động publish HTML**: Xem hướng dẫn tại [DASHBOARD_PUBLISH_GUIDE.md](file:///home/naeouad/deproject/docs/DASHBOARD_PUBLISH_GUIDE.md).
> - **Cập nhật dữ liệu thủ công local cho HTML**: Chạy lệnh: `docker exec -it airflow-container python /opt/airflow/project/scripts/generate_dashboard_backup.py`
