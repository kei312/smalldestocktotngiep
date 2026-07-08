# THESIS DIAGRAMS (MERMAID CODE DEFINITIONS)

This file contains the complete and correct source code for all the Mermaid diagrams referenced in the graduation thesis. These can be rendered directly using any markdown viewer supporting Mermaid or translated to PNG files.

---

## Hình 2.1 — Sơ đồ Lineage các mô hình dữ liệu trong dbt

```mermaid
flowchart LR
    bronze_prices[(bronze_prices)] --> silver_prices(silver_prices)
    silver_prices --> int_rsi14(int_rsi14)
    silver_prices --> int_ema12(int_ema12)
    silver_prices --> int_ema26(int_ema26)
    int_ema12 --> int_macd_line(int_macd_line)
    int_ema26 --> int_macd_line
    int_macd_line --> int_macd_signal(int_macd_signal)
    
    silver_prices --> fact_stock_price(fact_stock_price)
    int_rsi14 --> fact_stock_indicators(fact_stock_indicators)
    int_macd_line --> fact_stock_indicators
    int_macd_signal --> fact_stock_indicators
    
    bronze_vn30_components[(bronze_vn30_components)] --> dim_stock(dim_stock)
    silver_prices --> dim_stock
    
    silver_prices --> fact_market_summary(fact_market_summary)
    
    classDef default fill:#f9f9f9,stroke:#333,stroke-width:1px;
    classDef source fill:#e1f5fe,stroke:#0288d1,stroke-width:2px;
    classDef model fill:#fff9c4,stroke:#fbc02d,stroke-width:1px;
    class bronze_prices,bronze_vn30_components source;
    class silver_prices,int_rsi14,int_ema12,int_ema26,int_macd_line,int_macd_signal,fact_stock_price,fact_stock_indicators,dim_stock,fact_market_summary model;
```

---

## Hình 3.1 — Sơ đồ kiến trúc Luồng dữ liệu tổng thể

```mermaid
flowchart TD
    %% Định nghĩa các lớp
    subgraph ProviderLayer ["Provider Layer (vnstock / others)"]
        API_VCI[Nguồn VCI]
        API_KBSV[Nguồn KBSV]
    end

    subgraph IngestionLayer ["Ingestion Layer (Python)"]
        Fetcher[fetch_prices.py]
        RateLimit[Rate Limiter]
        ThreadPool[Thread Pool - 5 workers]
        
        API_VCI --> RateLimit
        API_KBSV --> RateLimit
        RateLimit --> ThreadPool
        ThreadPool --> Fetcher
    end

    subgraph BronzeLayer ["Bronze Layer (PostgreSQL)"]
        BronzePrices[(bronze_prices - JSONB, Partitioned)]
        BronzeIndex[(bronze_index)]
        BronzeVN30[(bronze_vn30_components)]
        
        Fetcher -- "UPSERT Idempotent" --> BronzePrices
    end

    subgraph SilverLayer ["Silver Layer (dbt)"]
        SilverPrices[silver_prices - Kiểm định chất lượng]
        
        BronzePrices -- "Clean, Cast, Flag is_valid" --> SilverPrices
    end

    subgraph GoldLayer ["Gold Layer (dbt - Star Schema)"]
        Intermediate[Intermediate Models - EMA, RSI, MACD]
        FactIndicators[fact_stock_indicators Table]
        FactPrices[fact_stock_price]
        DimStock[dim_stock]
        DimDate[dim_date]
        
        SilverPrices -- "Lọc is_valid=TRUE" --> FactPrices
        FactPrices --> Intermediate
        Intermediate --> FactIndicators
    end

    subgraph PresentationLayer ["Presentation Layer"]
        PowerBI[Power BI Dashboards - Import Mode]
        
        FactIndicators --> PowerBI
        FactPrices --> PowerBI
        DimStock --> PowerBI
        DimDate --> PowerBI
    end

    %% Luồng điều phối
    Airflow((Apache Airflow Orchestrator)) -.- IngestionLayer
    Airflow -.- SilverLayer
    Airflow -.- GoldLayer

    style Airflow fill:#f9f0ff,stroke:#d6a8ff
    style BronzeLayer fill:#fff3e0,stroke:#ffcc80
    style SilverLayer fill:#eceff1,stroke:#b0bec5
    style GoldLayer fill:#fff8e1,stroke:#ffe082
```

---

## Hình 3.2 — Sơ đồ Thực thể Liên kết (ERD)

```mermaid
erDiagram
    dim_date {
        date trade_date PK
        integer year
        integer month
        integer day
        integer quarter
        integer day_of_week
        boolean is_weekend
    }
    dim_stock {
        varchar symbol PK
        boolean is_vn30
    }
    fact_stock_price {
        varchar symbol PK,FK
        date trade_date PK,FK
        numeric open
        numeric high
        numeric low
        numeric close
        bigint volume
    }
    fact_stock_indicators {
        varchar symbol PK,FK
        date trade_date PK,FK
        numeric rsi_14
        numeric macd_line
        numeric macd_signal
        numeric macd_histogram
    }
    fact_market_summary {
        date trade_date PK,FK
        integer gainers
        integer losers
        integer unchanged
        integer total_symbols
        numeric breadth_ratio
    }
    
    dim_stock ||--o{ fact_stock_price : "has"
    dim_date ||--o{ fact_stock_price : "recorded_on"
    dim_stock ||--o{ fact_stock_indicators : "has"
    dim_date ||--o{ fact_stock_indicators : "calculated_on"
    dim_date ||--o{ fact_market_summary : "summarized_on"
```

---

## Hình 3.3 — Sơ đồ mô phỏng cơ chế ngắt mạch (Circuit Breaker)

```mermaid
graph TD
    subgraph KB_C [Kịch bản C: Ngắt mạch tầng Silver]
        C0[(Bronze Tables)] -->|Dữ liệu thô| C1[dbt_run_silver]
        C1 --> C3[dbt_test_silver]
        C3 -->|Phát hiện lỗi Cấu trúc| C4((Ngắt mạch))
        C4 -.->|Upstream Failed| C5[Tầng Gold]
        
        style C4 fill:#ffcccc,stroke:#ff0000
        style C5 fill:#f9f9f9,stroke:#999,stroke-dasharray: 5 5
    end

    subgraph KB_D [Kịch bản D: Ngắt mạch tầng Gold]
        D1[Tầng Silver] -->|Thành công| D2[dbt_run_gold]
        D2 --> D3[dbt_test_gold]
        D3 -->|Phát hiện lỗi Nghiệp vụ| D4((Ngắt mạch))
        D4 -.->|Ngăn chặn| D5[Power BI Refresh]
        
        style D4 fill:#ffcccc,stroke:#ff0000
        style D5 fill:#f9f9f9,stroke:#999,stroke-dasharray: 5 5
    end
```

---

## Hình 4.1 — Kiến trúc Triển khai Tổng thể (Docker Compose)

```mermaid
flowchart TD
    subgraph Host ["WSL Ubuntu / Linux Host"]
        subgraph Network ["stock-network (bridge)"]
            DB[db: postgres:17\n- port 5432:5432]
            Airflow[airflow: apache/airflow:3.2.2\n- port 8080:8080\n- command: standalone]
            DBT_Docs[dbt-docs: apache/airflow:3.2.2\n- port 8081:8081\n- command: dbt docs serve]
        end
        
        subgraph Volumes ["Volumes / Bind Mounts"]
            PGData[(pgdata)]
            Dags[./dags]
            Project[./]
            VnstockConfig[./vnstock_config]
        end
        
        DB -.->|Mount /var/lib/.../data| PGData
        Airflow -.->|Mount dags/| Dags
        Airflow -.->|Mount project root/| Project
        Airflow -.->|Mount vnstock_config/| VnstockConfig
        DBT_Docs -.->|Mount project root/| Project
        
        Airflow -->|Link DB_HOST=db| DB
        DBT_Docs -->|Link DB_HOST=db| DB
    end
```

---

## Hình 4.2 — Airflow UI DAG Graph View — daily_stock_pipeline

```mermaid
flowchart LR
    health_check(health_check) --> fetch_prices_vn30(fetch_prices_vn30)
    health_check --> fetch_index(fetch_index)
    fetch_prices_vn30 --> fetch_prices_others(fetch_prices_others)
    
    fetch_prices_others --> dbt_run_silver(dbt_run_silver)
    fetch_index --> dbt_run_silver
    
    dbt_run_silver --> dbt_test_silver(dbt_test_silver)
    dbt_test_silver --> dbt_run_gold(dbt_run_gold)
    dbt_run_gold --> dbt_test_gold(dbt_test_gold)
    dbt_test_gold --> notify_success(notify_success)
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    class health_check,fetch_prices_vn30,fetch_index,fetch_prices_others,dbt_run_silver,dbt_test_silver,dbt_run_gold,dbt_test_gold,notify_success success;
```

---

## Hình 4.3 — Airflow UI DAG Runs history

```mermaid
flowchart TD
    subgraph AirflowGridView ["Airflow Grid View — daily_stock_pipeline"]
        direction LR
        subgraph TaskList ["Tác vụ (Tasks)"]
            direction TB
            t1[health_check]
            t2[fetch_index]
            t3[fetch_prices_vn30]
            t4[fetch_prices_others]
            t5[dbt_run_silver]
            t6[dbt_test_silver]
            t7[dbt_run_gold]
            t8[dbt_test_gold]
            t9[notify_success]
        end
        
        subgraph Run_24 ["2026-06-24"]
            direction TB
            r1_1(((●)))
            r1_2(((●)))
            r1_3(((●)))
            r1_4(((●)))
            r1_5(((●)))
            r1_6(((●)))
            r1_7(((●)))
            r1_8(((●)))
            r1_9(((●)))
        end
        
        subgraph Run_25 ["2026-06-25"]
            direction TB
            r2_1(((●)))
            r2_2(((●)))
            r2_3(((●)))
            r2_4(((●)))
            r2_5(((●)))
            r2_6(((●)))
            r2_7(((●)))
            r2_8(((●)))
            r2_9(((●)))
        end
        
        subgraph Run_26 ["2026-06-26"]
            direction TB
            r3_1(((●)))
            r3_2(((●)))
            r3_3(((●)))
            r3_4(((●)))
            r3_5(((●)))
            r3_6(((●)))
            r3_7(((●)))
            r3_8(((●)))
            r3_9(((●)))
        end
    end
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,color:#2e7d32;
    class r1_1,r1_2,r1_3,r1_4,r1_5,r1_6,r1_7,r1_8,r1_9 success;
    class r2_1,r2_2,r2_3,r2_4,r2_5,r2_6,r2_7,r2_8,r2_9 success;
    class r3_1,r3_2,r3_3,r3_4,r3_5,r3_6,r3_7,r3_8,r3_9 success;
```

---

## Hình 5.1 — Airflow tự động ngắt mạch pipeline khi API thất bại

```mermaid
flowchart LR
    health_check(health_check) --> fetch_prices_vn30(fetch_prices_vn30)
    health_check --> fetch_index(fetch_index)
    fetch_prices_vn30 --> fetch_prices_others(fetch_prices_others)
    
    fetch_prices_others --> dbt_run_silver(dbt_run_silver)
    fetch_index --> dbt_run_silver
    
    dbt_run_silver --> dbt_test_silver(dbt_test_silver)
    dbt_test_silver --> dbt_run_gold(dbt_run_gold)
    dbt_run_gold --> dbt_test_gold(dbt_test_gold)
    dbt_test_gold --> notify_success(notify_success)
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef failed fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c;
    classDef upstream_failed fill:#fff3e0,stroke:#ef6c00,stroke-width:1px,stroke-dasharray: 5 5,color:#e65100;
    
    class health_check,fetch_prices_vn30,fetch_index success;
    class fetch_prices_others failed;
    class dbt_run_silver,dbt_test_silver,dbt_run_gold,dbt_test_gold,notify_success upstream_failed;
```

---

## Hình 5.2 — Giao diện Airflow tự động ngắt mạch pipeline khi dbt test tầng Silver thất bại

```mermaid
flowchart LR
    health_check(health_check) --> fetch_prices_vn30(fetch_prices_vn30)
    health_check --> fetch_index(fetch_index)
    fetch_prices_vn30 --> fetch_prices_others(fetch_prices_others)
    
    fetch_prices_others --> dbt_run_silver(dbt_run_silver)
    fetch_index --> dbt_run_silver
    
    dbt_run_silver --> dbt_test_silver(dbt_test_silver)
    dbt_test_silver --> dbt_run_gold(dbt_run_gold)
    dbt_run_gold --> dbt_test_gold(dbt_test_gold)
    dbt_test_gold --> notify_success(notify_success)
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef failed fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c;
    classDef upstream_failed fill:#fff3e0,stroke:#ef6c00,stroke-width:1px,stroke-dasharray: 5 5,color:#e65100;
    
    class health_check,fetch_prices_vn30,fetch_index,fetch_prices_others,dbt_run_silver success;
    class dbt_test_silver failed;
    class dbt_run_gold,dbt_test_gold,notify_success upstream_failed;
```

---

## Hình 5.3 — Giao diện Airflow ngắt mạch trước khi báo cáo cập nhật do dbt test tầng Gold thất bại

```mermaid
flowchart LR
    health_check(health_check) --> fetch_prices_vn30(fetch_prices_vn30)
    health_check --> fetch_index(fetch_index)
    fetch_prices_vn30 --> fetch_prices_others(fetch_prices_others)
    
    fetch_prices_others --> dbt_run_silver(dbt_run_silver)
    fetch_index --> dbt_run_silver
    
    dbt_run_silver --> dbt_test_silver(dbt_test_silver)
    dbt_test_silver --> dbt_run_gold(dbt_run_gold)
    dbt_run_gold --> dbt_test_gold(dbt_test_gold)
    dbt_test_gold --> notify_success(notify_success)
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef failed fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c;
    classDef upstream_failed fill:#fff3e0,stroke:#ef6c00,stroke-width:1px,stroke-dasharray: 5 5,color:#e65100;
    
    class health_check,fetch_prices_vn30,fetch_index,fetch_prices_others,dbt_run_silver,dbt_test_silver,dbt_run_gold success;
    class dbt_test_gold failed;
    class notify_success upstream_failed;
```

---

## Hình 5.4 — Sơ đồ rẽ nhánh có điều kiện (run_vn30_only) trong daily_stock_pipeline

```mermaid
flowchart LR
    health_check(health_check) --> fetch_prices_vn30(fetch_prices_vn30)
    health_check --> fetch_index(fetch_index)
    
    fetch_prices_vn30 --> check_param{run_vn30_only?}
    check_param -->|False| fetch_prices_others(fetch_prices_others)
    check_param -->|True| skip_others[skip: fetch_prices_others]
    
    fetch_prices_others --> dbt_run_silver(dbt_run_silver)
    skip_others --> dbt_run_silver
    fetch_index --> dbt_run_silver
    
    dbt_run_silver --> dbt_test_silver(dbt_test_silver)
    dbt_test_silver --> dbt_run_gold(dbt_run_gold)
    dbt_run_gold --> dbt_test_gold(dbt_test_gold)
    dbt_test_gold --> notify_success(notify_success)
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef skipped fill:#eceff1,stroke:#b0bec5,stroke-width:1px,stroke-dasharray: 5 5,color:#546e7a;
    classDef param fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#01579b;
    
    class health_check,fetch_prices_vn30,fetch_index,dbt_run_silver,dbt_test_silver,dbt_run_gold,dbt_test_gold,notify_success success;
    class check_param param;
    class skip_others skipped;
```

---

## Hình 5.5 — Sơ đồ luồng chạy tuần tự có điều kiện của manual_backfill_pipeline

```mermaid
flowchart LR
    health_check(health_check) --> backfill_vn30(backfill_vn30)
    backfill_vn30 --> check_param{run_vn30_only?}
    
    check_param -->|False| backfill_others(backfill_others)
    check_param -->|True| skip_others[skip: backfill_others]
    
    backfill_others --> dbt_run_silver(dbt_run_silver)
    skip_others --> dbt_run_silver
    
    dbt_run_silver --> dbt_test_silver(dbt_test_silver)
    dbt_test_silver --> dbt_run_gold(dbt_run_gold)
    dbt_run_gold --> dbt_test_gold(dbt_test_gold)
    
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef skipped fill:#eceff1,stroke:#b0bec5,stroke-width:1px,stroke-dasharray: 5 5,color:#546e7a;
    classDef param fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#01579b;
    
    class health_check,backfill_vn30,dbt_run_silver,dbt_test_silver,dbt_run_gold,dbt_test_gold success;
    class check_param param;
    class skip_others skipped;
```

