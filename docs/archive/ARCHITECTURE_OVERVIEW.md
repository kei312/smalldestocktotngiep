# ARCHITECTURE OVERVIEW

# PROJECT CONTEXT

> **AI:** Đọc file này khi: session mới, câu hỏi về kiến trúc/scope, không chắc về stack.
> Không cần đọc lại nếu đã đọc trong cùng session.

## Dự án

Vietnam Stock Market Data Engineering Pipeline — đồ án tốt nghiệp.

## Stack (đã chốt, không thay đổi)

| Thành phần | Version | Ghi chú |
|---|---|---|
| Python | 3.12.x | Đã có trên WSL (3.12.3) |
| PostgreSQL | 17.x | JSONB, partition — Docker image `postgres:17` |
| Apache Airflow | 3.2.x | Docker, LocalExecutor |
| dbt-core | 1.10.x (pin `1.10.19`) | Bản ổn định, nhiều bản vá  |
| dbt-postgres | 1.10.0 | Khớp release track với dbt-core 1.10.x |
| vnstock | 4.x | Unified UI, fallback VCI/TCBS/MSN |
| Power BI | Desktop | DirectQuery hoặc Import từ Gold |

## Kiến trúc

```
Provider Layer (VnstockProvider / MockProvider)
  → Ingestion Layer (Python, retry, idempotent UPSERT)
    → Bronze (PostgreSQL, raw_json JSONB, PK(code,date), partition/năm)
      → Silver (dbt: clean, cast, is_valid, dq_flag)
        → Gold (dbt: star schema — facts + dims)
          → Power BI / HTML (4 dashboards - docs/POWERBI_QUICKSTART.md)
Airflow điều phối từ provider layer đến gold layer
```

## Scope

- **Core:** Daily OHLCV (VN30 pilot → full), VNINDEX/VN30, MA5/MA20/RSI14/MACD/Bollinger, Airflow, Power BI
- **Mở rộng:** Fundamentals (PE/PB/ROE/ROA/EPS), full universe
- **Bỏ:** Intraday, streaming, trading

## Module layout

```
deproject/
├── providers/          # base.py, vnstock_provider.py, mock_provider.py, registry.py
├── ingestion/          # config.py, utils.py, fetch_prices.py, fetch_index.py, backfill.py, fetch_fundamentals.py
├── sql/                # init_schema.sql
├── dbt/
│   ├── macros/         # calculate_rsi.sql, calculate_ema.sql (generalized — xem SKILL_sql_indicators.md)
│   ├── models/silver/  # silver_prices.sql, silver_index.sql, schema.yml
│   ├── models/gold/
│   │   ├── intermediate/   # int_ema12, int_ema26, int_rsi14, int_macd_line, int_macd_signal (materialized='table')
│   │   ├── fact_stock_price.sql
│   │   ├── fact_stock_indicators.sql   # incremental, JOIN các intermediate ở trên
│   │   ├── fact_market_summary.sql
│   │   ├── dim_stock.sql
│   │   └── schema.yml
│   └── seeds/           # dim_date.csv
├── dags/                # dag_daily.py, dag_backfill.py
├── tests/               # fixtures/, test_providers.py, test_ingestion.py
├── scripts/             # verify_macd_g03.py
└── docs/                # CONTEXT.md (file này), PROJECT_RULES.md,
                          # SKILL_sql_indicators.md, SKILL_dbt_incremental.md,
                          # TEST_REPORTS.md, POWERBI_QUICKSTART.md
```

## Quyết định kiến trúc quan trọng

1. **1 VnstockProvider thay 4 provider viết tay** — vnstock 4.x tự fallback, không trùng lặp
2. **VNDirect loại khỏi production** — sự cố 3/2024, bị nhắc nhở 2025
3. **MockProvider cho test/CI** — chứng minh provider-agnostic, demo offline
4. **dbt-core 1.10.x** — bản ổn định lâu nhất hiện có (pin 1.10.19)
5. **Airflow 3.2.x** — kiến trúc khác 2.x, dùng docker-compose chính thức
6. **Pilot VN30 trước** — validate pipeline trước khi mở rộng
7. **Star schema ở Gold** — dim_stock + dim_date cho Power BI time-intelligence
8. **MACD Signal = EMA9 thật (không phải SMA xấp xỉ)** — SMA9 cho sai số 2–8% vs EMA9, vượt ngưỡng G-03 (<0.5%). Dùng `int_macd_signal.sql` qua macro `calculate_ema(9, ...)` đã generalize.
9. **Power BI giữ nguyên là deliverable chính** — thêm `POWERBI_QUICKSTART.md` hướng dẫn từng bước theo tên bảng Gold. Plan B HTML/Plotly nếu hết giờ.

## Idempotency contract

- Mọi fetch: `ON CONFLICT (code, date) DO UPDATE`
- Chạy 2 lần → không tăng dòng
- `ingested_at` UPDATE khi upsert → audit trail
- `source` ghi vci/tcbs/msn/mock → truy vết

## Bronze schema

```sql
bronze_prices: code VARCHAR(20), date DATE, open/high/low/close NUMERIC(18,4),
               volume BIGINT, raw_json JSONB, source TEXT, ingested_at TIMESTAMPTZ
               PK(code, date)
```

## Silver output

```
symbol, trade_date, open, high, low, close, volume, source, is_valid, dq_flag, loaded_at
```

## Gold tables

- `fact_stock_price` — OHLCV clean, grain (symbol, trade_date)
- `fact_stock_indicators` — MA5/MA20/RSI14/MACD/Bollinger, incremental
- `fact_market_summary` — gainers/losers/volume/vnindex/vn30, grain (trade_date)
- `dim_stock` — symbol, exchange, industry
- `dim_date` — calendar, is_trading_day
# Hướng Dẫn Thuyết Trình Cho Người Dùng Cuối (Business / End-Users)

Mục tiêu của phần này là đóng vai một Chuyên gia Dữ liệu (Data Analyst) hoặc Chuyên gia Tài chính để giải thích cho khách hàng/nhà đầu tư (hoặc giảng viên đóng vai khách hàng) thấy được **giá trị thực tiễn** của hệ thống, mà không dùng quá nhiều từ ngữ lập trình phức tạp.

---

## 1. Nói về Kỹ thuật Tính Toán: "Tại sao tôi phải tin số liệu của bạn?"

End-user thường không quan tâm đến dbt hay Postgres, họ quan tâm đến **Độ tin cậy** và **Tính cập nhật**. Cách bạn trình bày phải toát lên sự chuyên nghiệp và đảm bảo tính nguyên vẹn của dữ liệu:

*   **Cách trình bày (Bạn nói):** 
    > *"Thưa anh/chị, trên thị trường hiện nay có rất nhiều website cung cấp biểu đồ chứng khoán. Tuy nhiên, đôi khi mỗi trang lại hiển thị một số liệu chỉ báo (MACD, RSI) hơi khác nhau, nguyên nhân là do họ thường dùng công thức tính xấp xỉ, giảm bớt số ngày để tiết kiệm chi phí máy chủ.*
    > 
    > *Hệ thống của chúng ta giải quyết bài toán đó bằng cách kéo trực tiếp dữ liệu thô (giá chốt phiên hàng ngày - Daily OHLCV) từ nhà cung cấp chuẩn. Sau đó, hệ thống sử dụng các công thức toán học tài chính nguyên bản (ví dụ như thuật toán Hàm mũ hồi quy - Exponential Moving Average) để tự thân tính toán dựa trên chuỗi thời gian liên tục 100 ngày mà không làm tròn số ẩu. Quá trình xử lý nặng nề này được diễn ra tự động hoàn toàn vào 12h đêm mỗi ngày, để đảm bảo đúng 8h sáng hôm sau, anh/chị mở Dashboard lên là có số liệu chính xác và nóng hổi nhất."*

---

## 2. Giải thích Ý Nghĩa Chỉ Số: "Tôi dùng cái này để làm gì?"

Hãy chỉ trực tiếp vào các biểu đồ trên Power BI và "dịch" các thuật ngữ kỹ thuật sang hành động MUA/BÁN cho người dùng:

### a. Đường Trung Bình Động (MA5 & MA20)
*   **Ý nghĩa:** Đường MA5 giúp mượt hóa giá để phản ánh tâm lý ngắn hạn (1 tuần), còn MA20 phản ánh xu hướng trung hạn (1 tháng).
*   **Cách áp dụng (Bạn nói):** *"Anh chị hãy nhìn vào 2 đường này. Khi đường MA5 (ngắn hạn) cắt lên trên đường MA20 (trung hạn), giới tài chính gọi đó là điểm cắt vàng (Golden Cross), báo hiệu một chu kỳ tăng giá mới, đây là thời điểm tốt để mua vào. Ngược lại, nếu cắt xuống là tín hiệu bán cắt lỗ."*

### b. Chỉ báo Sức mạnh Tương đối (RSI 14 ngày)
*   **Ý nghĩa:** Nó giống như một chiếc nhiệt kế đo độ "Nóng / Lạnh" của cổ phiếu, chạy từ thang điểm 0 đến 100.
*   **Cách áp dụng (Bạn nói):** *"RSI giúp anh chị tránh mua đỉnh bán đáy. Nếu đường này vọt lên trên ngưỡng 70, tức là thị trường đang hưng phấn thái quá, cổ phiếu bị rơi vào vùng 'Quá Mua' (Overbought), có rủi ro bị xả hàng úp sọt. Ngược lại, nếu RSI rớt xuống dưới ngưỡng 30, tức là dân tình đang bán tháo hoảng loạn 'Quá Bán' (Oversold), đây lại là cơ hội tốt để anh chị 'bắt đáy'."*

### c. Đường MACD (Trung bình động Hội tụ / Phân kỳ)
*   **Ý nghĩa:** Đây là chỉ báo đo lường "gia tốc" (động lượng) của xu hướng.
*   **Cách áp dụng (Bạn nói):** *"MACD giúp anh chị đón đầu xu hướng cực kỳ tốt. Trên biểu đồ, nếu anh chị thấy cột Histogram chuyển từ dải màu đỏ (âm) sang dải màu xanh (dương), và đường MACD (nhanh) cắt vút lên trên đường Signal (chậm), đó là một lời khẳng định rất mạnh mẽ rằng phe Mua đã hoàn toàn làm chủ cuộc chơi."*

### d. Dải Bollinger Bands
*   **Ý nghĩa:** Giống như một con đường có 2 dải phân cách, đo lường mức độ biến động (Volatility). Theo lý thuyết thống kê, 95% thời gian giá cổ phiếu sẽ chỉ dao động bên trong 2 dải này.
*   **Cách áp dụng (Bạn nói):** *"Khi anh chị thấy giá cổ phiếu tự nhiên vụt tăng chạm vào Dải trên (Upper Band), nó thường có xu hướng bật ngược trở lại vào trong. Đặc biệt, nếu anh chị thấy 2 dải băng bóp nghẹt lại với nhau giống như 'nút thắt cổ chai' (Squeeze), hãy chuẩn bị tiền, vì giá sắp sửa có một đợt bùng nổ cực mạnh thoát ra khỏi vùng tích lũy."*

---

## 3. Cách Dẫn Dắt Dashboard (Power BI)

Khi trình bày Power BI, hãy áp dụng nguyên tắc **Từ Tổng quan đến Chi tiết (Top-Down Approach)**.

*   **Trang 1: Toàn Cảnh Thị Trường (Market Summary)**
    > *"Mỗi buổi sáng lúc uống cà phê, anh chị chỉ cần nhìn vào trang này. Ở đây hệ thống cho anh chị biết hôm qua VN-Index xanh hay đỏ, có bao nhiêu mã tăng, bao nhiêu mã giảm. Đặc biệt, bảng Top Gainers/Losers sẽ lập tức chỉ ra dòng tiền ngày hôm qua đang tập trung đánh thốc lên ngành nào (Ngân hàng, Bất động sản hay Thép), giúp anh chị có cái nhìn vĩ mô trước."*

*   **Trang 2: Phân Tích Chuyên Sâu (Technical Indicators)**
    > *"Sau khi chọn được 1 mã cổ phiếu ưng ý (ví dụ SSI), anh chị chuyển sang trang này. Lập tức hệ thống sẽ vẽ lại toàn bộ biểu đồ nến (Candlesticks) cùng các công cụ MACD, RSI, Bollinger Bands riêng cho mã SSI. 
    > 
    > Điểm đáng tiền nhất của hệ thống này là tính tương tác (Interactive). Chỉ bằng một cú click chuột đổi bộ lọc thời gian hay đổi mã chứng khoán, mọi biểu đồ phản hồi tức thì chưa tới 1 giây. Điều này có được là nhờ mọi thuật toán tính toán nặng nề nhất đã được 'nhà máy' Data Warehouse (Postgres/dbt) xử lý ngầm vào ban đêm, Power BI chỉ việc đọc kết quả trả về cho anh chị mà thôi."*
