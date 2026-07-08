# BÁO CÁO ĐỒ ÁN TỐT NGHIỆP

**Đề tài:** Xây dựng Hệ thống Kỹ thuật Dữ liệu cho Thị trường Chứng khoán Việt Nam (Vietnam Stock Market Data Engineering Pipeline)

---

## TÓM TẮT (ABSTRACT)

Báo cáo này trình bày thiết kế và hiện thực hóa một đường ống dữ liệu (Data Pipeline) cấp độ doanh nghiệp dành cho dữ liệu thị trường chứng khoán Việt Nam. Hệ thống giải quyết các thách thức cố hữu về sự phân mảnh, thiếu tin cậy của các API cung cấp dữ liệu, đồng thời chuyển đổi quy trình tính toán các chỉ báo tài chính kỹ thuật phức tạp (như MACD, RSI, Bollinger Bands) từ ứng dụng backend xuống tầng cơ sở dữ liệu (Pushdown Computation) thông qua SQL đệ quy. Kiến trúc hệ thống dựa trên mô hình Medallion (Bronze, Silver, Gold), kết hợp với dbt để chuẩn hóa, kiểm tra chất lượng dữ liệu và tính toán gia tăng (Incremental), dưới sự điều phối tự động của Apache Airflow. Kết quả đạt được là một hệ thống đảm bảo tính lũy đẳng (Idempotency), hiệu năng cao, sai số chỉ báo ở mức 0.0000% so với thư viện TA-Lib chuẩn quốc tế.

---

## CHƯƠNG 1: MỞ ĐẦU & BÀI TOÁN THỰC TẾ

### 1.1 Nhu cầu sử dụng dữ liệu thị trường chứng khoán Việt Nam
Trong kỷ nguyên dữ liệu số, dữ liệu thị trường tài chính và chứng khoán đóng vai trò cốt lõi trong việc ra quyết định đầu tư, xây dựng mô hình định lượng (Quant) và hiển thị bảng biểu giám sát (Dashboards). Đối với thị trường chứng khoán Việt Nam, nhu cầu truy cập dữ liệu giao dịch hằng ngày (OHLCV) nhanh chóng và chính xác là rất lớn.

### 1.2 Các thách thức hiện tại
Tuy nhiên, việc xây dựng một hệ thống dữ liệu chứng khoán đáng tin cậy tại Việt Nam gặp phải nhiều thách thức:
1. **Sự rời rạc và thiếu tin cậy của nguồn cấp dữ liệu**: Các nguồn API công cộng thường không có cấu trúc chuẩn hóa, dễ bị thay đổi Schema, và thường xuyên áp dụng giới hạn truy vấn (Rate Limiting) dẫn đến lỗi Timeout khi cào dữ liệu lịch sử hoặc dữ liệu diện rộng.
2. **Tính toán thủ công và điểm nghẽn hiệu suất**: Các công cụ truyền thống thường dùng Python/Pandas để lấy toàn bộ dữ liệu lên bộ nhớ và tính toán chỉ báo (MACD, RSI) on-the-fly. Khi tập dữ liệu lên đến hàng chục triệu dòng, ứng dụng dễ bị tràn RAM và không thể mở rộng.
3. **Rác dữ liệu và thiếu cơ chế lũy đẳng (Idempotency)**: Khi một tiến trình ETL bị lỗi giữa chừng và chạy lại, các hệ thống đơn giản thường sinh ra dữ liệu trùng lặp (Duplicate Data), gây sai lệch biểu đồ và các chỉ báo.

### 1.3 Mục tiêu và Phạm vi đồ án (Scope)
Nhằm giải quyết triệt để các vấn đề trên, đồ án này đặt ra mục tiêu thiết kế và xây dựng một Data Pipeline chuẩn Enterprise:
- **Core Scope**: Lấy dữ liệu OHLCV hằng ngày của rổ VN30 (Pilot Phase) và chỉ số VNINDEX.
- **Tính toán chỉ báo**: Tính toán hoàn toàn bằng cơ sở dữ liệu các chỉ báo MA5, MA20, RSI(14), MACD, Bollinger Bands.
- **Công nghệ**: PostgreSQL (Lưu trữ Database), dbt-core (Transformation), Apache Airflow (Orchestration), Power BI (Visualization), Vnstock (Python Ingestion).
- **Tính chất**: Lũy đẳng 100%, tự phục hồi lỗi, khả năng Backfill dữ liệu lịch sử dễ dàng.

---

## CHƯƠNG 2: CƠ SỞ LÝ THUYẾT

### 2.1 Kiến trúc Medallion (Data Lakehouse)
Đồ án áp dụng nguyên lý kiến trúc Medallion do Databricks đề xuất nhằm chia tách rõ ràng trách nhiệm quản trị dữ liệu:
- **Bronze Layer (Raw)**: Nơi tiếp nhận dữ liệu thô từ API ở định dạng JSONB, giữ nguyên bản chất dữ liệu (Append-only / Upsert). Cho phép tính toán lại lịch sử (re-play) bất cứ lúc nào mà không cần gọi lại API nguồn.
- **Silver Layer (Cleansed & Conformed)**: Nơi dữ liệu được dọn dẹp, ép kiểu (Cast) và đánh dấu chất lượng (Data Quality flag). Các bản ghi rác (VD: Giá đóng cửa <= 0, Giá cao nhất < Giá thấp nhất) được phân loại và lọc bỏ.
- **Gold Layer (Curated/Business-level)**: Dữ liệu được tổ chức theo cấu trúc hình sao (Star Schema - Fact & Dim tables). Đây là nơi các quy tắc nghiệp vụ phức tạp và các chỉ báo kỹ thuật tài chính được tính toán sẵn, tối ưu hóa cho truy vấn phân tích (Low Latency) của Power BI.

### 2.2 Tính Lũy Đẳng (Idempotency)
Lũy đẳng là nguyên lý tối thượng trong Data Engineering: *Một Data Pipeline chạy 1 lần hay n lần với cùng tham số đầu vào thì trạng thái dữ liệu đích cuối cùng vẫn không thay đổi.* 
Trong hệ thống này, tính lũy đẳng được đảm bảo tại hai tầng:
- **Ingestion**: Dùng lệnh `ON CONFLICT (code, date) DO UPDATE` (UPSERT).
- **Transformation (dbt)**: Dùng mô hình Incremental với chiến lược `delete+insert`, đảm bảo các dòng cũ bị xóa đi trước khi chèn dòng mới, không sinh bản sao.

### 2.3 Cơ sở Toán học của Chỉ báo Tài chính và SQL Đệ quy
Hệ thống tính toán các chỉ báo trực tiếp trong PostgreSQL. Một số chỉ báo như RSI và MACD đòi hỏi phép toán đệ quy, không thể chỉ dùng Window Function thông thường:
1. **RSI (Relative Strength Index)**: Đo lường động lượng. RSI nguyên bản của J. Welles Wilder Jr. yêu cầu phương pháp làm mượt Wilder Smoothing. Tại ngày T, RSI phụ thuộc vào giá trị làm mượt của ngày T-1.
2. **MACD (Moving Average Convergence Divergence)**: Đường Tín hiệu (Signal Line) của MACD là một đường trung bình động hàm mũ (EMA 9 chu kỳ).
   - Công thức EMA: `EMA(t) = Value(t) * α + EMA(t-1) * (1 - α)`
   - Sự bất tương thích SQL: SQL là ngôn ngữ hướng tập hợp (Set-based). Để giải quyết bài toán lặp tuần tự (Iterative) của EMA và Wilder Smoothing, hệ thống sử dụng cú pháp nâng cao `WITH RECURSIVE` (Common Table Expressions).

---

## CHƯƠNG 3: KIẾN TRÚC HỆ THỐNG VÀ QUYẾT ĐỊNH THIẾT KẾ (ADR)

### 3.1 Sơ đồ Data Flow
Kiến trúc hệ thống bao gồm:
```
Nguồn (API) -> VnstockProvider (Python/Airflow)
   -> Ingestion Layer (Python, Upsert Logic, Exponential Backoff)
   -> Tầng Bronze (PostgreSQL, JSONB payload)
   -> Tầng Silver (dbt views/tables: Clean, Cast, Schema Test)
   -> Tầng Gold (dbt facts/dims: Tính MACD, RSI, Incremental Load)
   -> Trực quan hóa (Power BI / HTML Backup)
```

### 3.2 Quyết định Công nghệ Quan trọng (ADR)
1. **PostgreSQL 17.x thay vì Cloud Data Warehouse**: Do Scope thí điểm ở mức VN30, lượng dữ liệu chưa đạt ngưỡng Big Data (hàng trăm GB). PostgreSQL với tính năng Table Partitioning theo năm và trường dữ liệu JSONB đủ sức đáp ứng và giúp tối ưu chi phí hạ tầng.
2. **Pushdown Computation bằng dbt-core**: Thay vì dùng Pandas, toàn bộ dữ liệu biến đổi và logic đệ quy được khai báo thành mã SQL trong dbt (`macros`), giúp tận dụng sức mạnh xử lý song song của CSDL, quản lý dễ dàng lịch sử version và tự động sinh test data quality (Schema.yml contracts).
3. **Apache Airflow 3.2.x**: Chọn phiên bản mới nhất nhằm điều phối tác vụ (Orchestration) hằng ngày (Daily) và lấp đầy dữ liệu lịch sử (Backfill) dưới dạng các DAGs tách biệt.

### 3.3 Hợp đồng Dữ liệu (Data Contracts)
Các tầng giao tiếp chặt chẽ thông qua Hợp đồng Dữ liệu (Data Contracts):
- **Bronze Output**: Cung cấp cấu trúc cơ bản `code, date, o, h, l, c, v, raw_json`.
- **Silver Validation**: Từ chối và gán cờ `dq_flag` cho các bản ghi bị sai logic `is_valid = FALSE` (VD: volume âm).
- **Gold Star Schema**: Cung cấp `fact_stock_price`, `fact_stock_indicators`, `fact_market_summary`, `dim_stock`, và `dim_date` làm đầu vào chuẩn cho Power BI.

---

## CHƯƠNG 4: HIỆN THỰC HÓA VÀ XỬ LÝ KỸ THUẬT CHI TIẾT

### 4.1 Lớp Ingestion (Bắt dữ liệu API) và Cơ chế Retry
Hệ thống triển khai mẫu thiết kế `Abstract Factory / Registry` cho các nhà cung cấp dữ liệu (VnstockProvider, MockProvider) nhằm cô lập các logic giao tiếp API rườm rà. Một trong những điểm yếu lớn nhất của Data Pipeline là các lỗi mạng tức thời (Transient Network Errors) hoặc bị giới hạn băng thông (HTTP 429 Too Many Requests). 
Để giải quyết triệt để, hệ thống tích hợp một custom Decorator `@retry` trong Python để áp dụng thuật toán **Exponential Backoff** kết hợp Jitter, chỉ bắt đúng 2 loại Exception đã được quy hoạch trước `ProviderRateLimitError` và `ProviderTimeoutError`:

```python
@retry(max_attempts=3, backoff_base=2.0, jitter=1.0,
       retry_on=(ProviderRateLimitError, ProviderTimeoutError))
def _fetch_with_retry(provider, symbols: List[str], start: date, end: date) -> pd.DataFrame:
    return provider.get_prices(symbols, start, end)
```
Cơ chế này giúp decouple (tách rời) hoàn toàn I/O network logic khỏi business logic biến đổi dữ liệu, khiến Code Ingestion gọn gàng và có tính Resilient (đàn hồi) cao.

### 4.2 Lớp dbt Gold & Thách thức Toán học (Pushdown Computation)
Đây là khâu xử lý kỹ thuật phức tạp nhất trong đồ án: Đưa logic toán học từ Application Layer xuống Database Layer (Pushdown).

**1. Tính toán EMA và RSI bằng Đệ quy (`WITH RECURSIVE`)**: 
Vì PostgreSQL không cho phép lồng các khối lệnh `WITH RECURSIVE`, hệ thống phải tách các chỉ báo (`int_ema12`, `int_rsi14`, v.v.) thành các model trung gian độc lập. Tại đây, Macro đệ quy được tổng quát hóa cực kỳ linh hoạt:
```sql
_ema_rec(symbol, rn, trade_date, ema_val) AS (
    -- Base case: Khởi tạo giá trị hạt giống (Seed) bằng SMA
    SELECT b.symbol, b.rn, b.trade_date, s.ema_val FROM _ema_base b JOIN _ema_seed s ON ...
    UNION ALL
    -- Recursive step: Val * Alpha + Prev_EMA * (1 - Alpha)
    SELECT b.symbol, b.rn, b.trade_date,
           b.val * (2.0 / ({{ period }} + 1.0)) + e.ema_val * (1.0 - 2.0 / ({{ period }} + 1.0))
    FROM _ema_base b JOIN _ema_rec e ON b.symbol = e.symbol AND b.rn = e.rn + 1
)
```

**2. Tối ưu hóa tính toán Window Function (Bollinger Bands)**:
Ngược lại với EMA/RSI cần đệ quy, các chỉ báo như Simple Moving Average (MA) và Dải Bollinger (Bollinger Bands) được hệ thống gói gọn trong duy nhất một lần duyệt bảng (single scan) bằng Window Functions. Dải Bollinger tận dụng trực tiếp hàm `STDDEV_POP()` có sẵn của hệ quản trị cơ sở dữ liệu để tìm độ lệch chuẩn của 20 phiên, kết hợp với phép tịnh tiến `ROWS BETWEEN 19 PRECEDING AND CURRENT ROW`, giúp tối ưu cực hạn năng lực I/O của đĩa cứng.

**3. Mô hình Tải gia tăng (Incremental Strategy & Lookback)**: 
Để tránh tính lại từ năm 2021 hằng ngày, `fact_stock_indicators` chạy ở chế độ `incremental`. Đặc thù của MACD Signal là cần 34 ngày giao dịch để Signal line hội tụ. Do đó, logic Incremental được cấu hình với Lookback Window 60 ngày dương lịch qua Jinja template của dbt:
```sql
{% if is_incremental() %}
WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
{% endif %}
```
Mỗi lần chạy, dbt sẽ thực hiện chiến lược `delete+insert`: Quét 60 ngày cũ, xóa (Delete) khỏi bảng đích, rồi chèn dữ liệu mới đã được tính toán lại (Insert). Điều này đảm bảo cả 2 yếu tố: Đủ dữ liệu lịch sử để mồi đệ quy (Warm-up) và Giữ tính lũy đẳng tuyệt đối.

### 4.3 Lớp Orchestration (Apache Airflow)
Toàn bộ Data Pipeline được tự động hóa điều phối qua Apache Airflow với file `dag_daily.py`. Hệ thống tuân thủ thiết kế "Linear Pipeline" nghiêm ngặt với các Quality Gates:
`health_check → [fetch_prices_vn30, fetch_prices_others, fetch_index] → dbt_silver → test_silver → dbt_gold → test_gold`
Nếu dbt test ở tầng Silver fail (ví dụ dữ liệu từ API bất ngờ bị rác và không vượt qua các generic data tests), DAG sẽ chặn không cho chạy tầng Gold. Điều này gọi là "Circuit Breaker", bảo vệ các Dashboard của người dùng cuối khỏi việc tiếp nhận dữ liệu sai lệch. Ngoài ra, hàm `on_failure_callback` tích hợp tự động đẩy cảnh báo vào hệ thống log để kỹ sư giám sát xử lý ngay lập tức.

---

## CHƯƠNG 5: ĐÁNH GIÁ HIỆU NĂNG VÀ KẾT QUẢ

### 5.1 Tính đúng đắn của dữ liệu (Data Correctness & Cross-Verification)
Xây dựng logic tài chính phức tạp trên SQL tiềm ẩn nguy cơ sai lệch số học do cách CSDL xử lý số thập phân (floating point math) và phép làm tròn. Để chứng minh thuật toán SQL đệ quy là chính xác tuyệt đối, đồ án đã triển khai kịch bản test chéo tự động **G-03 MACD Verification** (`scripts/verify_macd_g03.py`). 
Script này tải song song 2 luồng dữ liệu để đối chiếu:
1. Luồng 1 (Reference): Khai thác dữ liệu **đã làm sạch** (clean OHLCV data) từ bảng `gold.fact_stock_price` đưa lên Pandas Dataframe, sau đó chạy tính toán chỉ báo MACD (Line, Signal, Histogram) thông qua một Reference Implementation bằng Python gốc (mô phỏng logic chuẩn xác của thư viện TA-Lib).
2. Luồng 2 (Target): Truy vấn thẳng kết quả của các chỉ báo MACD đã được tính sẵn bởi SQL Đệ quy của dbt từ bảng đích `gold.fact_stock_indicators`.
Kết quả đối chiếu trên thực tế (cross-check error rate) cho thấy với hàng ngàn bản ghi xuyên suốt nhiều năm giao dịch (như mã HPG từ 2021-2024), tỷ lệ chênh lệch phần trăm sai số giữa Python và SQL luôn đạt ngưỡng **0.0000%**. Nó khẳng định mô hình Pushdown Computation hoàn toàn khả thi và có độ tin cậy cấp doanh nghiệp (Enterprise Grade).

### 5.2 Hiệu năng Tải Hệ thống (Performance & Scalability)
Bằng việc sử dụng chiến lược `delete+insert` Incremental với khoảng Lookback 60 ngày, Pipeline chạy hằng ngày (Daily DAG) cho nhóm VN30 hoàn thành toàn bộ chu trình (từ Ingestion, Bronze, Silver, dbt tests, Gold) **chỉ trong vòng chưa tới 2 phút** (1m30s - 1m50s). Các Data Quality tests chạy song song để đảm bảo dữ liệu không bị lỗi trước khi lên Power BI.

---

## CHƯƠNG 6: HẠN CHẾ VÀ KẾ HOẠCH MỞ RỘNG (HOSE SCALING PLAN)

Dù hệ thống hoạt động xuất sắc cho quy mô Pilot (VN30 - 30 mã cổ phiếu), việc mở rộng (Scale-up) hệ thống ra toàn bộ thị trường HoSE (~400 mã) sẽ đòi hỏi phải nâng cấp kiến trúc:

### 6.1 Thách thức lớn khi mở rộng
1. **API Rate Limits**: Tần suất 20 requests/phút sẽ khiến việc cào dữ liệu 400 mã mất tới hơn 20 phút hằng ngày.
2. **Dbt Bottleneck**: Việc tính lại các Intermediate Recursive Models (Table Materialization) cho 5 năm lịch sử của 400 mã (tương đương hơn 500,000 dòng dữ liệu gốc) sẽ làm PostgreSQL quá tải tính toán.
3. **Power BI Memory Limit**: Các tệp PBIX Import sẽ phình to quá mức khi tải toàn bộ Fact Data.

### 6.2 Đề xuất Kiến trúc Scale
1. **Dynamic Task Mapping trên Airflow**: Thay vì cào tuần tự 400 mã trong 1 task, Airflow sẽ phân lô (Batching) mỗi lô 50 mã và chạy song song (Parallel execution) ra nhiều workers, tối giản rủi ro ngắt kết nối.
2. **Tối ưu Hóa API / Proxy Rotation**: Cần sử dụng cơ chế xoay vòng Proxy/Tài khoản hoặc sử dụng phiên bản API Enterprise/Insiders để giảm ngưỡng nghẽn mạng xuống mức dưới 0.5s/request.
3. **Incremental Intermediate Models**: Các bảng `int_ema` và `int_rsi` cần cấu hình lại thành `incremental` kết hợp với lookback window, kết hợp xây dựng `Composite Indexes` trên PostgreSQL (symbol, trade_date).
4. **Hybrid Power BI Mode**: Ứng dụng mô hình DirectQuery truy xuất trực tiếp các Fact Table lớn thay vì Import toàn bộ lên RAM bộ nhớ cục bộ.

---

## TÀI LIỆU THAM KHẢO

1. **Databricks** (2020). *Medallion Architecture: A Data Lakehouse Paradigm*. Tài liệu kiến trúc phân lớp dữ liệu chuẩn công nghiệp.
2. **Appel, G.** (1979). *The Moving Average Convergence-Divergence Method*. Công thức và cơ sở toán học của chỉ báo MACD và đường trung bình động hàm mũ (EMA).
3. **Wilder, J. W.** (1978). *New Concepts in Technical Trading Systems*. Cơ sở thiết kế chỉ báo RSI và cơ chế Wilder Smoothing.
4. **Pasupuleti, S.** *The Medallion Architecture in Data Engineering: A Layered Approach to Data Quality and Governance*. DOI: 10.52783/jisem.v10i62s.13557.
5. **Harby, A. A., & Zulkernine, F.** *Data Lakehouse: A survey and experimental study*. DOI: 10.1016/j.is.2024.102460.
6. **Behera, L.** *Building Resilient Data Pipelines: Techniques for Fault-Tolerant Data Engineering*. DOI: 10.63282/3050-9416.IJAIBDCMS-V2I3P106.
7. **Zhang, C. et al.** (2024). *FinSQL: Model-Agnostic LLMs-based Text-to-SQL Framework for Financial Analysis*. SIGMOD '24. DOI: 10.1145/3626246.3653375.
8. Tài liệu chính thức **Apache Airflow** (v3.2.x) - *DAGs, Task Mapping & Idempotency Best Practices*.
9. Tài liệu chính thức **dbt-core** (v1.10.x) - *Incremental Strategies and Recursive CTE pushdown limits in PostgreSQL*.
