# POWERBI_QUICKSTART.md

> Viết riêng cho người **chưa từng dùng Power BI** + đang gấp deadline.
> Mục tiêu: dựng xong Dashboard 1 + 2 trong ~2–2.5h, KHÔNG cần học custom visual.
> Nếu sau 2h vẫn chưa xong Dashboard 1 → dừng, chuyển sang **Plan B** ở cuối file.

---

## 0. Quyết định đã chốt cho deadline 20/06

- **KHÔNG làm candlestick/OHLC chart.** Đây là loại chart duy nhất cần cài custom visual từ
  AppSource (Stock Chart) — rủi ro cao cho người mới, không đáng đánh đổi trong 1 ngày.
- **Thay bằng:** line chart `close` + overlay `ma5`/`ma20` — multi-series line chart, kéo-thả
  thuần, Power BI hỗ trợ native 100%. Vẫn thể hiện đúng technical indicator, hội đồng vẫn thấy
  được logic MA crossover.
- RSI và MACD vốn dĩ chỉ là line chart — không có gì khó, không cần custom visual.

---

## 1. Kết nối dữ liệu (15 phút)

1. Mở Power BI Desktop → **Get Data** → **PostgreSQL database**.
2. Server: giá trị `DB_HOST:DB_PORT` trong `.env` (vd `localhost:5432`).
3. Database: giá trị `DB_NAME` trong `.env`.
4. Chọn **DirectQuery** nếu muốn data luôn mới khi demo trực tiếp; chọn **Import** nếu chỉ cần
   snapshot ổn định để không lo mất kết nối lúc bảo vệ (khuyến nghị **Import** — an toàn hơn cho
   demo, ít rủi ro hơn khi không có ai canh DB lúc trình bày).
5. Tick chọn các bảng sau trong Navigator (đúng tên trong `gold` schema theo `CONTEXT.md`):
   - `gold.fact_stock_price`
   - `gold.fact_stock_indicators`
   - `gold.fact_market_summary`
   - `gold.dim_stock`
   - `gold.dim_date`
6. **Load**.

---

## 2. Tạo relationships (10 phút)

Vào **Model view** (icon thứ 3 bên trái). Kéo để nối:

| Từ | Đến | Cardinality |
|---|---|---|
| `fact_stock_price[symbol]` | `dim_stock[symbol]` | Many-to-one |
| `fact_stock_indicators[symbol]` | `dim_stock[symbol]` | Many-to-one |
| `fact_stock_price[trade_date]` | `dim_date[date]` | Many-to-one |
| `fact_stock_indicators[trade_date]` | `dim_date[date]` | Many-to-one |
| `fact_market_summary[trade_date]` | `dim_date[date]` | Many-to-one |

Click vào bảng `dim_date` → tab **Table tools** → **Mark as date table** → chọn cột `date`.
(Bước này bật time-intelligence, không bắt buộc nhưng giúp slicer ngày hoạt động đúng.)

---

## 3. Dashboard 1 — Market Overview (45 phút)

Tạo 1 trang report mới, đặt tên "Market Overview". Thêm 4 visual:

1. **Line chart** — VNINDEX
   - Axis: `fact_market_summary[trade_date]`
   - Values: `fact_market_summary[vnindex]`
2. **Clustered bar chart** — Gainers/Losers
   - Axis: lấy ngày gần nhất (filter trade_date = max) hoặc để slicer ngoài chọn
   - Values: `gainers`, `losers`, `unchanged` (kéo cả 3 cột vào Values)
3. **Card** — Volume
   - Field: `fact_market_summary[total_volume]` (hoặc tên cột volume thực tế trong bảng của bạn)
   - Aggregation: Sum hoặc lấy giá trị ngày gần nhất tuỳ thiết kế fact_market_summary
4. **Table** — Top movers
   - Columns: `symbol`, `close`, % thay đổi (nếu chưa có cột sẵn, có thể tạo Quick Measure
     "Percent change" ngay trong Power BI — không cần sửa dbt)

Thêm 1 **Slicer** ngày (`dim_date[date]`) để lọc toàn trang.

---

## 4. Dashboard 2 — Stock Analysis (45 phút)

Tạo trang mới "Stock Analysis". Thêm:

1. **Slicer** — `dim_stock[symbol]` (đặt ở đầu trang, set là single-select để dễ demo)
2. **Line chart** — Close + MA overlay
   - Axis: `trade_date`
   - Values: kéo **cả 3 field cùng lúc** vào Values: `close`, `ma5`, `ma20`
     (Power BI tự vẽ thành 3 đường cùng biểu đồ — đây chính là thứ thay thế candlestick)
3. **Line chart riêng** — RSI14
   - Axis: `trade_date`, Values: `rsi_14`
   - Optional: thêm 2 đường tham chiếu cố định 30/70 bằng **Analytics pane → Constant line**
4. **Line chart riêng** — MACD
   - Axis: `trade_date`, Values: `macd_line`, `macd_signal` (2 đường cùng chart)
   - Thêm **Column chart** nhỏ bên dưới cho `macd_histogram` nếu còn thời gian (không bắt buộc)
5. **Line chart** — Bollinger Bands
   - Axis: `trade_date`, Values: `close`, `bb_upper`, `bb_lower` (3 đường)

Tất cả visual trên trang này nên cùng bị lọc bởi slicer `symbol` ở bước 1 (mặc định Power BI tự
áp dụng filter cho mọi visual cùng trang, không cần cấu hình thêm).

---

## 5. Lưu file

**File → Save As** → `reports/stock_dashboard.pbix` (đúng path deliverable trong
`implementation_plan4.md`).

---

## 6. Lỗi hay gặp với người mới

| Triệu chứng | Nguyên nhân | Cách sửa |
|---|---|---|
| Kéo field vào Axis nhưng chart trống | Relationship chưa nối đúng chiều (many-to-one ngược) | Vào Model view, kiểm tra mũi tên filter direction |
| Slicer không lọc được visual | Visual đó nằm khác trang, hoặc field slicer không liên quan qua relationship | Đảm bảo cùng trang + có đường nối trong Model view |
| Card hiện số quá lớn/sai | Aggregation mặc định là Sum nhưng cột vốn đã là tổng theo ngày | Đổi aggregation sang Average hoặc Max tuỳ ngữ nghĩa cột |
| Connect PostgreSQL báo lỗi SSL | Driver yêu cầu SSL mode | Trong Advanced options của Get Data, thêm `sslmode=disable` nếu DB local không bật SSL |

---

## Plan B — Nếu quá 2h vẫn chưa xong (an toàn cho demo)

**Đây KHÔNG thay thế yêu cầu nộp `.pbix`** — chỉ là lưới an toàn để bạn luôn có thứ demo được
trước hội đồng nếu Power BI ăn hết thời gian.

Yêu cầu agent (Gemini Pro High, vì chỉ là task tạo file nội dung) làm:

> "Đọc CONTEXT.md. Viết script Python dùng `psycopg2` + `plotly` đọc trực tiếp 5 bảng Gold
> (`fact_stock_price`, `fact_stock_indicators`, `fact_market_summary`, `dim_stock`, `dim_date`),
> generate ra 1 file `reports/dashboard_backup.html` độc lập (không cần server, mở thẳng bằng
> trình duyệt) chứa: VNINDEX line, gainers/losers bar, 1 symbol close+MA line, RSI line, MACD
> line. Không cần tương tác, chỉ cần hiển thị đúng số liệu Gold để demo khi cần."

Việc này không cần biết Power BI, không cần thao tác UI — agent tự chạy xong trong vài phút, bạn
chỉ cần double-click file `.html` để mở.

