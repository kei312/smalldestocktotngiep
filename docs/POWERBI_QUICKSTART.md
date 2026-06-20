# POWERBI_QUICKSTART.md

> **Cập nhật (20/06):** mục 3 viết lại — dựng Slicer trước, dùng 2 DAX measure (`Percent Change`,
> `Latest Total Volume`) thay vì Quick Measure mặc định hoặc sửa dbt, làm rõ Axis trống cho bar
> chart, thêm Top N filter cho bảng Top Movers. Model Plan B đổi Pro High → Flash High (đồng bộ
> task3.md mục 3.3.9). Thêm mục 4b cho Dashboard 3/4.

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
   - `public.dim_date`
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

Tạo 1 trang report mới, đặt tên "Market Overview".

**Bước 0 — Tạo Slicer ngày TRƯỚC khi build visual khác.** Nhiều visual dưới đây phải xem đúng 1
ngày, nên dựng slicer trước để vừa build vừa kiểm tra số liệu đúng ngay:
1. Kéo `dim_date[date]` ra canvas → Visualizations pane → chọn icon **Slicer**.
2. Format pane → **Slicer settings** → đổi style sang **List** hoặc **Dropdown** (không để mặc
   định "Between"), và set **Single select** — để luôn chỉ chọn đúng 1 ngày tại 1 thời điểm.
3. Chọn sẵn ngày gần nhất có data để các visual bên dưới hiện đúng ngay khi bạn build.

**Bước 1 — Tạo 2 measure dùng chung trước khi build Card và Table** (Modeling tab → **New
Measure**, dán nguyên văn công thức):

```dax
Percent Change =
VAR LatestDate = CALCULATE(MAX(dim_date[date]), dim_date[is_trading_day] = TRUE)
VAR PrevDate =
    CALCULATE(
        MAX(dim_date[date]),
        dim_date[date] < LatestDate,
        dim_date[is_trading_day] = TRUE,
        ALL(dim_date)
    )
VAR CloseLatest = CALCULATE(SUM(fact_stock_price[close]), dim_date[date] = LatestDate)
VAR ClosePrev = CALCULATE(SUM(fact_stock_price[close]), dim_date[date] = PrevDate)
RETURN DIVIDE(CloseLatest - ClosePrev, ClosePrev)
```

```dax
Latest Total Volume =
VAR LatestDate = CALCULATE(MAX(dim_date[date]), dim_date[is_trading_day] = TRUE)
RETURN CALCULATE(SUM(fact_market_summary[total_volume]), dim_date[date] = LatestDate)
```
*(đổi `total_volume` thành đúng tên cột volume trong `fact_market_summary` của bạn nếu khác)*

**Vì sao dùng measure thay vì Quick Measure mặc định hay thêm cột ở dbt:** Quick Measure
"%-change" mặc định của Power BI so với "ngày liền trước theo lịch" (`DATEADD -1 DAY`) — sẽ trống
hoặc sai vào sáng thứ Hai/sau nghỉ lễ vì thị trường không giao dịch T7/CN. Measure trên dùng cờ
`dim_date[is_trading_day]` (đã có sẵn từ task 3.1.3) để luôn tìm đúng *phiên giao dịch liền
trước*, và `ALL(dim_date)` đảm bảo công thức vẫn đúng dù Slicer ở Bước 0 đang lọc về 1 ngày. Cách
này gói gọn trong Power BI, không cần sửa lại `fact_stock_price.sql` đã build/test xong — đỡ phải
chạy lại `dbt run` cho cả pipeline chỉ vì một cột hiển thị.
*Lưu ý: độ chính xác phụ thuộc cờ `is_trading_day` trong seed `dim_date` có khớp đúng lịch nghỉ
lễ VN hay không (không chỉ T7/CN) — kiểm tra nhanh seed nếu thấy `Percent Change` ra trống bất
thường vào ngày sau lễ.*

Giờ thêm 4 visual:

1. **Line chart** — VNINDEX
   - Axis: `fact_market_summary[trade_date]`
   - Values: `fact_market_summary[vnindex]`
2. **Clustered bar chart** — Gainers/Losers
   - **Không kéo gì vào Axis.** Kéo cả 3 cột `gainers`, `losers`, `unchanged` vào **Values** —
     Power BI tự vẽ 3 cột riêng theo tên field khi Axis để trống.
   - ⚠️ **Gotcha hay gặp nhất ở visual này:** aggregation mặc định là Sum. Nếu Slicer ở Bước 0
     chưa chọn đúng 1 ngày, 3 con số sẽ là **tổng cộng dồn nhiều ngày**, không phải số liệu hôm
     nay — nhìn rất giống đúng nhưng sai hoàn toàn. Luôn kiểm tra Slicer đang ở đúng 1 ngày trước
     khi đọc visual này.
3. **Card** — Volume
   - Field: kéo measure `Latest Total Volume` (Bước 1) vào, **không kéo cột raw `total_volume`**
     trực tiếp — measure tự resolve về đúng ngày giao dịch gần nhất kể cả khi quên set Slicer,
     cột raw thì không.
4. **Table** — Top movers
   1. Visualizations pane → chọn icon **Table**.
   2. Kéo `dim_stock[symbol]`, `fact_stock_price[close]`, và measure `Percent Change` (Bước 1)
      vào **Columns**.
   3. Click measure `Percent Change` trong Fields pane → tab **Measure tools** → **Format** →
      **Percentage**, 2 chữ số thập phân.
   4. Visual đang chọn → panel **Filters** → kéo `Percent Change` vào **Filters on this visual**
      → Filter type **Top N** → nhập `10` → **By value** chọn `Percent Change`. (Không có bước
      này, bảng sẽ liệt kê hết cả rổ mã thay vì chỉ "top".)
   5. Click header cột `Percent Change` đã render (hoặc nút **...** góc visual → **Sort by**) →
      **Descending** để mã tăng mạnh nhất lên đầu (đổi Ascending nếu muốn xem top losers).
   6. (Tuỳ chọn, làm đẹp) Format pane → **Cell elements** → **Conditional formatting** →
      **Background color** cho cột `Percent Change`, thang màu phân kỳ đỏ–xanh.

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

## 4b. Dashboard 3 & 4 — Stretch, tuỳ chọn (chỉ làm nếu Dashboard 1+2 đã ổn và còn thời gian)

> Lưu ý: Dashboard 3 ở đây được **định nghĩa lại** so với mô tả gốc trong task tracker. Bản gốc
> ("Gainers/Losers/Volume tables + time slicer") gần như lặp lại y hệt nội dung đã có ở Dashboard
> 1 (vốn đã có bar Gainers/Losers, card Volume, table Top movers, và slicer ngày) — chỉ khác là
> thêm slicer. Làm lại gần như nguyên Dashboard 1 không tạo thêm giá trị, nên đổi hướng:

**Dashboard 3 — Xu hướng Gainers/Losers theo thời gian** (khác Dashboard 1 ở chỗ Dashboard 1 chỉ
xem 1 ngày tại 1 thời điểm, Dashboard 3 xem xu hướng nhiều ngày):
1. **Slicer khoảng ngày** (date range, không phải single-day) — kéo `dim_date[date]`, đổi style
   slicer sang **Between**.
2. **Line/area chart** — Axis: `trade_date`; Values: `gainers`, `losers` (2 đường) từ
   `fact_market_summary` — cho thấy số mã tăng/giảm biến động ra sao qua nhiều phiên, thứ Dashboard
   1 (chỉ xem đúng 1 ngày) không thể hiện được.
3. (Tuỳ chọn) thêm area chart nhỏ `total_volume` theo cùng trục ngày để thấy tương quan volume.

**Dashboard 4 — Fundamentals:**
- ⚠️ **Phụ thuộc:** chỉ làm dashboard này SAU KHI đã có dữ liệu `fact_fundamentals` (tức là sau
  khi hoàn thành các task fetch_fundamentals.py + fact_fundamentals.sql). Nếu chưa có bảng đó,
  Power BI sẽ không có gì để kéo vào — đừng mở mục này trước.
- Card/Table đơn giản: PE, PB, ROE theo `symbol` (slicer dùng chung với Dashboard 2 nếu muốn).
- Nếu không kịp làm fundamentals, bỏ qua dashboard này hoàn toàn — không bắt buộc.

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
| Bar Gainers/Losers ra số khổng lồ | Slicer ngày chưa chọn đúng 1 ngày → Sum cộng dồn nhiều phiên | Kiểm tra Slicer ở Bước 0 đang chọn đúng 1 ngày trước khi đọc số |
| Connect PostgreSQL báo lỗi SSL | Driver yêu cầu SSL mode | Trong Advanced options của Get Data, thêm `sslmode=disable` nếu DB local không bật SSL |

---

## Plan B — Nếu quá 2h vẫn chưa xong (an toàn cho demo)

**Đây KHÔNG thay thế yêu cầu nộp `.pbix`** — chỉ là lưới an toàn để bạn luôn có thứ demo được
trước hội đồng nếu Power BI ăn hết thời gian.

Yêu cầu agent (Gemini Flash High — script có pattern lặp lại rõ ràng giữa 5 bảng, đúng khớp với
phân công đã ghi ở task3.md mục 3.3.9, không cần tốn token Pro High cho việc này) làm:

> "Đọc CONTEXT.md. Viết script Python dùng `psycopg2` + `plotly` đọc trực tiếp 5 bảng Gold
> (`fact_stock_price`, `fact_stock_indicators`, `fact_market_summary`, `dim_stock`, `dim_date`),
> generate ra 1 file `reports/dashboard_backup.html` độc lập (không cần server, mở thẳng bằng
> trình duyệt) chứa: VNINDEX line, gainers/losers bar, 1 symbol close+MA line, RSI line, MACD
> line. Không cần tương tác, chỉ cần hiển thị đúng số liệu Gold để demo khi cần."

Việc này không cần biết Power BI, không cần thao tác UI — agent tự chạy xong trong vài phút, bạn
chỉ cần double-click file `.html` để mở.