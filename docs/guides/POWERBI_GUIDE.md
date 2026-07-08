# POWERBI_GUIDE.md

> **Cập nhật mới nhất:** Đã bổ sung hướng dẫn Step-by-step vô cùng chi tiết cách tạo Table và Matrix Visual. Chỉnh sửa theo cấu trúc chuẩn, bổ sung hướng dẫn chi tiết cho Dashboard 3 và quyết định bỏ qua Dashboard 4.

> Viết riêng cho người **chưa từng dùng Power BI** + đang gấp deadline.
> Mục tiêu: dựng xong Dashboard 1, 2, 3 trong ~2–3h, KHÔNG cần học custom visual.
> Nếu sau 2h vẫn chưa xong Dashboard 1 → dừng, chuyển sang **Plan B** ở cuối file.

---

## 0. Quyết định đã chốt cho deadline

- **KHÔNG làm candlestick/OHLC chart.** Đây là loại chart duy nhất cần cài custom visual từ AppSource (Stock Chart) — rủi ro cao cho người mới, không đáng đánh đổi trong thời gian ngắn.
- **Thay bằng:** line chart `close_price` + overlay `ma50`/`ma200` — multi-series line chart, kéo-thả thuần, Power BI hỗ trợ native 100%. Vẫn thể hiện đúng technical indicator, hội đồng vẫn thấy được logic MA crossover.
- RSI và MACD vốn dĩ chỉ là line chart — không có gì khó, không cần custom visual.

---

## 1. Kết nối dữ liệu (15 phút)

> ⚠️ **QUAN TRỌNG:** Bảng `public.dim_date` mặc định không tự động nạp vào Database (nó là một seed file CSV). Trước khi mở Power BI, bạn phải chạy lệnh sau trên terminal để nạp bảng:
> ```bash
> docker exec airflow-container bash -c "cd /opt/airflow/project/dbt && dbt seed --profiles-dir ."
> ```

1. Mở Power BI Desktop → **Get Data** → **PostgreSQL database**.
2. **Server**: giá trị `DB_HOST:DB_PORT` trong `.env` (vd `localhost:5432`).
3. **Database**: giá trị `DB_NAME` trong `.env` (vd `stock_db`).
4. **Data Connectivity mode:** Chọn **Import** (Khuyến nghị để dữ liệu được nạp thẳng vào RAM máy tính, an toàn tuyệt đối khi đi bảo vệ đồ án mà không lo đứt mạng/sập Database).
5. Tick chọn các bảng sau trong Navigator (đúng tên trong `gold` schema theo `CONTEXT.md`):
   - `gold.fact_stock_price`
   - `gold.fact_stock_indicators`
   - `gold.fact_market_summary`
   - `gold.dim_stock`
   - `public.dim_date`
6. Bấm **Load**. Đợi vài phút để Power BI kéo dữ liệu vào bộ nhớ.

---

## 2. Tạo relationships (10 phút)

Vào **Model view** (icon thứ 3 bên trái). Kéo thả để nối các bảng tạo Star Schema:

| Từ (Fact - Đầu Nhiều `*`) | Đến (Dim - Đầu `1`) | Cardinality | Cross filter direction |
|---|---|---|---|
| `fact_stock_price[symbol]` | `dim_stock[symbol]` | Many-to-one | Single |
| `fact_stock_indicators[symbol]` | `dim_stock[symbol]` | Many-to-one | Single |
| `fact_stock_price[trade_date]` | `dim_date[date]` | Many-to-one | Single |
| `fact_stock_indicators[trade_date]` | `dim_date[date]` | Many-to-one | Single |
| `fact_market_summary[trade_date]` | `dim_date[date]` | Many-to-one | Single |

*Mẹo: Click vào bảng `dim_date` → tab **Table tools** → **Mark as date table** → chọn cột `date`. Giúp tính toán thời gian chuẩn xác hơn.*

---

## 3. Dashboard 1 — Market Overview (45 phút)

Tạo 1 trang report mới, đặt tên "Market Overview".

**Bước 0 — Tạo Slicer ngày TRƯỚC khi build visual khác.** Nhiều visual dưới đây phải xem đúng 1 ngày, nên dựng slicer trước để vừa build vừa kiểm tra số liệu đúng ngay:
1. Kéo `dim_date[date]` ra canvas → Visualizations pane → chọn icon **Slicer**.
2. Format pane → **Slicer settings** → đổi style sang **Dropdown** (không để mặc định "Between"), và bật tính năng **Single select** — để luôn chỉ xem đúng 1 ngày.
3. Chọn sẵn ngày gần nhất có data để các visual bên dưới hiện đúng ngay khi bạn build.
4. **Slicer VN30:** Kéo `dim_stock[is_vn30]` ra canvas → Chọn **Slicer** dạng Tile (nút bấm). Bấm `True` để xem rổ VN30.

**Bước 1 — Tạo các Measure DAX dùng chung trước khi build Card và Table** (Vào tab Modeling → **New Measure**, dán nguyên văn công thức).
Việc dùng DAX tính toán trực tiếp từ `fact_stock_price` thay vì kéo cột từ `fact_market_summary` sẽ giúp **Slicer VN30 hoạt động mượt mà** trên toàn bộ biểu đồ.

```dax
Percent Change = 
VAR LatestDate = CALCULATE ( MAX ( 'public dim_date'[date] ), 'public dim_date'[is_trading_day] = TRUE ) 
VAR PrevDate = CALCULATE ( MAX ( 'public dim_date'[date] ), 'public dim_date'[date] < LatestDate, 'public dim_date'[is_trading_day] = TRUE, ALL ( 'public dim_date' ) ) 
VAR CloseLatest = CALCULATE ( SUM ( 'public_gold fact_stock_price'[close_price] ), 'public dim_date'[date] = LatestDate ) 
VAR ClosePrev = CALCULATE ( SUM ( 'public_gold fact_stock_price'[close_price] ), 'public dim_date'[date] = PrevDate ) 
RETURN DIVIDE ( CloseLatest - ClosePrev, ClosePrev )
```

```dax
Latest Total Volume = 
VAR LatestDate = CALCULATE ( MAX ( 'public dim_date'[date] ), 'public dim_date'[is_trading_day] = TRUE ) 
RETURN CALCULATE ( SUM ( 'public_gold fact_stock_price'[volume] ), 'public dim_date'[date] = LatestDate )
```

```dax
Latest Gainers = 
VAR LatestDate = CALCULATE ( MAX ( 'public dim_date'[date] ), 'public dim_date'[is_trading_day] = TRUE ) 
RETURN CALCULATE ( COUNTROWS ( 'public_gold fact_stock_price' ), 'public dim_date'[date] = LatestDate, 'public_gold fact_stock_price'[close_price] > 'public_gold fact_stock_price'[open_price] )
```

```dax
Latest Losers = 
VAR LatestDate = CALCULATE ( MAX ( 'public dim_date'[date] ), 'public dim_date'[is_trading_day] = TRUE ) 
RETURN CALCULATE ( COUNTROWS ( 'public_gold fact_stock_price' ), 'public dim_date'[date] = LatestDate, 'public_gold fact_stock_price'[close_price] < 'public_gold fact_stock_price'[open_price] )
```

```dax
      Latest Unchanged = 
      VAR LatestDate = CALCULATE ( MAX ( 'public dim_date'[date] ), 'public dim_date'[is_trading_day] = TRUE ) 
      RETURN CALCULATE ( COUNTROWS ( 'public_gold fact_stock_price' ), 'public dim_date'[date] = LatestDate, 'public_gold fact_stock_price'[close_price] = 'public_gold fact_stock_price'[open_price] )
```

*Vì sao dùng measure thay vì Quick Measure mặc định: Quick Measure tính % thay đổi so với ngày hôm qua theo lịch sẽ ra sai hoặc trống vào thứ Hai. Dùng cờ `is_trading_day` giúp luôn tính so với phiên giao dịch thật liền trước.*

**Bước 2 — Dựng 4 visual chính:**

1. **Line chart** — VNINDEX
   - Axis: `fact_market_summary[trade_date]`
   - Values: `fact_market_summary[vnindex_close]`
   - 🛡️ **Edit Interactions (Sửa lỗi biểu đồ bị biến thành dấu chấm):** Vì Slicer thời gian đang bị khóa ở 1 ngày, Line chart VNINDEX sẽ bị co lại thành 1 chấm duy nhất (mất xu hướng lịch sử). 
   Khắc phục: Bạn bấm chọn cái **Slicer thời gian** -> Lên thanh menu trên cùng chọn tab **Format** -> Bấm nút **Edit interactions**. Lúc này trên các biểu đồ sẽ xuất hiện thêm các icon nhỏ. Bạn nhìn sang Line chart VNINDEX, bấm vào icon **None** (hình tròn có gạch chéo). Xong việc, bấm lại Edit interactions để tắt. VNINDEX giờ sẽ luôn hiện đủ toàn bộ lịch sử mặc kệ Slicer!
2. **Clustered bar chart** — Gainers/Losers
   - **Không kéo gì vào Axis.** Kéo 3 measure mới tạo: `Latest Gainers`, `Latest Losers`, `Latest Unchanged` vào **Values**.
   - 💡 **Ưu điểm:** Do measure được tính toán từ `fact_stock_price`, nó sẽ tự động thay đổi dữ liệu chính xác khi bạn chọn Slicer `is_vn30`! Lỗi "Gotcha cộng dồn lịch sử" cũng bị triệt tiêu do measure đã khóa cứng `LatestDate`.
3. **Card** — Volume
   - Field: kéo measure `Latest Total Volume` (Bước 1) vào. Tránh dùng cột raw `total_volume` vì nó không tự resolve về ngày gần nhất.
4. **Table** — Top Movers
   1. Visualizations pane → icon **Table**.
   2. Kéo `dim_stock[symbol]`, `fact_stock_price[close_price]`, và measure `Percent Change` vào **Columns**.
   3. Click measure `Percent Change` → tab **Measure tools** → **Format** → **Percentage (%)**, 2 chữ số thập phân.
   4. Chọn visual table → panel **Filters** → bấm mở rộng mục **symbol** (mục này đã có sẵn trong phần *Filters on this visual*) → Filter type chọn **Top N** → ô *Show items* gõ `10` → ô **By value** kéo Measure `Percent Change` thả vào → Apply. 
   5. Click đúp header cột `Percent Change` để sort **Descending**.

---

## 4. Dashboard 2 — Stock Analysis (45 phút)

Tạo trang mới "Stock Analysis" chuyên phân tích kỹ thuật từng mã.

1. **Slicer** — Kéo `dim_stock[symbol]` (đặt dạng Dropdown/Search, set single-select).
2. **Line chart** — Close + MA overlay
   - Axis: Kéo `dim_date[date]`
   - Values: Kéo **cả 3 field cùng lúc** từ bảng `fact_stock_indicators` vào Values: `close_price`, `ma50`, `ma200`. (Giải pháp thay thế cho đồ thị nến).
   - 💡 *Ý nghĩa (Moving Average): MA50 và MA200 đại diện cho xu hướng trung và dài hạn. Khi đường giá hoặc MA50 cắt chéo lên trên MA200 (Golden Cross), đó là tín hiệu xác nhận đảo chiều tăng giá. Ngược lại, cắt xuống (Death Cross) là tín hiệu xu hướng giảm.*
3. **Line chart riêng** — RSI14
   - Axis: Kéo `dim_date[date]`
   - Values: Kéo `fact_stock_indicators[rsi_14]`
   - Thêm 2 đường tham chiếu 30/70 bằng **Analytics pane → Constant line**.
   - 💡 *Ý nghĩa (Relative Strength Index): RSI đo lường sức mạnh của phe Mua/Bán. Khi đường RSI vượt lên trên mốc 70, cổ phiếu đang bị đẩy lên vùng "Quá Mua" (Overbought) - cảnh báo rủi ro chốt lời giảm giá. Khi rớt xuống dưới 30 là vùng "Quá Bán" (Oversold) - tín hiệu dòng tiền bắt đáy có thể xuất hiện.*
4. **Line chart riêng** — MACD
   - Axis: Kéo `dim_date[date]`
   - Values: Kéo 2 cột `fact_stock_indicators[macd_line]` và `fact_stock_indicators[macd_signal]` (chung 1 chart).
   - 💡 *Ý nghĩa (MACD): Chỉ báo đo lường động lượng (momentum) của giá. Tín hiệu Mua xuất hiện khi đường MACD cắt lên trên đường Signal. Tín hiệu Bán khi MACD cắt xuống dưới Signal. Độ rộng giữa 2 đường này càng lớn, xung lực tăng/giảm hiện tại càng dữ dội.*
5. **Line chart** — Bollinger Bands
   - Axis: Kéo `dim_date[date]`
   - Values: Kéo 3 cột `fact_stock_indicators[close_price]`, `fact_stock_indicators[bb_upper]`, `fact_stock_indicators[bb_lower]`.
   - 💡 *Ý nghĩa (Bollinger Bands): Dải băng đo lường độ biến động. Khi giá (close_price) chạm vào dải trên (Upper Band), giá có xu hướng bị dội xuống. Chạm dải dưới (Lower Band) thường nảy lên. Đặc biệt, nếu hai dải này thắt chặt lại với nhau (Nút thắt cổ chai - Squeeze), đó là điềm báo giá sắp có một đợt bùng nổ theo một hướng bất kỳ.*

*Lưu ý: Chỉ cần chọn mã (ví dụ FPT) trên Slicer `symbol`, toàn bộ các biểu đồ kỹ thuật sẽ lập tức nhảy theo dữ liệu của FPT. Tốc độ rất nhanh vì dữ liệu đệ quy khó nhất đã được dbt tính trước ở tầng Gold Database.*

---

## 5. Dashboard 3 — Market Trends (Mới)

Tạo trang mới "Market Trends" chuyên phân tích xu hướng và dòng tiền thị trường qua thời gian. Thêm các phần tử sau:

**Bước 1 — Tạo các Measure DAX cho Dashboard Trend** (Cho phép dữ liệu chạy dài theo thời gian, không bị ép vào ngày gần nhất như Dashboard 1):
```dax
Trend Gainers = CALCULATE ( COUNTROWS ( 'public_gold fact_stock_price' ), 'public_gold fact_stock_price'[close_price] > 'public_gold fact_stock_price'[open_price] )
```
```dax
Trend Losers = CALCULATE ( COUNTROWS ( 'public_gold fact_stock_price' ), 'public_gold fact_stock_price'[close_price] < 'public_gold fact_stock_price'[open_price] )
```
```dax
Trend Unchanged = CALCULATE ( COUNTROWS ( 'public_gold fact_stock_price' ), 'public_gold fact_stock_price'[close_price] = 'public_gold fact_stock_price'[open_price] )
```
```dax
Trend Volume = SUM ( 'public_gold fact_stock_price'[volume] )
```

**Bước 2 — Dựng các phần tử biểu đồ:**

1. **Bộ lọc Slicers:**
   - **Slicer VN30:** Copy nút chọn `is_vn30` từ Dashboard 1 sang để lọc nhanh nhóm VN30 hoặc Toàn thị trường.
   - **Slicer Khoảng thời gian (Date Range):** Kéo `dim_date[date]` ra canvas -> Chọn **Slicer** -> Chuyển kiểu hiển thị sang dạng **Between** (Thanh trượt). Cho phép người dùng kéo chọn khoảng thời gian bất kỳ (ví dụ: 1 tháng, 3 tháng, hoặc 1 năm) để phân tích xu hướng dài hạn.
2. **Biểu đồ Cột chồng Xu hướng Thị trường (Stacked Column Chart - Gainers vs Losers):**
   - **Chọn visual:** Biểu đồ `Stacked column chart`.
   - **X-axis:** Kéo `dim_date[date]`.
   - **Y-axis:** Kéo 3 measure vừa tạo: `Trend Gainers`, `Trend Losers`, `Trend Unchanged`.
   - **Định dạng màu:** Vào phần Format visual -> Columns -> Thiết lập màu trực quan: `Trend Gainers` (Xanh lá), `Trend Losers` (Đỏ), `Trend Unchanged` (Xám).
   - **Ý nghĩa:** Giúp quan sát tỷ lệ tương quan giữa số mã tăng và giảm qua các phiên lịch sử để nhận diện xu thế Uptrend/Downtrend của dòng tiền.
3. **Biểu đồ Thanh khoản Lịch sử (Clustered Column Chart - Market Volume):**
   - **Chọn visual:** Biểu đồ `Clustered column chart` hoặc `Area chart`.
   - **X-axis:** Kéo `dim_date[date]`.
   - **Y-axis:** Kéo measure `Trend Volume` vào.
   - **Ý nghĩa:** Giám sát tổng khối lượng giao dịch khớp lệnh hàng ngày, giúp xác nhận độ mạnh yếu của xu hướng giá (xu hướng tăng đi kèm thanh khoản lớn là tín hiệu tích cực).

> [!NOTE]
> **Lưu ý về Dashboard 4 (Fundamentals):** Dashboard phân tích chỉ số cơ bản (P/E, P/B, ROE) đã được thống nhất lược bỏ khỏi phạm vi thiết kế Power BI trong giai đoạn này do cơ sở dữ liệu PostgreSQL chưa tích hợp luồng cào dữ liệu tài chính doanh nghiệp thật.

---

## 6. Bí kíp làm đẹp Dashboard (Trông chuyên nghiệp hơn)

Power BI mặc định sử dụng tên trường trong Database (VD: `close_price`, `macd_line`) để ghép thành các tiêu đề dài ngoằng. Để làm báo cáo trông chuẩn "tài chính quốc tế", bạn cần thực hiện 2 thao tác "che giấu" tên kỹ thuật này:

**Cách 1: Sửa Tiêu đề Biểu đồ (Format Title)**
Thay vì để chữ tự động "Close, MA200 and MA50 by Date", bạn hãy gõ lại tên tiếng Anh xịn xò hơn.
1. Click chọn biểu đồ.
2. Sang phần **Format your visual** (biểu tượng cây bút/chổi sơn).
3. Bấm sang tab **General** → Mở rộng mục **Title**.
4. Xóa dòng chữ ở ô Text và gõ tiêu đề mới.

**Cách 2: Sửa tên trong Chú giải/Bảng (Rename for this visual)**
Để xóa dấu gạch dưới `_` ở mục chú giải (Legend) mà không làm hư cấu trúc Database:
1. Nhìn sang phần **Visualizations** pane (chỗ bạn kéo thả field vào X-axis, Y-axis, Values).
2. Tìm cái field bạn muốn đổi tên (vd: `macd_line`).
3. Click đúp chuột trực tiếp vào tên field đó (hoặc bấm mũi tên xổ xuống chọn **Rename for this visual**).
4. Gõ tên mới ngắn gọn (vd: `MACD`) và nhấn Enter. 

**Bảng gợi ý đặt tên chuẩn:**
- **Dashboard 1:** Đổi tên tiêu đề Line chart thành `Market Index`. Bar chart thành `Market Breadth (Today)`.
- **Dashboard 2:** 
  - Line chart giá: Tiêu đề `Price & Moving Averages`. Đổi tên field: `close_price` → `Close`, `ma50` → `MA(50)`, `ma200` → `MA(200)`.
  - RSI: Tiêu đề `Relative Strength Index (RSI)`. Đổi tên field `rsi_14` → `RSI`.
  - MACD: Tiêu đề `MACD Indicator`. Đổi tên field `macd_line` → `MACD`, `macd_signal` → `Signal`.
  - Bollinger Bands: Tiêu đề `Bollinger Bands`. Đổi tên field `bb_upper` → `Upper Band`, `bb_lower` → `Lower Band`.

---

## 7. Lưu file & Lỗi thường gặp

**File -> Save As** -> `reports/stock_dashboard.pbix`.

| Triệu chứng lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| Kéo field vào Axis nhưng chart trống | Relationship chưa nối đúng chiều (many-to-one ngược) | Vào Model view, kiểm tra mũi tên filter direction phải hướng từ bảng Dim -> Fact |
| Slicer không lọc được visual | Visual đó nằm khác trang, hoặc field slicer không liên quan qua relationship | Đảm bảo cùng trang + có đường nối One-to-Many trong Model view |
| Slicer `is_vn30` không lọc được dữ liệu | Chưa nối `dim_stock` với các bảng Fact | Vào Model view nối `dim_stock[symbol]` với `fact_stock_price[symbol]` |
| Bar Gainers/Losers hiện số khổng lồ | Slicer thời gian đang chọn sai (bị chọn tất cả các ngày) nên hàm tự tính SUM cộng dồn lịch sử | Chuyển Slicer thời gian về chế độ Single Select và chỉ chọn 1 ngày |
| Connect PostgreSQL báo lỗi SSL | Driver yêu cầu SSL mode | Trong Advanced options của Get Data, thêm `sslmode=disable` nếu DB local không bật SSL |

---

## Plan B — Nếu quá 2h vẫn chưa xong (an toàn cho demo)

Yêu cầu AI (Gemini) thực hiện prompt sau để sinh ra Dashboard HTML tự động (chỉ vài giây):
> "Dùng python, thư viện plotly, đọc 5 bảng trong schema public_gold. Vẽ dashboard HTML tĩnh lưu tại reports/dashboard_backup.html gồm: Line chart giá VNINDEX, Bar chart top movers, Line chart MACD, Line chart RSI."
*(Lưới an toàn tuyệt đối khi báo cáo đồ án nếu tool PowerBI gặp trục trặc)*
