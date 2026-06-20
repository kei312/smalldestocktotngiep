# Task Tracker — Step-by-Step (4 part)

> **v2 (19/06):** đã tích hợp 3 quyết định chốt:
> 1. MACD Signal dùng EMA9 thật (thêm task 2.3.3d/2.3.3e) — không còn SMA approximation.
> 2. Fail Protocol: khi gate/test fail → tra SKILL quick-reference, fix 1 lần, vẫn fail thì DỪNG.
>    Chi tiết: `AGENTS.md` Section 2.5. Ghi log bắt buộc vào `docs/TEST_REPORTS.md`.
> 3. Power BI: giữ nguyên làm chính, bỏ candlestick, có Plan B. Chi tiết: `docs/POWERBI_QUICKSTART.md`.

> **Chiến lược model — tiết kiệm Claude, Gemini token dồi dào:**
>
> | Model | Ký hiệu | Dùng cho | % tasks |
> |---|---|---|---|
> | Claude Opus 4.6 | 🔴 | Interface design, RSI/EMA macros, DAG orchestration, bug khó, đánh giá cuối | ~9% |
> | Claude Sonnet 4.6 | 🟠 | Core ingestion: vnstock_provider, fetch_prices, utils (retry), backfill | ~6% |
> | Gemini 3.1 Pro High | 🟢 | Tất cả task tạo file có nội dung: configs, Silver, Gold, tests, docs, báo cáo | ~70% |
> | Gemini 3.5 Flash High | ⚪ | Chỉ: git commit, chạy command (pytest/dbt/psql), `__init__.py` | ~15% |
>
> **Nguyên tắc:** Gemini token không giới hạn → dùng Pro High thay Flash cho mọi file có nội dung. Claude chỉ dùng cho logic phức tạp hoặc kiến trúc quan trọng.

---

> **Cách dùng mỗi buổi:**
> - Mở đúng model theo ký hiệu
> - Paste prompt: `"Đọc CONTEXT.md + PROJECT_RULES.md. Tạo [file]. Yêu cầu: [acceptance criteria]"`
> - Tick `[x]` khi xong, `[/]` khi đang làm
> - **Nếu gate/test fail:** xem `AGENTS.md` Section 2.5 trước khi tự sửa. Ghi log vào `docs/TEST_REPORTS.md`.

---

## part 1 — Foundation + Provider + Bronze

### 1.1 Hạ tầng (Sáng — ~2h)

| # | Task | Model | File/Command | Acceptance |
|---|---|---|---|---|
| 1.1.1 | [ ] Tạo thư mục project + git init | ⚪ Flash | `mkdir stock-pipeline && git init` | Repo trống |
| 1.1.2 | [ ] `.gitignore` (Python + .env + venv + __pycache__) | 🟢 Pro High | `.gitignore` | Đủ patterns |
| 1.1.3 | [ ] `requirements.txt` — pin versions | 🟢 Pro High | `requirements.txt` | vnstock, psycopg2-binary, pandas, apache-airflow==3.2.*, dbt-core==1.10.*, dbt-postgres==1.10.* |
| 1.1.4 | [ ] `.env.example` + `.env` | 🟢 Pro High | `.env.example`, `.env` | DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, PROVIDER |
| 1.1.5 | [ ] `docker-compose.yml` — Airflow 3.2.x + PostgreSQL 17 | 🟢 Pro High | `docker-compose.yml` | `docker compose up -d` thành công, Airflow UI accessible :8080 |
| 1.1.6 | [ ] `sql/init_schema.sql` — Bronze schema + partitions | 🟢 Pro High | `sql/init_schema.sql` | Schema `bronze`, table `bronze_prices` PK(code,date), partition 2020-2026, bronze_index |
| 1.1.7 | [ ] Chạy init_schema trên PostgreSQL | ⚪ Flash | `psql -f sql/init_schema.sql` | `\dt bronze.*` hiện tables |
| 1.1.8 | [ ] Test vnstock 4.x lấy thử VNM, 7 part | ⚪ Flash | Python REPL | DataFrame trả về, đủ cột OHLCV. Nếu fail vì mạng/API → set `PROVIDER=mock`, ghi `STATUS.md`, tiếp tục (xem AGENTS.md Section 0) |
| 1.1.9 | [ ] **Commit:** `chore: init project + docker + bronze schema` | ⚪ Flash | git | — |

> **⏸ Gate 1.1:** PostgreSQL running + vnstock hoạt động (hoặc MockProvider nếu vnstock fail). Nếu fail mà không khớp gotcha nào → xem AGENTS.md Section 2.5.

---

### 1.2 Provider Layer (Chiều — ~3h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 1.2.1 | [x] `providers/__init__.py` | ⚪ Flash | `providers/__init__.py` | Export classes |
| 1.2.2 | [x] `providers/base.py` — ABC + 3 exceptions | 🔴 **Opus** | `providers/base.py` | DataProvider ABC (get_prices, get_index, health_check), ProviderError/RateLimitError/TimeoutError/SchemaError, type hints, docstrings — **interface design là kiến trúc** |
| 1.2.3 | [x] `providers/vnstock_provider.py` | 🟠 Sonnet | `providers/vnstock_provider.py` | Bọc vnstock 4.x, map exceptions, health_check, trả DataFrame chuẩn |
| 1.2.4 | [x] `tests/fixtures/mock_prices.csv` — 30+ dòng VN30 | 🟢 Pro High | `tests/fixtures/mock_prices.csv` | Realistic OHLCV, ~30 dòng, 5–10 mã trong nhóm: VNM (60–75k), VCB (85–95k), HPG (25–32k), FPT (120–140k), VIC (40–55k), MWG (45–60k), MSN (65–80k), CTG (30–40k). Mọi dòng: high ≥ open,close ≥ low; volume > 0 |
| 1.2.5 | [x] `tests/fixtures/mock_index.csv` | 🟢 Pro High | `tests/fixtures/mock_index.csv` | VNINDEX + VN30 data |
| 1.2.6 | [x] `providers/mock_provider.py` | 🟢 Pro High | `providers/mock_provider.py` | Đọc CSV fixture, trả DataFrame cùng schema, support get_prices + get_index + health_check |
| 1.2.7 | [x] `providers/registry.py` | 🟢 Pro High | `providers/registry.py` | get_provider() đọc env PROVIDER → instance |
| 1.2.8 | [x] `tests/test_providers.py` — P-01 đến P-05 | 🟢 Pro High | `tests/test_providers.py` | 5 test functions, pytest |
| 1.2.9 | [x] Chạy pytest test_providers.py | ⚪ Flash | `pytest tests/test_providers.py -v` | All pass. Ghi kết quả vào `docs/TEST_REPORTS.md` |
| 1.2.10 | [x] **Commit:** `feat: provider layer (vnstock + mock + registry)` | ⚪ Flash | git | — |

---

### 1.3 Ingestion cơ bản (Tối — ~3h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 1.3.1 | [x] `ingestion/__init__.py` | ⚪ Flash | `ingestion/__init__.py` | — |
| 1.3.2 | [x] `ingestion/config.py` — Config dataclass | 🟢 Pro High | `ingestion/config.py` | @dataclass, đọc .env, fields: db_url, provider, symbols_pilot, batch_size, retry_max |
| 1.3.3 | [x] `ingestion/utils.py` — retry decorator + logger setup | 🟠 Sonnet | `ingestion/utils.py` | @retry(max_attempts, backoff_base, jitter, retry_on), logging config, validate_dataframe() |
| 1.3.4 | [x] `ingestion/fetch_prices.py` — UPSERT Bronze | 🟠 Sonnet | `ingestion/fetch_prices.py` | Gọi provider.get_prices(), validate schema, ON CONFLICT DO UPDATE, log row count, source tracking |
| 1.3.5 | [x] `ingestion/fetch_index.py` | 🟢 Pro High | `ingestion/fetch_index.py` | Tương tự fetch_prices cho VNINDEX/VN30 |
| 1.3.6 | [x] Chạy fetch_prices VN30 thật → Bronze | ⚪ Flash | `python -m ingestion.fetch_prices` | `SELECT COUNT(*) FROM bronze.bronze_prices` > 0 |
| 1.3.7 | [x] Chạy lại → verify idempotency (đếm dòng) | ⚪ Flash | SQL: `SELECT COUNT(*)` trước và sau | count bằng nhau |
| 1.3.8 | [x] `tests/test_ingestion.py` — B-01 đến B-04 | 🟢 Pro High | `tests/test_ingestion.py` | 4 test functions |
| 1.3.9 | [x] Chạy pytest test_ingestion.py | ⚪ Flash | `pytest tests/test_ingestion.py -v` | All pass. Ghi kết quả vào `docs/TEST_REPORTS.md` |
| 1.3.10 | [x] **Commit:** `feat: ingestion layer + idempotent UPSERT` | ⚪ Flash | git | — |
| 1.3.11 | [x] Cập nhật `STATUS.md` | ⚪ Flash | `STATUS.md` | Ghi đã xong gì part 1 |

> **⏸ Gate part 1:** Bronze có data VN30 thật. Provider + Ingestion tests pass. Idempotency verified.

---

## part 2 — Backfill + Silver + Gold Indicators

### 2.1 Backfill (Sáng — ~2.5h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 2.1.1 | [x] `ingestion/backfill.py` — batch, resume, rate-limit | 🟠 Sonnet | `ingestion/backfill.py` | Batch by symbols (batch_size), skip existing dates, sleep between batches, progress bar/logging, CLI args --start --end |
| 2.1.2 | [x] Chạy backfill VN30, 5 năm | ⚪ Flash | `python -m ingestion.backfill --start 2021-01-01 --end 2026-06-18` | ~37,800 dòng trong Bronze |
| 2.1.3 | [x] Verify phân bố năm | ⚪ Flash | SQL query | Mỗi năm có data |
| 2.1.4 | [x] Chạy backfill lần 2 → idempotency ở scale | ⚪ Flash | So sánh COUNT trước/sau | Không tăng |
| 2.1.5 | [x] **Commit:** `feat: backfill 5yr VN30 data` | ⚪ Flash | git | — |

---

### 2.2 Silver — dbt (Chiều — ~2.5h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 2.2.1 | [x] `dbt init` hoặc tạo structure thủ công | ⚪ Flash | `dbt/` folder structure | dbt project skeleton |
| 2.2.2 | [x] `dbt/dbt_project.yml` | 🟢 Pro High | `dbt/dbt_project.yml` | Project name, model paths, seed paths |
| 2.2.3 | [x] `dbt/profiles.yml` — PostgreSQL connection | 🟢 Pro High | `dbt/profiles.yml` | `dbt debug` pass |
| 2.2.4 | [x] `dbt/sources.yml` — khai báo Bronze tables | 🟢 Pro High | `dbt/models/sources.yml` | bronze_prices + bronze_index source definitions |
| 2.2.5 | [x] `dbt/models/silver/silver_prices.sql` | 🟢 Pro High | `dbt/models/silver/silver_prices.sql` | Cast types, is_valid logic, dq_flag CASE, loaded_at |
| 2.2.6 | [x] `dbt/models/silver/silver_index.sql` | 🟢 Pro High | `dbt/models/silver/silver_index.sql` | Tương tự cho index |
| 2.2.7 | [x] `dbt/models/silver/schema.yml` — tests | 🟢 Pro High | `dbt/models/silver/schema.yml` | not_null, unique, accepted_range, expression_is_true |
| 2.2.8 | [x] `dbt run --select silver` | ⚪ Flash | Terminal | Models materialized |
| 2.2.9 | [x] `dbt test --select silver` | ⚪ Flash | Terminal | Tests S-01, S-02, S-03 pass. Ghi vào `docs/TEST_REPORTS.md` |
| 2.2.10 | [x] Verify: query Silver kiểm tra is_valid | ⚪ Flash | SQL query | Dữ liệu clean |
| 2.2.11 | [x] **Commit:** `feat: Silver layer (dbt cleaning + tests)` | ⚪ Flash | git | — |

---

### 2.3 Gold — Indicators (Tối — ~3.5h) ⚠️ PHẦN KHÓ NHẤT

> **Đọc trước:** `SKILL_sql_indicators.md` (v2 — đã có MACD Signal EMA9 thật) và `SKILL_dbt_incremental.md`.

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 2.3.1 | [x] `dbt/models/gold/fact_stock_price.sql` | 🟢 Pro High | `dbt/models/gold/fact_stock_price.sql` | SELECT từ Silver WHERE is_valid=TRUE, grain (symbol, trade_date) |
| 2.3.2 | [x] `dbt/macros/calculate_rsi.sql` — RSI Wilder | 🔴 **Opus** | `dbt/macros/calculate_rsi.sql` | RSI14 đúng Wilder smoothing, xử lý warm-up, SQL recursive CTE hoặc window |
| 2.3.2b | [x] `dbt/models/gold/intermediate/int_rsi14.sql` | 🟢 Pro High | `int_rsi14.sql` | `materialized='table'`, gọi `calculate_rsi(14)` |
| 2.3.3 | [x] `dbt/macros/calculate_ema.sql` — EMA, **generalized** | 🔴 **Opus** | `dbt/macros/calculate_ema.sql` | EMA recursive, nhận thêm `source_relation`/`value_column` (default fact_stock_price/close) để dùng lại được cho MACD Signal — xem SKILL_sql_indicators.md mục 3 |
| 2.3.3b | [x] `dbt/models/gold/intermediate/int_ema12.sql` | 🟢 Pro High | `int_ema12.sql` | `materialized='table'`, gọi `calculate_ema(12)` |
| 2.3.3c | [x] `dbt/models/gold/intermediate/int_ema26.sql` | 🟢 Pro High | `int_ema26.sql` | `materialized='table'`, gọi `calculate_ema(26)` |
| 2.3.3d | [x] `dbt/models/gold/intermediate/int_macd_line.sql` **(NEW)** | 🟢 Pro High | `int_macd_line.sql` | `materialized='table'`, `ema12 - ema26` JOIN theo (symbol, trade_date), filter NOT NULL cả 2 |
| 2.3.3e | [x] `dbt/models/gold/intermediate/int_macd_signal.sql` **(NEW)** | 🟢 Pro High | `int_macd_signal.sql` | `materialized='table'`, gọi `calculate_ema(9, source_relation=ref('int_macd_line'), value_column='macd_line')` |
| 2.3.4 | [x] `dbt/models/gold/fact_stock_indicators.sql` — incremental | 🔴 **Opus** | `dbt/models/gold/fact_stock_indicators.sql` | MA5, MA20, RSI14 (từ int_rsi14), MACD line/signal/histogram (từ int_macd_line + int_macd_signal — **KHÔNG dùng SMA window approximation**), Bollinger upper/lower. Config incremental. — **tổ hợp tất cả intermediate, logic phức tạp nhất** |
| 2.3.5 | [x] `dbt run --select fact_stock_indicators+` | ⚪ Flash | Terminal | Materialized thành công (dùng `+` để build cả intermediate upstream) |
| 2.3.6 | [x] `dbt/models/gold/schema.yml` — Gold tests | 🟢 Pro High | `dbt/models/gold/schema.yml` | RSI range [0,100], BB upper>=lower, MA20 null-or-positive |
| 2.3.7 | [x] `dbt test --select gold` | ⚪ Flash | Terminal | G-01 (RSI), G-02 (MA20 warm-up) pass. Ghi vào `docs/TEST_REPORTS.md` |
| 2.3.8 | [x] Test G-03: MACD so tính tay 3 mã | 🔴 **Opus** | `scripts/verify_macd_g03.py` | Sai số < 0.5% (thật, không nới lỏng — vì SQL nay đã dùng EMA9 chuẩn khớp đúng Python reference). Ghi kết quả vào `docs/TEST_REPORTS.md` |
| 2.3.9 | [x] **Commit:** `feat: Gold indicators (MA/RSI/MACD-EMA9/Bollinger)` | ⚪ Flash | git | — |
| 2.3.10 | [x] Cập nhật `STATUS.md` | ⚪ Flash | `STATUS.md` | — |

> **⏸ Gate part 2:** `dbt run && dbt test` pass Silver + Gold (kể cả 5 intermediate models). Indicators tính đúng. G-03 < 0.5% thật.

> [!WARNING]
> **2.3.2 + 2.3.3 dùng Opus.** RSI Wilder + EMA recursive (kể cả bản generalized cho MACD Signal)
> trong pure SQL là phần khó nhất. Nếu bí → hỏi Opus debug. Không dùng Flash/Pro cho bước này.
> Các task `2.3.2b/2.3.3b/2.3.3c/2.3.3d/2.3.3e` chỉ là **gọi lại** macro đã có, nên dùng 🟢 Pro
> High là đủ — không cần Opus cho các model gọi macro.

---

## part 3 — Gold hoàn chỉnh + Airflow + Power BI

### 3.1 Gold hoàn chỉnh (Sáng — ~2h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 3.1.1 | [x] `dbt/models/gold/fact_market_summary.sql` | 🟢 Pro High | `dbt/models/gold/fact_market_summary.sql` | gainers/losers/unchanged/volume/vnindex/vn30 per trade_date |
| 3.1.2 | [x] `dbt/models/gold/dim_stock.sql` | 🟢 Pro High | `dbt/models/gold/dim_stock.sql` | DISTINCT symbol, exchange (+ industry nếu có) |
| 3.1.3 | [x] `dbt/seeds/dim_date.csv` — calendar 2020–2026 | 🟢 Pro High | `dbt/seeds/dim_date.csv` | date, day_of_week, month, quarter, year, is_trading_day |
| 3.1.4 | [x] `dbt seed` + verify dim_date | ⚪ Flash | `dbt seed` | Table materialized |
| 3.1.5 | [x] Test G-04: market summary totals | 🟢 Pro High | schema.yml update | gainers+losers+unchanged = total |
| 3.1.6 | [x] `dbt run && dbt test` — toàn bộ | ⚪ Flash | Terminal | All pass |
| 3.1.7 | [x] `dbt docs generate` → **📸 chụp lineage graph** | ⚪ Flash | `dbt docs generate && dbt docs serve` | Screenshot saved (lineage giờ có thêm 5 intermediate nodes — bình thường, không phải lỗi) |
| 3.1.8 | [x] **Commit:** `feat: complete Gold star schema + lineage` | ⚪ Flash | git | — |

---

### 3.2 Airflow (Chiều — ~2.5h)

> **⚠️ Thiết kế đã chỉnh sửa (20/06):**
> Kế hoạch gốc đặt "Mount DAGs" ở bước 3.2.3 (sau khi viết DAG). Đây là lỗi thứ tự:
> 1. `docker-compose.yml` hiện tại chỉ mount `./dags` — container không thấy `ingestion/`, `providers/`, `dbt/`.
> 2. `PythonOperator` sẽ crash (`ModuleNotFoundError`) vì thiếu source code.
> 3. `dbt` chưa được cài trong container (`_PIP_ADDITIONAL_REQUIREMENTS` thiếu `dbt-postgres`).
>
> **Giải pháp:** Đưa việc sửa `docker-compose.yml` lên **bước đầu tiên**, mount toàn bộ project,
> cài `dbt-postgres`, và chuyển DAG sang dùng `BashOperator` thay vì `PythonOperator`.

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 3.2.1 | [x] Sửa `docker-compose.yml` — mount project + cài dbt | 🔴 **Opus** | `docker-compose.yml` | Thêm volume `./:/opt/airflow/project`, thêm `dbt-postgres==1.10.0` vào `_PIP_ADDITIONAL_REQUIREMENTS`, fix quyền `dags/` (`sudo chown`). `docker compose up -d` thành công. — **quyết định kiến trúc container** |
| 3.2.2 | [x] `dags/dag_daily.py` — full pipeline DAG (**BashOperator**) | 🔴 **Opus** | `dags/dag_daily.py` | health_check → fetch_prices → fetch_index → dbt_silver → test_silver → dbt_gold → test_gold → notify. Dùng `BashOperator` gọi `python -m ingestion.fetch_prices` và `dbt run` từ `/opt/airflow/project`. Retry 3x exponential. on_failure_callback. — **orchestration là kiến trúc** |
| 3.2.3 | [x] `dags/dag_backfill.py` — manual trigger (**BashOperator**) | 🟢 Pro High | `dags/dag_backfill.py` | Params: start_date, end_date. Manual trigger only. Gọi `python -m ingestion.backfill` từ `/opt/airflow/project`. |
| 3.2.4 | [x] Restart Airflow → verify DAGs visible | ⚪ Flash | `docker compose restart airflow` | DAGs xuất hiện trong Airflow UI (:8080) |
| 3.2.5 | [x] Trigger dag_daily → verify success → **📸 screenshot DAG success** | ⚪ Flash | `airflow dags list-runs` / Airflow UI | CLI xác nhận state=success, chụp ảnh UI làm bằng chứng |
| 3.2.6 | [x] Test A-01: fail fetch → Silver skipped + **📸 screenshot retry** | ⚪ Flash | `airflow tasks states-for-dag-run` / Airflow UI | Silver không chạy khi fetch fail. Retry visible trong UI |
| 3.2.7 | [x] **Commit:** `feat: Airflow DAGs (daily + backfill)` | ⚪ Flash | git | — |

---

### 3.3 Power BI — 2 dashboard đầu (Tối — ~2.5h)

> **Trước khi bắt đầu, đọc `docs/POWERBI_QUICKSTART.md`** — guide rút gọn theo đúng tên bảng Gold
> của dự án, viết riêng cho người chưa dùng Power BI trước đó. Time-box: nếu sau 2h chưa xong
> Dashboard 1 → chuyển Plan B (mục 3.3.9).

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 3.3.0 | [ ] Đọc `docs/POWERBI_QUICKSTART.md` (15 phút) | — (UI) | — | Hiểu rõ các bước chốt cho deadline 20/06 trước khi mở Power BI |
| 3.3.1 | [ ] Kết nối Power BI → PostgreSQL Gold | — (UI) | Power BI Desktop | Connection OK, chọn Import (an toàn) hoặc DirectQuery (xem mục 1 trong quickstart) |
| 3.3.2 | [ ] Data model: relationships fact↔dim | — (UI) | Power BI | Star schema (nối dim_stock, dim_date). Mark as date table cho dim_date (xem mục 2 trong quickstart) |
| 3.3.3 | [ ] **Dashboard 1: Market Overview** | — (UI) | Power BI | Slicer part (Single select) dựng trước. Measure DAX (Percent Change, Latest Total Volume). Visuals: VNINDEX line, Gainers/Losers bar (Values: gainers/losers/unchanged, Axis trống, gotcha sum cộng dồn), Volume card (measure), Top Movers table (Top 10 filter, sort Desc) (xem mục 3 trong quickstart) |
| 3.3.4 | [ ] **Dashboard 2: Stock Analysis (đã đơn giản hoá)** | — (UI) | Power BI | Symbol slicer (single select), Close+MA overlay line chart (close, ma5, ma20), RSI14 line chart (Constant line 30/70), MACD line+signal chart, Bollinger Bands line chart (close, bb_upper, bb_lower) (xem mục 4 trong quickstart) |
| 3.3.5 | [ ] **📸 Screenshot cả 2 dashboards** | — | Ảnh | — |
| 3.3.6 | [ ] Lưu `.pbix` | — | `reports/stock_dashboard.pbix` | — |
| 3.3.7 | [ ] **Commit:** `feat: Power BI dashboards 1+2` | ⚪ Flash | git | — |
| 3.3.8 | [ ] Cập nhật `STATUS.md` | ⚪ Flash | `STATUS.md` | — |
| 3.3.9 | [x] **[Plan B — chỉ làm nếu quá time-box 2h]** Generate `reports/dashboard_backup.html` |  ⚪ Flash | `reports/dashboard_backup.html` | Script Python (psycopg2 + plotly) đọc trực tiếp Gold, xuất HTML tĩnh không cần Power BI. KHÔNG thay thế yêu cầu nộp `.pbix` — chỉ là lưới an toàn demo (xem mục Plan B trong quickstart) |

> **⏸ Gate part 3:** Airflow E2E thành công (verify qua CLI). 2 dashboards hiển thị đúng (hoặc Plan B nếu quá time-box). Lineage exported.

---

## part 4 — Hoàn thiện + Docs + Báo cáo

### 4.1 Power BI hoàn thiện (Sáng — ~1.5h) 

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 4.1.1 | [ ] **Dashboard 3: Xu hướng Gainers/Losers theo thời gian** (Định nghĩa lại) | — (UI) | Power BI | Slicer khoảng part (Between style). Line/Area chart xem xu hướng gainers vs losers theo part (trục trade_date), optional area chart cho total_volume (xem mục 4b trong quickstart) |
| 4.1.2 | [ ] **Dashboard 4: Fundamentals** (nếu có data) | — (UI) | Power BI | Chỉ làm SAU KHI có fact_fundamentals. Card/Table PE, PB, ROE theo symbol, slicer dùng chung (xem mục 4b trong quickstart) |
| 4.1.3 | [ ] **📸 Screenshot all dashboards** | — | Ảnh | — |
| 4.1.4 | [ ] **Commit:** `feat: Power BI dashboards 3+4` | ⚪ Flash | git | — |

### 4.2 Mở rộng — nếu thời gian cho phép (Sáng — ~1.5h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 4.2.1 | [ ] `ingestion/fetch_fundamentals.py` | ⚪ Flash | `ingestion/fetch_fundamentals.py` | PE/PB/ROE/ROA/EPS theo quý |
| 4.2.2 | [ ] `dbt/models/gold/fact_fundamentals.sql` | ⚪ Flash | `dbt/models/gold/fact_fundamentals.sql` | Materialized |
| 4.2.3 | [x] Kế hoạch mở rộng HoSE (~400 mã) | 🟢 Pro High | docs/HOSE_SCALING_PLAN.md | Viết tài liệu kế hoạch mở rộng hệ thống |
| 4.2.4 | [ ] **Commit:** `feat: fundamentals + expanded universe` | ⚪ Flash | git | — |

---

### 4.3 Documentation (Chiều — ~2h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 4.3.1 | [x] `docs/CONTEXT.md` — copy vào repo | 🟢 Pro High | `docs/CONTEXT.md` | Copy từ artifact đã tạo (v2), review + chỉnh nếu cần |
| 4.3.2 | [x] `docs/PROJECT_RULES.md` — copy vào repo | 🟢 Pro High | `docs/PROJECT_RULES.md` | Copy từ artifact đã tạo, review + chỉnh nếu cần |
| 4.3.3 | [x] `docs/DATA_CONTRACTS.md` | 🟢 Pro High | `docs/DATA_CONTRACTS.md` | Mở rộng từ PROJECT_RULES section 5 |
| 4.3.4 | [x] `docs/ADR/ADR-001-postgres.md` | 🟢 Pro High | `docs/ADR/ADR-001-postgres.md` | Context, Decision, Consequences |
| 4.3.5 | [x] `docs/ADR/ADR-002-dbt.md` | 🟢 Pro High | `docs/ADR/ADR-002-dbt.md` | Tại sao 1.10 không 2.0 |
| 4.3.6 | [x] `docs/ADR/ADR-003-provider.md` | 🟢 Pro High | `docs/ADR/ADR-003-provider.md` | 1 VnstockProvider thay 4 |
| 4.3.6b | [x] `docs/ADR/ADR-004-macd-signal.md` **(NEW)** | 🟢 Pro High | `docs/ADR/ADR-004-macd-signal.md` | Context: SMA approximation rủi ro fail G-03. Decision: EMA9 thật qua int_macd_line/int_macd_signal + macro generalized. Consequences: thêm 2 model nhỏ, đổi lại độ chính xác đúng định nghĩa chuẩn |
| 4.3.7 | [x] `README.md` — setup guide | 🟢 Pro High | `README.md` | Clone → .env → docker up → init → backfill → dbt → Power BI (link tới docs/POWERBI_QUICKSTART.md) |
| 4.3.8 | [x] `STATUS.md` hoàn chỉnh | 🟢 Pro High | `STATUS.md` | All tasks done, format đẹp |
| 4.3.9 | [x] **Commit:** `docs: ADRs + data contracts + README` | ⚪ Flash | git | — |

---

### 4.4 Demo & Verification (Chiều — ~1h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 4.4.1 | [ ] Demo: `PROVIDER=mock` → DAG chạy offline | ⚪ Flash | Airflow UI | **📸 Screenshot** |
| 4.4.2 | [ ] Demo: backfill 2 lần → count bằng nhau | ⚪ Flash | SQL | Idempotency proven |
| 4.4.3 | [ ] Full E2E test (E-01) | ⚪ Flash | Trigger dag_daily | Bronze→Silver→Gold→BI all OK |
| 4.4.4 | [ ] Verify Power BI (hoặc Plan B HTML) khớp Gold | — | So sánh | Match |
| 4.4.5 | [ ] **📸 dbt test output screenshot** | ⚪ Flash | Terminal | — |

---

### 4.5 Báo cáo nháp (Tối — ~2h)

| # | Task | Model | File | Acceptance |
|---|---|---|---|---|
| 4.5.1 | [x] Thu thập screenshots | — | Folder | ≥6 ảnh |
| 4.5.2 | [x] Viết: Giới thiệu + Bài toán | 🟢 Pro High | Báo cáo | — |
| 4.5.3 | [x] Viết: Kiến trúc (sơ đồ + giải thích) | 🟢 Pro High | Báo cáo | Copy + mở rộng từ CONTEXT.md |
| 4.5.4 | [x] Viết: Thiết kế chi tiết | 🟢 Pro High | Báo cáo | Modules, schema, data flow (bao gồm intermediate models cho indicators) |
| 4.5.5 | [x] Viết: Kết quả + screenshots | 🟢 Pro High | Báo cáo | Embed ảnh |
| 4.5.6 | [x] Viết: Đánh giá + Hạn chế + Kết luận | 🔴 **Opus** | Báo cáo | Suy luận sâu: đã làm gì, hướng phát triển |
| 4.5.7 | [x] **Commit:** `docs: draft report` | ⚪ Flash | git | — |
| 4.5.8 | [x] **Final commit:** `chore: project complete` | ⚪ Flash | git | — |

> **⏸ Gate part 4:** Sản phẩm E2E. Báo cáo nháp đủ sections + screenshots. Git log đều 4 part.
>
> **Lưu ý báo cáo:** nếu trường có template/outline chính thức cho báo cáo ĐATN, ưu tiên bám theo
> đó thay vì cấu trúc generic ở trên — đưa file template vào `docs/` nếu có.

---

## Tổng kết phân bổ Model

| Model | Số tasks | Tasks cụ thể |
|---|---|---|
| 🔴 **Claude Opus** | 7 (~8%) | `base.py` (interface), RSI macro, EMA macro (generalized), `fact_stock_indicators.sql`, MACD verify, `dag_daily.py`, báo cáo đánh giá |
| 🟠 **Claude Sonnet** | 5 (~6%) | `vnstock_provider.py`, `utils.py` (retry), `fetch_prices.py`, `backfill.py`, `init_schema.sql` |
| 🟢 **Gemini Pro High** | ~59 (~70%) | Tất cả file có nội dung: configs, fixtures, mock_provider, registry, Silver models, Gold (fact_stock_price, dims, market_summary), 5 intermediate models, schema.yml, tests, docs, ADRs (kể cả ADR-004 mới), README, báo cáo |
| ⚪ **Gemini Flash High** | ~13 (~15%) | git commits, `__init__.py`, chạy commands (pytest/dbt run/dbt test/psql/dbt seed), screenshots |

### Token Claude dự kiến

| | Tasks | Đặc điểm |
|---|---|---|
| Opus | 7 tasks | Interface design + toán khó (RSI/EMA generalized) + orchestration + đánh giá |
| Sonnet | 5 tasks | Core ingestion modules cần idempotency/retry chặt |
| **Tổng Claude** | **12 tasks / ~84** | **~14% — giữ nguyên tinh thần, chỉ dồn chất lượng Opus lên** |

---

## Non-code Deliverables

- [ ] `docker-compose.yml`
- [ ] `requirements.txt`
- [ ] `.env.example` + `.gitignore`
- [ ] `STATUS.md` (cập nhật 4 lần)
- [ ] `README.md`
- [ ] `docs/CONTEXT.md`
- [ ] `docs/PROJECT_RULES.md`
- [ ] `docs/DATA_CONTRACTS.md`
- [ ] `docs/ADR/ADR-001-postgres.md`
- [ ] `docs/ADR/ADR-002-dbt.md`
- [ ] `docs/ADR/ADR-003-provider.md`
- [ ] `docs/ADR/ADR-004-macd-signal.md` **(NEW)**
- [ ] `docs/TEST_REPORTS.md` **(NEW — phải tồn tại từ đầu part 1)**
- [ ] `docs/POWERBI_QUICKSTART.md` **(NEW)**
- [ ] `dbt/seeds/dim_date.csv`
- [ ] `tests/fixtures/mock_prices.csv` + `mock_index.csv`
- [ ] Screenshots (≥6): Airflow DAG, retry, dbt test, dbt lineage, Power BI ×3-4, offline demo
- [ ] `reports/stock_dashboard.pbix`
- [ ] `reports/dashboard_backup.html` **(chỉ nếu dùng Plan B)**
- [ ] Báo cáo nháp


## Giai đoạn chốt sổ (Chuẩn bị nộp/bảo vệ)

- [ ] Lên danh sách chốt các thư viện vào `requirements.txt`.
- [ ] Tạo `Dockerfile` để nén cứng thư viện (`vnstock`, `requests`...) vào Local Image.
- [ ] Cập nhật `docker-compose.yml` (đổi `image` thành `build: .`).
- [ ] Xóa biến `_PIP_ADDITIONAL_REQUIREMENTS` và test chạy luồng E2E trong chế độ Offline (tắt mạng Internet).