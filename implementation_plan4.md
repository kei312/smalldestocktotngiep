# Kế hoạch Thi công: Vietnam Stock Data Engineering Pipeline

> **Phiên bản:** 3.1 — Full Build, 4 ngày
> **Ngày bắt đầu:** 17/06/2026 | **Deadline:** 20/06/2026
>
> **AI:** File này chứa scope overview và checklist bảo vệ.
> Chi tiết từng task, model routing → xem `task3.md`.
> Stack, kiến trúc, schema → xem `docs/CONTEXT.md`.

---

## Scope đầy đủ

| Nhóm | Nội dung |
|---|---|
| **CORE** ✅ | Daily OHLCV (pilot VN30 → full); VNINDEX/VN30; MA5/MA20/RSI14/MACD/Bollinger; fact_market_summary; dim_stock/dim_date; Airflow daily + retry + alerting; Power BI 4 dashboards; idempotency; 17 test cases; lineage; STATUS.md |
| **MỞ RỘNG** 🔵 | Fundamentals quý (PE/PB/ROE/ROA/EPS); mở rộng universe đầy đủ |
| **STRETCH** 🟡 | Foreign trading (chỉ nếu xác minh được nguồn) |
| **LOẠI BỎ** ❌ | Intraday/tick; streaming; đặt lệnh |

---

## Lộ trình 4 ngày — High Level

| Ngày | Gate check |
|---|---|
| **Ngày 1** (17/06) | Bronze có data VN30 thật. Provider + Ingestion tests pass. |
| **Ngày 2** (18/06) | `dbt run && dbt test` pass Silver + Gold. Indicators tính đúng. |
| **Ngày 3** (19/06) | Airflow DAG chạy E2E thành công. Ít nhất 2 dashboards hiển thị đúng. |
| **Ngày 4** (20/06) | Sản phẩm E2E. Báo cáo nháp đủ sections + screenshots. Git log đều 4 ngày. |

> Chi tiết từng buổi, từng task → xem `task3.md`.

---

## Checklist bảo vệ

- [ ] Rút mạng → MockProvider, pipeline không crash
- [ ] Backfill 2 lần → idempotency (số dòng không tăng)
- [ ] dbt test fail có chủ đích → error rõ ràng
- [ ] Lineage graph từ dbt docs
- [ ] STATUS.md + git log trải đều
- [ ] Airflow UI show retry thành công
- [ ] Power BI khớp Gold
- [ ] "Vì sao không Big Data?" → ~500k dòng, PostgreSQL đủ
- [ ] "Nguồn chết thì sao?" → Đổi PROVIDER, thêm provider qua interface
- [ ] "Fundamentals?" → Đã có slot, implement nếu có thời gian

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
- [ ] `dbt/seeds/dim_date.csv`
- [ ] `tests/fixtures/mock_prices.csv` + `mock_index.csv`
- [ ] Screenshots (≥6): Airflow DAG, retry, dbt test, dbt lineage, Power BI ×3-4, offline demo
- [ ] `reports/stock_dashboard.pbix`
- [ ] Báo cáo nháp

---

## Giai đoạn chốt sổ (Chuẩn bị nộp/bảo vệ)

- [ ] Lên danh sách chốt các thư viện vào `requirements.txt`.
- [ ] Tạo `Dockerfile` để nén cứng thư viện (`vnstock`, `requests`...) vào Local Image.
- [ ] Cập nhật `docker-compose.yml` (đổi `image` thành `build: .`).
- [ ] Xóa biến `_PIP_ADDITIONAL_REQUIREMENTS` và test chạy luồng E2E trong chế độ Offline (tắt mạng Internet).
