# TEST_REPORTS.md

> Ghi log MỌI lần chạy `pytest` / `dbt test` / SQL verification liên quan đến acceptance criteria của task3.md.
> Mục đích: bằng chứng cho hội đồng + lịch sử debug. Cập nhật ngay sau khi chạy, không gom cuối ngày.
> Quy tắc ghi: xem AGENTS.md Section 2.5 (Fail Protocol).

---

## Template cho mỗi entry

```
### [Task ID] — [Tên task]
- Lệnh:      `<command>`
- Kết quả:   <tóm tắt output, vd "5 passed in 1.2s" hoặc "FAILED: rsi_14 out of range 3 rows">
- Thời gian: <YYYY-MM-DD HH:MM>
- Trạng thái: ✅ PASS | ❌ FAIL | 🔧 FAIL → FIXED (lần 2 pass)
```

---

## Ngày 1 — Foundation + Provider + Bronze

### [1.2.9] — Pytest Provider Layer
- Lệnh:      `pytest tests/test_providers.py -v`
- Kết quả:   `5 passed in 6.65s`
- Thời gian: 2026-06-19 16:04
- Trạng thái: ✅ PASS

### [1.3.6] & [1.3.7] — Ingestion E2E Test
- Lệnh:      `python -m ingestion.fetch_prices ...` & `SELECT COUNT(*) ...`
- Kết quả:   Upserted 40 rows (VCB=8, VNM=8, FPT=8).
- Thời gian: 2026-06-19 16:50
- Trạng thái: 🔧 FAIL → FIXED (lần 2 pass)

### [1.3.10] — Pytest Ingestion Layer
- Lệnh:      `pytest tests/test_ingestion.py -v`
- Kết quả:   `4 passed in 0.51s`
- Thời gian: 2026-06-19 16:51
- Trạng thái: ✅ PASS

## Ngày 2 — Backfill + Silver + Gold Indicators

_(chưa có entry)_

## Ngày 3 — Airflow + Power BI

_(chưa có entry)_

## Ngày 4 — Hoàn thiện + Docs + Báo cáo

_(chưa có entry)_

---

## Fail Log (theo AGENTS.md Section 2.5)

> Khi gate/test fail: tra bảng "Quick Reference" trong SKILL file liên quan, thử fix **đúng 1 lần** theo
> hướng dẫn đã có sẵn, ghi cả lần fail gốc và kết quả lần fix vào đây. Nếu lần fix vẫn fail hoặc lỗi không
> khớp gotcha nào đã biết → DỪNG, escalate cho user. Không tự ý sửa lần 2, không tự ý sang task kế tiếp.

```
### [Task ID] — Fail attempt
- Lỗi gốc:        <error message>
- Gotcha tra cứu:  <tên gotcha trong SKILL_xxx.md mục Y, hoặc "không khớp gotcha nào">
- Hành động fix:   <mô tả ngắn>
- Kết quả sau fix: ✅ PASS / ❌ vẫn FAIL → đã dừng, báo user
```

### [1.3.6] — Fail attempt
- Lỗi gốc:        `psycopg2.ProgrammingError: can't adapt type 'numpy.int64'`
- Gotcha tra cứu:  không khớp gotcha nào
- Hành động fix:   Đăng ký `register_adapter(np.int64, AsIs)` vào `db.py`
- Kết quả sau fix: 🔧 FAIL → FIXED (đã upsert thành công 40 rows)