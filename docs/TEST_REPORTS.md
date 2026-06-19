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

_(chưa có entry)_