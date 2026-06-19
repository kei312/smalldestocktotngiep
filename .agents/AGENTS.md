# AGENTS.md — Hướng dẫn cho AI Agent

> File này được tự động nạp mỗi session. Không cần user nhắc lại context.

---

## 0. Pre-flight Check (BẮT BUỘC — đầu mỗi session làm việc với code/infra)

Trước khi bắt đầu bất kỳ task nào liên quan đến Docker, dbt, hoặc PostgreSQL:

```bash
# 1. Docker đang chạy?
docker ps --filter "name=postgres" --filter "name=airflow" --format "{{.Names}} {{.Status}}"
# Expected: thấy postgres và airflow-* với status "Up"

# 2. PostgreSQL accessible?
psql $DATABASE_URL -c "SELECT 1;" 2>&1 | head -1
# Expected: "?column? \n----------\n 1"

# 3. dbt connection OK? (nếu làm task dbt)
dbt debug --profiles-dir dbt/ 2>&1 | tail -5
# Expected: "All checks passed!"
```

**Nếu Docker không chạy → `docker compose up -d` trước, đợi 30s, check lại.**
**Nếu vnstock fail (task 1.1.8 hoặc 1.3.6) → switch `PROVIDER=mock` trong `.env`, ghi note vào TEST_REPORTS.md, tiếp tục với mock data.**

---

## 1. Đọc tài liệu theo ngữ cảnh (Token-Optimized Reading Protocol)

**Đọc có điều kiện — không đọc hết cùng một lúc:**

| Tình huống | Đọc file |
|---|---|
| Session mới, chưa biết dự án | `docs/CONTEXT.md` |
| Sắp sinh code bất kỳ | `docs/PROJECT_RULES.md` |
| Bắt đầu task cụ thể (VD: "làm 1.3.4") | Chỉ section đó trong `task3.md` |
| Trước task 2.3.2 / 2.3.3 / 2.3.4 / 2.3.8 | `docs/SKILL_sql_indicators.md` |
| Trước task 2.3.4 (incremental config) | `docs/SKILL_dbt_incremental.md` |
| Ghi kết quả pytest / dbt test / SQL verify | `docs/TEST_REPORTS.md` |
| Cần review scope / checklist bảo vệ | `implementation_plan4.md` |
| Câu hỏi về data contract giữa các layer | `docs/PROJECT_RULES.md` Section 5 |

**Quy tắc tiết kiệm token:**
- Đọc file 1 lần rồi cache trong session — KHÔNG re-read nếu không có thay đổi
- KHÔNG đọc `task3.md` toàn bộ — chỉ đọc section ngày/buổi đang làm
- KHÔNG đọc cả `CONTEXT.md` + `PROJECT_RULES.md` nếu chỉ hỏi về 1 topic
- Nếu user paste code snippet → không cần đọc lại `CONTEXT.md`

---

## 2. Kiểm soát chất lượng output

### Trước khi sinh code, kiểm tra:
- Naming có đúng `PROJECT_RULES.md` Section 1 không?
- Logging có đúng Section 3 không? (Không nuốt lỗi im lặng)
- Error handling có đúng hierarchy Section 4 không?
- Schema có khớp `CONTEXT.md` Bronze/Silver/Gold contract không?

### Bất biến — không được vi phạm:
- Mọi exception phải log — không `except: pass`
- Không tạo schema/column khác với `CONTEXT.md`
- Không thay đổi provider interface ngoài `providers/base.py`
- `ingestion/config.py` là nơi duy nhất đọc `.env` — không scatter
- **Bắt buộc lưu bằng chứng test:** Mọi thao tác kiểm duyệt Acceptance (chạy `pytest`, `dbt test`, hoặc SQL verification) nếu pass thì phải lưu output log vào file `docs/TEST_REPORTS.md` để user theo dõi.

### Ngôn ngữ output:
- **Giải thích, trả lời:** Tiếng Việt
- **Code, comments trong code, tên biến:** Tiếng Anh

---

## 2.5 Fail Protocol (BẮT BUỘC — khi gate/test thất bại)

**Thứ tự xử lý khi pytest / dbt test / SQL verify FAIL:**

```
Bước 1: Đọc error message + stack trace đầy đủ
Bước 2: Tra bảng "Quick Reference — Xem Nhanh Khi Debug"
         trong SKILL file tương ứng (SKILL_sql_indicators.md hoặc SKILL_dbt_incremental.md)
Bước 3:
  - Nếu lỗi KHỚP với 1 gotcha đã liệt kê sẵn
    → Tự sửa ĐÚNG THEO hướng dẫn trong SKILL file — được phép sửa 1 lần
    → Ghi lại lỗi + cách sửa vào docs/TEST_REPORTS.md (mục Fail Log)
    → Chạy lại test
    → Nếu PASS → ghi kết quả pass vào TEST_REPORTS.md, tiếp tục
    → Nếu vẫn FAIL → chuyển sang Bước 4

  - Nếu lỗi KHÔNG KHỚP bất kỳ gotcha nào đã biết
    → Chuyển thẳng sang Bước 4 (không tự đoán mò)

Bước 4: DỪNG HOÀN TOÀN
         Ghi lỗi đầy đủ vào docs/TEST_REPORTS.md (mục Fail Log)
         Báo user: paste chính xác error message + bước đang làm
         KHÔNG tự ý tiếp tục task tiếp theo khi foundation chưa pass
```

> **Lý do:** Code tài chính — sai công thức mà không phát hiện nguy hiểm hơn là dừng lại hỏi.

---

## 3. Quy trình kiểm tra Version (BẮT BUỘC trước khi cài)

```
Bước 1: CHECK — chạy lệnh kiểm tra trước
  python3 --version | pip show <package> | docker images | <tool> --version

Bước 2: SO SÁNH với stack trong docs/CONTEXT.md
  - Nếu ĐÃ CÀI & version đủ mới → SKIP, không cài lại, thông báo cho user
  - Nếu CHƯA CÀI → cài bản ổn định mới nhất trong supported range
  - Nếu VERSION CŨ HƠN YÊU CẦU → hỏi user trước khi upgrade

Bước 3: Không bao giờ cài lại nếu không cần thiết
```

**Môi trường hiện tại (đã xác nhận):**
- Python: `3.12.3` — có sẵn trên WSL
- Docker images đã có: `apache/airflow:3.2.2`, `postgres:17`
- dbt, Airflow (host): chưa cài (dùng Docker)

---

## 4. Model Routing — Phân bổ AI model theo task

> Chi tiết phân bổ từng task xem header `task3.md`. Bảng này là quick reference.

| Model | Ký hiệu | Dùng cho |
|---|---|---|
| Claude Opus 4.6 | 🔴 | Architecture decision, RSI/EMA recursive SQL, DAG orchestration, bug khó |
| Claude Sonnet 4.6 | 🟠 | Core modules: providers, ingestion (vnstock_provider, fetch_prices, utils) |
| Gemini 3.1 Pro High | 🟢 | Silver models, tests, schema.yml, Gold configs, docs, báo cáo |
| Gemini 3.5 Flash High | ⚪ | Boilerplate: git commit, chạy command, `__init__.py`, fixtures nhỏ |

**Nguyên tắc:** Context dài → dùng Pro High thay Flash. Claude chỉ khi cần code chất lượng cao hoặc logic toán khó.

---

## 5. Làm việc trong session

### Khi user nói "bắt đầu task [X.Y.Z]":
1. Đọc chỉ section `X.Y` trong `task3.md`
2. Xác nhận acceptance criteria
3. Check version nếu task liên quan đến cài đặt
4. Sinh code / chạy lệnh
5. Báo kết quả — tick `[x]` trong `task3.md` nếu pass gate

### Khi user paste lỗi / debug:
1. Không cần đọc lại toàn bộ context
2. Focus vào error message + stack trace
3. Đối chiếu với PROJECT_RULES.md Section 4 (error hierarchy)
4. Tra Quick Reference trong SKILL file liên quan trước khi tự sửa

### Anti-hallucination rules:
- KHÔNG assume version nếu chưa chạy check command
- KHÔNG tạo file mới ngoài module layout trong `CONTEXT.md`
- KHÔNG đổi naming convention dù user không nhắc
- KHÔNG bịa data contract — luôn đối chiếu `PROJECT_RULES.md` Section 5

---

## 6. Cấu trúc tài liệu dự án (MECE)

| File | Nội dung | Đọc khi |
|---|---|---|
| `.agents/AGENTS.md` | Hướng dẫn AI, routing, token rules | Tự động — mỗi session |
| `docs/CONTEXT.md` | Stack, kiến trúc, scope, schema | Session mới hoặc câu hỏi kiến trúc |
| `docs/PROJECT_RULES.md` | Naming, logging, error, data contracts | Trước khi sinh code |
| `docs/SKILL_sql_indicators.md` | Công thức RSI/EMA/MACD, intermediate models | Trước task 2.3.2–2.3.8 |
| `docs/SKILL_dbt_incremental.md` | dbt incremental gotchas, lookback config | Trước task 2.3.4 |
| `docs/TEST_REPORTS.md` | Log kết quả test pass/fail | Sau mỗi lần chạy test |
| `task3.md` | Step-by-step tasks với model routing | Khi thực hiện task cụ thể |
| `implementation_plan4.md` | Scope overview, defense checklist | Cần review tổng thể |
