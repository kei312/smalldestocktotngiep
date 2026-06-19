# SKILL: dbt Incremental Model trên PostgreSQL (dbt-postgres 1.10.x)

> **Đọc file này khi:** sắp làm task 2.3.4 (`fact_stock_indicators.sql`).
> Không cần đọc cho Silver models (Silver dùng `materialized='table'`).

---

## Tại sao cần đọc trước

`fact_stock_indicators` dùng `materialized='incremental'` — **phần duy nhất trong dự án**.
dbt-postgres 1.10.x có 3 gotcha ngầm mà nếu bỏ qua sẽ mất 1–2h debug giữa deadline.

---

## 1. Config Block Chuẩn

```sql
-- dbt/models/gold/fact_stock_indicators.sql
{{ config(
    materialized          = 'incremental',
    unique_key            = ['symbol', 'trade_date'],   -- PHẢI là list, không phải string
    incremental_strategy  = 'delete+insert'             -- xem giải thích bên dưới
) }}
```

**Tại sao `delete+insert` không phải `merge`?**

| Strategy | dbt-postgres 1.10.x | Hành vi |
|---|---|---|
| `append` | ✅ | Chỉ thêm mới, KHÔNG update. Không phù hợp — sẽ duplicate khi chạy lại |
| `delete+insert` | ✅ | Xóa rows khớp `unique_key`, insert lại. Idempotent. **Dùng cái này.** |
| `merge` | ⚠️ | Hỗ trợ từ 1.6 nhưng cần test; `delete+insert` ổn định hơn trên Postgres 17 |

---

## 2. is_incremental() + Lookback 60 Ngày (CRITICAL)

```sql
WITH source AS (
    SELECT * FROM {{ ref('fact_stock_price') }}

    {% if is_incremental() %}
    -- ⚠️ KHÔNG dùng: WHERE trade_date > (SELECT MAX(trade_date) FROM {{ this }})
    -- Nếu chỉ lấy ngày mới nhất, indicator sẽ tính thiếu warm-up → sai hoàn toàn.
    --
    -- ĐÚNG: lookback đủ để warm-up chỉ báo dài nhất (MACD Signal = 35 trading days)
    -- 60 calendar days ≈ 42 trading days — đủ margin an toàn
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
)
```

**Tại sao phải lookback 60 ngày?**

MACD Signal cần `EMA26 + EMA9(MACD)` = tối thiểu 34 trading days để có giá trị.
Nếu incremental chỉ lấy ngày hôm nay, EMA recursive sẽ không có đủ lịch sử → NULL toàn bộ.

`delete+insert` sẽ xóa 60 ngày cũ và insert lại → đúng giá trị, idempotent.

---

## 3. Bốn Gotcha Của dbt-postgres 1.10.x

### Gotcha 1 — unique_key phải là list

```sql
-- ❌ SAI: string → dbt silently dùng as-is, sẽ lỗi runtime
unique_key = 'symbol,trade_date'

-- ✅ ĐÚNG
unique_key = ['symbol', 'trade_date']
```

### Gotcha 2 — `this` không tồn tại lần đầu

```sql
-- is_incremental() = FALSE khi chạy lần đầu
-- → đoạn WHERE bị bỏ qua hoàn toàn — dbt xử lý đúng tự động
-- Không cần thêm IF EXISTS check
{% if is_incremental() %}
WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
{% endif %}
```

### Gotcha 3 — Intermediate models KHÔNG phải incremental

```sql
-- int_ema12.sql, int_ema26.sql, int_rsi14.sql dùng:
{{ config(materialized='table') }}   -- table, KHÔNG phải incremental

-- Lý do: mỗi lần fact_stock_indicators chạy lookback 60 ngày,
-- intermediate models cần rebuild toàn bộ để cung cấp đúng lịch sử.
-- Nếu intermediate là incremental, sẽ thiếu dữ liệu warm-up.
```

### Gotcha 4 — `dbt run` không chạy tests

```bash
# Chạy model
dbt run --select fact_stock_indicators

# Chạy tests (riêng biệt)
dbt test --select fact_stock_indicators

# Hoặc chạy cả hai
dbt build --select fact_stock_indicators
```

---

## 4. Full Rebuild Khi Cần

```bash
# Khi muốn rebuild hoàn toàn (bỏ qua incremental logic)
dbt run --select fact_stock_indicators --full-refresh

# Dùng khi:
# - Thêm column mới vào model
# - Schema thay đổi
# - Muốn reset toàn bộ data để test
# - Sau khi backfill thêm dữ liệu lịch sử vào Bronze/Silver
```

---

## 5. Testing Incremental — Quy trình Verify

```bash
# Bước 1: Đếm rows trước
psql -c "SELECT COUNT(*) FROM gold.fact_stock_indicators;"

# Bước 2: Chạy incremental run
dbt run --select fact_stock_indicators

# Bước 3: Đếm rows sau
psql -c "SELECT COUNT(*) FROM gold.fact_stock_indicators;"

# Expected: COUNT tăng thêm đúng số ngày mới × số mã
# Nếu COUNT tăng gấp đôi → đang dùng 'append' thay vì 'delete+insert'

# Bước 4: Verify không duplicate
psql -c "
    SELECT symbol, trade_date, COUNT(*)
    FROM gold.fact_stock_indicators
    GROUP BY symbol, trade_date
    HAVING COUNT(*) > 1
    LIMIT 5;
"
# Expected: 0 rows → OK
```

---

## 6. Schema.yml cho Incremental Model

```yaml
# dbt/models/gold/schema.yml — phần fact_stock_indicators
models:
  - name: fact_stock_indicators
    description: "OHLCV indicators: MA5/MA20/RSI14/MACD/Bollinger. Incremental daily."
    columns:
      - name: rsi_14
        tests:
          - dbt_utils.expression_is_true:
              expression: "rsi_14 >= 0 AND rsi_14 <= 100"
              where: "rsi_14 IS NOT NULL"   # bỏ qua warm-up period
        description: "RSI14 Wilder. NULL trong 14 ngày đầu mỗi mã."

      - name: ma20
        tests:
          - dbt_utils.expression_is_true:
              expression: "ma20 > 0"
              where: "ma20 IS NOT NULL"     # bỏ qua warm-up period

      - name: bb_upper
        tests:
          - dbt_utils.expression_is_true:
              expression: "bb_upper >= bb_lower"
              where: "bb_upper IS NOT NULL AND bb_lower IS NOT NULL"
```

> **Ghi chú:** Mọi test phải có `where: "... IS NOT NULL"` để pass trong warm-up period.
> Thiếu điều kiện này → test fail ngay lần đầu do NULL trong 14–34 ngày đầu.

---

## 7. Quick Reference — Xem Nhanh Khi Debug

```
Model không tăng row?
→ Kiểm tra unique_key có phải list không

Indicators NULL toàn bộ sau incremental run?
→ Lookback interval quá ngắn, không đủ warm-up

dbt test RSI range fail?
→ WHERE clause trong test thiếu "IS NOT NULL"

Schema drift sau thêm column?
→ dbt run --full-refresh để rebuild

int_ema12 chạy chậm?
→ Thêm index trên fact_stock_price(symbol, trade_date) nếu chưa có
```
