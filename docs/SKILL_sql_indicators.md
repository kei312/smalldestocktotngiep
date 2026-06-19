# SKILL: SQL Financial Indicators (RSI / EMA / MACD / Bollinger)

> **Đọc file này khi:** sắp làm task 2.3.2 / 2.3.3 / 2.3.4 / 2.3.8.
> Không cần đọc nếu task không liên quan đến indicators.
>
> **v2 (19/06):** MACD Signal nay dùng EMA9 chính xác (không còn SMA approximation).
> Macro `calculate_ema` đã được generalize để dùng lại được cho cả EMA12/26 (trên giá close)
> lẫn EMA9 (trên macd_line) — xem mục 3.

---

## Tại sao cần đọc trước

RSI Wilder và EMA recursive trong PostgreSQL **không thể dùng window function thông thường**.
Cả hai yêu cầu `WITH RECURSIVE` — mỗi hàng phụ thuộc vào hàng trước.

PostgreSQL không cho phép lồng `WITH RECURSIVE` bên trong subquery của một `WITH` khác.
→ **Giải pháp bắt buộc:** mỗi indicator recursive là một **intermediate model** riêng, materialized dưới dạng table.

---

## Cấu trúc file (bổ sung so với task3.md)

```
dbt/
├── macros/
│   ├── calculate_ema.sql       ← task 2.3.3 — generalized: nhận source_relation + value_column
│   └── calculate_rsi.sql       ← task 2.3.2 — macro sinh toàn bộ SELECT recursive
└── models/gold/
    ├── intermediate/
    │   ├── int_ema12.sql        ← gọi calculate_ema(12), materialized='table'
    │   ├── int_ema26.sql        ← gọi calculate_ema(26), materialized='table'
    │   ├── int_rsi14.sql        ← gọi calculate_rsi(14), materialized='table'
    │   ├── int_macd_line.sql    ← NEW — ema12 - ema26, materialized='table'
    │   └── int_macd_signal.sql  ← NEW — calculate_ema(9) trên int_macd_line, materialized='table'
    └── fact_stock_indicators.sql ← task 2.3.4 — JOIN tất cả intermediate + window function
```

---

## 1. MA5 & MA20 — Window Function (không cần recursive)

```sql
-- Trong fact_stock_indicators.sql hoặc CTE riêng
SELECT
    symbol,
    trade_date,
    close,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS rn,

    -- MA5: NULL nếu chưa đủ 5 ngày
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 5
        THEN AVG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )
        ELSE NULL
    END AS ma5,

    -- MA20: NULL nếu chưa đủ 20 ngày
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 20
        THEN AVG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )
        ELSE NULL
    END AS ma20

FROM {{ ref('fact_stock_price') }}
```

---

## 2. Bollinger Bands — Window Function (không cần recursive)

```sql
-- Thêm vào cùng CTE với MA
CASE
    WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 20
    THEN AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
         + 2.0 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ELSE NULL
END AS bb_upper,

CASE
    WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 20
    THEN AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
         - 2.0 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ELSE NULL
END AS bb_lower
```

> **Lưu ý:** `STDDEV_POP` (population) không phải `STDDEV_SAMP`. Bollinger Bands dùng population stddev.

---

## 3. Macro: `calculate_ema.sql` (task 2.3.3) — GENERALIZED

**File:** `dbt/macros/calculate_ema.sql`

**Công thức:** α = 2/(period+1) — Standard EMA, dùng cho MACD (không phải Wilder)
**Seed:** SMA của `period` rows đầu tiên

**Thay đổi so với bản đầu:** macro nay nhận thêm 2 tham số optional `source_relation` và
`value_column`, để dùng lại được cho cả EMA trên giá `close` (fact_stock_price) lẫn EMA trên
`macd_line` (int_macd_line) — tránh viết trùng logic recursive 2 lần.

```sql
{% macro calculate_ema(period, source_relation=none, value_column='close') %}

{% if source_relation is none %}
    {% set source_relation = ref('fact_stock_price') %}
{% endif %}

WITH RECURSIVE
_ema_base AS (
    SELECT
        symbol,
        trade_date,
        {{ value_column }}                                                AS val,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date)        AS rn
    FROM {{ source_relation }}
),

-- SMA seed: trung bình period rows đầu, tính TẠI row thứ period
_ema_seed AS (
    SELECT
        symbol,
        {{ period }}            AS rn,
        AVG(val)                AS ema_val
    FROM _ema_base
    WHERE rn <= {{ period }}
    GROUP BY symbol
),

-- Recursive: mỗi row = val*α + prev_ema*(1−α)
_ema_rec(symbol, rn, trade_date, ema_val) AS (

    -- Base case: seed tại row = period
    SELECT b.symbol, b.rn, b.trade_date, s.ema_val
    FROM _ema_base b
    JOIN _ema_seed s ON b.symbol = s.symbol AND b.rn = s.rn

    UNION ALL

    -- Recursive: α = 2/(period+1)
    SELECT
        b.symbol,
        b.rn,
        b.trade_date,
        b.val * (2.0 / ({{ period }} + 1.0))
            + e.ema_val * (1.0 - 2.0 / ({{ period }} + 1.0))
    FROM _ema_base b
    JOIN _ema_rec   e ON b.symbol = e.symbol AND b.rn = e.rn + 1
)

SELECT symbol, trade_date, ema_val AS ema_{{ period }}
FROM _ema_rec

{% endmacro %}
```

**Cách dùng cho EMA12/EMA26 (không đổi behavior so với bản cũ — dùng default fact_stock_price/close):**

```sql
-- dbt/models/gold/intermediate/int_ema12.sql
{{ config(materialized='table') }}
{{ calculate_ema(12) }}

-- dbt/models/gold/intermediate/int_ema26.sql
{{ config(materialized='table') }}
{{ calculate_ema(26) }}
```

**Cách dùng cho MACD Signal (EMA9 trên macd_line — xem mục 3b):**

```sql
{{ calculate_ema(9, source_relation=ref('int_macd_line'), value_column='macd_line') }}
```

> **Gotcha:** nếu gọi `calculate_ema()` mà quên truyền `value_column` khi source không phải
> `fact_stock_price`, macro sẽ mặc định tìm cột `close` trong bảng đó → lỗi "column close does
> not exist" (vì `int_macd_line` không có cột `close`). Luôn truyền rõ `value_column` khi
> `source_relation` khác mặc định.

---

## 3b. `int_macd_line.sql` và `int_macd_signal.sql` (NEW — thay cho SMA approximation)

**Tại sao cần 2 model này:** MACD Signal đúng định nghĩa là EMA9 của đường MACD line — không
phải SMA(9). Dùng SMA xấp xỉ EMA cho ra sai số 2–8% tuỳ giai đoạn biến động, gần như chắc chắn
fail ngưỡng < 0.5% của test G-03 (task 2.3.8). Vì macro EMA đã generalize ở mục 3, chi phí làm
đúng chỉ là 2 model nhỏ thêm — không phải viết lại recursive logic.

```sql
-- dbt/models/gold/intermediate/int_macd_line.sql
{{ config(materialized='table') }}

SELECT
    e12.symbol,
    e12.trade_date,
    e12.ema_12 - e26.ema_26 AS macd_line
FROM {{ ref('int_ema12') }} e12
JOIN {{ ref('int_ema26') }} e26
    ON e12.symbol = e26.symbol AND e12.trade_date = e26.trade_date
WHERE e12.ema_12 IS NOT NULL AND e26.ema_26 IS NOT NULL
```

```sql
-- dbt/models/gold/intermediate/int_macd_signal.sql
{{ config(materialized='table') }}

WITH signal AS (
    {{ calculate_ema(9, source_relation=ref('int_macd_line'), value_column='macd_line') }}
)

SELECT
    symbol,
    trade_date,
    ema_9 AS macd_signal
FROM signal
```

**Vì sao warm-up vẫn đúng:** `int_macd_line` chỉ chứa các dòng từ rn=26 trở đi (do filter
`IS NOT NULL`). Macro `calculate_ema(9, ...)` tự đánh số lại `ROW_NUMBER()` trên chính
`int_macd_line`, nên seed SMA(9) được tính trên 9 dòng macd_line đầu tiên — tương đương trading
day thứ 26 đến 34 của chuỗi gốc. Kết quả: `macd_signal` có giá trị đầu tiên tại rn gốc = 34,
khớp đúng bảng Warm-up Reference ở mục 5 bên dưới (không cần chỉnh gì thêm).

**Lưu ý materialization:** cả 2 model này **PHẢI** là `materialized='table'`, không phải
`'incremental'` — giống int_ema12/int_ema26/int_rsi14 (xem Gotcha 3 trong
`SKILL_dbt_incremental.md`). Lý do: mỗi lần `fact_stock_indicators` chạy lookback 60 ngày, các
intermediate model cần có đủ lịch sử đầy đủ để cung cấp đúng warm-up.

---

## 4. Macro: `calculate_rsi.sql` (task 2.3.2)

**File:** `dbt/macros/calculate_rsi.sql`

**Công thức:** Wilder Smoothing — α = 1/period (KHÁC với EMA standard)
**Seed:** SMA của `period` gains/losses đầu tiên

```sql
{% macro calculate_rsi(period) %}

WITH RECURSIVE
_rsi_base AS (
    SELECT
        symbol,
        trade_date,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date)  AS daily_change,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date)          AS rn
    FROM {{ ref('fact_stock_price') }}
),

_rsi_gl AS (
    SELECT
        symbol,
        trade_date,
        rn,
        GREATEST(COALESCE(daily_change, 0), 0)       AS gain,
        ABS(LEAST(COALESCE(daily_change, 0),    0))  AS loss
    FROM _rsi_base
),

-- SMA seed tại row (period+1): dùng period changes đầu tiên (rows 2..period+1)
-- rn=1 không có change (LAG null), nên changes bắt đầu từ rn=2
_rsi_seed AS (
    SELECT
        symbol,
        {{ period }} + 1          AS rn,
        AVG(gain)                 AS avg_gain,
        AVG(loss)                 AS avg_loss
    FROM _rsi_gl
    WHERE rn BETWEEN 2 AND {{ period }} + 1   -- period changes: rows 2 → period+1
    GROUP BY symbol
),

-- Recursive: Wilder smoothing = (prev * (period-1) + current) / period
_rsi_rec(symbol, rn, trade_date, avg_gain, avg_loss) AS (

    -- Base case: seed tại row = period+1 (row 15 khi period=14)
    SELECT gl.symbol, gl.rn, gl.trade_date, s.avg_gain, s.avg_loss
    FROM _rsi_gl gl
    JOIN _rsi_seed s ON gl.symbol = s.symbol AND gl.rn = s.rn

    UNION ALL

    -- Recursive: Wilder — α = 1/period
    SELECT
        gl.symbol,
        gl.rn,
        gl.trade_date,
        (r.avg_gain * ({{ period }} - 1) + gl.gain) / {{ period }},
        (r.avg_loss * ({{ period }} - 1) + gl.loss) / {{ period }}
    FROM _rsi_gl gl
    JOIN _rsi_rec r ON gl.symbol = r.symbol AND gl.rn = r.rn + 1
)

SELECT
    symbol,
    trade_date,
    CASE
        WHEN avg_loss  = 0 AND avg_gain  = 0 THEN NULL        -- giá không đổi
        WHEN avg_loss  = 0                   THEN 100.0       -- chỉ toàn tăng
        ELSE ROUND(100.0 - (100.0 / (1.0 + avg_gain / avg_loss)), 4)
    END AS rsi_{{ period }}
FROM _rsi_rec

{% endmacro %}
```

**Intermediate model:**

```sql
-- dbt/models/gold/intermediate/int_rsi14.sql
{{ config(materialized='table') }}
{{ calculate_rsi(14) }}
```

---

## 5. Warm-up Reference Table

| Indicator | Cần tối thiểu | Row đầu tiên có giá trị | NULL trước đó |
|---|---|---|---|
| MA5 | 5 rows | rn = 5 | rn 1–4 |
| MA20 | 20 rows | rn = 20 | rn 1–19 |
| Bollinger Bands | 20 rows | rn = 20 | rn 1–19 |
| RSI14 (Wilder) | 15 rows | rn = 15 | rn 1–14 |
| EMA12 | 12 rows | rn = 12 | rn 1–11 |
| EMA26 | 26 rows | rn = 26 | rn 1–25 |
| MACD line | 26 rows | rn = 26 | rn 1–25 |
| MACD Signal (EMA9 trên macd_line) | 26 + 9 − 1 = 34 rows | rn = 34 | rn 1–33 |

> **Kiểm tra bảo vệ:** "Tại sao RSI cần 15 rows mà không phải 14?"
> → rn=1 không có change (LAG = NULL), nên 14 changes đầu tiên là rn=2..15. Seed tại rn=15.
>
> **Kiểm tra bảo vệ:** "Tại sao MACD Signal lại EMA chứ không phải SMA?"
> → Định nghĩa chuẩn của MACD Signal (Appel, 1979) là EMA9 của MACD line, không phải SMA. Dự án
> dùng `int_macd_signal.sql` với macro `calculate_ema(9, ...)` để tính đúng, xác minh qua G-03
> (task 2.3.8) với reference Python implementation.

---

## 6. `fact_stock_indicators.sql` — Cấu trúc (task 2.3.4)

```sql
-- dbt/models/gold/fact_stock_indicators.sql
{{ config(
    materialized = 'incremental',
    unique_key   = ['symbol', 'trade_date']
) }}
-- ⚠️ Xem SKILL_dbt_incremental.md để biết strategy và lookback đúng

WITH
source AS (
    SELECT * FROM {{ ref('fact_stock_price') }}
    {% if is_incremental() %}
    -- Lookback 60 calendar days để đảm bảo warm-up (34 trading days cho MACD Signal)
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
),

ma_bb AS (
    SELECT
        symbol,
        trade_date,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS rn,
        -- MA5
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 5
             THEN AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS ma5,
        -- MA20
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 20
             THEN AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS ma20,
        -- Bollinger upper
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 20
             THEN AVG(close)     OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                + 2.0 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS bb_upper,
        -- Bollinger lower
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 20
             THEN AVG(close)     OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                - 2.0 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS bb_lower
    FROM source
),

-- RSI từ intermediate model (không inline — tránh nested WITH RECURSIVE)
rsi AS (
    SELECT symbol, trade_date, rsi_14
    FROM {{ ref('int_rsi14') }}
    {% if is_incremental() %}
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
),

-- MACD line + signal từ intermediate models (EMA9 thật, không còn SMA approximation)
macd_full AS (
    SELECT
        ml.symbol,
        ml.trade_date,
        ml.macd_line,
        ms.macd_signal,
        ml.macd_line - ms.macd_signal AS macd_histogram
    FROM {{ ref('int_macd_line') }} ml
    LEFT JOIN {{ ref('int_macd_signal') }} ms
        ON ml.symbol = ms.symbol AND ml.trade_date = ms.trade_date
    {% if is_incremental() %}
    WHERE ml.trade_date > (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
)

-- Final JOIN
SELECT
    mb.symbol,
    mb.trade_date,
    mb.ma5,
    mb.ma20,
    mb.bb_upper,
    mb.bb_lower,
    r.rsi_14,
    mf.macd_line,
    mf.macd_signal,
    mf.macd_histogram
FROM ma_bb mb
LEFT JOIN rsi       r  ON mb.symbol = r.symbol  AND mb.trade_date = r.trade_date
LEFT JOIN macd_full mf ON mb.symbol = mf.symbol AND mb.trade_date = mf.trade_date
```

> **Khác so với bản trước:** không còn `ema12`/`ema26`/`macd_line` CTE tính trực tiếp trong file
> này — toàn bộ logic đó đã chuyển vào `int_macd_line.sql`/`int_macd_signal.sql` để tránh trùng
> lặp recursive logic. `fact_stock_indicators.sql` giờ chỉ JOIN kết quả có sẵn.

---

## 7. G-03 Verification Script (task 2.3.8)

**Không đổi so với bản trước** — script Python `verify_macd_g03.py` (xem nguyên file gốc) vốn dĩ
**đã** implement EMA9 chuẩn cho phần reference (hàm `macd_reference`), nên không cần sửa gì ở đây.
Phần SQL nay đã khớp đúng cách tính của reference này → kỳ vọng pass < 0.5% thật sự, không phải
nới lỏng threshold.

```python
# scripts/verify_macd_g03.py
"""
Task 2.3.8 — G-03: MACD verification
So sánh SQL output (từ PostgreSQL) với Python reference implementation.
Acceptance: max error < 0.5% cho mọi giá trị MACD trên 3 mã.
"""
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ── Python Reference Implementation ──────────────────────────────────────────

def ema_reference(closes: list, period: int) -> list:
    """
    EMA với SMA seed. Alpha = 2/(period+1).
    Seed = SMA của period rows đầu (index 0..period-1).
    First valid value tại index period-1.
    """
    n = len(closes)
    result = [None] * n
    if n < period:
        return result
    alpha = 2.0 / (period + 1.0)
    result[period - 1] = sum(closes[:period]) / period  # SMA seed
    for i in range(period, n):
        result[i] = closes[i] * alpha + result[i - 1] * (1.0 - alpha)
    return result


def macd_reference(closes: list, fast=12, slow=26, signal=9) -> dict:
    """
    MACD reference: line = EMA(fast) - EMA(slow), signal = EMA(signal) of line.
    """
    n = len(closes)
    ema_f = ema_reference(closes, fast)
    ema_s = ema_reference(closes, slow)

    macd_line = [None] * n
    for i in range(n):
        if ema_f[i] is not None and ema_s[i] is not None:
            macd_line[i] = ema_f[i] - ema_s[i]

    # Signal = EMA(signal) applied to macd_line values starting from first non-None
    signal_line = [None] * n
    start = next((i for i, v in enumerate(macd_line) if v is not None), None)
    if start is not None:
        macd_slice = [v for v in macd_line[start:] if v is not None]
        sig_slice = ema_reference(macd_slice, signal)
        for j, val in enumerate(sig_slice):
            signal_line[start + j] = val

    histogram = [
        (macd_line[i] - signal_line[i])
        if (macd_line[i] is not None and signal_line[i] is not None)
        else None
        for i in range(n)
    ]
    return {"macd_line": macd_line, "macd_signal": signal_line, "macd_histogram": histogram}


# ── Main Verification ─────────────────────────────────────────────────────────

SYMBOLS_TO_TEST = ["VNM", "VCB", "HPG"]  # 3 mã cho G-03
DATE_RANGE = ("2021-01-01", "2024-12-31")

def fetch_sql_indicators(conn, symbol):
    query = """
        SELECT trade_date, macd_line, macd_signal, macd_histogram
        FROM gold.fact_stock_indicators
        WHERE symbol = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """
    return pd.read_sql(query, conn, params=(symbol, *DATE_RANGE))


def fetch_closes(conn, symbol):
    query = """
        SELECT trade_date, close
        FROM gold.fact_stock_price
        WHERE symbol = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """
    return pd.read_sql(query, conn, params=(symbol, *DATE_RANGE))


def pct_error(ref_val, sql_val):
    if ref_val is None or sql_val is None:
        return None
    if abs(ref_val) < 1e-10:
        return 0.0
    return abs(ref_val - sql_val) / abs(ref_val) * 100.0


def run_verification():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )

    print("=== G-03 MACD Verification (task 2.3.8) ===\n")
    all_pass = True

    for symbol in SYMBOLS_TO_TEST:
        closes_df = fetch_closes(conn, symbol)
        sql_df    = fetch_sql_indicators(conn, symbol)
        closes    = closes_df["close"].tolist()

        ref = macd_reference(closes)
        ref_df = pd.DataFrame({
            "trade_date":     closes_df["trade_date"],
            "ref_macd_line":  ref["macd_line"],
            "ref_macd_signal":ref["macd_signal"],
        })

        merged = sql_df.merge(ref_df, on="trade_date")
        valid  = merged.dropna(subset=["macd_line", "ref_macd_line"])

        errors_line   = [pct_error(r, s) for r, s in zip(valid["ref_macd_line"],   valid["macd_line"])]
        errors_signal = [pct_error(r, s) for r, s in zip(valid["ref_macd_signal"], valid["macd_signal"])]

        max_line   = max((e for e in errors_line   if e is not None), default=0)
        max_signal = max((e for e in errors_signal if e is not None), default=0)

        status_line   = "✅ PASS" if max_line   < 0.5 else "❌ FAIL"
        status_signal = "✅ PASS" if max_signal < 0.5 else "❌ FAIL"

        print(f"[{symbol}]  MACD line   max error: {max_line:.4f}%  {status_line}")
        print(f"[{symbol}]  MACD signal max error: {max_signal:.4f}%  {status_signal}")
        print()

        if max_line >= 0.5 or max_signal >= 0.5:
            all_pass = False

    conn.close()
    print("=== RESULT:", "ALL PASS ✅" if all_pass else "FAILED ❌", "===")
    return all_pass


if __name__ == "__main__":
    ok = run_verification()
    exit(0 if ok else 1)
```

**Chạy:** `python scripts/verify_macd_g03.py`
**Ghi kết quả vào:** `docs/TEST_REPORTS.md`

---

## 8. Các Lỗi Phổ Biến — Không Được Mắc

| Lỗi | Đúng |
|---|---|
| RSI seed tại `rn = period` (14) | Seed tại `rn = period + 1` (15) vì rn=1 không có change |
| RSI alpha = 2/(14+1) | RSI alpha = 1/14 (Wilder), EMA alpha = 2/(period+1) (Standard) |
| MACD Signal dùng SMA window thay EMA9 | **Đã sửa** — dùng `int_macd_signal.sql` qua macro generalized |
| Gọi `calculate_ema()` cho macd_line mà không truyền `value_column` | Macro mặc định tìm cột `close` → lỗi. Luôn truyền `value_column='macd_line'` khi source khác mặc định |
| Bollinger dùng STDDEV_SAMP | Dùng STDDEV_POP |
| Lồng WITH RECURSIVE trong CTE khác | Tạo intermediate model riêng, materialized |
| MA20 trả về 0 thay NULL khi warm-up | CASE WHEN rn >= 20 THEN ... ELSE NULL END |
| `int_macd_line`/`int_macd_signal` để `materialized='incremental'` | Phải là `'table'` — full rebuild mỗi lần, giống int_ema12/26/rsi14 |
| Lỗi `function round(double precision, integer) does not exist` | PostgreSQL không cho `ROUND(float, decimals)`. Phải cast sang NUMERIC: `ROUND(expr::NUMERIC, 4)` |