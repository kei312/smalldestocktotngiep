# DATA ANALYTICS GUIDE

# Data Catalog Raw (Gold Layer)

## 1. Column Metadata

| table_name | column_name | data_type | character_maximum_length | is_nullable |
| :--- | :--- | :--- | :--- | :--- |
| dim_stock | symbol | character varying | 20 | YES |
| dim_stock | exchange | text | | YES |
| dim_stock | default_exchange | text | | YES |
| fact_market_summary | trade_date | date | | YES |
| fact_market_summary | gainers | bigint | | YES |
| fact_market_summary | losers | bigint | | YES |
| fact_market_summary | unchanged | bigint | | YES |
| fact_market_summary | total_symbols | bigint | | YES |
| fact_market_summary | total_volume | numeric | | YES |
| fact_market_summary | vnindex_close | double precision | | YES |
| fact_market_summary | vn30_close | double precision | | YES |
| fact_stock_indicators | symbol | character varying | 20 | YES |
| fact_stock_indicators | trade_date | date | | YES |
| fact_stock_indicators | close_price | double precision | | YES |
| fact_stock_indicators | ma50 | double precision | | YES |
| fact_stock_indicators | ma200 | double precision | | YES |
| fact_stock_indicators | bb_upper | double precision | | YES |
| fact_stock_indicators | bb_lower | double precision | | YES |
| fact_stock_indicators | rsi_14 | numeric | | YES |
| fact_stock_indicators | macd_line | double precision | | YES |
| fact_stock_indicators | macd_signal | double precision | | YES |
| fact_stock_indicators | macd_histogram | double precision | | YES |
| fact_stock_price | symbol | character varying | 20 | YES |
| fact_stock_price | trade_date | date | | YES |
| fact_stock_price | row_num | bigint | | YES |
| fact_stock_price | open_price | double precision | | YES |
| fact_stock_price | high_price | double precision | | YES |
| fact_stock_price | low_price | double precision | | YES |
| fact_stock_price | close_price | double precision | | YES |
| fact_stock_price | volume | bigint | | YES |
| fact_stock_price | source | text | | YES |
| fact_stock_price | silver_loaded_at | timestamp with time zone | | YES |
| fact_stock_price | gold_loaded_at | timestamp with time zone | | YES |
| fact_stock_price | gain | double precision | | YES |
| fact_stock_price | loss | double precision | | YES |

## 2. Data Example: `fact_stock_indicators` (VNM)

| symbol | trade_date | close_price | ma50 | ma200 | bb_upper | bb_lower | rsi_14 | macd_line | macd_signal | macd_histogram |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| VNM | 2026-06-18 | 37080.5979 | 35721.94996 | 35229.78162 | 36971.59919767186 | 33487.964042328145 | 60.2125 | 119.80882296048367 | -86.00953709484509 | 205.81836005532875 |
| VNM | 2026-06-17 | 36115.3676 | 35431.05662 | 35199.875745 | 36830.5497451204 | 33569.201744879596 | 55.2227 | -25.278674256333034 | -137.46412710867727 | 112.18545285234424 |
| VNM | 2026-06-16 | 35793.4218 | 35177.801120000004 | 35198.66631500001 | 36826.6562028511 | 33570.676427148916 | 53.4131 | -111.81739580570866 | -165.5104903217633 | 53.69309451605466 |
| VNM | 2026-06-15 | 34681.8576 | 35140.92794 | 35210.31413500001 | 36858.36843132309 | 33562.259838676924 | 46.4787 | -187.86590228064597 | -178.93376395077698 | -8.932138329868991 |
| VNM | 2026-06-12 | 34938.5049 | 35251.10214 | 35279.62463000001 | 36949.40368422165 | 33609.845575778374 | 48.0109 | -169.02140978457464 | -176.7007293683097 | 7.679319583735065 |
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

## 1. MA50 & MA200 — Window Function (không cần recursive)

```sql
-- Trong fact_stock_indicators.sql hoặc CTE riêng
SELECT
    symbol,
    trade_date,
    close,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS rn,

    -- MA50: NULL nếu chưa đủ 50 ngày
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 50
        THEN AVG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        )
        ELSE NULL
    END AS ma50,

    -- MA200: NULL nếu chưa đủ 200 ngày
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 200
        THEN AVG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        )
        ELSE NULL
    END AS ma200

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
{% macro calculate_ema(period, source_relation=none, value_column='close_price') %}

{% if source_relation is none %}
    {% set source_relation = ref('fact_stock_price') %}
{% endif %}

WITH RECURSIVE
-- Seed SMA calculation using pre-calculated row_num (indexed)
_ema_seed AS (
    SELECT
        symbol,
        {{ period }}            AS rn,
        AVG({{ value_column }}) AS ema_val
    FROM {{ source_relation }}
    WHERE row_num <= {{ period }}
    GROUP BY symbol
),

-- Recursive: each row = val * alpha + prev_ema * (1 - alpha)
-- Alpha = 2/(period+1)  — Standard EMA (NOT Wilder 1/period)
_ema_rec(symbol, rn, trade_date, ema_val) AS (

    -- Base case: seed at row = period
    SELECT b.symbol, b.row_num, b.trade_date, s.ema_val
    FROM {{ source_relation }} b
    JOIN _ema_seed s ON b.symbol = s.symbol AND b.row_num = s.rn

    UNION ALL

    -- Recursive step: alpha = 2/(period+1)
    -- Joins with the physical index on (symbol, row_num)
    SELECT
        b.symbol,
        b.row_num,
        b.trade_date,
        b.{{ value_column }} * (2.0 / ({{ period }} + 1.0))
            + e.ema_val * (1.0 - 2.0 / ({{ period }} + 1.0))
    FROM {{ source_relation }} b
    JOIN _ema_rec   e ON b.symbol = e.symbol AND b.row_num = e.rn + 1
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
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['symbol', 'trade_date'], 'unique': True},
      {'columns': ['symbol', 'row_num'], 'unique': True}
    ]
) }}

WITH base AS (
    SELECT
        e12.symbol,
        e12.trade_date,
        e12.ema_12 - e26.ema_26                     AS macd_line
    FROM {{ ref('int_ema12') }} e12
    JOIN {{ ref('int_ema26') }} e26
        ON e12.symbol = e26.symbol AND e12.trade_date = e26.trade_date
    WHERE e12.ema_12 IS NOT NULL
      AND e26.ema_26 IS NOT NULL
)
SELECT
    symbol,
    trade_date,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS row_num,
    macd_line
FROM base
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
`SKILL_dbt_incremental.md`). Lý do: mỗi lần `fact_stock_indicators` chạy lookback 120 ngày, các
intermediate model cần có đủ lịch sử đầy đủ để cung cấp đúng warm-up.

---

## 4. Macro: `calculate_rsi.sql` (task 2.3.2)

**File:** `dbt/macros/calculate_rsi.sql`

**Công thức:** Wilder Smoothing — α = 1/period (KHÁC với EMA standard)
**Seed:** SMA của `period` gains/losses đầu tiên

```sql
{% macro calculate_rsi(period) %}

WITH RECURSIVE
-- Seed SMA calculation using pre-calculated row_num, gain, and loss (indexed)
_rsi_seed AS (
    SELECT
        symbol,
        {{ period }} + 1          AS rn,
        AVG(gain)                 AS avg_gain,
        AVG(loss)                 AS avg_loss
    FROM {{ ref('fact_stock_price') }}
    WHERE row_num BETWEEN 2 AND {{ period }} + 1
    GROUP BY symbol
),

-- Recursive: Wilder smoothing = (prev * (period-1) + current) / period
-- Alpha = 1/period  (NOT 2/(period+1) — that's standard EMA)
_rsi_rec(symbol, rn, trade_date, avg_gain, avg_loss) AS (

    -- Base case: seed at row = period+1 (row 15 when period=14)
    SELECT b.symbol, b.row_num, b.trade_date, s.avg_gain, s.avg_loss
    FROM {{ ref('fact_stock_price') }} b
    JOIN _rsi_seed s ON b.symbol = s.symbol AND b.row_num = s.rn

    UNION ALL

    -- Recursive step: Wilder — α = 1/period
    -- Joins with the physical index on (symbol, row_num)
    SELECT
        b.symbol,
        b.row_num,
        b.trade_date,
        (r.avg_gain * ({{ period }} - 1) + b.gain) / {{ period }}::DOUBLE PRECISION,
        (r.avg_loss * ({{ period }} - 1) + b.loss) / {{ period }}::DOUBLE PRECISION
    FROM {{ ref('fact_stock_price') }} b
    JOIN _rsi_rec r ON b.symbol = r.symbol AND b.row_num = r.rn + 1
)

SELECT
    symbol,
    trade_date,
    CASE
        WHEN avg_loss  = 0 AND avg_gain  = 0 THEN NULL
        WHEN avg_loss  = 0                    THEN 100.0
        ELSE ROUND((100.0 - (100.0 / (1.0 + avg_gain / avg_loss)))::NUMERIC, 4)
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
| MA50 | 50 rows | rn = 50 | rn 1–49 |
| MA200 | 200 rows | rn = 200 | rn 1–199 |
| Bollinger Bands | 200 rows | rn = 200 | rn 1–199 |
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
    -- Lookback 360 calendar days để đảm bảo warm-up (cho SMA 200)
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '360 days' FROM {{ this }})
    {% endif %}
),

ma_bb AS (
    SELECT
        symbol,
        trade_date,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS rn,
        -- MA50
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 50
             THEN AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS ma50,
        -- MA200
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 200
             THEN AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS ma200,
        -- Bollinger upper
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 200
             THEN AVG(close)     OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
                + 2.0 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS bb_upper,
        -- Bollinger lower
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) >= 200
             THEN AVG(close)     OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
                - 2.0 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
             ELSE NULL END  AS bb_lower
    FROM source
),

-- RSI từ intermediate model (không inline — tránh nested WITH RECURSIVE)
rsi AS (
    SELECT symbol, trade_date, rsi_14
    FROM {{ ref('int_rsi14') }}
    {% if is_incremental() %}
    WHERE trade_date > (SELECT MAX(trade_date) - INTERVAL '360 days' FROM {{ this }})
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
    WHERE ml.trade_date > (SELECT MAX(trade_date) - INTERVAL '360 days' FROM {{ this }})
    {% endif %}
)

-- Final JOIN
SELECT
    mb.symbol,
    mb.trade_date,
    mb.ma50,
    mb.ma200,
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
| MA200 trả về 0 thay NULL khi warm-up | CASE WHEN rn >= 200 THEN ... ELSE NULL END |
| `int_macd_line`/`int_macd_signal` để `materialized='incremental'` | Phải là `'table'` — full rebuild mỗi lần, giống int_ema12/26/rsi14 |
