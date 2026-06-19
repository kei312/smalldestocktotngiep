"""
Task 2.3.8 — G-03: MACD verification
So sánh SQL output (từ PostgreSQL) với Python reference implementation.
Acceptance: max error < 0.5% cho mọi giá trị MACD trên 3 mã.
"""
import math
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


def fetch_sql_indicators(conn, symbol):
    """Fetch MACD values computed by SQL (dbt model)."""
    query = """
        SELECT trade_date, macd_line, macd_signal, macd_histogram
        FROM public_gold.fact_stock_indicators
        WHERE symbol = %s
        ORDER BY trade_date
    """
    return pd.read_sql(query, conn, params=(symbol,))


def fetch_closes(conn, symbol):
    """Fetch ALL close prices — no date filter to match SQL model's full history."""
    query = """
        SELECT trade_date, close_price
        FROM public_gold.fact_stock_price
        WHERE symbol = %s
        ORDER BY trade_date
    """
    return pd.read_sql(query, conn, params=(symbol,))


def pct_error(ref_val, sql_val):
    if ref_val is None or sql_val is None:
        return None
    if pd.isna(ref_val) or pd.isna(sql_val):
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

        if len(closes_df) < 35:
            print(f"[{symbol}]  SKIP — only {len(closes_df)} rows (need >= 35 for MACD signal)")
            print()
            continue

        sql_df = fetch_sql_indicators(conn, symbol)
        closes = closes_df["close_price"].tolist()

        ref = macd_reference(closes)
        ref_df = pd.DataFrame({
            "trade_date":      closes_df["trade_date"],
            "ref_macd_line":   ref["macd_line"],
            "ref_macd_signal": ref["macd_signal"],
        })

        merged = sql_df.merge(ref_df, on="trade_date")

        # Separate valid sets for line and signal
        valid_line   = merged.dropna(subset=["macd_line", "ref_macd_line"])
        valid_signal = merged.dropna(subset=["macd_signal", "ref_macd_signal"])

        if valid_line.empty:
            print(f"[{symbol}]  SKIP — no overlapping non-NULL MACD values")
            print()
            continue

        errors_line   = [pct_error(r, s) for r, s in zip(valid_line["ref_macd_line"],     valid_line["macd_line"])]
        errors_signal = [pct_error(r, s) for r, s in zip(valid_signal["ref_macd_signal"], valid_signal["macd_signal"])]

        max_line   = max((e for e in errors_line   if e is not None), default=0)
        max_signal = max((e for e in errors_signal if e is not None), default=0)

        status_line   = "✅ PASS" if max_line   < 0.5 else "❌ FAIL"
        status_signal = "✅ PASS" if max_signal < 0.5 else "❌ FAIL"

        print(f"[{symbol}]  line rows: {len(valid_line)}, signal rows: {len(valid_signal)}")
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
