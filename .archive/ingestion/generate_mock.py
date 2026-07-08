"""
ingestion/generate_mock.py
Sinh CSV fixtures cho MockProvider.

Mặc định sinh cả 2 loại:
  mock_prices.csv      — data sạch (dùng bình thường)
  mock_prices_dq.csv   — data có 10% lỗi mỗi loại (dùng test Silver DQ)
  mock_index.csv       — VNINDEX + VN30 sạch

Chạy từ project root:
  python -m ingestion.generate_mock                   # sinh cả 2 loại
  python -m ingestion.generate_mock --error-rate 0    # chỉ sinh data sạch
  python -m ingestion.generate_mock --error-rate 0.2  # lỗi 20%
  python -m ingestion.generate_mock --clean-only      # chỉ sinh file sạch
  python -m ingestion.generate_mock --dq-only         # chỉ sinh file DQ
"""

import argparse
import os
import random
import numpy as np
import pandas as pd
from datetime import date, timedelta

random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Symbol profiles
# Đơn vị GIÁ: nghìn VND (khớp với vnstock output — FPT=70.4 = 70,400 VND)
# Đơn vị VOLUME: cổ phiếu (giữ nguyên)
# ---------------------------------------------------------------------------

# Khớp với _VN30_SYMBOLS trong mock_provider.py
STOCK_PROFILES = {
    # --- VN30 mock basket (30 mã, có thể khác VN30 thực tế — mục đích test pipeline) ---
    "ACB":  {"base":  28.0,  "vol": 0.018, "avg_volume":  8_000_000},
    "BCM":  {"base":  45.0,  "vol": 0.015, "avg_volume":  1_500_000},
    "BID":  {"base":  42.0,  "vol": 0.016, "avg_volume":  5_000_000},
    "BVH":  {"base":  55.0,  "vol": 0.014, "avg_volume":    800_000},
    "CTG":  {"base":  32.0,  "vol": 0.016, "avg_volume": 10_000_000},
    "FPT":  {"base":  72.0,  "vol": 0.017, "avg_volume":  3_000_000},
    "GAS":  {"base":  88.0,  "vol": 0.015, "avg_volume":  1_200_000},
    "GVR":  {"base":  18.0,  "vol": 0.018, "avg_volume":  4_000_000},
    "HDB":  {"base":  27.0,  "vol": 0.019, "avg_volume":  5_500_000},
    "HPG":  {"base":  24.0,  "vol": 0.022, "avg_volume": 20_000_000},
    "MBB":  {"base":  22.0,  "vol": 0.018, "avg_volume": 12_000_000},
    "MSN":  {"base":  90.0,  "vol": 0.020, "avg_volume":  2_500_000},
    "MWG":  {"base": 130.0,  "vol": 0.022, "avg_volume":  2_000_000},
    "NVL":  {"base":  68.0,  "vol": 0.025, "avg_volume":  3_000_000},
    "PDR":  {"base":  35.0,  "vol": 0.030, "avg_volume":  4_000_000},
    "PLX":  {"base":  48.0,  "vol": 0.015, "avg_volume":  1_500_000},
    "POW":  {"base":  13.0,  "vol": 0.018, "avg_volume":  6_000_000},
    "SAB":  {"base": 180.0,  "vol": 0.013, "avg_volume":    300_000},
    "SSI":  {"base":  28.0,  "vol": 0.022, "avg_volume":  8_000_000},
    "STB":  {"base":  18.0,  "vol": 0.020, "avg_volume": 12_000_000},
    "TCB":  {"base":  42.0,  "vol": 0.019, "avg_volume":  8_000_000},
    "TPB":  {"base":  28.0,  "vol": 0.018, "avg_volume":  5_000_000},
    "VCB":  {"base":  88.0,  "vol": 0.014, "avg_volume":  4_000_000},
    "VHM":  {"base":  92.0,  "vol": 0.020, "avg_volume":  3_000_000},
    "VIB":  {"base":  32.0,  "vol": 0.018, "avg_volume":  5_000_000},
    "VIC":  {"base": 110.0,  "vol": 0.018, "avg_volume":  2_500_000},
    "VJC":  {"base": 120.0,  "vol": 0.018, "avg_volume":  1_500_000},
    "VNM":  {"base":  80.0,  "vol": 0.015, "avg_volume":  2_500_000},
    "VPB":  {"base":  32.0,  "vol": 0.022, "avg_volume": 10_000_000},
    "VRE":  {"base":  28.0,  "vol": 0.018, "avg_volume":  5_000_000},
    # --- Extra HOSE — khớp với _EXTRA_HOSE_SYMBOLS trong mock_provider.py ---
    "DXG":  {"base":  20.0,  "vol": 0.028, "avg_volume":  5_000_000},
    "DIG":  {"base":  18.0,  "vol": 0.030, "avg_volume":  4_000_000},
    "NLG":  {"base":  30.0,  "vol": 0.022, "avg_volume":  2_000_000},
    "KDH":  {"base":  32.0,  "vol": 0.022, "avg_volume":  2_500_000},
    "HDG":  {"base":  28.0,  "vol": 0.025, "avg_volume":  2_000_000},
    "REE":  {"base":  52.0,  "vol": 0.018, "avg_volume":  1_500_000},
    "PNJ":  {"base":  90.0,  "vol": 0.018, "avg_volume":  1_500_000},
    "EVF":  {"base":  18.0,  "vol": 0.022, "avg_volume":  3_000_000},
    "LPB":  {"base":  15.0,  "vol": 0.020, "avg_volume":  6_000_000},
    "SHB":  {"base":  14.0,  "vol": 0.020, "avg_volume":  8_000_000},
}

# Đơn vị: điểm chỉ số (không phải nghìn VND)
INDEX_PROFILES = {
    "VNINDEX": {"base": 1_150.0, "vol": 0.010, "avg_volume": 450_000_000},
    "VN30":    {"base": 1_100.0, "vol": 0.010, "avg_volume": 350_000_000},
}

START_DATE = date(2021, 1, 4)
END_DATE   = date(2026, 6, 20)


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def _trading_days(start: date, end: date):
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _simulate_ohlcv(days, base: float, vol: float, avg_volume: int) -> list:
    """
    GBM với biên độ ±7%/ngày (quy định HOSE).
    Volume có spike 3–8x ngẫu nhiên 5% phiên (mô phỏng tin tức).
    """
    closes = [float(base)]
    for _ in range(len(days) - 1):
        ret = np.random.normal(0.0002, vol)
        ret = max(min(ret, 0.07), -0.07)
        closes.append(max(closes[-1] * (1 + ret), 0.1))  # min 0.1 nghìn VND = 100 VND

    rows = []
    for c in closes:
        spread = c * random.uniform(0.003, 0.015)
        high   = round(c + random.uniform(0, spread), 2)   # 2 decimal places (đơn vị: nghìn VND)
        low    = round(max(c - random.uniform(0, spread), 0.1), 2)   # min 100 VND
        open_  = round(random.uniform(low, high), 2)
        v = int(np.random.normal(avg_volume, avg_volume * 0.25))
        if random.random() < 0.05:
            v = int(v * random.uniform(3, 8))
        rows.append({"open": open_, "high": high, "low": low,
                     "close": round(c, 2), "volume": max(v, 0)})
    return rows


def _build_df(profiles: dict, days: list, source: str = "mock") -> pd.DataFrame:
    frames = []
    for sym, p in profiles.items():
        rows = _simulate_ohlcv(days, p["base"], p["vol"], p["avg_volume"])
        df = pd.DataFrame(rows)
        df["code"]   = sym
        df["date"]   = [str(d) for d in days]
        df["source"] = source
        frames.append(df[["code", "date", "open", "high", "low", "close", "volume", "source"]])
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# DQ error injection
# ---------------------------------------------------------------------------

# Map: tên lỗi → (mô tả, seed)
_INJECTORS = [
    ("invalid_close",   "close <= 0          → Silver: dq_flag='invalid_close'",   1),
    ("high_lt_low",     "high < low          → Silver: dq_flag='high_lt_low'",     2),
    ("negative_volume", "volume < 0          → Silver: dq_flag='negative_volume'", 3),
    ("null_code",       "code = NULL         → dbt not_null FAIL",                 4),
    ("null_date",       "date = NULL         → dbt not_null FAIL",                 5),
]


def _inject_errors(df: pd.DataFrame, error_rate: float) -> pd.DataFrame:
    """Inject mỗi loại lỗi độc lập với tỉ lệ error_rate."""
    for name, desc, seed in _INJECTORS:
        idx = df.sample(frac=error_rate, random_state=seed).index
        if name == "invalid_close":
            df.loc[idx, "close"] = [
                0.0 if i % 2 == 0 else round(-random.uniform(500, 80_000), 0)
                for i in range(len(idx))
            ]
        elif name == "high_lt_low":
            h = df.loc[idx, "high"].values.copy()
            l = df.loc[idx, "low"].values.copy()
            df.loc[idx, "high"] = l - 100
            df.loc[idx, "low"]  = h
        elif name == "negative_volume":
            df.loc[idx, "volume"] = df.loc[idx, "volume"].apply(lambda v: -abs(int(v)) - 1)
        elif name == "null_code":
            df.loc[idx, "code"] = None
        elif name == "null_date":
            df.loc[idx, "date"] = None
        print(f"  [{name:<20}] {len(idx):>6,} dòng — {desc}")
    return df


def _print_dq_stats(df: pd.DataFrame):
    total = len(df)
    c = pd.to_numeric(df["close"],  errors="coerce")
    h = pd.to_numeric(df["high"],   errors="coerce")
    l = pd.to_numeric(df["low"],    errors="coerce")
    v = pd.to_numeric(df["volume"], errors="coerce")
    bad = c.le(0) | (h < l) | v.lt(0) | df["code"].isna() | df["date"].isna()
    print(f"\n  Dòng có lỗi  : {bad.sum():,} / {total:,} ({bad.sum()/total*100:.1f}%)")
    print(f"  Dòng sạch    : {(~bad).sum():,} / {total:,} ({(~bad).sum()/total*100:.1f}%)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(error_rate: float, clean_only: bool, dq_only: bool):
    # ingestion/ nằm 1 cấp dưới project root
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    out_dir      = os.path.join(project_root, "tests", "fixtures")
    os.makedirs(out_dir, exist_ok=True)

    days = _trading_days(START_DATE, END_DATE)
    print(f"Khoảng thời gian : {START_DATE} → {END_DATE} ({len(days)} ngày giao dịch)")
    print(f"Symbols          : {len(STOCK_PROFILES)} mã cổ phiếu + {len(INDEX_PROFILES)} index")
    print(f"Output dir       : {out_dir}\n")

    # --- Sinh base data ---
    print("Sinh base data...")
    prices_df = _build_df(STOCK_PROFILES, days)
    index_df  = _build_df(INDEX_PROFILES,  days)

    # --- File sạch ---
    if not dq_only:
        prices_path = os.path.join(out_dir, "mock_prices.csv")
        index_path  = os.path.join(out_dir, "mock_index.csv")
        prices_df.to_csv(prices_path, index=False)
        index_df.to_csv(index_path,   index=False)
        print(f"✓ mock_prices.csv — {len(prices_df):,} dòng")
        print(f"  VNM sample: {prices_df[prices_df.code=='VNM'][['date','open','high','low','close','volume']].head(2).to_string(index=False)}")
        print(f"✓ mock_index.csv  — {len(index_df):,} dòng\n")

    # --- File DQ ---
    if not clean_only and error_rate > 0:
        print(f"Inject DQ errors ({error_rate*100:.0f}% mỗi loại, độc lập)...")
        dq_df = _inject_errors(prices_df.copy(), error_rate)
        _print_dq_stats(dq_df)
        dq_path = os.path.join(out_dir, "mock_prices_dq.csv")
        dq_df.to_csv(dq_path, index=False)
        print(f"\n✓ mock_prices_dq.csv — {len(dq_df):,} dòng")
        print("  Dùng: MockProvider(prices_csv='tests/fixtures/mock_prices_dq.csv')")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh mock CSV fixtures cho MockProvider.")
    parser.add_argument(
        "--error-rate", type=float, default=0.10,
        help="Tỉ lệ lỗi mỗi loại DQ (0.0–1.0, mặc định 0.10 = 10%%)",
    )
    parser.add_argument("--clean-only", action="store_true", help="Chỉ sinh file sạch")
    parser.add_argument("--dq-only",    action="store_true", help="Chỉ sinh file DQ")
    args = parser.parse_args()
    main(args.error_rate, args.clean_only, args.dq_only)