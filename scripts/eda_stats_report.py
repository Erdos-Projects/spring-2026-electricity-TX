#!/usr/bin/env python3
"""
EDA statistics report for all ERCOT datasets and weather files.
Reads 2024-12 (and where needed 2023-12) monthly CSVs, computes
detailed column-level statistics, and prints a structured report.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from pathlib import Path

PROJ = Path("/Users/cielo69/github/spring-2026-electricity-TX")
RAW = PROJ / "data/raw/ercot"
WEATHER = PROJ / "data/raw/weather"

PERCENTILES = [0.05, 0.25, 0.50, 0.75, 0.95]


def load_csv(path, chunksize=200_000):
    """Load a CSV (possibly large) via chunked reading."""
    chunks = []
    for chunk in pd.read_csv(path, low_memory=False, chunksize=chunksize):
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)


def load_months(dataset, months):
    """Load and concat specific YYYYMM months for a dataset."""
    dfs = []
    for ym in months:
        yyyy = str(ym)[:4]
        mm = str(ym)[4:]
        p = RAW / dataset / yyyy / mm / f"{dataset}_{ym}.csv"
        if p.exists():
            print(f"  Loading {p.name} ...")
            dfs.append(load_csv(p))
        else:
            print(f"  MISSING: {p}")
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def pct_null(series):
    return 100.0 * series.isna().mean()


def numeric_stats(series):
    s = series.dropna()
    if len(s) == 0:
        return {"count": 0, "null_pct": 100.0}
    return {
        "count": len(series),
        "null_pct": pct_null(series),
        "mean": s.mean(),
        "std": s.std(),
        "min": s.min(),
        "p5": s.quantile(0.05),
        "p25": s.quantile(0.25),
        "p50": s.quantile(0.50),
        "p75": s.quantile(0.75),
        "p95": s.quantile(0.95),
        "max": s.max(),
    }


def categorical_stats(series):
    return {
        "count": len(series),
        "null_pct": pct_null(series),
        "n_unique": series.nunique(),
        "top5": series.value_counts().head(5).to_dict(),
    }


def report_dataset(name, df, flags=None):
    print(f"\n{'='*72}")
    print(f"  DATASET: {name}  ({len(df):,} rows x {len(df.columns)} columns)")
    print(f"{'='*72}")
    print(f"\nColumns and dtypes:")
    for col in df.columns:
        print(f"  {col:<45} {str(df[col].dtype)}")

    print(f"\nNumeric columns:")
    for col in df.select_dtypes(include=[np.number]).columns:
        st = numeric_stats(df[col])
        if st.get("count", 0) == 0:
            print(f"  {col:<45} [ALL NULL]")
            continue
        null_flag = " *** NULL" if st["null_pct"] > 0 else ""
        print(
            f"  {col:<45} "
            f"mean={st['mean']:>12.4f}  std={st['std']:>10.4f}  "
            f"min={st['min']:>12.4f}  p5={st['p5']:>12.4f}  "
            f"p25={st['p25']:>12.4f}  p50={st['p50']:>12.4f}  "
            f"p75={st['p75']:>12.4f}  p95={st['p95']:>12.4f}  "
            f"max={st['max']:>12.4f}  null%={st['null_pct']:.2f}{null_flag}"
        )

    print(f"\nCategorical/string columns:")
    for col in df.select_dtypes(include=["object", "category"]).columns:
        st = categorical_stats(df[col])
        print(
            f"  {col:<45} n_unique={st['n_unique']:>6}  null%={st['null_pct']:.2f}"
        )
        for val, cnt in list(st["top5"].items())[:5]:
            pct = 100.0 * cnt / len(df)
            print(f"      {str(val):<40} {cnt:>8,}  ({pct:.2f}%)")

    if flags:
        print(f"\nData quality flags:")
        for f in flags:
            print(f"  *** {f}")


def check_flags(name, df):
    """Return list of data quality flag strings."""
    flags = []

    # Duplicates
    n_dup = df.duplicated().sum()
    if n_dup > 0:
        flags.append(f"{n_dup:,} fully-duplicate rows detected")

    # Duplicate columns (after dropping exact content dupes)
    dup_cols = df.columns[df.columns.duplicated()].tolist()
    if dup_cols:
        flags.append(f"Duplicate column names: {dup_cols}")

    # postDateTime nulls
    if "postDateTime" in df.columns:
        pdt_null = df["postDateTime"].isna().sum()
        if pdt_null > 0:
            flags.append(f"postDateTime has {pdt_null:,} nulls ({100*pdt_null/len(df):.2f}%)")

    # Any numeric column with >1% nulls
    for col in df.select_dtypes(include=[np.number]).columns:
        pn = pct_null(df[col])
        if pn > 1.0:
            flags.append(f"Column '{col}' has {pn:.2f}% nulls")

    # Extreme numeric values (>5 sigma from mean)
    for col in df.select_dtypes(include=[np.number]).columns:
        s = df[col].dropna()
        if len(s) < 10:
            continue
        mean, std = s.mean(), s.std()
        if std == 0:
            continue
        n_extreme = ((s - mean).abs() > 5 * std).sum()
        if n_extreme > 0:
            pct = 100.0 * n_extreme / len(s)
            flags.append(
                f"Column '{col}': {n_extreme:,} values >5 sigma from mean "
                f"({pct:.3f}%) — min={s.min():.2f}, max={s.max():.2f}"
            )

    return flags


# ============================================================
# NP6-346-CD: 5-zone load forecast (NORTH, SOUTH, WEST, HOUSTON, TOTAL)
# ============================================================
print("\n\nLoading NP6-346-CD (5-zone load forecast)...")
df_346 = load_months("NP6-346-CD", [202412, 202312])
if df_346 is not None:
    flags = check_flags("NP6-346-CD", df_346)
    report_dataset("NP6-346-CD — 5-Zone Load Forecast (2024-12 + 2023-12)", df_346, flags)
    print(f"\nSUMMARY: {len(df_346):,} rows; 9 cols; hourly load forecasts by 4 zones + TOTAL; "
          f"values ~40k-90k MW range; DSTFlag column present.")


# ============================================================
# NP6-345-CD: 8-zone load forecast
# ============================================================
print("\n\nLoading NP6-345-CD (8-zone load forecast)...")
df_345 = load_months("NP6-345-CD", [202412, 202312])
if df_345 is not None:
    flags = check_flags("NP6-345-CD", df_345)
    report_dataset("NP6-345-CD — 8-Zone Load Forecast (2024-12 + 2023-12)", df_345, flags)
    print(f"\nSUMMARY: {len(df_345):,} rows; 11 cols; same as NP6-346-CD but with 8 settlement zones.")


# ============================================================
# NP3-565-CD: Wind + load actual/forecast by zone (2024-12 only — large)
# ============================================================
print("\n\nLoading NP3-565-CD (wind generation + forecast, 2024-12 only)...")
df_565 = load_months("NP3-565-CD", [202412])
if df_565 is not None:
    flags = check_flags("NP3-565-CD", df_565)
    report_dataset("NP3-565-CD — Wind + Load Actual/Forecast (2024-12 only)", df_565, flags)
    print(f"\nSUMMARY: {len(df_565):,} rows; 13 cols; system-wide + LZ_SOUTH_HOUSTON wind generation "
          f"actual, COP_HSL, STWPF, WGRPP forecast columns; nulls in actual generation rows "
          f"where delivery is future (forecast rows).")


# ============================================================
# NP6-905-CD: RTM 15-min settlement point prices (2024-12 only — large)
# ============================================================
print("\n\nLoading NP6-905-CD (RTM 15-min settlement point prices, 2024-12 only)...")
df_905 = load_months("NP6-905-CD", [202412])
if df_905 is not None:
    flags = check_flags("NP6-905-CD", df_905)
    report_dataset("NP6-905-CD — RTM 15-min Settlement Prices (2024-12 only)", df_905, flags)
    print(f"\nSUMMARY: {len(df_905):,} rows; 8 cols; 15-min RTM prices by SettlementPointName "
          f"and SettlementPointType; SettlementPointPrice is the target signal.")


# ============================================================
# NP4-732-CD: Wind generation forecast (2024-12 only — large)
# ============================================================
print("\n\nLoading NP4-732-CD (wind generation forecast, 2024-12 only)...")
df_732 = load_months("NP4-732-CD", [202412])
if df_732 is not None:
    flags = check_flags("NP4-732-CD", df_732)
    report_dataset("NP4-732-CD — Wind Generation Forecast (2024-12 only)", df_732, flags)
    print(f"\nSUMMARY: {len(df_732):,} rows; wind forecast columns SYSTEM_WIDE and LZ_SOUTH_HOUSTON "
          f"with COP_HSL/STWPF/WGRPP variants; SYSTEM_WIDE_GEN is actual, others are forecasts.")


# ============================================================
# NP4-190-CD: DAM settlement point prices (2024-12 only — large)
# ============================================================
print("\n\nLoading NP4-190-CD (DAM settlement point prices, 2024-12 only)...")
df_190 = load_months("NP4-190-CD", [202412])
if df_190 is not None:
    flags = check_flags("NP4-190-CD", df_190)
    report_dataset("NP4-190-CD — DAM Settlement Point Prices (2024-12 only)", df_190, flags)
    print(f"\nSUMMARY: {len(df_190):,} rows; 6 cols; day-ahead market prices by SettlementPoint "
          f"and HourEnding; similar structure to NP6-905-CD but DAM, posted day-ahead.")


# ============================================================
# NP4-523-CD: System lambda (2024-12 + 2023-12)
# ============================================================
print("\n\nLoading NP4-523-CD (system lambda, 2024-12 + 2023-12)...")
df_523 = load_months("NP4-523-CD", [202412, 202312])
if df_523 is not None:
    flags = check_flags("NP4-523-CD", df_523)
    report_dataset("NP4-523-CD — System Lambda (2024-12 + 2023-12)", df_523, flags)
    print(f"\nSUMMARY: {len(df_523):,} rows; 5 cols; DAM system-wide shadow price (lambda) by "
          f"HourEnding; single numeric price column.")


# ============================================================
# NP4-188-CD: Ancillary service MCPC prices (2024-12 + 2023-12)
# ============================================================
print("\n\nLoading NP4-188-CD (ancillary MCPC, 2024-12 + 2023-12)...")
df_188 = load_months("NP4-188-CD", [202412, 202312])
if df_188 is not None:
    flags = check_flags("NP4-188-CD", df_188)
    report_dataset("NP4-188-CD — Ancillary Service MCPC (2024-12 + 2023-12)", df_188, flags)
    print(f"\nSUMMARY: {len(df_188):,} rows; 6 cols; DAM market clearing price for capacity "
          f"(MCPC) by AncillaryType (REGUP, REGDN, RRS, ECRS, NSRS, etc.).")


# ============================================================
# NP3-233-CD: Generation capacity by zone (2024-12 only — large)
# ============================================================
print("\n\nLoading NP3-233-CD (generation capacity by zone, 2024-12 only)...")
df_233 = load_months("NP3-233-CD", [202412])
if df_233 is not None:
    flags = check_flags("NP3-233-CD", df_233)
    report_dataset("NP3-233-CD — Generation Capacity by Zone (2024-12 only)", df_233, flags)
    print(f"\nSUMMARY: {len(df_233):,} rows; 14 cols; total resource, IRR, and new equipment "
          f"MW by zone (South/North/West/Houston); integer MW values.")


# ============================================================
# WEATHER FILES (read all 4 fully)
# ============================================================
print("\n\n" + "="*72)
print("  WEATHER FILES")
print("="*72)

weather_files = {
    "houston_avg.csv": "Houston average weather (12 stations avg)",
    "houston_stdev.csv": "Houston weather standard deviation across stations",
    "texas_avg.csv": "Texas statewide weather average",
    "texas_stdev.csv": "Texas statewide weather standard deviation",
}

for fname, description in weather_files.items():
    fpath = WEATHER / fname
    if not fpath.exists():
        print(f"\nMISSING: {fname}")
        continue
    print(f"\nLoading {fname} ...")
    dfw = load_csv(fpath, chunksize=200_000)
    flags = check_flags(fname, dfw)
    report_dataset(f"WEATHER: {fname} — {description}", dfw, flags)
    # Date range
    if "datetime" in dfw.columns:
        dfw["datetime"] = pd.to_datetime(dfw["datetime"])
        print(f"  Date range: {dfw['datetime'].min()} -> {dfw['datetime'].max()}")
        print(f"  Total hourly rows: {len(dfw):,}")
    print(f"\nSUMMARY: {fname} — {description}; {len(dfw):,} rows; "
          f"5 numeric weather vars (temp_f, humidity_pct, precip_in, wind_gust_mph, pressure_hpa).")

print("\n\n" + "="*72)
print("  END OF REPORT")
print("="*72)
