#!/usr/bin/env python3
"""
Analyze postDateTime vs delivery time discrepancies across ERCOT datasets.
Computes horizon_h = (delivery_datetime - postDateTime).total_seconds() / 3600
"""

import os
import glob
import gzip
import io
import pandas as pd
import numpy as np

BASE = "/Users/cielo69/github/spring-2026-electricity-TX"

DATASETS = {
    "NP6-346-CD": {"pdt_col": "postDateTime", "date_col": "OperDay",      "hour_col": "HourEnding"},
    "NP6-345-CD": {"pdt_col": "postDateTime", "date_col": "OperDay",      "hour_col": "HourEnding"},
    "NP3-565-CD": {"pdt_col": "postDateTime", "date_col": "DeliveryDate", "hour_col": "HourEnding"},
    "NP6-905-CD": {"pdt_col": "postDateTime", "date_col": "DeliveryDate", "hour_col": "DeliveryHour"},
    "NP4-732-CD": {"pdt_col": "postDateTime", "date_col": "DELIVERY_DATE","hour_col": "HOUR_ENDING"},
    "NP4-190-CD": {"pdt_col": "postDateTime", "date_col": "DeliveryDate", "hour_col": "HourEnding"},
    "NP4-523-CD": {"pdt_col": "postDateTime", "date_col": "DeliveryDate", "hour_col": "HourEnding"},
    "NP4-188-CD": {"pdt_col": "postDateTime", "date_col": "DeliveryDate", "hour_col": "HourEnding"},
    "NP3-233-CD": {"pdt_col": "postDateTime", "date_col": "Date",         "hour_col": "HourEnding"},
}

def find_csv_files(dataset, months=("2024/12", "2024/11", "2024/10")):
    """Find CSV files for a dataset, trying months in order."""
    found = []
    used_month = None
    for month in months:
        pattern = os.path.join(BASE, "data/raw/ercot", dataset, month, "*.csv")
        files = sorted(glob.glob(pattern))
        # exclude .docids files
        files = [f for f in files if not f.endswith(".docids")]
        if files:
            found = files[:3]  # up to 3 files
            used_month = month
            break
    return found, used_month

def parse_hour(val, is_delivery_hour=False):
    """Parse hour value to integer 1-24."""
    if is_delivery_hour:
        # Already integer 1-24
        return int(val)
    s = str(val).strip()
    if ":" in s:
        # Format "01:00" — take first part
        return int(s.split(":")[0])
    return int(float(s))

def read_csv_file(fpath):
    """Read a CSV file (plain or .gz)."""
    if fpath.endswith(".gz"):
        with gzip.open(fpath, "rt") as f:
            return pd.read_csv(f, low_memory=False)
    else:
        return pd.read_csv(fpath, low_memory=False)

def compute_horizon(df, pdt_col, date_col, hour_col, is_delivery_hour=False):
    """Compute horizon_h column."""
    # Parse postDateTime
    pdt = pd.to_datetime(df[pdt_col], errors="coerce")

    # Parse delivery date
    del_date = pd.to_datetime(df[date_col], errors="coerce")

    # Parse hour
    hour_int = df[hour_col].apply(lambda x: parse_hour(x, is_delivery_hour))

    # delivery_datetime = date + (hour - 1) hours (naive)
    del_dt = del_date + pd.to_timedelta(hour_int - 1, unit="h")

    # Remove timezone info from pdt if present (for naive comparison)
    if pdt.dt.tz is not None:
        pdt_naive = pdt.dt.tz_localize(None)
    else:
        pdt_naive = pdt

    horizon_h = (del_dt - pdt_naive).dt.total_seconds() / 3600
    return horizon_h

def summarize(horizon_h):
    """Compute summary stats."""
    h = horizon_h.dropna()
    if len(h) == 0:
        return None
    return {
        "n_rows": len(h),
        "min": h.min(),
        "p1": h.quantile(0.01),
        "mean": h.mean(),
        "p99": h.quantile(0.99),
        "max": h.max(),
        "n_extreme": (h.abs() > 200).sum(),
    }

print("=" * 90)
print(f"{'ERCOT Dataset Horizon Analysis':^90}")
print(f"{'horizon_h = (delivery_datetime - postDateTime) in hours':^90}")
print("=" * 90)

all_results = []

for dataset, cols in DATASETS.items():
    pdt_col   = cols["pdt_col"]
    date_col  = cols["date_col"]
    hour_col  = cols["hour_col"]
    is_dh     = (dataset == "NP6-905-CD")

    files, used_month = find_csv_files(dataset)

    if not files:
        print(f"\n[{dataset}] NO FILES FOUND in 2024/12, 2024/11, 2024/10")
        all_results.append({"dataset": dataset, "status": "NO FILES", "month": None})
        continue

    dfs = []
    errors = []
    for fp in files:
        try:
            df = read_csv_file(fp)
            dfs.append(df)
        except Exception as e:
            errors.append(f"{os.path.basename(fp)}: {e}")

    if not dfs:
        print(f"\n[{dataset}] FAILED to read any files: {errors}")
        all_results.append({"dataset": dataset, "status": "READ ERROR", "month": used_month})
        continue

    combined = pd.concat(dfs, ignore_index=True)

    # Check columns exist
    missing_cols = [c for c in [pdt_col, date_col, hour_col] if c not in combined.columns]
    if missing_cols:
        print(f"\n[{dataset}] MISSING COLUMNS: {missing_cols}")
        print(f"  Available columns: {list(combined.columns[:20])}")
        all_results.append({"dataset": dataset, "status": f"MISSING COLS: {missing_cols}", "month": used_month})
        continue

    try:
        horizon_h = compute_horizon(combined, pdt_col, date_col, hour_col, is_dh)
    except Exception as e:
        print(f"\n[{dataset}] ERROR computing horizon: {e}")
        all_results.append({"dataset": dataset, "status": f"COMPUTE ERROR: {e}", "month": used_month})
        continue

    stats = summarize(horizon_h)

    if stats is None:
        print(f"\n[{dataset}] ALL NaN horizons (check postDateTime parsing)")
        all_results.append({"dataset": dataset, "status": "ALL NaN", "month": used_month})
        continue

    # Flag suspicious cases
    flags = []
    if stats["min"] < -1:
        flags.append(f"NEGATIVE min={stats['min']:.1f}h")
    if stats["max"] > 500:
        flags.append(f"HUGE max={stats['max']:.1f}h")
    if stats["n_extreme"] > 0:
        flags.append(f"OUTLIERS n={stats['n_extreme']}")
    if stats["mean"] < 0:
        flags.append("NEGATIVE mean")

    flag_str = " *** " + " | ".join(flags) if flags else ""

    print(f"\n[{dataset}]  month={used_month}  n_rows={stats['n_rows']:,}  files={len(files)}{flag_str}")
    print(f"  {'min':>8}  {'p1':>8}  {'mean':>8}  {'p99':>8}  {'max':>8}  {'|h|>200':>8}")
    print(f"  {stats['min']:>8.2f}  {stats['p1']:>8.2f}  {stats['mean']:>8.2f}  {stats['p99']:>8.2f}  {stats['max']:>8.2f}  {stats['n_extreme']:>8,}")

    all_results.append({
        "dataset": dataset,
        "status": "OK",
        "month": used_month,
        "n_rows": stats["n_rows"],
        "min_h": round(stats["min"], 2),
        "p1_h": round(stats["p1"], 2),
        "mean_h": round(stats["mean"], 2),
        "p99_h": round(stats["p99"], 2),
        "max_h": round(stats["max"], 2),
        "n_extreme": int(stats["n_extreme"]),
        "flags": flags,
    })

    # Show a few extreme rows if any
    if stats["n_extreme"] > 0:
        extreme_mask = horizon_h.abs() > 200
        sample = combined[extreme_mask][[pdt_col, date_col, hour_col]].head(5)
        sample["horizon_h"] = horizon_h[extreme_mask].values[:5]
        print("  Sample extreme rows:")
        print(sample.to_string(index=False, indent=4))

print("\n" + "=" * 90)
print("SUMMARY TABLE")
print("=" * 90)
hdr = f"{'Dataset':<16} {'Month':<10} {'Status':<10} {'min_h':>8} {'p1_h':>8} {'mean_h':>8} {'p99_h':>8} {'max_h':>8} {'|h|>200':>8} {'Flags'}"
print(hdr)
print("-" * 90)
for r in all_results:
    if r["status"] == "OK":
        flags_str = " | ".join(r.get("flags", []))
        line = (f"{r['dataset']:<16} {r['month']:<10} {r['status']:<10} "
                f"{r['min_h']:>8.2f} {r['p1_h']:>8.2f} {r['mean_h']:>8.2f} "
                f"{r['p99_h']:>8.2f} {r['max_h']:>8.2f} {r['n_extreme']:>8,}  {flags_str}")
    else:
        line = f"{r['dataset']:<16} {str(r.get('month','N/A')):<10} {r['status']}"
    print(line)
print("=" * 90)
