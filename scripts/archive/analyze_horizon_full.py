#!/usr/bin/env python3
"""
analyze_horizon_full.py

Reads ALL monthly CSV files for every ERCOT dataset and computes
postDateTime vs delivery discrepancy (horizon) statistics.

Usage: python3 scripts/analyze_horizon_full.py
"""

import os
import glob
import re
import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime

PROJECT_ROOT = "/Users/cielo69/github/spring-2026-electricity-TX"
DATA_ROOT = os.path.join(PROJECT_ROOT, "data/raw/ercot")

# Dataset configuration: (date_col, date_fmt, hour_col, hour_is_delivery_hour)
# hour_is_delivery_hour=True: column is integer delivery hour (not HourEnding offset)
# hour_is_delivery_hour=False: column is HourEnding (delivery_hour = parsed int)
DATASET_CONFIG = {
    "NP6-346-CD": {
        "date_col": "OperDay",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",  # parse int, delivery = date + (he-1)h
    },
    "NP6-345-CD": {
        "date_col": "OperDay",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",
    },
    "NP3-565-CD": {
        "date_col": "DeliveryDate",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",
    },
    "NP6-905-CD": {
        "date_col": "DeliveryDate",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "DeliveryHour",
        "hour_type": "DeliveryHour",  # integer delivery hour (1-24), delivery = date + (dh-1)h
    },
    "NP4-732-CD": {
        "date_col": "DELIVERY_DATE",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HOUR_ENDING",
        "hour_type": "HourEnding",
    },
    "NP4-190-CD": {
        "date_col": "DeliveryDate",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",
    },
    "NP4-523-CD": {
        "date_col": "DeliveryDate",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",
    },
    "NP4-188-CD": {
        "date_col": "DeliveryDate",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",
    },
    "NP3-233-CD": {
        "date_col": "Date",
        "date_fmt": "%m/%d/%Y",
        "hour_col": "HourEnding",
        "hour_type": "HourEnding",
    },
}

# Datasets expected to be purely retrospective (horizon should be <= 0)
RETROSPECTIVE_DATASETS = {"NP6-346-CD", "NP6-345-CD", "NP6-905-CD"}

# Day-ahead datasets (horizon should be <= 36h)
DAY_AHEAD_DATASETS = {"NP4-190-CD", "NP4-523-CD", "NP4-188-CD"}

CHUNK_SIZE = 200_000


def parse_hour_ending(series):
    """
    Parse HourEnding column to integer hour.
    Handles formats like '01:00', '1:00', '24', '1', etc.
    Returns integer series (1-24).
    """
    s = series.astype(str).str.strip()
    # If it contains ':', extract the hour part
    mask_colon = s.str.contains(":", na=False)
    result = pd.Series(index=series.index, dtype="Int64")
    if mask_colon.any():
        result[mask_colon] = s[mask_colon].str.split(":").str[0].astype("Int64")
    if (~mask_colon).any():
        result[~mask_colon] = pd.to_numeric(s[~mask_colon], errors="coerce").astype("Int64")
    return result


def compute_horizons_for_chunk(chunk, date_col, date_fmt, hour_col, hour_type):
    """
    Compute horizon_h (float32 array) for a dataframe chunk.
    Returns numpy float32 array of valid horizon values.
    """
    # Drop rows with null postDateTime
    chunk = chunk.dropna(subset=["postDateTime"])
    chunk = chunk[chunk["postDateTime"].astype(str).str.strip() != ""]

    if chunk.empty:
        return np.array([], dtype=np.float32)

    # Parse postDateTime
    try:
        pdt = pd.to_datetime(chunk["postDateTime"], errors="coerce")
    except Exception:
        return np.array([], dtype=np.float32)

    # Drop rows where postDateTime failed to parse
    valid_pdt = pdt.notna()
    chunk = chunk[valid_pdt]
    pdt = pdt[valid_pdt]

    if chunk.empty:
        return np.array([], dtype=np.float32)

    # Parse delivery date
    try:
        delivery_date = pd.to_datetime(chunk[date_col], format=date_fmt, errors="coerce")
    except Exception:
        return np.array([], dtype=np.float32)

    # Parse delivery hour
    if hour_type == "DeliveryHour":
        # Integer delivery hour (1-24)
        hour_int = pd.to_numeric(chunk[hour_col], errors="coerce")
    else:
        # HourEnding: parse from '01:00' or integer string
        hour_int = parse_hour_ending(chunk[hour_col])

    # delivery_datetime = date + (hour - 1) hours  [naive, no DST]
    try:
        delivery_dt = delivery_date + pd.to_timedelta(hour_int - 1, unit="h")
    except Exception:
        return np.array([], dtype=np.float32)

    # Compute horizon in hours
    horizon = (delivery_dt - pdt).dt.total_seconds() / 3600.0

    # Drop NaN horizons
    valid = horizon.notna()
    horizon = horizon[valid].values.astype(np.float32)

    return horizon


def process_dataset(dataset, config):
    """
    Process all CSV files for a dataset and compute horizon statistics.
    Returns a dict with statistics.
    """
    date_col = config["date_col"]
    date_fmt = config["date_fmt"]
    hour_col = config["hour_col"]
    hour_type = config["hour_type"]

    cols_to_load = ["postDateTime", date_col, hour_col]

    dataset_dir = os.path.join(DATA_ROOT, dataset)
    if not os.path.isdir(dataset_dir):
        return {"error": f"Directory not found: {dataset_dir}"}

    # Glob all CSV files, skip .docids files
    pattern = os.path.join(dataset_dir, "**", "*.csv")
    all_files = sorted(glob.glob(pattern, recursive=True))
    all_files = [f for f in all_files if ".docids" not in f]

    if not all_files:
        return {"error": "No CSV files found"}

    total_rows = 0
    all_horizons = []
    file_errors = []

    t0 = time.time()
    for i, fpath in enumerate(all_files):
        fname = os.path.relpath(fpath, DATA_ROOT)
        try:
            reader = pd.read_csv(
                fpath,
                usecols=cols_to_load,
                chunksize=CHUNK_SIZE,
                low_memory=False,
            )
            for chunk in reader:
                total_rows += len(chunk)
                horizons = compute_horizons_for_chunk(
                    chunk, date_col, date_fmt, hour_col, hour_type
                )
                if len(horizons) > 0:
                    all_horizons.append(horizons)
        except Exception as e:
            file_errors.append(f"{fname}: {e}")

    elapsed = time.time() - t0

    if file_errors:
        print(f"  [WARN] {dataset}: {len(file_errors)} file error(s):")
        for fe in file_errors[:5]:
            print(f"    {fe}")
        if len(file_errors) > 5:
            print(f"    ... and {len(file_errors) - 5} more")

    if not all_horizons:
        return {
            "n_files": len(all_files),
            "total_rows": total_rows,
            "valid_rows": 0,
            "elapsed_s": elapsed,
            "error": "No valid horizon values computed",
        }

    horizons = np.concatenate(all_horizons, dtype=np.float32)
    valid_rows = len(horizons)

    count_extreme = int(np.sum(np.abs(horizons) > 200))
    count_negative = int(np.sum(horizons < 0))

    percentiles = np.percentile(horizons, [1, 5, 25, 50, 75, 95, 99])

    return {
        "n_files": len(all_files),
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "min": float(np.min(horizons)),
        "max": float(np.max(horizons)),
        "mean": float(np.mean(horizons)),
        "std": float(np.std(horizons)),
        "p1": float(percentiles[0]),
        "p5": float(percentiles[1]),
        "p25": float(percentiles[2]),
        "p50": float(percentiles[3]),
        "p75": float(percentiles[4]),
        "p95": float(percentiles[5]),
        "p99": float(percentiles[6]),
        "count_extreme": count_extreme,
        "count_negative": count_negative,
        "elapsed_s": elapsed,
    }


def format_h(val, decimals=1):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{val:.{decimals}f}"


def print_report(results):
    print()
    print("=" * 100)
    print("ERCOT DATASET HORIZON ANALYSIS — FULL DATA")
    print("postDateTime vs delivery_datetime discrepancy (hours)")
    print("horizon_h = delivery_datetime - postDateTime (positive = future delivery)")
    print("=" * 100)
    print()

    col_headers = [
        "Dataset", "Files", "Rows", "Valid",
        "Min", "Max", "Mean", "Std",
        "P1", "P5", "P25", "P50", "P75", "P95", "P99",
        "|h|>200", "Neg", "Flags"
    ]

    # Print header
    print(f"{'Dataset':<15} {'Files':>5} {'TotalRows':>12} {'ValidRows':>12} "
          f"{'Min':>8} {'Max':>8} {'Mean':>8} {'Std':>7} "
          f"{'P1':>7} {'P5':>7} {'P25':>7} {'P50':>7} {'P75':>7} {'P95':>7} {'P99':>7} "
          f"{'|h|>200':>8} {'Neg':>8}  Flags")
    print("-" * 170)

    for dataset, stats in results.items():
        if "error" in stats and "min" not in stats:
            print(f"{'Dataset':<15}  ERROR: {stats['error']}")
            continue

        flags = []

        # Check anomalies
        if stats.get("count_extreme", 0) > 0:
            flags.append(f"EXTREME:{stats['count_extreme']}")

        if dataset in RETROSPECTIVE_DATASETS:
            # Should have no positive horizons (retrospective = delivery already happened)
            # Actually retrospective means postDateTime comes AFTER delivery, so horizon < 0
            # But check for positive horizons > some threshold
            neg = stats.get("count_negative", 0)
            valid = stats.get("valid_rows", 1)
            pct_neg = neg / valid * 100 if valid > 0 else 0
            if stats.get("max", 0) > 1.0:
                flags.append(f"RETRO_POS_MAX:{stats['max']:.1f}h")

        if dataset in DAY_AHEAD_DATASETS:
            if stats.get("max", 0) > 36:
                flags.append(f"DA_MAX>{36}h:{stats['max']:.1f}h")

        if stats.get("min", 0) < -200:
            flags.append(f"LARGE_NEG:{stats['min']:.1f}h")

        flags_str = " | ".join(flags) if flags else "ok"

        print(
            f"{dataset:<15} {stats.get('n_files',0):>5} {stats.get('total_rows',0):>12,} "
            f"{stats.get('valid_rows',0):>12,} "
            f"{format_h(stats.get('min')):>8} {format_h(stats.get('max')):>8} "
            f"{format_h(stats.get('mean')):>8} {format_h(stats.get('std')):>7} "
            f"{format_h(stats.get('p1')):>7} {format_h(stats.get('p5')):>7} "
            f"{format_h(stats.get('p25')):>7} {format_h(stats.get('p50')):>7} "
            f"{format_h(stats.get('p75')):>7} {format_h(stats.get('p95')):>7} "
            f"{format_h(stats.get('p99')):>7} "
            f"{stats.get('count_extreme',0):>8} {stats.get('count_negative',0):>8}  {flags_str}"
        )

    print()
    print("=" * 100)
    print("FLAG LEGEND:")
    print("  EXTREME:N         — N rows where |horizon_h| > 200h")
    print("  RETRO_POS_MAX:Xh  — retrospective dataset (346/345/905) has max positive horizon > 1h")
    print("  DA_MAX>36h:Xh     — day-ahead dataset (190/523/188) has max horizon > 36h")
    print("  LARGE_NEG:Xh      — min horizon < -200h")
    print("  ok                — no anomalies detected")
    print()


def main():
    print(f"Starting full horizon analysis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data root: {DATA_ROOT}")
    print(f"Chunk size: {CHUNK_SIZE:,} rows")
    print()

    results = {}
    total_t0 = time.time()

    for dataset in sorted(DATASET_CONFIG.keys()):
        config = DATASET_CONFIG[dataset]
        print(f"Processing {dataset}...", flush=True)
        t0 = time.time()
        stats = process_dataset(dataset, config)
        elapsed = time.time() - t0
        results[dataset] = stats
        n_files = stats.get("n_files", 0)
        total_rows = stats.get("total_rows", 0)
        valid_rows = stats.get("valid_rows", 0)
        if "error" in stats and "min" not in stats:
            print(f"  -> ERROR: {stats['error']} ({elapsed:.1f}s)")
        else:
            print(
                f"  -> {n_files} files, {total_rows:,} rows, {valid_rows:,} valid horizons "
                f"in {elapsed:.1f}s"
            )

    total_elapsed = time.time() - total_t0
    print(f"\nTotal elapsed: {total_elapsed:.1f}s")

    print_report(results)

    # Print detailed flag summary
    print("DETAILED FLAG SUMMARY:")
    print("-" * 60)
    any_flags = False
    for dataset, stats in results.items():
        if "min" not in stats:
            continue
        issues = []
        if stats.get("count_extreme", 0) > 0:
            issues.append(f"|horizon| > 200h: {stats['count_extreme']} rows")
        if dataset in RETROSPECTIVE_DATASETS and stats.get("max", 0) > 1.0:
            issues.append(
                f"Retrospective dataset has positive horizon max={stats['max']:.2f}h "
                f"(mean={stats['mean']:.2f}h, p99={stats['p99']:.2f}h)"
            )
        if dataset in DAY_AHEAD_DATASETS and stats.get("max", 0) > 36:
            issues.append(
                f"Day-ahead dataset max horizon={stats['max']:.2f}h > 36h threshold"
            )
        if stats.get("min", 0) < -200:
            issues.append(f"Large negative horizon: min={stats['min']:.2f}h")
        if issues:
            any_flags = True
            print(f"\n{dataset}:")
            for issue in issues:
                print(f"  *** {issue}")
    if not any_flags:
        print("No anomalies flagged across all datasets.")
    print()


if __name__ == "__main__":
    main()
