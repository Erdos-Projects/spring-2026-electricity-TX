#!/usr/bin/env python3
"""Filter Houston-specific data from all ERCOT datasets into data/processed/houston/.

Outputs one consolidated CSV per dataset covering the full date range.
Run from project root:
    python3 scripts/build_houston_dataset.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "ercot"
OUT_DIR = PROJECT_ROOT / "data" / "processed" / "houston"


def read_monthly_csvs(dataset_id: str) -> pd.DataFrame:
    dataset_dir = RAW_ROOT / dataset_id
    paths = sorted(dataset_dir.glob("**/*.csv"))
    # Keep only monthly consolidated files (<DATASET>_<YYYYMM>.csv); skip per-doc
    # source files (<DATASET>_<YYYYMM>__<docId>.csv) and sidecar files.
    paths = [
        p for p in paths
        if "__" not in p.stem
        and not any(s in p.name for s in (".docids", ".sortcache"))
    ]
    if not paths:
        raise FileNotFoundError(f"No CSVs found for {dataset_id} under {dataset_dir}")
    chunks = []
    for p in paths:
        try:
            chunks.append(pd.read_csv(p, low_memory=False))
        except Exception as exc:
            print(f"  WARN: skipping {p.name}: {exc}")
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def save(df: pd.DataFrame, name: str) -> None:
    out_path = OUT_DIR / name
    df.to_csv(out_path, index=False)
    rows, cols = df.shape
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"  saved {rows:,} rows × {cols} cols → {out_path.name} ({size_mb:.1f} MB)")


def build_rt_prices() -> None:
    """NP6-905-CD: RT settlement prices — HB_HOUSTON hub + LZ_HOUSTON zone."""
    print("NP6-905-CD  (RT settlement prices — Houston hub + zone)")
    df = read_monthly_csvs("NP6-905-CD")
    houston = df[df["SettlementPointName"].isin({"HB_HOUSTON", "LZ_HOUSTON"})].copy()
    houston.sort_values(
        ["DeliveryDate", "DeliveryHour", "DeliveryInterval", "SettlementPointName"],
        inplace=True,
    )
    save(houston, "NP6-905-CD_houston.csv")


def build_dam_prices() -> None:
    """NP4-190-CD: DAM settlement prices — HB_HOUSTON hub."""
    print("NP4-190-CD  (DAM settlement prices — Houston hub)")
    df = read_monthly_csvs("NP4-190-CD")
    # Strip any accidental leading/trailing whitespace in the settlement point column
    df["SettlementPoint"] = df["SettlementPoint"].astype(str).str.strip()
    houston = df[df["SettlementPoint"] == "HB_HOUSTON"].copy()
    houston.sort_values(["DeliveryDate", "HourEnding"], inplace=True)
    save(houston, "NP4-190-CD_houston.csv")


def build_actual_load() -> None:
    """NP6-346-CD: Actual system load — Houston forecast zone column."""
    print("NP6-346-CD  (Actual load — Houston forecast zone)")
    df = read_monthly_csvs("NP6-346-CD")
    keep = ["postDateTime", "OperDay", "HourEnding", "HOUSTON", "TOTAL", "DSTFlag"]
    keep = [c for c in keep if c in df.columns]
    houston = df[keep].copy()
    houston.sort_values(["OperDay", "HourEnding"], inplace=True)
    save(houston, "NP6-346-CD_houston.csv")


def build_wind() -> None:
    """NP4-732-CD: Wind production — LZ_SOUTH_HOUSTON zone columns."""
    print("NP4-732-CD  (Wind production — LZ_SOUTH_HOUSTON zone)")
    df = read_monthly_csvs("NP4-732-CD")
    houston_cols = [c for c in df.columns if "SOUTH_HOUSTON" in c]
    keep = ["postDateTime", "DELIVERY_DATE", "HOUR_ENDING", "SYSTEM_WIDE_GEN",
            "STWPF_SYSTEM_WIDE", "WGRPP_SYSTEM_WIDE"] + houston_cols + ["DSTFlag"]
    keep = [c for c in keep if c in df.columns]
    houston = df[keep].copy()
    houston.sort_values(["DELIVERY_DATE", "HOUR_ENDING"], inplace=True)
    save(houston, "NP4-732-CD_houston.csv")


def build_load_forecast() -> None:
    """NP3-565-CD: Load forecast — Coast weather zone (Houston proxy), InUseFlag=Y only."""
    print("NP3-565-CD  (Load forecast — Coast zone / InUseFlag=Y)")
    df = read_monthly_csvs("NP3-565-CD")
    # Keep only the active forecast model
    if "InUseFlag" in df.columns:
        df = df[df["InUseFlag"] == "Y"]
    keep = ["postDateTime", "DeliveryDate", "HourEnding", "Coast", "SystemTotal",
            "Model", "InUseFlag", "DSTFlag"]
    keep = [c for c in keep if c in df.columns]
    houston = df[keep].copy()
    houston.sort_values(["DeliveryDate", "HourEnding"], inplace=True)
    save(houston, "NP3-565-CD_houston_coast.csv")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output: {OUT_DIR}\n")

    steps = [
        ("NP6-346-CD", build_actual_load),
        ("NP4-190-CD", build_dam_prices),
        ("NP4-732-CD", build_wind),
        ("NP3-565-CD", build_load_forecast),
        ("NP6-905-CD", build_rt_prices),  # largest — last
    ]

    failed = []
    for dataset_id, fn in steps:
        dataset_dir = RAW_ROOT / dataset_id
        if not dataset_dir.exists():
            print(f"{dataset_id}  SKIP (not in raw data)")
            continue
        try:
            fn()
        except Exception as exc:
            print(f"  ERROR: {exc}")
            failed.append(dataset_id)

    print("\nDone.")
    if failed:
        print(f"Failed datasets: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
