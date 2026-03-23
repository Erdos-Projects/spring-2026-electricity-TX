#!/usr/bin/env python3
"""Estimate per-dataset total storage from monthly consolidated CSV sizes."""

from __future__ import annotations

import argparse
import calendar
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Optional

EARLIEST_BY_DATASET: Dict[str, Optional[date]] = {
    "NP6-346-CD": date(2017, 6, 29),
    "NP6-905-CD": date(2010, 11, 30),
    "NP4-732-CD": date(2010, 11, 28),
    "NP4-745-CD": date(2022, 6, 30),
    "NP3-233-CD": date(2013, 10, 20),
    "NP3-565-CD": date(2017, 6, 28),
    "NP4-523-CD": date(2013, 12, 11),
    "NP6-788-CD": date(2010, 11, 30),
    "NP6-331-CD": date(2025, 12, 5),
    "NP4-188-CD": date(2010, 11, 29),
    "NP3-911-ER": date(2011, 1, 29),
    "NP3-912-ER": None,
}


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        default="data/raw/ercot",
        help="Root directory containing dataset monthly files.",
    )
    parser.add_argument(
        "--as-of",
        type=parse_iso_date,
        default=date.today(),
        help="Estimate horizon date (inclusive), format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        default=[],
        help="Optional dataset filter. Repeat --dataset for multiple IDs.",
    )
    return parser.parse_args()


def human_bytes(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    units = ("B", "KB", "MB", "GB", "TB")
    idx = 0
    v = float(value)
    while v >= 1024.0 and idx < len(units) - 1:
        v /= 1024.0
        idx += 1
    return f"{v:.2f} {units[idx]}"


def iter_monthly_csvs(dataset_dir: Path, dataset_id: str) -> Iterable[Path]:
    for path in sorted(dataset_dir.glob("**/*.csv")):
        # Consolidated monthly files use this naming convention.
        if path.name.startswith(f"{dataset_id}_"):
            yield path


def parse_year_month(path: Path) -> Optional[tuple[int, int]]:
    stem = path.stem
    parts = stem.split("_")
    if not parts:
        return None
    yyyymm = parts[-1]
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        return None
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    if month < 1 or month > 12:
        return None
    return year, month


def historical_days(earliest: Optional[date], as_of: date) -> Optional[int]:
    if earliest is None:
        return None
    if earliest > as_of:
        return 0
    return (as_of - earliest).days + 1


def markdown_row(values: List[str]) -> str:
    return "| " + " | ".join(values) + " |"


def main() -> None:
    args = parse_args()
    data_root = Path(args.data_root)
    selected = set(args.datasets)

    if selected:
        dataset_ids = [ds for ds in sorted(EARLIEST_BY_DATASET) if ds in selected]
        unknown = sorted(selected - set(dataset_ids))
        for ds in unknown:
            print(f"Warning: unknown dataset '{ds}' ignored.")
    else:
        dataset_ids = sorted(EARLIEST_BY_DATASET)

    print(f"As-of date: {args.as_of.isoformat()}")
    print(f"Data root: {data_root}")
    print("")
    headers = [
        "Dataset ID",
        "Earliest Date",
        "Historical Days",
        "Monthly Files Used",
        "Avg Monthly Size",
        "Avg Daily Size (monthly/day)",
        "Estimated Total Size",
        "Notes",
    ]
    print(markdown_row(headers))
    print(markdown_row(["---"] * len(headers)))

    total_estimable_bytes = 0.0
    estimable_count = 0

    for dataset_id in dataset_ids:
        earliest = EARLIEST_BY_DATASET[dataset_id]
        hdays = historical_days(earliest, args.as_of)
        dataset_dir = data_root / dataset_id
        monthly_files = list(iter_monthly_csvs(dataset_dir, dataset_id)) if dataset_dir.exists() else []

        if not monthly_files:
            print(
                markdown_row(
                    [
                        f"`{dataset_id}`",
                        earliest.isoformat() if earliest else "unresolved",
                        f"{hdays:,}" if hdays is not None else "N/A",
                        "0",
                        "N/A",
                        "N/A",
                        "N/A",
                        "no local monthly CSV files",
                    ]
                )
            )
            continue

        monthly_sizes = [float(path.stat().st_size) for path in monthly_files]
        avg_monthly = mean(monthly_sizes)

        per_day_samples: List[float] = []
        bad_monthly_name = 0
        for path in monthly_files:
            parsed = parse_year_month(path)
            if parsed is None:
                bad_monthly_name += 1
                continue
            year, month = parsed
            days_in_month = calendar.monthrange(year, month)[1]
            per_day_samples.append(float(path.stat().st_size) / days_in_month)

        avg_daily = mean(per_day_samples) if per_day_samples else None
        est_total = (avg_daily * hdays) if (avg_daily is not None and hdays is not None) else None
        if est_total is not None:
            total_estimable_bytes += est_total
            estimable_count += 1

        notes = [f"month-derived files={len(per_day_samples)}"]
        if bad_monthly_name:
            notes.append(f"files with unparsable YYYYMM={bad_monthly_name}")

        print(
            markdown_row(
                [
                    f"`{dataset_id}`",
                    earliest.isoformat() if earliest else "unresolved",
                    f"{hdays:,}" if hdays is not None else "N/A",
                    f"{len(monthly_files):,}",
                    human_bytes(avg_monthly),
                    human_bytes(avg_daily),
                    human_bytes(est_total),
                    "; ".join(notes),
                ]
            )
        )

    print("")
    if estimable_count:
        print(f"Total estimated size across estimable datasets: {human_bytes(total_estimable_bytes)}")
    else:
        print("Total estimated size across estimable datasets: N/A")


if __name__ == "__main__":
    main()
