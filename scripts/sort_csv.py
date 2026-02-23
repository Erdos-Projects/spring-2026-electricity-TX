#!/usr/bin/env python3
"""Sort local ERCOT monthly CSV files without downloading from API."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Iterable, List

from download_ercot_public_reports import monthly_csv_in_window, sort_monthly_csv


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def resolve_datasets(data_root: Path, requested: Iterable[str]) -> List[str]:
    normalized = [value.strip().upper() for value in requested if value and value.strip()]
    if normalized:
        seen = set()
        ordered: List[str] = []
        for dataset_id in normalized:
            if dataset_id in seen:
                continue
            seen.add(dataset_id)
            ordered.append(dataset_id)
        return ordered
    return sorted(path.name for path in data_root.iterdir() if path.is_dir())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data/raw/ercot", help="Root directory containing dataset folders.")
    parser.add_argument("--dataset", action="append", default=[], help="Dataset ID to include (repeatable).")
    parser.add_argument("--from-date", type=parse_date, help="Optional date lower bound (YYYY-MM-DD).")
    parser.add_argument("--to-date", type=parse_date, help="Optional date upper bound (YYYY-MM-DD).")
    parser.add_argument("--order", choices=("ascending", "descending"), default="ascending")
    parser.add_argument("--strategy", choices=("auto", "timestamp", "forecast-aware"), default="forecast-aware")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    if not data_root.exists():
        raise SystemExit(f"Data root not found: {data_root}")
    if (args.from_date is None) != (args.to_date is None):
        raise SystemExit("Use both --from-date and --to-date together, or omit both.")
    if args.from_date is not None and args.to_date is not None and args.from_date > args.to_date:
        raise SystemExit("--from-date must be on or before --to-date.")

    dataset_ids = resolve_datasets(data_root, args.dataset)
    if not dataset_ids:
        raise SystemExit("No dataset directories found.")

    sorted_count = 0
    already_count = 0
    skipped_count = 0
    failures = 0
    candidates = 0

    for dataset_id in dataset_ids:
        dataset_root = data_root / dataset_id
        if not dataset_root.is_dir():
            print(f"DATASET_SKIP dataset={dataset_id} reason=missing_dir")
            continue

        monthly_files = sorted(path for path in dataset_root.glob("**/*.csv") if path.is_file())
        if args.from_date is not None and args.to_date is not None:
            monthly_files = [
                path
                for path in monthly_files
                if monthly_csv_in_window(path, dataset_root, args.from_date, args.to_date)
            ]

        if not monthly_files:
            print(f"DATASET_SKIP dataset={dataset_id} reason=no_monthly_files")
            continue

        print(
            f"DATASET_PLAN dataset={dataset_id} files={len(monthly_files)} "
            f"order={args.order} strategy={args.strategy}"
        )
        for path in monthly_files:
            candidates += 1
            try:
                status = sort_monthly_csv(path, args.order, args.strategy)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"SORT_ERROR dataset={dataset_id} file={path} error={exc}")
                continue

            if status == "sorted":
                sorted_count += 1
            elif status == "already":
                already_count += 1
            else:
                skipped_count += 1
            print(f"SORT_DONE dataset={dataset_id} file={path} status={status}")

    print(
        "SORT_SUMMARY "
        f"datasets={len(dataset_ids)} candidates={candidates} sorted={sorted_count} "
        f"already={already_count} skipped={skipped_count} failures={failures}"
    )
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
