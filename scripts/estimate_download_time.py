#!/usr/bin/env python3
"""Estimate dataset download time from per-day completion logs."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, List, Optional, Sequence

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

DAY_COMPLETE_RE = re.compile(
    r"^DAY[_ ]COMPLETE\s+dataset=(?P<dataset>[A-Z0-9-]+)\s+date=(?P<day>[0-9-]+)"
    r".*completed_at=(?P<completed_at>[^\\s]+)"
)

DATE_COLUMN_CANDIDATES: Dict[str, Sequence[str]] = {
    "NP6-346-CD": ("OperDay",),
    "NP6-905-CD": ("DeliveryDate",),
    "NP4-732-CD": ("DELIVERY_DATE", "DeliveryDate", "HOUR_ENDING", "HourEnding", "Date"),
    "NP4-745-CD": ("DELIVERY_DATE",),
    "NP3-233-CD": ("DeliveryDate", "OperDay", "Date"),
    "NP3-565-CD": ("DeliveryDate",),
    "NP4-523-CD": ("DeliveryDate", "OperDay"),
    "NP6-788-CD": ("DeliveryDate", "OperDay", "SCEDTimestamp", "SCEDTimestampGMT", "Date"),
    "NP6-331-CD": ("DeliveryDate", "OperDay"),
    "NP4-188-CD": ("DeliveryDate", "OperDay", "Date"),
    "NP3-911-ER": ("DeliveryDate", "OperDay", "Date"),
    "NP3-912-ER": ("DeliveryDate", "OperDay", "Date"),
}


@dataclass
class DayEvent:
    dataset: str
    day: date
    completed_at: datetime
    run_dir: Path


@dataclass
class Estimate:
    mean_sec_per_day: float
    std_sec_per_day: float
    sample_intervals: int
    source: str
    note: str


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", default="logs/downloads", help="Downloader logs root directory.")
    parser.add_argument("--data-root", default="data/raw/ercot", help="Local raw data root for fallback estimates.")
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Dataset ID filter (repeatable). Defaults to all known datasets.",
    )
    parser.add_argument(
        "--as-of",
        type=parse_iso_date,
        default=date.today(),
        help="Estimate horizon date (inclusive), format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--min-day-samples",
        type=int,
        default=3,
        help="Minimum number of per-day intervals required to report an estimate.",
    )
    parser.add_argument(
        "--fallback-from-mtime",
        dest="fallback_from_mtime",
        action="store_true",
        default=True,
        help="Use local CSV mtime + covered-days fallback when log samples are insufficient (default: on).",
    )
    parser.add_argument(
        "--no-fallback-from-mtime",
        dest="fallback_from_mtime",
        action="store_false",
        help="Disable mtime fallback and use only DAY COMPLETE log-derived samples.",
    )
    parser.add_argument(
        "--fallback-min-covered-days",
        type=int,
        default=2,
        help="Minimum covered data days required for mtime fallback estimate.",
    )
    return parser.parse_args()


def load_run_events(logs_dir: Path) -> List[DayEvent]:
    events: List[DayEvent] = []
    if not logs_dir.exists():
        return events
    for run_dir in sorted(path for path in logs_dir.iterdir() if path.is_dir()):
        run_log = run_dir / "run.log"
        if not run_log.exists():
            continue
        for line in run_log.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = DAY_COMPLETE_RE.match(line.strip())
            if not match:
                continue
            completed_at_raw = match.group("completed_at")
            try:
                completed_at = datetime.fromisoformat(completed_at_raw)
            except ValueError:
                continue
            try:
                day_value = date.fromisoformat(match.group("day"))
            except ValueError:
                continue
            events.append(
                DayEvent(
                    dataset=match.group("dataset"),
                    day=day_value,
                    completed_at=completed_at,
                    run_dir=run_dir,
                )
            )
    return events


def compute_day_intervals(events: List[DayEvent]) -> Dict[str, List[float]]:
    grouped: Dict[str, Dict[Path, List[DayEvent]]] = {}
    for event in events:
        grouped.setdefault(event.dataset, {}).setdefault(event.run_dir, []).append(event)

    intervals: Dict[str, List[float]] = {}
    for dataset, runs in grouped.items():
        dataset_intervals: List[float] = []
        for run_events in runs.values():
            ordered = sorted(run_events, key=lambda e: e.completed_at)
            if len(ordered) < 2:
                continue
            for prev, curr in zip(ordered, ordered[1:]):
                delta = (curr.completed_at - prev.completed_at).total_seconds()
                # Ignore negative, zero, or unreasonably large gaps from pauses.
                if delta <= 0 or delta > 24 * 3600:
                    continue
                dataset_intervals.append(delta)
        intervals[dataset] = dataset_intervals
    return intervals


def inclusive_days(start: date, end: date) -> int:
    return (end - start).days + 1


def parse_day_value(raw: str) -> Optional[date]:
    value = (raw or "").strip()
    if not value:
        return None
    if "T" in value:
        value = value.split("T", 1)[0]
    if " " in value:
        value = value.split(" ", 1)[0]
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    for fmt in ("%m/%d/%Y", "%Y/%m/%d", "%m/%d/%y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def choose_date_column(fieldnames: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    direct = set(fieldnames)
    for candidate in candidates:
        if candidate in direct:
            return candidate
    lowered = {name.lower(): name for name in fieldnames}
    for candidate in candidates:
        actual = lowered.get(candidate.lower())
        if actual:
            return actual
    return None


def estimate_from_mtime_and_days(
    data_root: Path,
    dataset_id: str,
    min_covered_days: int,
) -> Optional[Estimate]:
    dataset_root = data_root / dataset_id
    if not dataset_root.exists():
        return None
    csv_files = sorted(dataset_root.rglob("*.csv"))
    if not csv_files:
        return None

    mtimes = [path.stat().st_mtime for path in csv_files]
    if not mtimes:
        return None
    span_seconds = max(mtimes) - min(mtimes)
    if span_seconds <= 0:
        return None

    candidates = DATE_COLUMN_CANDIDATES.get(
        dataset_id,
        ("DeliveryDate", "OperDay", "Date", "SCEDTimestamp", "DELIVERY_DATE", "HOUR_ENDING"),
    )
    covered_days: set[date] = set()
    for path in csv_files:
        try:
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    continue
                day_col = choose_date_column(reader.fieldnames, candidates)
                if not day_col:
                    continue
                for row in reader:
                    parsed = parse_day_value(str(row.get(day_col, "")))
                    if parsed is not None:
                        covered_days.add(parsed)
        except Exception:  # noqa: BLE001
            continue

    covered_day_count = len(covered_days)
    if covered_day_count < min_covered_days:
        return None
    intervals = covered_day_count - 1
    if intervals <= 0:
        return None
    mean_sec = span_seconds / intervals
    return Estimate(
        mean_sec_per_day=mean_sec,
        std_sec_per_day=0.0,
        sample_intervals=intervals,
        source="fallback-mtime",
        note=f"covered_days={covered_day_count}, csv_files={len(csv_files)}",
    )


def normalize_dataset_ids(values: Sequence[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        dataset_id = value.strip().upper()
        if not dataset_id or dataset_id in seen:
            continue
        seen.add(dataset_id)
        result.append(dataset_id)
    return result


def print_markdown_table(rows: List[Dict[str, str]]) -> None:
    headers = [
        "Dataset ID",
        "Earliest Date",
        "Historical Days (to as-of)",
        "Mean Sec/Day",
        "Sample Day Intervals",
        "Source",
        "Estimated Total Hours",
        "Estimated Total Days",
        "Notes",
    ]
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join(["---"] * len(headers)) + "|")
    for row in rows:
        print(
            "| "
            + " | ".join(
                [
                    row["dataset_id"],
                    row["earliest_date"],
                    row["historical_days"],
                    row["mean_sec_per_day"],
                    row["sample_intervals"],
                    row["source"],
                    row["est_hours"],
                    row["est_days"],
                    row["notes"],
                ]
            )
            + " |"
        )


def main() -> None:
    args = parse_args()
    logs_dir = Path(args.logs_dir)
    data_root = Path(args.data_root)
    events = load_run_events(logs_dir)
    intervals = compute_day_intervals(events)
    selected_ids = normalize_dataset_ids(args.dataset) or sorted(EARLIEST_BY_DATASET)

    print(f"As-of date: {args.as_of.isoformat()}")
    print(f"Logs directory: {logs_dir}")
    print(f"Data root: {data_root}")
    print(f"DAY_COMPLETE events found: {len(events)}")
    print(f"Mtime fallback: {'enabled' if args.fallback_from_mtime else 'disabled'}")
    print("")
    print("Per-dataset day-based estimate")
    print("==============================")
    rows: List[Dict[str, str]] = []
    total_hours = 0.0
    any_estimate = False
    preliminary_estimates: Dict[str, Optional[Estimate]] = {}
    missing_notes: Dict[str, str] = {}
    global_fallback_stats: Optional[tuple[float, float, int]] = None

    for dataset_id in selected_ids:
        if dataset_id not in EARLIEST_BY_DATASET:
            continue
        earliest = EARLIEST_BY_DATASET[dataset_id]
        if earliest is None:
            continue

        day_samples = intervals.get(dataset_id, [])
        estimate: Optional[Estimate] = None
        if len(day_samples) >= args.min_day_samples:
            estimate = Estimate(
                mean_sec_per_day=mean(day_samples),
                std_sec_per_day=pstdev(day_samples) if len(day_samples) > 1 else 0.0,
                sample_intervals=len(day_samples),
                source="log-daily",
                note="from DAY COMPLETE run logs",
            )
        elif args.fallback_from_mtime:
            estimate = estimate_from_mtime_and_days(
                data_root=data_root,
                dataset_id=dataset_id,
                min_covered_days=args.fallback_min_covered_days,
            )

        preliminary_estimates[dataset_id] = estimate
        if estimate is None:
            fallback_note = (
                f"log={len(day_samples)} (<{args.min_day_samples}); "
                "mtime fallback unavailable"
                if args.fallback_from_mtime
                else f"log={len(day_samples)} (<{args.min_day_samples}); fallback disabled"
            )
            missing_notes[dataset_id] = fallback_note

    candidate_means = [
        estimate.mean_sec_per_day
        for estimate in preliminary_estimates.values()
        if estimate is not None
    ]
    if candidate_means:
        fallback_mean = mean(candidate_means)
        fallback_std = pstdev(candidate_means) if len(candidate_means) > 1 else 0.0
        global_fallback_stats = (fallback_mean, fallback_std, len(candidate_means))
        for dataset_id, estimate in preliminary_estimates.items():
            if estimate is not None:
                continue
            reason = missing_notes.get(dataset_id, "insufficient local timing samples")
            preliminary_estimates[dataset_id] = Estimate(
                mean_sec_per_day=fallback_mean,
                std_sec_per_day=fallback_std,
                sample_intervals=len(candidate_means),
                source="global-fallback",
                note=f"mean across {len(candidate_means)} datasets; {reason}",
            )

    for dataset_id in selected_ids:
        if dataset_id not in EARLIEST_BY_DATASET:
            rows.append(
                {
                    "dataset_id": f"`{dataset_id}`",
                    "earliest_date": "unknown",
                    "historical_days": "N/A",
                    "mean_sec_per_day": "N/A",
                    "sample_intervals": "N/A",
                    "source": "N/A",
                    "est_hours": "N/A",
                    "est_days": "N/A",
                    "notes": "dataset not in earliest-date table",
                }
            )
            continue
        earliest = EARLIEST_BY_DATASET[dataset_id]
        if earliest is None:
            rows.append(
                {
                    "dataset_id": f"`{dataset_id}`",
                    "earliest_date": "unresolved",
                    "historical_days": "N/A",
                    "mean_sec_per_day": "N/A",
                    "sample_intervals": "N/A",
                    "source": "N/A",
                    "est_hours": "N/A",
                    "est_days": "N/A",
                    "notes": "earliest date unresolved",
                }
            )
            continue
        if earliest > args.as_of:
            hist_days = 0
        else:
            hist_days = inclusive_days(earliest, args.as_of)

        day_samples = intervals.get(dataset_id, [])
        estimate = preliminary_estimates.get(dataset_id)

        if estimate is None:
            fallback_note = missing_notes.get(
                dataset_id,
                (
                    f"log={len(day_samples)} (<{args.min_day_samples}); "
                    "mtime fallback unavailable"
                    if args.fallback_from_mtime
                    else f"log={len(day_samples)} (<{args.min_day_samples}); fallback disabled"
                ),
            )
            rows.append(
                {
                    "dataset_id": f"`{dataset_id}`",
                    "earliest_date": earliest.isoformat(),
                    "historical_days": f"{hist_days:,}",
                    "mean_sec_per_day": "N/A",
                    "sample_intervals": f"{len(day_samples)}",
                    "source": "N/A",
                    "est_hours": "N/A",
                    "est_days": "N/A",
                    "notes": fallback_note,
                }
            )
            continue

        mean_sec = estimate.mean_sec_per_day
        est_hours = hist_days * mean_sec / 3600.0
        est_days = est_hours / 24.0
        total_hours += est_hours
        any_estimate = True
        rows.append(
            {
                "dataset_id": f"`{dataset_id}`",
                "earliest_date": earliest.isoformat(),
                "historical_days": f"{hist_days:,}",
                "mean_sec_per_day": f"{mean_sec:.2f} (sd {estimate.std_sec_per_day:.2f})",
                "sample_intervals": str(estimate.sample_intervals),
                "source": estimate.source,
                "est_hours": f"{est_hours:.2f}",
                "est_days": f"{est_days:.2f}",
                "notes": estimate.note,
            }
        )

    print_markdown_table(rows)
    print("")
    if any_estimate:
        print(f"Total estimated hours (datasets with enough samples): {total_hours:.2f}")
        if global_fallback_stats is not None:
            fallback_mean, fallback_std, fallback_count = global_fallback_stats
            used_global_fallback = any(
                estimate is not None and estimate.source == "global-fallback"
                for estimate in preliminary_estimates.values()
            )
            if used_global_fallback:
                print(
                    "Global fallback applied: "
                    f"{fallback_mean:.2f} sec/day mean across {fallback_count} datasets "
                    f"(sd {fallback_std:.2f})."
                )
    else:
        print(
            "No dataset had enough timing samples for estimation. "
            "Run downloads with --file-timing-frequency daily or ensure local CSV coverage exists for mtime fallback."
        )


if __name__ == "__main__":
    main()
