#!/usr/bin/env python3
"""Audit postDateTime quality across state cache files and monthly CSVs.

Scans:
  1. state/*.archive_docs.jsonl  — checks that every stored postDatetime is a
     parseable ISO timestamp (not a placeholder, sequential index, or filename).
  2. data/raw/ercot/**/<DATASET>_<YYYYMM>.csv — checks that every non-empty
     postDateTime cell is a parseable ISO timestamp.

Prints one line per issue found, plus a summary.  Exit code 0 = clean,
exit code 1 = issues found.

Usage:
    python3 scripts/audit_post_datetime_quality.py
    python3 scripts/audit_post_datetime_quality.py --state-dir state --data-root data/raw/ercot
    python3 scripts/audit_post_datetime_quality.py --dataset NP6-346-CD
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------------
# ISO timestamp parser (mirrors parse_api_datetime from download script)
# ---------------------------------------------------------------------------
try:
    from download_ercot_public_reports import parse_api_datetime
except ImportError:
    from datetime import datetime

    def parse_api_datetime(value: str) -> Optional[datetime]:  # type: ignore[misc]
        """Minimal fallback parser for ISO-8601 timestamps."""
        value = value.strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M%z",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None


POST_DATETIME_FIELD_NAMES = {
    "postDateTime",
    "postDatetime",
    "PostingTime",
    "post_datetime",
}
POST_DATETIME_FIELD_LOWER = {n.lower() for n in POST_DATETIME_FIELD_NAMES}


def _detect_post_field(fieldnames: List[str]) -> Optional[str]:
    for name in fieldnames:
        if name.lower() in POST_DATETIME_FIELD_LOWER:
            return name
    return None


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def audit_jsonl_file(jsonl_path: Path) -> List[str]:
    """Return list of issue strings for one .archive_docs.jsonl file."""
    issues: List[str] = []
    line_num = 0
    try:
        with open(jsonl_path, encoding="utf-8") as fh:
            for raw_line in fh:
                line_num += 1
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    record = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    issues.append(
                        f"JSONL_PARSE_ERROR file={jsonl_path} line={line_num} error={exc}"
                    )
                    continue
                doc = record.get("doc", {}) if isinstance(record, dict) else {}
                doc_id = str(doc.get("docId") or "").strip()
                post_dt = str(doc.get("postDatetime") or "").strip()
                if not post_dt:
                    continue  # blank is expected (missing from API) — not malformed
                if parse_api_datetime(post_dt) is None:
                    issues.append(
                        f"INVALID_POST_DATETIME file={jsonl_path} line={line_num} "
                        f"doc_id={doc_id or '-'} value={post_dt!r}"
                    )
    except OSError as exc:
        issues.append(f"READ_ERROR file={jsonl_path} error={exc}")
    return issues


def audit_monthly_csv(csv_path: Path) -> List[str]:
    """Return list of issue strings for one monthly CSV file."""
    issues: List[str] = []
    try:
        with open(csv_path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                return issues
            post_field = _detect_post_field(list(reader.fieldnames))
            if post_field is None:
                return issues  # no postDateTime column yet — nothing to validate
            for row_num, row in enumerate(reader, start=2):  # 1 = header
                value = str(row.get(post_field) or "").strip()
                if not value:
                    continue
                if parse_api_datetime(value) is None:
                    issues.append(
                        f"INVALID_POST_DATETIME file={csv_path} row={row_num} "
                        f"field={post_field} value={value!r}"
                    )
    except OSError as exc:
        issues.append(f"READ_ERROR file={csv_path} error={exc}")
    return issues


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default="state", help="Directory containing state/*.jsonl files.")
    parser.add_argument("--data-root", default="data/raw/ercot", help="Root directory for raw ERCOT data.")
    parser.add_argument(
        "--dataset", action="append", default=[],
        help="Limit to specific dataset ID(s) (repeatable). Omit to scan all.",
    )
    parser.add_argument("--skip-csvs", action="store_true", help="Skip monthly CSV scan (faster).")
    parser.add_argument("--skip-state", action="store_true", help="Skip state JSONL scan.")
    args = parser.parse_args()

    state_dir = Path(args.state_dir)
    data_root = Path(args.data_root)
    dataset_filter = {d.strip().upper() for d in args.dataset if d.strip()}

    total_issues = 0
    scanned_jsonl = 0
    scanned_csvs = 0

    # --- 1. Scan state JSONL files ---
    if not args.skip_state and state_dir.exists():
        for jsonl_path in sorted(state_dir.glob("*.archive_docs.jsonl")):
            dataset_id = jsonl_path.name.replace(".archive_docs.jsonl", "").upper()
            if dataset_filter and dataset_id not in dataset_filter:
                continue
            scanned_jsonl += 1
            issues = audit_jsonl_file(jsonl_path)
            for issue in issues:
                print(f"STATE  {issue}")
            total_issues += len(issues)

    # --- 2. Scan monthly CSVs ---
    if not args.skip_csvs and data_root.exists():
        for dataset_dir in sorted(data_root.iterdir()):
            if not dataset_dir.is_dir():
                continue
            dataset_id = dataset_dir.name.upper()
            if dataset_filter and dataset_id not in dataset_filter:
                continue
            for csv_path in sorted(dataset_dir.glob("**/*.csv")):
                if not csv_path.is_file():
                    continue
                # Skip per-doc source files (they have __<docId> suffix)
                if "__" in csv_path.stem:
                    continue
                scanned_csvs += 1
                issues = audit_monthly_csv(csv_path)
                for issue in issues:
                    print(f"CSV    {issue}")
                total_issues += len(issues)

    print(
        f"AUDIT_SUMMARY scanned_state_files={scanned_jsonl} "
        f"scanned_monthly_csvs={scanned_csvs} total_issues={total_issues}"
    )

    if total_issues:
        sys.exit(1)


if __name__ == "__main__":
    main()
