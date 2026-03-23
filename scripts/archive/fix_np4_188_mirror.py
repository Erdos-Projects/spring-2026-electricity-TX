#!/usr/bin/env python3
"""Fix mirrored postDateTime in NP4-188-CD monthly CSVs.

The issue: from Nov 2017 onward, monthly CSVs have postDateTime sorted
descending while DeliveryDate is sorted ascending within the same file.
Each (postDateTime, DeliveryDate) pair is wrong, but the avg delta is
still ~1 day because the correct pairings are just in reverse order.

Fix: for each month, re-pair postDateTime values with delivery dates by
sorting both ascending — since postDateTime[i] = DeliveryDate[i] - 1 day,
sorting both ascending naturally restores the correct 1-day relationship.

Run from project root:
    python3 scripts/fix_np4_188_mirror.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from datetime import datetime
from pathlib import Path

DATASET_ID = "NP4-188-CD"
RAW_ROOT = Path("data/raw/ercot")
DD_COL = "DeliveryDate"
PDT_COL = "postDateTime"
DD_FMT = "%m/%d/%Y"


def is_mirrored(df_rows: list[dict], dd_col: str, pdt_col: str) -> bool:
    """Return True if postDateTime and DeliveryDate are sorted in opposite orders."""
    pairs = []
    for row in df_rows:
        pdt = str(row.get(pdt_col, "")).strip()[:10]
        dd = str(row.get(dd_col, "")).strip()
        try:
            delta = (datetime.strptime(dd, DD_FMT).date()
                     - datetime.fromisoformat(pdt).date()).days
            pairs.append(delta)
        except Exception:
            pass
    if not pairs:
        return False
    mn, mx = min(pairs), max(pairs)
    # Clean: all deltas == 1. Mirrored: spans both negative and large positive.
    return mn < 0 and mx > 5


def fix_month(path: Path, dry_run: bool) -> str:
    """Re-pair postDateTime values with delivery dates. Returns status string."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if not rows or PDT_COL not in fieldnames or DD_COL not in fieldnames:
        return "skip:no_data"

    if not is_mirrored(rows, DD_COL, PDT_COL):
        return "ok:not_mirrored"

    # Collect unique delivery dates and postDateTimes, sorted ascending.
    dd_to_pdt: dict[str, str] = {}
    for row in rows:
        dd = str(row.get(DD_COL, "")).strip()
        pdt = str(row.get(PDT_COL, "")).strip()
        if dd and pdt:
            dd_to_pdt.setdefault(dd, pdt)

    try:
        sorted_dds = sorted(dd_to_pdt.keys(),
                            key=lambda s: datetime.strptime(s, DD_FMT))
        sorted_pdts = sorted(dd_to_pdt.values(),
                             key=lambda s: s[:10])
    except Exception as exc:
        return f"error:sort_failed:{exc}"

    if len(sorted_dds) != len(sorted_pdts):
        return "error:length_mismatch"

    # Map each delivery date to its correct postDateTime (sorted ascending → aligned).
    correct_pdt: dict[str, str] = {dd: pdt for dd, pdt in zip(sorted_dds, sorted_pdts)}

    # Rewrite rows with corrected postDateTime.
    fixed_rows = []
    for row in rows:
        dd = str(row.get(DD_COL, "")).strip()
        new_pdt = correct_pdt.get(dd)
        if new_pdt:
            row = dict(row)
            row[PDT_COL] = new_pdt
        fixed_rows.append(row)

    if dry_run:
        # Verify fix resolves the mirror.
        still_mirrored = is_mirrored(fixed_rows, DD_COL, PDT_COL)
        return f"dry_run:would_fix (still_mirrored={still_mirrored})"

    # Write back atomically via a temp file.
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(fixed_rows)
    tmp.replace(path)
    return f"fixed:{len(sorted_dds)}_days"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Check and report without writing.")
    args = parser.parse_args()

    dataset_dir = RAW_ROOT / DATASET_ID
    if not dataset_dir.exists():
        raise SystemExit(f"Dataset dir not found: {dataset_dir}")

    paths = sorted(p for p in dataset_dir.glob("**/*.csv")
                   if "__" not in p.stem
                   and ".docids" not in p.name
                   and ".sortcache" not in p.name)

    fixed = skipped = errors = 0
    for path in paths:
        status = fix_month(path, dry_run=args.dry_run)
        tag = status.split(":")[0]
        print(f"  {path.name:<32} {status}")
        if tag in ("fixed", "dry_run"):
            fixed += 1
        elif tag == "ok":
            skipped += 1
        else:
            errors += 1

    print(f"\n{'DRY RUN — ' if args.dry_run else ''}Summary: "
          f"fixed={fixed} already_ok={skipped} errors={errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
