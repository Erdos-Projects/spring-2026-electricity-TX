#!/usr/bin/env python3
"""
Move per-doc source files from data/raw/ercot/<DS>/ to data/archive/ercot/<DS>/
once the monthly consolidated CSVs are in place.

Per-doc source files: any file NOT matching <DS>_<YYYYMM>.csv, *.docids, *.sortcache.json
Monthly CSVs and their sidecars (.docids, .sortcache.json) stay in data/raw/.

Usage:
    python3 scripts/archive_raw_sources.py [--dataset DS] [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path


RAW_ROOT = Path("data/raw/ercot")
ARCHIVE_ROOT = Path("data/archive/ercot")

# Patterns to KEEP in raw/ (monthly CSVs and their sidecars)
_KEEP = re.compile(r"^[A-Z0-9-]+_\d{6}(\.csv(\.docids|\.sortcache\.json)?)?$")


def is_monthly_sidecar(path: Path) -> bool:
    return bool(_KEEP.match(path.name))


def archive_dataset(ds: str, dry_run: bool = False) -> tuple[int, int]:
    raw_ds = RAW_ROOT / ds
    arc_ds = ARCHIVE_ROOT / ds
    if not raw_ds.exists():
        print(f"  [SKIP] {ds}: not found in {RAW_ROOT}")
        return 0, 0

    moved, skipped = 0, 0
    for src in sorted(raw_ds.rglob("*")):
        if src.is_dir():
            continue
        if is_monthly_sidecar(src):
            skipped += 1
            continue

        # Preserve YYYY/MM sub-structure
        rel = src.relative_to(raw_ds)
        dst = arc_ds / rel
        if dst.exists():
            print(f"  [EXISTS] {rel} — skipping")
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] would move: {src} → {dst}")
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), dst)
        moved += 1

    return moved, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", "-d", action="append", dest="datasets",
                        help="Dataset ID(s) to archive (default: all in data/raw/ercot)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be moved without moving anything")
    args = parser.parse_args()

    datasets = args.datasets or [p.name for p in sorted(RAW_ROOT.iterdir()) if p.is_dir()]

    total_moved = total_skipped = 0
    for ds in datasets:
        print(f"\n── {ds} {'[DRY-RUN] ' if args.dry_run else ''}──")
        moved, skipped = archive_dataset(ds, dry_run=args.dry_run)
        print(f"   moved={moved}  kept/skipped={skipped}")
        total_moved += moved
        total_skipped += skipped

    print(f"\nDone. Total moved={total_moved}  kept/skipped={total_skipped}")


if __name__ == "__main__":
    main()
