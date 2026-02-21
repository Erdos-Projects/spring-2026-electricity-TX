#!/usr/bin/env python3
"""Check true API earliest date by dataset tier and estimate full dataset size.

Usage:
  export ERCOT_API_USERNAME="..."
  export ERCOT_API_PASSWORD="..."
  export ERCOT_SUBSCRIPTION_KEY="..."
  python3 scripts/check_api_earliest_and_size_estimate.py --to-date 2026-02-21
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from download_ercot_public_reports import (  # type: ignore
    API_BASE_URL,
    DEFAULT_CLIENT_ID,
    DEFAULT_SCOPE,
    TOKEN_URL,
    ErcotPublicReportsClient,
    authenticate,
    find_earliest_available_date,
    maybe_product_archive_href,
    parse_date,
)


TIER_DATASETS: List[Tuple[str, str]] = [
    ("Tier 1", "NP6-346-CD"),
    ("Tier 1", "NP6-905-CD"),
    ("Tier 1", "NP4-732-CD"),
    ("Tier 1", "NP4-745-CD"),
    ("Tier 1", "NP3-233-CD"),
    ("Tier 2", "NP3-565-CD"),
    ("Tier 2", "NP4-523-CD"),
    ("Tier 2", "NP6-788-CD"),
    ("Tier 3", "NP6-331-CD"),
    ("Tier 3", "NP4-188-CD"),
    ("Tier 3", "NP3-911-ER"),
    ("Tier 3", "NP3-912-ER"),
]


def months_inclusive(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def format_bytes(value: Optional[float]) -> str:
    if value is None:
        return "-"
    units = ["B", "K", "M", "G", "T"]
    amount = float(value)
    unit = 0
    while amount >= 1024 and unit < len(units) - 1:
        amount /= 1024
        unit += 1
    if unit == 0:
        return f"{int(amount)}{units[unit]}"
    return f"{amount:.1f}{units[unit]}"


def local_monthly_avg_bytes(dataset_id: str, outdir: Path) -> Tuple[Optional[float], float, int]:
    root = outdir / dataset_id
    if not root.exists():
        return None, 0.0, 0

    current_total = 0.0
    for p in root.rglob("*"):
        if p.is_file():
            current_total += p.stat().st_size

    monthly_sizes: List[Tuple[int, int, int]] = []
    for year_dir in root.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            month = int(month_dir.name)
            monthly_csv = month_dir / f"{dataset_id}_{year}{month:02d}.csv"
            if monthly_csv.exists():
                monthly_sizes.append((year, month, monthly_csv.stat().st_size))

    if not monthly_sizes:
        return None, current_total, 0

    monthly_sizes.sort()
    recent = monthly_sizes[-5:] if len(monthly_sizes) >= 5 else monthly_sizes
    avg = sum(size for _, _, size in recent) / len(recent)
    return avg, current_total, len(monthly_sizes)


def parse_args() -> argparse.Namespace:
    today = date.today()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", help="ERCOT username. Falls back to ERCOT_API_USERNAME.")
    parser.add_argument("--password", help="ERCOT password. Falls back to ERCOT_API_PASSWORD.")
    parser.add_argument(
        "--subscription-key",
        help="ERCOT subscription key. Falls back to ERCOT_SUBSCRIPTION_KEY.",
    )
    parser.add_argument(
        "--to-date",
        type=parse_date,
        default=today,
        help="End date for earliest-date search and month count (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--search-from",
        type=parse_date,
        default=date(2000, 1, 1),
        help="Search start date for earliest-date detection (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--outdir",
        default="data/raw/ercot",
        help="Local raw data directory used for size estimation.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=60, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=8, help="HTTP retry count.")
    parser.add_argument(
        "--retry-sleep-seconds",
        type=float,
        default=4.0,
        help="Retry backoff factor for API requests.",
    )
    parser.add_argument(
        "--archive-listing-retries",
        type=int,
        default=20,
        help="Retries for archive listing during earliest-date detection.",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=1.0,
        help="Minimum delay between API requests.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Optional dataset ID filter (repeatable). If omitted, all tier datasets are checked.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    username = args.username or os.getenv("ERCOT_API_USERNAME")
    password = args.password or os.getenv("ERCOT_API_PASSWORD")
    subscription_key = args.subscription_key or os.getenv("ERCOT_SUBSCRIPTION_KEY")
    if not username or not password or not subscription_key:
        raise SystemExit(
            "Missing credentials. Set ERCOT_API_USERNAME/ERCOT_API_PASSWORD/ERCOT_SUBSCRIPTION_KEY "
            "or pass --username/--password/--subscription-key."
        )

    token = authenticate(
        username=username,
        password=password,
        client_id=DEFAULT_CLIENT_ID,
        scope=DEFAULT_SCOPE,
        token_url=TOKEN_URL,
        timeout_seconds=args.timeout_seconds,
    )

    client = ErcotPublicReportsClient(
        bearer_token=token,
        subscription_key=subscription_key,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        retry_sleep_seconds=args.retry_sleep_seconds,
        request_interval_seconds=args.request_interval_seconds,
        reauth_config={
            "username": username,
            "password": password,
            "client_id": DEFAULT_CLIENT_ID,
            "scope": DEFAULT_SCOPE,
            "token_url": TOKEN_URL,
            "timeout_seconds": args.timeout_seconds,
        },
    )

    try:
        products = client.list_public_reports()
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed to list public reports catalog: {exc}") from exc

    product_by_id = {
        str(product.get("emilId", "")).upper().strip(): product
        for product in products
        if str(product.get("emilId", "")).strip()
    }

    outdir = Path(args.outdir)

    print(
        "tier\tdataset\tapi_earliest\tmonths_to_end\t"
        "local_months\tavg_month_local\test_total_size\tcurrent_local_size\tnote"
    )

    selected = {item.strip().upper() for item in args.dataset if item.strip()}
    dataset_rows = [row for row in TIER_DATASETS if not selected or row[1] in selected]
    if not dataset_rows:
        raise SystemExit("No datasets selected after --dataset filter.")

    total = len(dataset_rows)
    for index, (tier, dataset_id) in enumerate(dataset_rows, start=1):
        print(f"[{index}/{total}] checking {dataset_id}...", flush=True)
        product = product_by_id.get(dataset_id, {})
        archive_url = maybe_product_archive_href(product) or f"{API_BASE_URL}/archive/{dataset_id.lower()}"

        avg_month_local, current_local_size, local_months = local_monthly_avg_bytes(dataset_id, outdir)

        note = "ok"
        earliest: Optional[date] = None
        try:
            earliest = find_earliest_available_date(
                client=client,
                archive_url=archive_url,
                dataset_id=dataset_id,
                search_from=args.search_from,
                search_to=args.to_date,
                archive_listing_retries=args.archive_listing_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            note = f"earliest_lookup_failed: {str(exc).replace(chr(9), ' ')}"

        if earliest is None:
            months = "-"
            est_total_size = "-"
            earliest_text = "-"
            if note == "ok":
                note = "no_docs_in_window"
        else:
            months_int = months_inclusive(earliest, args.to_date)
            months = str(months_int)
            earliest_text = earliest.isoformat()
            if avg_month_local is None:
                est_total_size = "-"
                note = "no_local_monthly_size_for_estimate"
            else:
                est_total_size = format_bytes(avg_month_local * months_int)

        print(
            f"{tier}\t{dataset_id}\t{earliest_text}\t{months}\t{local_months}\t"
            f"{format_bytes(avg_month_local)}\t{est_total_size}\t{format_bytes(current_local_size)}\t{note}"
        , flush=True)


if __name__ == "__main__":
    main()
