#!/usr/bin/env python3
"""Show recommended ERCOT datasets for analysis profiles."""

from __future__ import annotations

import argparse
import json
from typing import Dict, List

from ercot_dataset_catalog import DATASETS, PROFILES, available_profiles, resolve_dataset_ids


def _build_payload(selected_ids: List[str], selected_profiles: List[str]) -> Dict[str, object]:
    dataset_rows = []
    for dataset_id in selected_ids:
        metadata = DATASETS.get(dataset_id, {})
        dataset_rows.append(
            {
                "dataset_id": dataset_id,
                "title": metadata.get("title", "Unknown dataset"),
                "category": metadata.get("category", "unknown"),
                "reason": metadata.get("reason", "No reason provided in catalog."),
            }
        )

    return {
        "selected_profiles": selected_profiles,
        "profiles": {name: PROFILES[name]["description"] for name in selected_profiles},
        "dataset_count": len(dataset_rows),
        "datasets": dataset_rows,
    }


def _print_text(payload: Dict[str, object]) -> None:
    profile_names: List[str] = payload["selected_profiles"]  # type: ignore[assignment]
    print("Recommended ERCOT datasets")
    print("==========================")
    print(f"Profiles: {', '.join(profile_names)}")
    for name in profile_names:
        print(f"- {name}: {PROFILES[name]['description']}")

    print("")
    print(f"Total datasets: {payload['dataset_count']}")
    for row in payload["datasets"]:  # type: ignore[assignment]
        print(
            f"- {row['dataset_id']}: {row['title']} "
            f"[{row['category']}]"
        )
        print(f"  reason: {row['reason']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        action="append",
        choices=available_profiles(),
        help="Analysis profile to include. Can be passed multiple times. Defaults to 'core'.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Extra dataset ID to include (for example NP6-905-CD). Can be passed multiple times.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of text output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profiles = args.profile or ["core"]
    selected_ids = resolve_dataset_ids(profiles, args.dataset)
    payload = _build_payload(selected_ids, profiles)
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        _print_text(payload)


if __name__ == "__main__":
    main()
