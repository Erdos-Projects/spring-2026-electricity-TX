#!/usr/bin/env python3
"""Dataset catalog for ERCOT Texas electricity analysis."""

from __future__ import annotations

from typing import Dict, Iterable, List, Sequence

DATASETS: Dict[str, Dict[str, str]] = {
    "NP6-346-CD": {
        "title": "Actual System Load by Forecast Zone",
        "category": "load",
        "reason": "Baseline demand signal for every hour/interval across ERCOT forecast zones.",
    },
    "NP3-565-CD": {
        "title": "Seven-Day Load Forecast by Model and Weather Zone",
        "category": "load_forecast",
        "reason": "Primary public load forecast series by weather zone for demand forecast analysis.",
    },
    "NP4-732-CD": {
        "title": "Wind Power Production - Hourly Averaged Actual and Forecasted Values",
        "category": "renewables",
        "reason": "Core variable for wind variability, curtailment pressure, and price impact analysis.",
    },
    "NP4-745-CD": {
        "title": "Solar Power Production - Hourly Averaged Actual and Forecasted Values by Geographical Region",
        "category": "renewables",
        "reason": "Needed for solar ramp and net-load analysis in daylight hours.",
    },
    "NP6-905-CD": {
        "title": "Settlement Point Prices at Resource Nodes, Hubs and Load Zones",
        "category": "real_time_prices",
        "reason": "Primary real-time price series for node/zone/hub spatial analysis.",
    },
    "NP6-788-CD": {
        "title": "LMPs by Resource Nodes, Load Zones and Trading Hubs",
        "category": "real_time_prices",
        "reason": "Detailed nodal/zonal/hub LMP feed for spatial price analysis.",
    },
    "NP4-523-CD": {
        "title": "DAM System Lambda",
        "category": "day_ahead_prices",
        "reason": "Day-ahead benchmark needed for DA vs RT spread analysis.",
    },
    "NP6-331-CD": {
        "title": "Real-Time Clearing Prices for Capacity by 15-Minute Settlement Interval",
        "category": "ancillary_services",
        "reason": "Tracks real-time ancillary service price pressure and reserve scarcity.",
    },
    "NP4-188-CD": {
        "title": "DAM Clearing Prices for Capacity",
        "category": "ancillary_services",
        "reason": "Day-ahead ancillary service pricing to pair with RT capacity prices.",
    },
    "NP3-233-CD": {
        "title": "Hourly Resource Outage Capacity",
        "category": "reliability",
        "reason": "Critical reliability input for explaining scarcity and price spikes.",
    },
    "NP3-911-ER": {
        "title": "COP HSL and Actual Output for WGRs, PVGRs and ONLRTPF",
        "category": "renewables",
        "reason": "Resource-level capability vs output for deeper renewable performance diagnostics.",
    },
    "NP3-912-ER": {
        "title": "Temperature and Weather Zone Load Forecast",
        "category": "weather",
        "reason": "Weather driver for load sensitivity, peak risk, and seasonality analysis.",
    },
}

PROFILES: Dict[str, Dict[str, object]] = {
    "core": {
        "description": "Minimum dataset for price, load, renewables, and outage analysis.",
        "datasets": [
            "NP6-346-CD",
            "NP3-565-CD",
            "NP4-732-CD",
            "NP4-745-CD",
            "NP6-905-CD",
            "NP4-523-CD",
            "NP3-233-CD",
        ],
    },
    "market": {
        "description": "Detailed market pricing and ancillary service behavior.",
        "datasets": [
            "NP6-905-CD",
            "NP6-788-CD",
            "NP4-523-CD",
            "NP6-331-CD",
            "NP4-188-CD",
        ],
    },
    "reliability": {
        "description": "Generation adequacy and stress conditions.",
        "datasets": [
            "NP3-233-CD",
            "NP3-911-ER",
            "NP3-912-ER",
            "NP6-346-CD",
            "NP3-565-CD",
        ],
    },
    "all": {
        "description": "All datasets in this catalog.",
        "datasets": list(DATASETS.keys()),
    },
}

DEFAULT_PROFILE = "core"

# Maintain compatibility with older/incorrect IDs seen in earlier scripts.
DATASET_ID_ALIASES: Dict[str, str] = {
    "NP6-348-CD": "NP3-565-CD",
    "NP6-787-ER": "NP4-523-CD",
    "NP6-788-ER": "NP6-788-CD",
    "NP6-86-CP": "NP6-331-CD",
    "NP6-975-MCPE": "NP4-188-CD",
}


def available_profiles() -> List[str]:
    return sorted(PROFILES.keys())


def normalize_dataset_ids(dataset_ids: Iterable[str]) -> List[str]:
    unique: List[str] = []
    seen = set()
    for dataset_id in dataset_ids:
        cleaned = dataset_id.strip().upper()
        cleaned = DATASET_ID_ALIASES.get(cleaned, cleaned)
        if not cleaned or cleaned in seen:
            continue
        unique.append(cleaned)
        seen.add(cleaned)
    return unique


def resolve_dataset_ids(
    profile_names: Sequence[str] | None,
    explicit_dataset_ids: Sequence[str] | None,
) -> List[str]:
    selected: List[str] = []
    seen = set()

    selected_profiles = list(profile_names or [DEFAULT_PROFILE])
    for profile in selected_profiles:
        if profile not in PROFILES:
            raise KeyError(f"Unknown profile '{profile}'. Choices: {', '.join(available_profiles())}")
        for dataset_id in PROFILES[profile]["datasets"]:  # type: ignore[index]
            canonical_id = DATASET_ID_ALIASES.get(dataset_id, dataset_id)
            if canonical_id not in seen:
                selected.append(canonical_id)
                seen.add(canonical_id)

    for dataset_id in normalize_dataset_ids(explicit_dataset_ids or []):
        if dataset_id not in seen:
            selected.append(dataset_id)
            seen.add(dataset_id)

    return selected
