#!/usr/bin/env python3
"""Download ERCOT public report data by dataset ID and date range."""

from __future__ import annotations

import argparse
import calendar
import csv
import io
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlparse

import requests
try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency fallback
    yaml = None
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional progress bar
    def tqdm(iterable, **_):  # type: ignore[misc]
        return iterable

from ercot_dataset_catalog import (
    DATASETS,
    available_profiles,
    normalize_dataset_ids,
    resolve_dataset_ids,
)

TOKEN_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
)
API_BASE_URL = "https://api.ercot.com/api/public-reports"
DEFAULT_CLIENT_ID = "fec253ea-0d06-4272-a5e6-b478baeecd70"
DEFAULT_SCOPE = f"openid {DEFAULT_CLIENT_ID} offline_access"
EARLIEST_ARCHIVE_FROM = date(2000, 1, 1)
DEFAULT_TO_DATE = date(2025, 12, 31)
DEFAULT_RANGE_YEARS = 10
DEFAULT_FROM_DATE = date(DEFAULT_TO_DATE.year - DEFAULT_RANGE_YEARS + 1, 1, 1)
CSV_PARSE_CACHE_SIZE = 262144
MONTHLY_SORT_CACHE_VERSION = 1
POST_DATETIME_COLUMN = "postDateTime"
POST_DATETIME_COLUMN_ALIASES = (
    POST_DATETIME_COLUMN,
    "postDatetime",
    "PostingTime",
    "post_datetime",
)
# TODO(after-full-download): Evaluate storage-format migration (.csv.gz or parquet).


@dataclass
class DownloadStats:
    downloaded: int = 0
    skipped_existing: int = 0
    skipped_missing_doc_id: int = 0
    skipped_unavailable_dataset: int = 0
    consolidated_updates: int = 0
    monthly_sorted: int = 0
    monthly_already_sorted: int = 0
    monthly_sort_skipped: int = 0
    monthly_sort_failures: int = 0
    failures: int = 0


class TeeStream:
    def __init__(self, *streams: object) -> None:
        self.streams = streams

    def write(self, text: str) -> int:
        for stream in self.streams:
            stream.write(text)
        return len(text)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()

    def isatty(self) -> bool:
        return any(getattr(stream, "isatty", lambda: False)() for stream in self.streams)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _log_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "null"
    text = str(value)
    if re.fullmatch(r"[A-Za-z0-9._:/+\-]+", text):
        return text
    return json.dumps(text, ensure_ascii=True)


def log_event(event: str, **fields: object) -> None:
    parts = [event]
    for key, value in fields.items():
        parts.append(f"{key}={_log_value(value)}")
    print(" ".join(parts))


def extract_zip_from_memory(byte_data: bytes) -> Dict[str, bytes]:
    # Read ZIP payload bytes directly from memory.
    zip_buffer = io.BytesIO(byte_data)
    with zipfile.ZipFile(zip_buffer) as archive:
        extracted_content: Dict[str, bytes] = {}
        for file_name in archive.namelist():
            with archive.open(file_name) as handle:
                extracted_content[file_name] = handle.read()
        return extracted_content


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    raise ValueError(f"Cannot parse boolean from value '{value}'.")


def load_config_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    suffix = path.suffix.lower()
    raw = path.read_text(encoding="utf-8")
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise SystemExit("PyYAML is required for --config .yaml files. Install with: pip install pyyaml")
        data = yaml.safe_load(raw)
    elif suffix == ".json":
        data = json.loads(raw)
    else:
        raise SystemExit("Unsupported config file extension. Use .yaml/.yml or .json.")
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise SystemExit("Config root must be a mapping/object.")
    return data


def flatten_config(data: Dict[str, Any]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                flattened[f"{key}_{nested_key}"] = nested_value
        else:
            flattened[key] = value
    return flattened


def _coerce_config_date(value: object, key: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise SystemExit(f"Config key '{key}' must be YYYY-MM-DD.") from exc
    raise SystemExit(f"Config key '{key}' must be a date string (YYYY-MM-DD).")


def _coerce_config_list(value: object, key: str) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        items: List[str] = []
        for item in value:
            if item is None:
                continue
            items.append(str(item))
        return items
    raise SystemExit(f"Config key '{key}' must be a string or list of strings.")


def config_to_parser_defaults(config_data: Dict[str, Any]) -> Dict[str, Any]:
    cfg = flatten_config(config_data)
    defaults: Dict[str, Any] = {}
    removed_date_keys = [
        key
        for key in ("window_months", "download_window_months", "time_duration", "download_time_duration")
        if key in cfg
    ]
    if removed_date_keys:
        raise SystemExit("Deprecated date-range keys are not supported. Use from_date and optional to_date only.")
    scalar_map = {
        "username": "username",
        "credentials_username": "username",
        "password": "password",
        "credentials_password": "password",
        "subscription_key": "subscription_key",
        "credentials_subscription_key": "subscription_key",
        "outdir": "outdir",
        "download_outdir": "outdir",
        "page_size": "page_size",
        "download_page_size": "page_size",
        "network_page_size": "page_size",
        "max_docs_per_dataset": "max_docs_per_dataset",
        "download_max_docs_per_dataset": "max_docs_per_dataset",
        "timeout_seconds": "timeout_seconds",
        "download_timeout_seconds": "timeout_seconds",
        "network_timeout_seconds": "timeout_seconds",
        "max_retries": "max_retries",
        "download_max_retries": "max_retries",
        "network_max_retries": "max_retries",
        "retry_sleep_seconds": "retry_sleep_seconds",
        "download_retry_sleep_seconds": "retry_sleep_seconds",
        "network_retry_sleep_seconds": "retry_sleep_seconds",
        "archive_listing_retries": "archive_listing_retries",
        "download_archive_listing_retries": "archive_listing_retries",
        "network_archive_listing_retries": "archive_listing_retries",
        "archive_progress_pages": "archive_progress_pages",
        "download_archive_progress_pages": "archive_progress_pages",
        "network_archive_progress_pages": "archive_progress_pages",
        "max_consecutive_network_failures": "max_consecutive_network_failures",
        "download_max_consecutive_network_failures": "max_consecutive_network_failures",
        "network_max_consecutive_network_failures": "max_consecutive_network_failures",
        "network_failure_cooldown_seconds": "network_failure_cooldown_seconds",
        "download_network_failure_cooldown_seconds": "network_failure_cooldown_seconds",
        "network_network_failure_cooldown_seconds": "network_failure_cooldown_seconds",
        "file_timing_frequency": "file_timing_frequency",
        "download_file_timing_frequency": "file_timing_frequency",
        "sort_monthly_output": "sort_monthly_output",
        "download_sort_monthly_output": "sort_monthly_output",
        "monthly_sort_strategy": "monthly_sort_strategy",
        "download_monthly_sort_strategy": "monthly_sort_strategy",
        "download_order": "download_order",
        "download_download_order": "download_order",
        "request_interval_seconds": "request_interval_seconds",
        "download_request_interval_seconds": "request_interval_seconds",
        "network_request_interval_seconds": "request_interval_seconds",
        "token_url": "token_url",
        "auth_token_url": "token_url",
        "client_id": "client_id",
        "auth_client_id": "client_id",
        "scope": "scope",
        "auth_scope": "scope",
        "state_dir": "state_dir",
        "resume_state_dir": "state_dir",
        "logs_dir": "logs_dir",
        "logging_logs_dir": "logs_dir",
    }
    bool_map = {
        "from_earliest_available": "from_earliest_available",
        "download_from_earliest_available": "from_earliest_available",
        "auto_detect_earliest_per_dataset": "auto_detect_earliest_per_dataset",
        "download_auto_detect_earliest_per_dataset": "auto_detect_earliest_per_dataset",
        "datasets_only": "datasets_only",
        "download_datasets_only": "datasets_only",
        "extract_zips": "extract_zips",
        "download_extract_zips": "extract_zips",
        "consolidate_monthly": "consolidate_monthly",
        "download_consolidate_monthly": "consolidate_monthly",
        "delete_source_after_consolidation": "delete_source_after_consolidation",
        "download_delete_source_after_consolidation": "delete_source_after_consolidation",
        "dry_run": "dry_run",
        "download_dry_run": "dry_run",
        "list_api_products": "list_api_products",
        "sort_existing_monthly": "sort_existing_monthly",
        "download_sort_existing_monthly": "sort_existing_monthly",
        "write_manifest": "write_manifest",
        "download_write_manifest": "write_manifest",
        "disable_bulk_download": "disable_bulk_download",
        "download_disable_bulk_download": "disable_bulk_download",
        "print_file_timing": "print_file_timing",
        "download_print_file_timing": "print_file_timing",
        "resume_state": "resume_state",
        "resume_resume_state": "resume_state",
    }

    for source_key, target_key in scalar_map.items():
        if source_key in cfg:
            defaults[target_key] = cfg[source_key]
    for source_key, target_key in bool_map.items():
        if source_key in cfg:
            defaults[target_key] = _parse_bool(cfg[source_key])

    if "from_date" in cfg:
        defaults["from_date"] = _coerce_config_date(cfg["from_date"], "from_date")
    elif "download_from_date" in cfg:
        defaults["from_date"] = _coerce_config_date(cfg["download_from_date"], "download_from_date")
    if "to_date" in cfg:
        defaults["to_date"] = _coerce_config_date(cfg["to_date"], "to_date")
    elif "download_to_date" in cfg:
        defaults["to_date"] = _coerce_config_date(cfg["download_to_date"], "download_to_date")

    list_map = {
        "profile": "profile",
        "profiles": "profile",
        "download_profiles": "profile",
        "dataset": "dataset",
        "datasets": "dataset",
        "download_datasets": "dataset",
        "exclude_dataset": "exclude_dataset",
        "exclude_datasets": "exclude_dataset",
        "download_exclude_datasets": "exclude_dataset",
    }
    for source_key, target_key in list_map.items():
        if source_key in cfg:
            defaults[target_key] = _coerce_config_list(cfg[source_key], source_key)
    return defaults


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def format_exception_message(exc: BaseException) -> str:
    text = str(exc).strip()
    if text:
        return text
    rep = repr(exc).strip()
    if rep and rep != f"{type(exc).__name__}()":
        return rep
    return type(exc).__name__


def load_dataset_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def save_dataset_state(state_path: Path, payload: Dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    payload["updated_at"] = utc_now_iso()
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(state_path)


def dataset_docs_cache_path(state_dir: Path, dataset_id: str) -> Path:
    return state_dir / f"{dataset_id}.archive_docs.jsonl"


def load_archive_docs_cache(cache_path: Path) -> Tuple[List[Dict[str, Any]], int]:
    docs: List[Dict[str, Any]] = []
    max_page = 0
    if not cache_path.exists():
        return docs, max_page
    with open(cache_path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and "doc" in payload:
                page = _safe_int(payload.get("page"), 0)
                doc = payload.get("doc")
            else:
                page = _safe_int(payload.get("page"), 0) if isinstance(payload, dict) else 0
                doc = payload
            if not isinstance(doc, dict):
                continue
            row = dict(doc)
            if page > 0:
                row["__archive_page"] = page
            docs.append(row)
            if page > max_page:
                max_page = page
    return docs, max_page


def append_archive_docs_cache(cache_path: Path, page: int, docs: List[Dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "a", encoding="utf-8") as handle:
        for doc in docs:
            payload = {"page": page, "doc": doc}
            handle.write(json.dumps(payload, separators=(",", ":")))
            handle.write("\n")


def stats_as_dict(stats: DownloadStats) -> Dict[str, int]:
    return {
        "downloaded": stats.downloaded,
        "skipped_existing": stats.skipped_existing,
        "skipped_missing_doc_id": stats.skipped_missing_doc_id,
        "skipped_unavailable_dataset": stats.skipped_unavailable_dataset,
        "consolidated_updates": stats.consolidated_updates,
        "monthly_sorted": stats.monthly_sorted,
        "monthly_already_sorted": stats.monthly_already_sorted,
        "monthly_sort_skipped": stats.monthly_sort_skipped,
        "monthly_sort_failures": stats.monthly_sort_failures,
        "failures": stats.failures,
    }


def dataset_state_is_compatible(
    state: Dict[str, Any],
    *,
    dataset_id: str,
    window_from: date,
    window_to: date,
    page_size: int,
    download_order: str,
    max_docs_per_dataset: int,
    archive_url: str,
) -> bool:
    if not state:
        return False
    return (
        str(state.get("dataset_id", "")) == dataset_id
        and str(state.get("window_from", "")) == window_from.isoformat()
        and str(state.get("window_to", "")) == window_to.isoformat()
        and _safe_int(state.get("page_size"), -1) == page_size
        and str(state.get("download_order", "")) == download_order
        and _safe_int(state.get("max_docs_per_dataset"), -1) == max_docs_per_dataset
        and str(state.get("archive_url", "")) == archive_url
    )


def initial_dataset_state(
    *,
    dataset_id: str,
    window_from: date,
    window_to: date,
    page_size: int,
    download_order: str,
    max_docs_per_dataset: int,
    archive_url: str,
) -> Dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "window_from": window_from.isoformat(),
        "window_to": window_to.isoformat(),
        "page_size": page_size,
        "download_order": download_order,
        "max_docs_per_dataset": max_docs_per_dataset,
        "archive_url": archive_url,
        "status": "running",
        "listing_complete": False,
        "last_listed_page": 0,
        "total_listed_docs": 0,
        "next_doc_index": 0,
        "last_completed_doc_id": None,
        "last_completed_stampdate": None,
        "last_completed_page": 0,
    }


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def parse_bulk_chunk_size(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--bulk-chunk-size must be an integer.") from exc
    if parsed < 1 or parsed > 2048:
        raise argparse.ArgumentTypeError("--bulk-chunk-size must be between 1 and 2048.")
    return parsed


def cli_repeatable_values(argv: Sequence[str], flag: str) -> List[str]:
    values: List[str] = []
    index = 0
    prefix = f"{flag}="
    while index < len(argv):
        token = argv[index]
        if token == "--":
            break
        if token == flag:
            if index + 1 < len(argv):
                next_token = argv[index + 1]
                if not next_token.startswith("-"):
                    values.append(next_token)
                    index += 2
                    continue
            index += 1
            continue
        if token.startswith(prefix):
            value = token[len(prefix) :].strip()
            if value:
                values.append(value)
        index += 1
    return values


def to_start_iso(value: date) -> str:
    return datetime(value.year, value.month, value.day, 0, 0, 0).isoformat()


def to_end_iso(value: date) -> str:
    return datetime(value.year, value.month, value.day, 23, 59, 59).isoformat()


def parse_api_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    candidate = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def safe_filename(value: str) -> str:
    trimmed = value.strip()
    trimmed = trimmed.replace("\\", "_").replace("/", "_")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", trimmed) or "ercot_document.bin"


def expected_size(metadata: Dict[str, object]) -> int:
    raw = metadata.get("size")
    if raw is None:
        return -1
    try:
        return int(raw)
    except (TypeError, ValueError):
        return -1


def parse_retry_after_seconds(value: Optional[str]) -> float:
    if not value:
        return 0.0
    value = value.strip()
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        # HTTP-date Retry-After values are uncommon here; use default backoff.
        return 0.0


def is_name_resolution_failure(exc: BaseException) -> bool:
    text = str(exc).lower()
    markers = (
        "nameresolutionerror",
        "failed to resolve",
        "nodename nor servname provided",
        "temporary failure in name resolution",
        "name or service not known",
        "getaddrinfo failed",
    )
    return any(marker in text for marker in markers)


def extract_doc_id(doc: Dict[str, object]) -> str:
    for key in ("docId", "docLookupId", "doclookupId"):
        value = doc.get(key)
        if value is None:
            continue
        candidate = str(value).strip()
        if candidate:
            return candidate
    return ""


def read_text_fallback(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def read_doc_csv_text(path: Path) -> str:
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, "r") as archive:
            members = [name for name in archive.namelist() if not name.endswith("/")]
            if not members:
                return ""
            preferred = [name for name in members if name.lower().endswith(".csv")]
            target = preferred[0] if preferred else members[0]
            return read_text_fallback(archive.read(target))
    return read_text_fallback(path.read_bytes())


def detect_post_datetime_column(fieldnames: Sequence[str]) -> Optional[str]:
    normalized = {
        name.strip().lower(): name
        for name in fieldnames
        if isinstance(name, str) and name.strip()
    }
    for candidate in POST_DATETIME_COLUMN_ALIASES:
        found = normalized.get(candidate.lower())
        if found:
            return found
    return None


def migrate_monthly_csv_add_post_datetime(monthly_path: Path) -> List[str]:
    with open(monthly_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        fieldnames = list(reader.fieldnames)
        posting_col = detect_post_datetime_column(fieldnames)
        if posting_col is not None:
            return fieldnames
        rows = list(reader)

    migrated_fieldnames = [POST_DATETIME_COLUMN] + fieldnames
    temp_path = monthly_path.with_suffix(f"{monthly_path.suffix}.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=migrated_fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            row[POST_DATETIME_COLUMN] = ""
            writer.writerow(row)
    temp_path.replace(monthly_path)
    return migrated_fieldnames


def append_doc_to_monthly_csv(
    source_path: Path,
    monthly_path: Path,
    post_datetime: str = "",
) -> int:
    csv_text = read_doc_csv_text(source_path)
    if not csv_text.strip():
        return 0
    lines = csv_text.splitlines()
    if not lines:
        return 0
    monthly_path.parent.mkdir(parents=True, exist_ok=True)
    has_existing = monthly_path.exists() and monthly_path.stat().st_size > 0
    existing_has_post_datetime = False
    if has_existing:
        with open(monthly_path, "r", encoding="utf-8", newline="") as handle:
            existing_reader = csv.reader(handle)
            existing_header = next(existing_reader, [])
        existing_has_post_datetime = detect_post_datetime_column(existing_header) is not None

    # Keep column alignment stable once monthly output has a postDateTime column,
    # even when current archive metadata has blank postDatetime.
    use_structured_append = bool(post_datetime) or existing_has_post_datetime
    if use_structured_append:
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return 0
        source_fieldnames = list(reader.fieldnames)
        rows = list(reader)
        if not rows:
            return 0

        source_posting_col = detect_post_datetime_column(source_fieldnames)
        target_posting_col = POST_DATETIME_COLUMN
        target_fieldnames: List[str]

        if has_existing:
            # Existing monthly files from older runs may not have postDateTime.
            # Upgrade once so all future appends use a stable schema.
            existing_fieldnames = migrate_monthly_csv_add_post_datetime(monthly_path)
            if existing_fieldnames:
                target_fieldnames = existing_fieldnames
                existing_posting_col = detect_post_datetime_column(existing_fieldnames)
                if existing_posting_col:
                    target_posting_col = existing_posting_col
                else:
                    target_fieldnames = [target_posting_col] + existing_fieldnames
            else:
                has_existing = False

        if not has_existing:
            excluded = {target_posting_col}
            if source_posting_col and source_posting_col != target_posting_col:
                excluded.add(source_posting_col)
            target_fieldnames = [target_posting_col] + [
                name for name in source_fieldnames if name not in excluded
            ]

        with open(monthly_path, "a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=target_fieldnames,
                extrasaction="ignore",
                lineterminator="\n",
            )
            if not has_existing:
                writer.writeheader()
            for row in rows:
                if source_posting_col and source_posting_col != target_posting_col:
                    source_value = str(row.get(source_posting_col) or "").strip()
                    row[target_posting_col] = source_value or post_datetime
                elif source_posting_col == target_posting_col:
                    current = str(row.get(target_posting_col) or "").strip()
                    if not current:
                        row[target_posting_col] = post_datetime
                else:
                    row[target_posting_col] = post_datetime
                writer.writerow(row)
        return len(rows)

    # Legacy path: raw line copy when no postDateTime is available in archive metadata.
    payload = lines[1:] if has_existing else lines
    if not payload:
        return 0
    with open(monthly_path, "a", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(payload))
        handle.write("\n")
    return len(payload)


def authenticate(
    username: str,
    password: str,
    client_id: str,
    scope: str,
    token_url: str,
    timeout_seconds: int,
) -> str:
    response = requests.post(
        token_url,
        headers={"content-type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "password",
            "client_id": client_id,
            "scope": scope,
            "response_type": "id_token",
            "username": username,
            "password": password,
        },
        timeout=timeout_seconds,
    )
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                detail = (
                    str(payload.get("error_description") or payload.get("error") or "")
                ).strip()
        except Exception:  # noqa: BLE001
            detail = ""
        if not detail:
            detail = (response.text or "").strip()
        if not detail:
            detail = "No error payload returned by token endpoint."
        raise RuntimeError(
            f"Token request failed (HTTP {response.status_code}): {detail}"
        )
    response.raise_for_status()
    payload = response.json()
    token = payload.get("id_token") or payload.get("access_token")
    if not token:
        raise RuntimeError("Authentication succeeded but no id_token/access_token was returned.")
    return token


def _find_first_list_of_dicts(payload: object) -> Optional[List[Dict[str, object]]]:
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
        if rows:
            return rows
        for item in payload:
            nested = _find_first_list_of_dicts(item)
            if nested:
                return nested
        return None

    if isinstance(payload, dict):
        preferred_keys = (
            "items",
            "value",
            "data",
            "results",
            "records",
            "documents",
            "reports",
            "publicReports",
            "archives",
            "_embedded",
        )
        for key in preferred_keys:
            if key not in payload:
                continue
            nested = _find_first_list_of_dicts(payload[key])
            if nested:
                return nested
        for value in payload.values():
            nested = _find_first_list_of_dicts(value)
            if nested:
                return nested
    return None


def _looks_like_empty_archive_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    if "product" not in payload:
        return False

    # Typical empty archive payload includes metadata and product details only.
    if set(payload.keys()).issubset({"_links", "_meta", "product"}):
        meta = payload.get("_meta")
        if isinstance(meta, dict):
            for key in ("count", "total", "totalCount", "totalRecords", "totalElements", "recordCount"):
                raw = meta.get(key)
                if raw is None:
                    continue
                try:
                    if int(raw) == 0:
                        return True
                except (TypeError, ValueError):
                    continue
        # If count fields are absent, still treat this shape as an empty archive.
        return True
    return False


def coerce_list(payload: object) -> List[Dict[str, object]]:
    rows = _find_first_list_of_dicts(payload)
    if rows is not None:
        return rows

    if _looks_like_empty_archive_payload(payload):
        return []

    if isinstance(payload, dict):
        keys = ", ".join(sorted(payload.keys()))
        raise RuntimeError(
            "Unexpected API response shape. No list of objects was found. "
            f"Top-level keys: {keys}"
        )
    raise RuntimeError("Unexpected API response shape. No list of objects was found.")


class ErcotPublicReportsClient:
    def __init__(
        self,
        bearer_token: str,
        subscription_key: str,
        timeout_seconds: int,
        max_retries: int,
        retry_sleep_seconds: float,
        request_interval_seconds: float,
        reauth_config: Optional[Dict[str, object]] = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_sleep_seconds = retry_sleep_seconds
        self.request_interval_seconds = max(0.0, request_interval_seconds)
        self.next_request_at = 0.0
        self.reauth_config = reauth_config
        self.session = requests.Session()
        # Keep headers minimal. For archive downloads, default Requests negotiation
        # is more robust than forcing Accept/User-Agent values.
        self.session.headers.update(
            {
                "Authorization": f"Bearer {bearer_token}",
                "Ocp-Apim-Subscription-Key": subscription_key,
            }
        )

    def _refresh_bearer_token(self) -> bool:
        if not self.reauth_config:
            return False
        username = str(self.reauth_config.get("username", ""))
        password = str(self.reauth_config.get("password", ""))
        client_id = str(self.reauth_config.get("client_id", ""))
        scope = str(self.reauth_config.get("scope", ""))
        token_url = str(self.reauth_config.get("token_url", ""))
        timeout_seconds = int(self.reauth_config.get("timeout_seconds", self.timeout_seconds))
        if not all((username, password, client_id, scope, token_url)):
            return False
        token = authenticate(
            username=username,
            password=password,
            client_id=client_id,
            scope=scope,
            token_url=token_url,
            timeout_seconds=timeout_seconds,
        )
        self.session.headers["Authorization"] = f"Bearer {token}"
        return True

    def _request(
        self,
        method: str,
        url: str,
        *,
        stream: bool = False,
        **kwargs: object,
    ) -> requests.Response:
        refreshed_auth = False
        for attempt in range(1, self.max_retries + 1):
            try:
                if self.request_interval_seconds > 0:
                    now = time.monotonic()
                    if now < self.next_request_at:
                        time.sleep(self.next_request_at - now)
                response = self.session.request(
                    method,
                    url,
                    timeout=self.timeout_seconds,
                    stream=stream,
                    **kwargs,
                )
                self.next_request_at = time.monotonic() + self.request_interval_seconds
                if response.status_code == 401 and attempt < self.max_retries and not refreshed_auth:
                    response.close()
                    if self._refresh_bearer_token():
                        refreshed_auth = True
                        continue
                if response.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    retry_after = parse_retry_after_seconds(response.headers.get("Retry-After"))
                    response.close()
                    time.sleep(max(self.retry_sleep_seconds * attempt, retry_after))
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.retry_sleep_seconds * attempt)
        raise RuntimeError("Retry loop exhausted unexpectedly.")

    def list_public_reports(self) -> List[Dict[str, object]]:
        response = self._request("GET", API_BASE_URL)
        return coerce_list(response.json())

    def iter_archive_docs(
        self,
        archive_url: str,
        post_datetime_from: str,
        post_datetime_to: str,
        page_size: int,
    ) -> Iterator[Dict[str, object]]:
        page = 1
        while True:
            rows = self.list_archive_page(
                archive_url=archive_url,
                post_datetime_from=post_datetime_from,
                post_datetime_to=post_datetime_to,
                page_size=page_size,
                page=page,
            )
            if not rows:
                break
            for row in rows:
                yield row
            if len(rows) < page_size:
                break
            page += 1

    def list_archive_page(
        self,
        archive_url: str,
        post_datetime_from: str,
        post_datetime_to: str,
        page_size: int,
        page: int = 1,
    ) -> List[Dict[str, object]]:
        response = self._request(
            "GET",
            archive_url,
            params={
                "postDatetimeFrom": post_datetime_from,
                "postDatetimeTo": post_datetime_to,
                "size": page_size,
                "page": page,
            },
        )
        return coerce_list(response.json())

    def download_doc(
        self,
        report_id: str,
        doc_id: str,
        destination: Path,
        archive_doc: Dict[str, object],
    ) -> None:
        candidates = build_download_candidates(report_id, doc_id, archive_doc)
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        for index, (url, params) in enumerate(candidates):
            try:
                with self._request(
                    "GET",
                    url,
                    params=params,
                    stream=True,
                ) as response:
                    with open(tmp_path, "wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                tmp_path.replace(destination)
                return
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                # Try the next candidate on lookup misses or transient throttling.
                if status in (400, 404, 429, 500, 502, 503, 504) and index < len(candidates) - 1:
                    continue
                raise
        raise RuntimeError("All download URL candidates failed.")

    def download_docs(
        self,
        report_id: str,
        doc_ids: List[str],
        strict_count: bool = True,
    ) -> Dict[str, bytes]:
        """Bulk-download multiple docs via a single POST request.

        When *strict_count* is True (default, used by the main downloader) the
        method raises if the response does not contain exactly the requested
        number of documents.  Pass ``strict_count=False`` for recovery/backfill
        callers that want to accept a partial response gracefully.
        """
        ret: Dict[str, bytes] = {}
        url = f"https://api.ercot.com/api/public-reports/archive/{report_id}/download"
        with self._request(
            "POST",
            url,
            json={"docIds": doc_ids},
        ) as response:
            unzipped = extract_zip_from_memory(response.content)
            if strict_count and len(unzipped) != len(doc_ids):
                raise RuntimeError(
                    "Bulk response count mismatch: "
                    f"requested={len(doc_ids)} returned={len(unzipped)}."
                )
            for filename, zipped_doc in unzipped.items():
                doc_id = filename.split(".", 1)[0]
                try:
                    inner_unzipped = extract_zip_from_memory(zipped_doc)
                except Exception:
                    if strict_count:
                        raise
                    continue
                if len(inner_unzipped) != 1:
                    if strict_count:
                        raise RuntimeError(
                            "Bulk nested ZIP payload mismatch: "
                            f"doc_id={doc_id or '-'} files={len(inner_unzipped)}."
                        )
                    if not inner_unzipped:
                        continue
                doc_content = list(inner_unzipped.items())[0][1]
                ret[doc_id] = doc_content
        return ret


def choose_filename(doc: Dict[str, object]) -> str:
    for key in ("constructedName", "friendlyName"):
        raw = doc.get(key)
        if isinstance(raw, str) and raw.strip():
            return safe_filename(raw)
    doc_id = str(doc.get("docId", "")).strip()
    return safe_filename(f"{doc_id}.bin" if doc_id else "ercot_document.bin")


def with_doc_id_suffix(filename: str, doc_id: str) -> str:
    # Some archive docs share the same constructed filename.
    # Append doc ID so files do not overwrite each other within a month folder.
    base, ext = os.path.splitext(filename)
    return f"{base}__{doc_id}{ext}" if doc_id else filename


def monthly_csv_path(outdir: Path, dataset_id: str, dataset_subdir: Path) -> Path:
    parts = dataset_subdir.parts
    if len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
        year = parts[-2]
        month = parts[-1]
        return outdir / dataset_id / year / month / f"{dataset_id}_{year}{month}.csv"
    return outdir / dataset_id / dataset_subdir / f"{dataset_id}_undated.csv"


def monthly_csv_month_start(path: Path, dataset_root: Path) -> Optional[date]:
    try:
        relative = path.relative_to(dataset_root)
    except ValueError:
        return None
    parts = relative.parts
    if len(parts) < 3:
        return None
    year_text, month_text = parts[0], parts[1]
    if not (year_text.isdigit() and month_text.isdigit()):
        return None
    year = int(year_text)
    month = int(month_text)
    if month < 1 or month > 12:
        return None
    return date(year, month, 1)


def monthly_csv_in_window(path: Path, dataset_root: Path, window_start: date, window_end: date) -> bool:
    month_start = monthly_csv_month_start(path, dataset_root)
    if month_start is None:
        return False
    window_month_start = date(window_start.year, window_start.month, 1)
    window_month_end = date(window_end.year, window_end.month, 1)
    return window_month_start <= month_start <= window_month_end


def marker_path_for_monthly(monthly_path: Path) -> Path:
    return monthly_path.with_suffix(monthly_path.suffix + ".docids")


def load_marker_doc_ids(marker_path: Path) -> Set[str]:
    if not marker_path.exists():
        return set()
    doc_ids = set()
    with open(marker_path, "r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip()
            if value:
                doc_ids.add(value)
    return doc_ids


def append_marker_doc_id(marker_path: Path, doc_id: str) -> None:
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    with open(marker_path, "a", encoding="utf-8") as handle:
        handle.write(f"{doc_id}\n")


def maybe_href(doc: Dict[str, object], rel: str) -> Optional[str]:
    links = doc.get("_links")
    if not isinstance(links, dict):
        return None
    rel_obj = links.get(rel)
    if not isinstance(rel_obj, dict):
        return None
    href = rel_obj.get("href")
    if isinstance(href, str) and href.strip():
        return href.strip()
    return None


def maybe_product_archive_href(product: Dict[str, object]) -> Optional[str]:
    links = product.get("_links")
    if not isinstance(links, dict):
        return None
    archive = links.get("archive")
    if not isinstance(archive, dict):
        return None
    href = archive.get("href")
    if isinstance(href, str) and href.strip():
        return href.strip()
    return None


def build_download_candidates(
    report_id: str,
    doc_id: str,
    archive_doc: Dict[str, object],
) -> List[Tuple[str, Optional[Dict[str, str]]]]:
    candidates: List[Tuple[str, Optional[Dict[str, str]]]] = []
    for rel in ("download", "file", "endpoint", "self"):
        href = maybe_href(archive_doc, rel)
        if not href:
            continue
        parsed = urlparse(href)
        query_keys = {key.lower() for key in parse_qs(parsed.query).keys()}
        if {"docid", "doclookupid", "download"} & query_keys:
            candidates.append((href, None))
            continue
        candidates.append((href, {"docId": doc_id}))
        candidates.append((href, {"docLookupId": doc_id}))
        candidates.append((href, {"doclookupId": doc_id}))
        candidates.append((href, None))

    fallback_base = f"{API_BASE_URL}/{report_id.lower()}"
    candidates.append((fallback_base, {"docId": doc_id}))
    candidates.append((fallback_base, {"docLookupId": doc_id}))
    candidates.append((fallback_base, {"doclookupId": doc_id}))

    deduped: List[Tuple[str, Optional[Dict[str, str]]]] = []
    seen = set()
    for url, params in candidates:
        key = (url, tuple(sorted((params or {}).items())))
        if key in seen:
            continue
        seen.add(key)
        deduped.append((url, params))
    return deduped


def dataset_subdir_from_doc(doc: Dict[str, object]) -> Path:
    parsed = parse_api_datetime(str(doc.get("postDatetime", "")).strip())
    if parsed is None:
        return Path("undated")
    return Path(parsed.strftime("%Y")) / parsed.strftime("%m")


def maybe_extract_zip(path: Path) -> None:
    if path.suffix.lower() != ".zip":
        return
    target_dir = path.parent.resolve()
    with zipfile.ZipFile(path, "r") as archive:
        for member in archive.namelist():
            member_resolved = (target_dir / member).resolve()
            if not str(member_resolved).startswith(str(target_dir) + os.sep) and member_resolved != target_dir:
                raise RuntimeError(
                    f"ZIP path traversal rejected: member {member!r} resolves outside target directory."
                )
        archive.extractall(path.parent)


def list_selected_datasets(dataset_ids: Iterable[str]) -> None:
    for dataset_id in dataset_ids:
        metadata = DATASETS.get(dataset_id, {})
        title = metadata.get("title", "Unknown dataset")
        reason = metadata.get("reason", "No reason in catalog.")
        log_event("DATASET_SELECTED", dataset=dataset_id, title=title, reason=reason)


def list_archive_docs_with_retries(
    client: ErcotPublicReportsClient,
    archive_url: str,
    dataset_id: str,
    post_datetime_from: str,
    post_datetime_to: str,
    page_size: int,
    archive_listing_retries: int,
    retry_sleep_seconds: float,
    progress_every_pages: int,
    start_page: int = 1,
    seed_docs: Optional[List[Dict[str, Any]]] = None,
    on_page_listed: Optional[Callable[[int, List[Dict[str, Any]], int], None]] = None,
) -> List[Dict[str, object]]:
    docs: List[Dict[str, object]] = list(seed_docs or [])
    page = max(1, start_page)
    _list_pbar = tqdm(desc=f"Listing {dataset_id}", unit="page", leave=False)
    try:
        while True:
            listing_attempt = 0
            while True:
                try:
                    rows = client.list_archive_page(
                        archive_url=archive_url,
                        post_datetime_from=post_datetime_from,
                        post_datetime_to=post_datetime_to,
                        page_size=page_size,
                        page=page,
                    )
                    break
                except requests.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else None
                    if status == 429 and listing_attempt < archive_listing_retries:
                        listing_attempt += 1
                        retry_after = (
                            parse_retry_after_seconds(exc.response.headers.get("Retry-After"))
                            if exc.response is not None
                            else 0.0
                        )
                        cooldown_seconds = max(
                            retry_after,
                            retry_sleep_seconds * (2 ** listing_attempt),
                        )
                        log_event(
                            "ARCHIVE_LISTING_RETRY",
                            dataset=dataset_id,
                            page=page,
                            attempt=f"{listing_attempt}/{archive_listing_retries}",
                            sleep_seconds=f"{cooldown_seconds:.1f}",
                            reason="http_429",
                        )
                        time.sleep(cooldown_seconds)
                        continue
                    raise

            if not rows:
                break
            tagged_rows: List[Dict[str, Any]] = []
            for row in rows:
                tagged = dict(row)
                tagged["__archive_page"] = page
                tagged_rows.append(tagged)
            docs.extend(tagged_rows)
            _list_pbar.update(1)
            _list_pbar.set_postfix(docs=len(docs))
            if on_page_listed is not None:
                on_page_listed(page, tagged_rows, len(docs))
            if progress_every_pages > 0 and (page == 1 or page % progress_every_pages == 0):
                log_event(
                    "ARCHIVE_LISTING_PROGRESS",
                    dataset=dataset_id,
                    page=page,
                    docs_collected=len(docs),
                )
            if len(rows) < page_size:
                break
            page += 1
    finally:
        _list_pbar.close()
    return docs


def archive_window_has_docs(
    client: ErcotPublicReportsClient,
    archive_url: str,
    dataset_id: str,
    window_start: date,
    window_end: date,
    archive_listing_retries: int,
    retry_sleep_seconds: float,
) -> bool:
    post_datetime_from = to_start_iso(window_start)
    post_datetime_to = to_end_iso(window_end)
    listing_attempt = 0
    while True:
        try:
            first_page = client.list_archive_page(
                archive_url=archive_url,
                post_datetime_from=post_datetime_from,
                post_datetime_to=post_datetime_to,
                page_size=1,
                page=1,
            )
            return bool(first_page)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 429 and listing_attempt < archive_listing_retries:
                listing_attempt += 1
                retry_after = (
                    parse_retry_after_seconds(exc.response.headers.get("Retry-After"))
                    if exc.response is not None
                    else 0.0
                )
                cooldown_seconds = max(
                    retry_after,
                    retry_sleep_seconds * (2 ** listing_attempt),
                )
                log_event(
                    "ARCHIVE_PROBE_RETRY",
                    dataset=dataset_id,
                    attempt=f"{listing_attempt}/{archive_listing_retries}",
                    sleep_seconds=f"{cooldown_seconds:.1f}",
                    reason="http_429",
                )
                time.sleep(cooldown_seconds)
                continue
            raise


def find_earliest_available_date(
    client: ErcotPublicReportsClient,
    archive_url: str,
    dataset_id: str,
    search_from: date,
    search_to: date,
    archive_listing_retries: int,
    retry_sleep_seconds: float,
) -> Optional[date]:
    if search_from > search_to:
        return None

    # Probe earliest availability from coarse to fine: year, then month, then day.
    for year in range(search_from.year, search_to.year + 1):
        year_start = max(search_from, date(year, 1, 1))
        year_end = min(search_to, date(year, 12, 31))
        if year_start > year_end:
            continue
        if not archive_window_has_docs(
            client=client,
            archive_url=archive_url,
            dataset_id=dataset_id,
            window_start=year_start,
            window_end=year_end,
            archive_listing_retries=archive_listing_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        ):
            continue

        for month in range(year_start.month, year_end.month + 1):
            month_start = max(year_start, date(year, month, 1))
            month_last_day = calendar.monthrange(year, month)[1]
            month_end = min(year_end, date(year, month, month_last_day))
            if month_start > month_end:
                continue
            if not archive_window_has_docs(
                client=client,
                archive_url=archive_url,
                dataset_id=dataset_id,
                window_start=month_start,
                window_end=month_end,
                archive_listing_retries=archive_listing_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            ):
                continue

            day_count = (month_end - month_start).days + 1
            for offset in range(day_count):
                day = month_start + timedelta(days=offset)
                if archive_window_has_docs(
                    client=client,
                    archive_url=archive_url,
                    dataset_id=dataset_id,
                    window_start=day,
                    window_end=day,
                    archive_listing_retries=archive_listing_retries,
                    retry_sleep_seconds=retry_sleep_seconds,
                ):
                    return day

            # Fallback: month-level probe succeeded but no day-level hit was found.
            return month_start

        # Fallback: year-level probe succeeded but no month-level hit was found.
        return year_start

    return None


def doc_post_datetime_for_sort(doc: Dict[str, object]) -> Optional[datetime]:
    parsed = parse_api_datetime(str(doc.get("postDatetime", "")).strip())
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def order_archive_docs(docs: List[Dict[str, object]], order: str) -> List[Dict[str, object]]:
    if order == "api":
        return docs

    decorated: List[Tuple[Optional[datetime], str, Dict[str, object]]] = []
    for doc in docs:
        decorated.append((doc_post_datetime_for_sort(doc), extract_doc_id(doc), doc))

    if order == "newest-first":
        sorted_rows = sorted(
            decorated,
            key=lambda row: (row[0] is not None, row[0] or datetime.min, row[1]),
            reverse=True,
        )
        return [row[2] for row in sorted_rows]

    if order == "oldest-first":
        sorted_rows = sorted(
            decorated,
            key=lambda row: (row[0] is None, row[0] or datetime.max, row[1]),
        )
        return [row[2] for row in sorted_rows]

    raise ValueError(f"Unknown download order '{order}'.")


@lru_cache(maxsize=CSV_PARSE_CACHE_SIZE)
def _parse_csv_date_cached(raw: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _parse_csv_date(value: str) -> Optional[datetime]:
    raw = value.strip()
    if not raw:
        return None
    return _parse_csv_date_cached(raw)


@lru_cache(maxsize=CSV_PARSE_CACHE_SIZE)
def _parse_hour_ending_cached(raw: str) -> Optional[Tuple[int, int]]:
    if ":" in raw:
        left = "".join(ch for ch in raw.split(":", 1)[0] if ch.isdigit())
    else:
        left = "".join(ch for ch in raw if ch.isdigit())
    if not left:
        return None
    hour = int(left)
    if hour < 0 or hour > 24:
        return None
    if hour == 24:
        return 23, 59
    return hour, 0


def _parse_hour_ending(value: str) -> Optional[Tuple[int, int]]:
    raw = value.strip()
    if not raw:
        return None
    return _parse_hour_ending_cached(raw)


@lru_cache(maxsize=CSV_PARSE_CACHE_SIZE)
def _parse_csv_datetime_cached(raw: str) -> Optional[datetime]:
    if ":" not in raw:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    parsed = parse_api_datetime(raw)
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_csv_datetime(value: str) -> Optional[datetime]:
    raw = value.strip()
    if not raw:
        return None
    return _parse_csv_datetime_cached(raw)


def _csv_row_timestamp(row: Dict[str, str], lower_to_name: Dict[str, str]) -> Optional[datetime]:
    def get(name: str) -> str:
        actual = lower_to_name.get(name.lower())
        if actual is None:
            return ""
        return str(row.get(actual, "") or "")

    # Single-column timestamps.
    for key in (
        "scedtimestamp",
        "scedtimestamputc",
        "deliveryinterval",
        "intervalending",
        "intervalend",
        "intervaltime",
        "datetime",
        "timestamp",
        "postingtime",
        "postdatetime",
        "hourendingdatetime",
        "deliverydatetime",
    ):
        parsed = _parse_csv_datetime(get(key))
        if parsed is not None:
            return parsed

    # Older wind files store full datetime in HOUR_ENDING.
    parsed_hour_ending_dt = _parse_csv_datetime(get("hour_ending"))
    if parsed_hour_ending_dt is not None:
        return parsed_hour_ending_dt

    # Date and hour column pairs.
    for date_key, hour_key in (
        ("deliverydate", "hourending"),
        ("delivery_date", "hour_ending"),
        ("operday", "hourending"),
        ("deliverydate", "deliveryhour"),
    ):
        day = _parse_csv_date(get(date_key))
        hm = _parse_hour_ending(get(hour_key))
        if day is not None and hm is not None:
            return day.replace(hour=hm[0], minute=hm[1])

    return None


def _csv_row_target_timestamp(row: Dict[str, str], lower_to_name: Dict[str, str]) -> Optional[datetime]:
    def get(name: str) -> str:
        actual = lower_to_name.get(name.lower())
        if actual is None:
            return ""
        return str(row.get(actual, "") or "")

    for key in (
        "deliveryinterval",
        "intervalending",
        "intervalend",
        "intervaltime",
        "hourendingdatetime",
        "deliverydatetime",
        "scedtimestamp",
        "scedtimestamputc",
        "datetime",
        "timestamp",
    ):
        parsed = _parse_csv_datetime(get(key))
        if parsed is not None:
            return parsed

    for date_key, hour_key in (
        ("deliverydate", "hourending"),
        ("delivery_date", "hour_ending"),
        ("deliverydate", "deliveryhour"),
        ("operday", "hourending"),
        ("operatingday", "hourending"),
        ("marketday", "hourending"),
        ("date", "hourending"),
    ):
        day = _parse_csv_date(get(date_key))
        hm = _parse_hour_ending(get(hour_key))
        if day is not None and hm is not None:
            return day.replace(hour=hm[0], minute=hm[1])

    return None


def _csv_row_issue_timestamp(row: Dict[str, str], lower_to_name: Dict[str, str]) -> Optional[datetime]:
    def get(name: str) -> str:
        actual = lower_to_name.get(name.lower())
        if actual is None:
            return ""
        return str(row.get(actual, "") or "")

    for key in (
        "postingtime",
        "postdatetime",
        "publishdatetime",
        "issuetime",
        "issue_datetime",
        "forecastissuedatetime",
        "createdatetime",
        "createdat",
    ):
        parsed = _parse_csv_datetime(get(key))
        if parsed is not None:
            return parsed

    for key in ("postingdate", "publishdate", "issuedate", "issue_date"):
        parsed_date = _parse_csv_date(get(key))
        if parsed_date is not None:
            return parsed_date

    return None


def resolve_monthly_sort_strategy(sort_strategy: str, lower_to_name: Dict[str, str]) -> str:
    if sort_strategy == "postdatetime":
        return "postdatetime"
    if sort_strategy == "timestamp":
        return "timestamp"
    if sort_strategy == "forecast-aware":
        return "forecast-aware"
    if sort_strategy != "auto":
        raise ValueError(f"Unknown monthly sort strategy '{sort_strategy}'.")

    available = set(lower_to_name)
    target_hints = {
        "deliveryinterval",
        "intervalending",
        "intervalend",
        "intervaltime",
        "hourendingdatetime",
        "deliverydatetime",
        "deliverydate",
        "delivery_date",
        "operday",
        "operatingday",
        "marketday",
        "hourending",
        "hour_ending",
        "deliveryhour",
    }
    issue_hints = {
        "postingtime",
        "postdatetime",
        "publishdatetime",
        "issuetime",
        "issue_datetime",
        "forecastissuedatetime",
        "createdatetime",
        "createdat",
        "postingdate",
        "publishdate",
        "issuedate",
        "issue_date",
    }
    has_target = any(key in available for key in target_hints)
    has_issue = any(key in available for key in issue_hints)
    return "forecast-aware" if has_target and has_issue else "timestamp"


def _csv_row_sort_key(
    row: Dict[str, str],
    lower_to_name: Dict[str, str],
    sort_strategy: str,
) -> Optional[Tuple[datetime, ...]]:
    if sort_strategy == "postdatetime":
        issue_time = _csv_row_issue_timestamp(row, lower_to_name)
        if issue_time is not None:
            return (issue_time,)
        fallback = _csv_row_timestamp(row, lower_to_name)
        return (fallback,) if fallback is not None else None

    if sort_strategy == "timestamp":
        timestamp = _csv_row_timestamp(row, lower_to_name)
        return (timestamp,) if timestamp is not None else None

    if sort_strategy == "forecast-aware":
        target_time = _csv_row_target_timestamp(row, lower_to_name)
        issue_time = _csv_row_issue_timestamp(row, lower_to_name)

        if target_time is None and issue_time is None:
            fallback = _csv_row_timestamp(row, lower_to_name)
            if fallback is None:
                return None
            return (fallback, fallback)
        if target_time is None:
            target_time = issue_time
        if issue_time is None:
            issue_time = target_time
        return (target_time, issue_time)

    raise ValueError(f"Unknown monthly sort strategy '{sort_strategy}'.")


def resolve_monthly_sort_order(sort_option: str, download_order: str) -> Optional[str]:
    if sort_option == "none":
        return None
    if sort_option == "ascending":
        return "ascending"
    if sort_option == "descending":
        return "descending"
    if sort_option == "match-download-order":
        if download_order == "newest-first":
            return "descending"
        return "ascending"
    raise ValueError(f"Unknown monthly sort option '{sort_option}'.")


def _monthly_sort_cache_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".sortcache.json")


def _monthly_sort_file_signature(path: Path) -> Tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _cache_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _read_monthly_sort_cache(path: Path) -> Optional[Dict[str, object]]:
    cache_path = _monthly_sort_cache_path(path)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _write_monthly_sort_cache(
    path: Path,
    *,
    sort_order: str,
    sort_strategy: str,
    classification: str,
    size_bytes: int,
    mtime_ns: int,
) -> None:
    cache_path = _monthly_sort_cache_path(path)
    payload = {
        "version": MONTHLY_SORT_CACHE_VERSION,
        "sort_order": sort_order,
        "sort_strategy": sort_strategy,
        "classification": classification,
        "size_bytes": size_bytes,
        "mtime_ns": mtime_ns,
        "updated_at": utc_now_iso(),
    }
    try:
        cache_path.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            encoding="utf-8",
        )
    except Exception:  # noqa: BLE001
        return


def _cached_monthly_sort_classification(
    path: Path,
    *,
    sort_order: str,
    sort_strategy: str,
    size_bytes: int,
    mtime_ns: int,
) -> Optional[str]:
    cached = _read_monthly_sort_cache(path)
    if cached is None:
        return None
    if _cache_int(cached.get("version")) != MONTHLY_SORT_CACHE_VERSION:
        return None
    if cached.get("sort_order") != sort_order:
        return None
    if cached.get("sort_strategy") != sort_strategy:
        return None
    if _cache_int(cached.get("size_bytes")) != size_bytes:
        return None
    if _cache_int(cached.get("mtime_ns")) != mtime_ns:
        return None
    classification = str(cached.get("classification") or "")
    if classification in {"sorted", "skipped"}:
        return classification
    return None


def sort_monthly_csv(path: Path, sort_order: str, sort_strategy: str = "auto") -> str:
    if sort_order not in {"ascending", "descending"}:
        raise ValueError(f"Unknown sort order '{sort_order}'.")
    try:
        size_before, mtime_before = _monthly_sort_file_signature(path)
    except OSError:
        size_before, mtime_before = -1, -1
    cached_classification = _cached_monthly_sort_classification(
        path,
        sort_order=sort_order,
        sort_strategy=sort_strategy,
        size_bytes=size_before,
        mtime_ns=mtime_before,
    )
    if cached_classification == "sorted":
        return "already"
    if cached_classification == "skipped":
        return "skipped"

    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            _write_monthly_sort_cache(
                path,
                sort_order=sort_order,
                sort_strategy=sort_strategy,
                classification="skipped",
                size_bytes=size_before,
                mtime_ns=mtime_before,
            )
            return "skipped"
        fieldnames = list(reader.fieldnames)
        lower_to_name = {name.lower(): name for name in fieldnames}
        effective_strategy = resolve_monthly_sort_strategy(sort_strategy, lower_to_name)
        parsed_rows: List[Tuple[Tuple[datetime, ...], Dict[str, str]]] = []
        unparsed_rows: List[Dict[str, str]] = []
        saw_rows = False
        saw_unparsed = False
        already_sorted = True
        previous_key: Optional[Tuple[datetime, ...]] = None

        for row in reader:
            saw_rows = True
            sort_key = _csv_row_sort_key(row, lower_to_name, effective_strategy)
            if sort_key is None:
                saw_unparsed = True
                unparsed_rows.append(row)
                continue

            parsed_rows.append((sort_key, row))

            # Output layout is parsed rows first, then unparsed rows.
            if saw_unparsed:
                already_sorted = False

            if previous_key is not None:
                if sort_order == "ascending" and sort_key < previous_key:
                    already_sorted = False
                elif sort_order == "descending" and sort_key > previous_key:
                    already_sorted = False
            previous_key = sort_key

    if not saw_rows:
        _write_monthly_sort_cache(
            path,
            sort_order=sort_order,
            sort_strategy=sort_strategy,
            classification="sorted",
            size_bytes=size_before,
            mtime_ns=mtime_before,
        )
        return "already"
    if not parsed_rows:
        _write_monthly_sort_cache(
            path,
            sort_order=sort_order,
            sort_strategy=sort_strategy,
            classification="skipped",
            size_bytes=size_before,
            mtime_ns=mtime_before,
        )
        return "skipped"
    if already_sorted:
        _write_monthly_sort_cache(
            path,
            sort_order=sort_order,
            sort_strategy=sort_strategy,
            classification="sorted",
            size_bytes=size_before,
            mtime_ns=mtime_before,
        )
        return "already"

    ordered_parsed = sorted(
        parsed_rows,
        key=lambda item: item[0],
        reverse=(sort_order == "descending"),
    )

    ordered_rows = [item[1] for item in ordered_parsed] + unparsed_rows

    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered_rows)
    try:
        size_after, mtime_after = _monthly_sort_file_signature(path)
    except OSError:
        size_after, mtime_after = size_before, mtime_before
    _write_monthly_sort_cache(
        path,
        sort_order=sort_order,
        sort_strategy=sort_strategy,
        classification="sorted",
        size_bytes=size_after,
        mtime_ns=mtime_after,
    )
    return "sorted"


def parse_args() -> argparse.Namespace:
    default_from = DEFAULT_FROM_DATE

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", help="Path to YAML/JSON config file.")
    pre_args, _ = pre_parser.parse_known_args()
    config_defaults: Dict[str, Any] = {}
    if pre_args.config:
        config_defaults = config_to_parser_defaults(load_config_file(Path(pre_args.config)))

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="Path to YAML/JSON config file.")
    parser.add_argument("--username", help="ERCOT API portal username. Falls back to ERCOT_API_USERNAME.")
    parser.add_argument("--password", help="ERCOT API portal password. Falls back to ERCOT_API_PASSWORD.")
    parser.add_argument(
        "--subscription-key",
        help="API subscription key. Falls back to ERCOT_SUBSCRIPTION_KEY.",
    )
    parser.add_argument(
        "--from-date",
        type=parse_date,
        default=default_from,
        help=(
            "Start date (YYYY-MM-DD). "
            f"Defaults to {default_from.isoformat()} "
            f"(10-year default range ending {DEFAULT_TO_DATE.isoformat()})."
        ),
    )
    parser.add_argument(
        "--to-date",
        type=parse_date,
        default=None,
        help=f"End date (YYYY-MM-DD). If omitted, defaults to {DEFAULT_TO_DATE.isoformat()}.",
    )
    parser.add_argument(
        "--from-earliest-available",
        action="store_true",
        help=(
            "Use an early floor start date (2000-01-01) so each selected dataset "
            "downloads from its earliest available archive records."
        ),
    )
    parser.add_argument(
        "--auto-detect-earliest-per-dataset",
        action="store_true",
        help=(
            "Probe archive availability and start each dataset at its earliest day "
            "between --from-date and --to-date."
        ),
    )
    parser.add_argument(
        "--profile",
        action="append",
        choices=available_profiles(),
        help="Dataset profile to include (repeatable). Defaults to 'core'.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Extra dataset ID (EMIL ID) to include (repeatable).",
    )
    parser.add_argument(
        "--datasets-only",
        action="store_true",
        help=(
            "Use only --dataset IDs (no profiles). "
            "When CLI --dataset is provided, it overrides configured dataset lists."
        ),
    )
    parser.add_argument(
        "--exclude-dataset",
        action="append",
        default=[],
        help="Dataset ID (EMIL ID) to exclude after profile + dataset selection (repeatable).",
    )
    parser.add_argument("--outdir", default="data/raw/ercot", help="Output directory for downloads.")
    parser.add_argument("--page-size", type=int, default=1000, help="Archive API page size.")
    parser.add_argument("--max-docs-per-dataset", type=int, default=0, help="0 means unlimited.")
    parser.add_argument(
        "--bulk-chunk-size",
        type=parse_bulk_chunk_size,
        default=256,
        help="Bulk download chunk size (natural number 1..2048). Smaller values reduce 429 throttling risk.",
    )
    parser.add_argument(
        "--disable-bulk-download",
        action="store_true",
        help="Skip bulk archive downloads and fetch files one-by-one via per-doc fallback.",
    )
    parser.add_argument("--extract-zips", action="store_true", help="Extract each downloaded ZIP archive.")
    parser.add_argument(
        "--consolidate-monthly",
        action="store_true",
        help="Append each archive doc into one monthly CSV per dataset (fewer files).",
    )
    parser.add_argument(
        "--delete-source-after-consolidation",
        action="store_true",
        help="Delete per-doc source files after successful monthly append.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show planned downloads without downloading files.")
    parser.add_argument("--list-api-products", action="store_true", help="List available products and exit.")
    parser.add_argument("--timeout-seconds", type=int, default=60, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=4, help="HTTP retry count.")
    parser.add_argument("--retry-sleep-seconds", type=float, default=1.5, help="Retry backoff factor.")
    parser.add_argument(
        "--archive-listing-retries",
        type=int,
        default=6,
        help="Extra retries for archive listing when a dataset hits HTTP 429.",
    )
    parser.add_argument(
        "--archive-progress-pages",
        type=int,
        default=10,
        help="Print archive listing progress every N pages (0 to disable).",
    )
    parser.add_argument(
        "--bulk-progress-every",
        type=int,
        default=10,
        help=(
            "Print BULK_REQUEST/BULK_DONE every N chunks "
            "(first and last chunks are always printed). "
            "Set 0 to print only first/last chunk progress."
        ),
    )
    parser.add_argument(
        "--max-consecutive-network-failures",
        type=int,
        default=25,
        help="Stop the run after this many consecutive DNS/network resolution failures.",
    )
    parser.add_argument(
        "--network-failure-cooldown-seconds",
        type=float,
        default=20.0,
        help="Sleep after a DNS/network resolution failure before continuing.",
    )
    parser.add_argument(
        "--print-file-timing",
        action="store_true",
        help=(
            "Legacy alias for --file-timing-frequency every-file. "
            "Print completion timestamp and elapsed seconds for each successfully processed file."
        ),
    )
    parser.add_argument(
        "--file-timing-frequency",
        choices=(
            "off",
            "every-file",
            "1-stampdate",
            "12-stampdates",
            "24-stampdates",
            "1-month",
            "daily",
            "bi-month",
            "tri-month",
            "quad-month",
        ),
        default=None,
        help=(
            "How often to print timing logs: per file, every N completed stampdates, every completed day, or every completed month. "
            "If omitted, defaults to 'off' unless --print-file-timing is set."
        ),
    )
    parser.add_argument(
        "--sort-monthly-output",
        choices=("none", "ascending", "descending", "match-download-order"),
        default="ascending",
        help=(
            "Post-sort each touched monthly CSV by timestamp after dataset processing. "
            "Default is 'ascending'. "
            "'match-download-order' uses descending for newest-first downloads, ascending otherwise."
        ),
    )
    parser.add_argument(
        "--monthly-sort-strategy",
        choices=("auto", "timestamp", "forecast-aware", "postdatetime"),
        default="auto",
        help=(
            "Sort key strategy for monthly CSV sorting. "
            "'auto' uses forecast-aware key when both target-time and issue-time columns exist, "
            "otherwise timestamp-only. "
            "'postdatetime' sorts by PostingTime/postDateTime fields when available."
        ),
    )
    parser.add_argument(
        "--sort-existing-monthly",
        action="store_true",
        help="Also sort already-existing monthly CSV files for each selected dataset.",
    )
    parser.add_argument(
        "--download-order",
        choices=("api", "newest-first", "oldest-first"),
        default="api",
        help="Processing order for archive docs after listing.",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=0.60,
        help="Minimum delay between API requests to reduce 429 throttling.",
    )
    parser.add_argument("--token-url", default=TOKEN_URL, help="Token endpoint URL.")
    parser.add_argument("--client-id", default=DEFAULT_CLIENT_ID, help="OIDC client_id for ERCOT token call.")
    parser.add_argument(
        "--scope",
        default=DEFAULT_SCOPE,
        help="OIDC scope for ERCOT token call.",
    )
    parser.add_argument(
        "--write-manifest",
        action="store_true",
        help="Write download metadata manifest JSON into output directory.",
    )
    parser.add_argument(
        "--state-dir",
        default="state",
        help="Directory for per-dataset resume checkpoint files.",
    )
    parser.add_argument(
        "--resume-state",
        dest="resume_state",
        action="store_true",
        default=True,
        help="Resume from checkpoint files in --state-dir (default: enabled).",
    )
    parser.add_argument(
        "--no-resume-state",
        dest="resume_state",
        action="store_false",
        help="Disable checkpoint resume for this run.",
    )
    parser.add_argument(
        "--logs-dir",
        default="logs/downloads",
        help="Directory where per-run logs are written.",
    )

    if config_defaults:
        parser.set_defaults(**config_defaults)

    args = parser.parse_args()
    cli_argv = sys.argv[1:]
    args.cli_dataset = normalize_dataset_ids(cli_repeatable_values(cli_argv, "--dataset"))
    return args


def main() -> None:
    args = parse_args()
    run_started_at = utc_now_iso()
    run_started_monotonic = time.monotonic()
    run_dir = Path(args.logs_dir) / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    run_log_path = run_dir / "run.log"
    failures_csv_path = run_dir / "failures.csv"
    summary_json_path = run_dir / "summary.json"

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    run_log_handle = open(run_log_path, "a", encoding="utf-8")
    failures_handle = open(failures_csv_path, "w", encoding="utf-8", newline="")
    failure_writer = csv.DictWriter(
        failures_handle,
        fieldnames=("timestamp", "dataset_id", "stage", "doc_id", "page", "error"),
    )
    failure_writer.writeheader()
    failures_handle.flush()
    sys.stdout = TeeStream(original_stdout, run_log_handle)
    sys.stderr = TeeStream(original_stderr, run_log_handle)

    stats = DownloadStats()
    manifest_rows: List[Dict[str, object]] = []
    dataset_summaries: Dict[str, Dict[str, Any]] = {}
    selected_ids: List[str] = []
    monthly_sort_order: Optional[str] = None
    summary_status = "completed"
    fatal_error: Optional[str] = None

    def record_failure(
        *,
        dataset_id: str,
        stage: str,
        error: str,
        doc_id: str = "",
        page: int = 0,
    ) -> None:
        failure_writer.writerow(
            {
                "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
                "dataset_id": dataset_id,
                "stage": stage,
                "doc_id": doc_id,
                "page": page,
                "error": error,
            }
        )
        failures_handle.flush()

    try:
        log_event("RUN_PATHS", run_dir=run_dir, run_log=run_log_path, failure_log=failures_csv_path)
        if args.config:
            log_event("RUN_CONFIG", config=args.config)

        username = args.username or os.getenv("ERCOT_API_USERNAME")
        password = args.password or os.getenv("ERCOT_API_PASSWORD")
        subscription_key = args.subscription_key or os.getenv("ERCOT_SUBSCRIPTION_KEY")
        if not username or not password or not subscription_key:
            raise SystemExit(
                "Missing credentials. Set --username/--password/--subscription-key "
                "or env vars ERCOT_API_USERNAME, ERCOT_API_PASSWORD, ERCOT_SUBSCRIPTION_KEY."
            )
        if args.from_earliest_available:
            args.from_date = EARLIEST_ARCHIVE_FROM
            log_event("DATE_MODE", mode="from_earliest_available", from_date=args.from_date.isoformat())
        if args.to_date is None:
            args.to_date = DEFAULT_TO_DATE
        if args.from_date > args.to_date:
            raise SystemExit("--from-date must be on or before --to-date.")
        if args.page_size <= 0:
            raise SystemExit("--page-size must be greater than 0.")
        try:
            args.bulk_chunk_size = int(args.bulk_chunk_size)
        except (TypeError, ValueError) as exc:
            raise SystemExit("--bulk-chunk-size must be an integer between 1 and 2048.") from exc
        if args.bulk_chunk_size < 1 or args.bulk_chunk_size > 2048:
            raise SystemExit("--bulk-chunk-size must be between 1 and 2048.")
        if args.bulk_progress_every < 0:
            raise SystemExit("--bulk-progress-every must be 0 or a positive integer.")
        if args.delete_source_after_consolidation and not args.consolidate_monthly:
            raise SystemExit("--delete-source-after-consolidation requires --consolidate-monthly.")
        monthly_sort_order = resolve_monthly_sort_order(args.sort_monthly_output, args.download_order)
        if args.sort_existing_monthly and monthly_sort_order is None:
            raise SystemExit("--sort-existing-monthly requires --sort-monthly-output not equal to 'none'.")
        if args.file_timing_frequency is None:
            args.file_timing_frequency = "every-file" if args.print_file_timing else "off"
        stampdate_thresholds = {
            "1-stampdate": 1,
            "12-stampdates": 12,
            "24-stampdates": 24,
        }
        calendar_day_schedules = {
            "bi-month": {1, 15},
            "tri-month": {1, 10, 20},
            "quad-month": {1, 7, 15, 22},
        }

        selected_profiles = args.profile
        explicit_cli_datasets = normalize_dataset_ids(getattr(args, "cli_dataset", []))
        if args.datasets_only:
            selected_profiles = []
            if explicit_cli_datasets:
                args.dataset = explicit_cli_datasets
                log_event("DATASET_MODE", mode="datasets_only_cli", datasets=",".join(args.dataset))
            elif args.dataset:
                args.dataset = normalize_dataset_ids(args.dataset)
                log_event("DATASET_MODE", mode="datasets_only_configured")
        elif selected_profiles is None:
            selected_profiles = ["core"]
        selected_ids = resolve_dataset_ids(selected_profiles, args.dataset)
        excluded_ids = set(normalize_dataset_ids(args.exclude_dataset or []))
        if excluded_ids:
            selected_ids = [dataset_id for dataset_id in selected_ids if dataset_id not in excluded_ids]
            log_event("DATASET_EXCLUDED", datasets=",".join(sorted(excluded_ids)))
        if not selected_ids:
            raise SystemExit("No datasets selected after exclusions.")
        list_selected_datasets(selected_ids)

        state_dir = Path(args.state_dir)
        if args.resume_state:
            state_dir.mkdir(parents=True, exist_ok=True)
            log_event("RESUME_STATE", enabled=True, state_dir=state_dir)
        else:
            log_event("RESUME_STATE", enabled=False, state_dir=state_dir)

        token = authenticate(
            username=username,
            password=password,
            client_id=args.client_id,
            scope=args.scope,
            token_url=args.token_url,
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
                "client_id": args.client_id,
                "scope": args.scope,
                "token_url": args.token_url,
                "timeout_seconds": args.timeout_seconds,
            },
        )

        try:
            public_reports = client.list_public_reports()
        except Exception as exc:  # noqa: BLE001
            if args.list_api_products:
                raise SystemExit(f"Could not list API products: {exc}") from exc
            log_event("CATALOG_WARN", message="unable_to_list_public_reports", error=str(exc))
            public_reports = []
        product_by_id: Dict[str, Dict[str, object]] = {}
        for product in public_reports:
            report_id = str(product.get("emilId", "")).upper().strip()
            if report_id:
                product_by_id[report_id] = product

        if args.list_api_products:
            for report_id in sorted(product_by_id):
                title = str(product_by_id[report_id].get("reportName", ""))
                log_event("API_PRODUCT", report_id=report_id, title=title)
            return

        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        marker_cache: Dict[Path, Set[str]] = {}
        consecutive_network_failures = 0

        for dataset_id in tqdm(selected_ids, desc="Datasets", unit="dataset"):
            product = product_by_id.get(dataset_id)
            if product_by_id and product is None:
                log_event(
                    "DATASET_SKIP",
                    dataset=dataset_id,
                    reason="not_in_current_catalog_for_subscription",
                    tip="run_with_list_api_products",
                )
                stats.skipped_unavailable_dataset += 1
                dataset_summaries[dataset_id] = {"status": "skipped_unavailable"}
                continue
            product = product or {}
            product_title = str(
                product.get("reportName") or product.get("name") or DATASETS.get(dataset_id, {}).get("title", "")
            ).strip()
            archive_url = maybe_product_archive_href(product) or f"{API_BASE_URL}/archive/{dataset_id.lower()}"
            log_event("DATASET_START", dataset=dataset_id, title=product_title, archive=archive_url)

            dataset_summary: Dict[str, Any] = {
                "title": product_title,
                "status": "running",
                "window_from": None,
                "window_to": args.to_date.isoformat(),
                "docs_listed": 0,
                "docs_processed": 0,
                "docs_downloaded": 0,
                "docs_failed": 0,
                "resume_start_page": 1,
                "resume_start_index": 0,
                "last_completed_page": 0,
                "last_completed_doc_id": None,
                "last_completed_stampdate": None,
            }
            dataset_summaries[dataset_id] = dataset_summary

            touched_monthly_paths: Set[Path] = set()
            current_stampdate: Optional[str] = None
            current_stampdate_files = 0
            current_stampdate_year = "-"
            current_stampdate_month = "-"
            completed_stampdates = 0
            current_date_key: Optional[str] = None
            current_date_files = 0
            current_date_year = "-"
            current_date_month = "-"
            current_month_key: Optional[str] = None
            current_month_files = 0
            printed_calendar_dates: Set[str] = set()
            dataset_from_date = args.from_date
            if args.auto_detect_earliest_per_dataset:
                try:
                    detected_from_date = find_earliest_available_date(
                        client=client,
                        archive_url=archive_url,
                        dataset_id=dataset_id,
                        search_from=args.from_date,
                        search_to=args.to_date,
                        archive_listing_retries=args.archive_listing_retries,
                        retry_sleep_seconds=args.retry_sleep_seconds,
                    )
                except Exception as exc:  # noqa: BLE001
                    stats.failures += 1
                    dataset_summary["status"] = "earliest_detection_failed"
                    record_failure(
                        dataset_id=dataset_id,
                        stage="earliest-date-detection",
                        error=str(exc),
                    )
                    log_event("EARLIEST_DATE_ERROR", dataset=dataset_id, error=str(exc))
                    continue
                if detected_from_date is None:
                    dataset_summary["status"] = "no_docs_in_window"
                    log_event(
                        "EARLIEST_DATE_EMPTY",
                        dataset=dataset_id,
                        search_from=args.from_date.isoformat(),
                        search_to=args.to_date.isoformat(),
                    )
                    continue
                dataset_from_date = detected_from_date
                log_event("EARLIEST_DATE_FOUND", dataset=dataset_id, from_date=dataset_from_date.isoformat())
            dataset_summary["window_from"] = dataset_from_date.isoformat()

            dataset_state_path = state_dir / f"{dataset_id}.json"
            dataset_cache_path = dataset_docs_cache_path(state_dir, dataset_id)
            dataset_state = initial_dataset_state(
                dataset_id=dataset_id,
                window_from=dataset_from_date,
                window_to=args.to_date,
                page_size=args.page_size,
                download_order=args.download_order,
                max_docs_per_dataset=args.max_docs_per_dataset,
                archive_url=archive_url,
            )
            cached_docs: List[Dict[str, Any]] = []
            resume_start_page = 1
            if args.resume_state:
                loaded_state = load_dataset_state(dataset_state_path)
                if dataset_state_is_compatible(
                    loaded_state,
                    dataset_id=dataset_id,
                    window_from=dataset_from_date,
                    window_to=args.to_date,
                    page_size=args.page_size,
                    download_order=args.download_order,
                    max_docs_per_dataset=args.max_docs_per_dataset,
                    archive_url=archive_url,
                ):
                    dataset_state.update(loaded_state)
                    cached_docs, cached_last_page = load_archive_docs_cache(dataset_cache_path)
                    dataset_state["last_listed_page"] = cached_last_page
                    dataset_state["total_listed_docs"] = len(cached_docs)
                    if cached_last_page == 0:
                        dataset_state["listing_complete"] = False
                    resume_start_page = _safe_int(dataset_state.get("last_listed_page"), 0) + 1
                    dataset_summary["resume_start_page"] = max(1, resume_start_page)
                    if cached_docs:
                        log_event(
                            "ARCHIVE_CACHE_LOADED",
                            dataset=dataset_id,
                            pages_le=dataset_state["last_listed_page"],
                            docs=len(cached_docs),
                        )
                else:
                    if dataset_cache_path.exists():
                        dataset_cache_path.unlink()
                dataset_state["status"] = "running"
                save_dataset_state(dataset_state_path, dataset_state)

            dataset_post_datetime_from = to_start_iso(dataset_from_date)
            dataset_post_datetime_to = to_end_iso(args.to_date)
            docs: List[Dict[str, Any]]
            listing_complete = bool(dataset_state.get("listing_complete"))
            if args.resume_state and listing_complete and dataset_cache_path.exists():
                docs, cached_last_page = load_archive_docs_cache(dataset_cache_path)
                dataset_state["last_listed_page"] = max(_safe_int(dataset_state.get("last_listed_page"), 0), cached_last_page)
                dataset_state["total_listed_docs"] = len(docs)
                log_event(
                    "ARCHIVE_CACHE_REUSED",
                    dataset=dataset_id,
                    pages=dataset_state["last_listed_page"],
                    docs=len(docs),
                )
            else:
                def on_page_listed(page: int, page_docs: List[Dict[str, Any]], total_docs: int) -> None:
                    if not args.resume_state:
                        return
                    append_archive_docs_cache(dataset_cache_path, page, page_docs)
                    dataset_state["last_listed_page"] = page
                    dataset_state["total_listed_docs"] = total_docs
                    dataset_state["listing_complete"] = False
                    dataset_state["status"] = "running"
                    save_dataset_state(dataset_state_path, dataset_state)

                try:
                    docs = list_archive_docs_with_retries(
                        client=client,
                        archive_url=archive_url,
                        dataset_id=dataset_id,
                        post_datetime_from=dataset_post_datetime_from,
                        post_datetime_to=dataset_post_datetime_to,
                        page_size=args.page_size,
                        archive_listing_retries=args.archive_listing_retries,
                        retry_sleep_seconds=args.retry_sleep_seconds,
                        progress_every_pages=args.archive_progress_pages,
                        start_page=resume_start_page,
                        seed_docs=cached_docs,
                        on_page_listed=on_page_listed if args.resume_state else None,
                    )
                except Exception as exc:  # noqa: BLE001
                    stats.failures += 1
                    dataset_summary["status"] = "archive_listing_failed"
                    record_failure(
                        dataset_id=dataset_id,
                        stage="archive-listing",
                        error=str(exc),
                        page=_safe_int(dataset_state.get("last_listed_page"), 0),
                    )
                    if args.resume_state:
                        dataset_state["status"] = "failed"
                        dataset_state["failure"] = str(exc)
                        save_dataset_state(dataset_state_path, dataset_state)
                    log_event("ARCHIVE_LISTING_ERROR", dataset=dataset_id, error=str(exc))
                    continue
                if args.resume_state:
                    dataset_state["listing_complete"] = True
                    dataset_state["total_listed_docs"] = len(docs)
                    dataset_state["status"] = "running"
                    save_dataset_state(dataset_state_path, dataset_state)

            dataset_summary["docs_listed"] = len(docs)
            log_event(
                "ARCHIVE_DOCS_FOUND",
                dataset=dataset_id,
                from_date=dataset_from_date.isoformat(),
                to_date=args.to_date.isoformat(),
                docs=len(docs),
            )
            if not docs:
                dataset_summary["status"] = "no_docs_in_window"
                if args.resume_state:
                    dataset_state["status"] = "completed"
                    save_dataset_state(dataset_state_path, dataset_state)
                log_event("DATASET_EMPTY", dataset=dataset_id, reason="no_docs_in_window")
                continue
            if args.download_order != "api":
                log_event("DOC_ORDER_SORT", dataset=dataset_id, docs=len(docs), order=args.download_order)
            docs = order_archive_docs(docs, args.download_order)
            if args.download_order != "api":
                log_event("DOC_ORDER_APPLIED", dataset=dataset_id, order=args.download_order)
            if args.max_docs_per_dataset > 0:
                docs = docs[: args.max_docs_per_dataset]
                log_event("DOC_LIMIT_APPLIED", dataset=dataset_id, docs=len(docs))

            resume_doc_index = _safe_int(dataset_state.get("next_doc_index"), 0) if args.resume_state else 0
            if resume_doc_index < 0:
                resume_doc_index = 0
            if resume_doc_index > len(docs):
                resume_doc_index = len(docs)
            dataset_summary["resume_start_index"] = resume_doc_index
            if resume_doc_index > 0 and resume_doc_index < len(docs):
                log_event(
                    "DOC_RESUME",
                    dataset=dataset_id,
                    resume_index=resume_doc_index + 1,
                    docs=len(docs),
                )
            docs_remaining = len(docs) - resume_doc_index
            log_event(
                "DOC_PARSE_PLAN",
                dataset=dataset_id,
                listed=len(docs),
                resume_index=resume_doc_index,
                remaining=docs_remaining,
                bulk_chunk_size=args.bulk_chunk_size,
            )

            def save_doc_checkpoint(doc_index: int, doc_id: str, doc: Dict[str, Any]) -> None:
                if not args.resume_state:
                    return
                dataset_state["next_doc_index"] = doc_index + 1
                dataset_state["last_completed_doc_id"] = doc_id or None
                dataset_state["last_completed_stampdate"] = str(doc.get("postDatetime") or "") or None
                dataset_state["last_completed_page"] = _safe_int(doc.get("__archive_page"), 0)
                dataset_state["status"] = "running"
                save_dataset_state(dataset_state_path, dataset_state)

            chunk_size = args.bulk_chunk_size
            bulk_written_doc_ids: Set[str] = set()
            doc_chunks: List[List[Dict[str, Any]]]
            if args.disable_bulk_download:
                doc_chunks = []
            else:
                doc_chunks = [docs[i : i + chunk_size] for i in range(resume_doc_index, len(docs), chunk_size)]
            total_chunks = len(doc_chunks)
            bulk_disabled_after_error = False

            def should_log_bulk_progress(chunk_id: int) -> bool:
                if total_chunks <= 1:
                    return True
                if chunk_id == 1 or chunk_id == total_chunks:
                    return True
                if args.bulk_progress_every <= 0:
                    return False
                return chunk_id % args.bulk_progress_every == 0

            if args.disable_bulk_download:
                log_event("BULK_DISABLED", dataset=dataset_id, reason="flag_disable_bulk_download")
            elif doc_chunks:
                log_event("BULK_QUEUE", dataset=dataset_id, chunks=len(doc_chunks), chunk_size=chunk_size)
            for chunk_id, doc_chunk in tqdm(enumerate(doc_chunks, start=1), total=total_chunks, desc=f"Bulk {dataset_id}", unit="chunk", leave=False):
                chunk_started_at = time.monotonic()
                chunk_label = f"{chunk_id}/{len(doc_chunks)}"
                chunk_doc_ids: List[str] = []
                missing_doc_id_count = 0
                for doc in doc_chunk:
                    doc_id = extract_doc_id(doc)
                    if not doc_id:
                        missing_doc_id_count += 1
                        continue
                    filename = choose_filename(doc)
                    filename = with_doc_id_suffix(filename, doc_id)
                    dataset_subdir = dataset_subdir_from_doc(doc)
                    if args.consolidate_monthly:
                        monthly_path = monthly_csv_path(outdir, dataset_id, dataset_subdir)
                        marker_path = marker_path_for_monthly(monthly_path)
                        known_doc_ids = marker_cache.get(marker_path)
                        if known_doc_ids is None:
                            known_doc_ids = load_marker_doc_ids(marker_path)
                            marker_cache[marker_path] = known_doc_ids
                        if doc_id in known_doc_ids:
                            # Already consolidated; do not bulk-redownload a source file that would be skipped later.
                            continue
                    destination = outdir / dataset_id / dataset_subdir / filename
                    wanted_size = expected_size(doc)
                    exists_and_matches = (
                        destination.exists()
                        and (wanted_size < 0 or destination.stat().st_size == wanted_size)
                    )
                    if not exists_and_matches:
                        chunk_doc_ids.append(doc_id)

                if missing_doc_id_count > 0:
                    log_event(
                        "BULK_WARN",
                        dataset=dataset_id,
                        chunk=chunk_label,
                        missing_doc_id=missing_doc_id_count,
                    )
                if not chunk_doc_ids:
                    reason = "all_files_exist" if missing_doc_id_count == 0 else "no_bulk_candidates"
                    if should_log_bulk_progress(chunk_id):
                        log_event(
                            "BULK_SKIP",
                            dataset=dataset_id,
                            chunk=chunk_label,
                            reason=reason,
                        )
                    chunk_elapsed_seconds = time.monotonic() - chunk_started_at
                    log_event(
                        "BULK_DONE",
                        dataset=dataset_id,
                        chunk=chunk_label,
                        requested=0,
                        wrote=0,
                        status="skipped",
                        elapsed_seconds=f"{chunk_elapsed_seconds:.2f}",
                    )
                    continue

                if should_log_bulk_progress(chunk_id):
                    log_event(
                        "BULK_REQUEST",
                        dataset=dataset_id,
                        chunk=chunk_label,
                        docs=len(chunk_doc_ids),
                    )
                try:
                    doc_contents = client.download_docs(dataset_id, chunk_doc_ids)
                except Exception as exc:  # noqa: BLE001
                    error_text = format_exception_message(exc)
                    stats.failures += 1
                    dataset_summary["status"] = "running_with_failures"
                    record_failure(
                        dataset_id=dataset_id,
                        stage="bulk-download",
                        error=error_text,
                        page=_safe_int(doc_chunk[0].get("__archive_page"), 0),
                    )
                    log_event(
                        "BULK_ERROR",
                        dataset=dataset_id,
                        chunk=chunk_label,
                        docs=len(chunk_doc_ids),
                        error=error_text,
                    )
                    chunk_elapsed_seconds = time.monotonic() - chunk_started_at
                    log_event(
                        "BULK_DONE",
                        dataset=dataset_id,
                        chunk=chunk_label,
                        requested=len(chunk_doc_ids),
                        wrote=0,
                        status="error",
                        elapsed_seconds=f"{chunk_elapsed_seconds:.2f}",
                    )
                    bulk_disabled_after_error = True
                    log_event(
                        "BULK_DISABLED",
                        dataset=dataset_id,
                        reason="error_fallback_to_per_doc",
                        chunk=chunk_label,
                    )
                    break

                missing_payload_ids = [doc_id for doc_id in chunk_doc_ids if doc_id not in doc_contents]
                if missing_payload_ids:
                    log_event(
                        "BULK_WARN",
                        dataset=dataset_id,
                        chunk=chunk_label,
                        missing_payload=len(missing_payload_ids),
                    )

                written_count = 0
                for doc in doc_chunk:
                    doc_id = extract_doc_id(doc)
                    if not doc_id:
                        continue
                    content = doc_contents.get(doc_id)
                    if content is None:
                        continue
                    filename = choose_filename(doc)
                    filename = with_doc_id_suffix(filename, doc_id)
                    dataset_subdir = dataset_subdir_from_doc(doc)
                    destination = outdir / dataset_id / dataset_subdir / filename
                    try:
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        with open(destination, "wb") as handle:
                            handle.write(content)
                    except Exception as exc:  # noqa: BLE001
                        stats.failures += 1
                        dataset_summary["status"] = "running_with_failures"
                        record_failure(
                            dataset_id=dataset_id,
                            stage="bulk-write",
                            error=str(exc),
                            doc_id=doc_id,
                            page=_safe_int(doc.get("__archive_page"), 0),
                        )
                        log_event(
                            "BULK_ERROR",
                            dataset=dataset_id,
                            chunk=chunk_label,
                            doc_id=doc_id,
                            error=str(exc),
                        )
                        continue
                    bulk_written_doc_ids.add(doc_id)
                    written_count += 1

                chunk_elapsed_seconds = time.monotonic() - chunk_started_at
                log_event(
                    "BULK_DONE",
                    dataset=dataset_id,
                    chunk=chunk_label,
                    requested=len(chunk_doc_ids),
                    wrote=written_count,
                    status="ok",
                    elapsed_seconds=f"{chunk_elapsed_seconds:.2f}",
                )
            if bulk_disabled_after_error:
                log_event("BULK_FALLBACK", dataset=dataset_id, mode="per_doc")

            for doc_index, doc in tqdm(enumerate(docs[resume_doc_index:], start=resume_doc_index), total=len(docs) - resume_doc_index, desc=f"Docs {dataset_id}", unit="doc", leave=False):
                doc_id = extract_doc_id(doc)
                if not doc_id:
                    stats.skipped_missing_doc_id += 1
                    log_event(
                        "DOC_WARN",
                        dataset=dataset_id,
                        index=f"{doc_index + 1}/{len(docs)}",
                        reason="missing_doc_id",
                    )
                    dataset_summary["docs_processed"] += 1
                    save_doc_checkpoint(doc_index, "", doc)
                    continue
                doc_started_at = time.monotonic()
                filename = choose_filename(doc)
                filename = with_doc_id_suffix(filename, doc_id)
                dataset_subdir = dataset_subdir_from_doc(doc)
                destination = outdir / dataset_id / dataset_subdir / filename
                monthly_path = monthly_csv_path(outdir, dataset_id, dataset_subdir)
                marker_path = marker_path_for_monthly(monthly_path)
                if args.consolidate_monthly:
                    known_doc_ids = marker_cache.get(marker_path)
                    if known_doc_ids is None:
                        known_doc_ids = load_marker_doc_ids(marker_path)
                        marker_cache[marker_path] = known_doc_ids
                    if doc_id in known_doc_ids:
                        if args.delete_source_after_consolidation and not args.dry_run and destination.exists():
                            try:
                                destination.unlink()
                            except Exception as exc:  # noqa: BLE001
                                log_event(
                                    "SOURCE_DELETE_WARN",
                                    dataset=dataset_id,
                                    doc_id=doc_id,
                                    file=destination,
                                    error=str(exc),
                                )
                        stats.skipped_existing += 1
                        dataset_summary["docs_processed"] += 1
                        save_doc_checkpoint(doc_index, doc_id, doc)
                        continue
                wanted_size = expected_size(doc)
                exists_and_matches = (
                    destination.exists()
                    and (wanted_size < 0 or destination.stat().st_size == wanted_size)
                )
                if exists_and_matches and not args.consolidate_monthly:
                    if doc_id in bulk_written_doc_ids:
                        bulk_written_doc_ids.remove(doc_id)
                        stats.downloaded += 1
                        dataset_summary["docs_downloaded"] += 1
                        if args.extract_zips:
                            maybe_extract_zip(destination)
                    else:
                        stats.skipped_existing += 1
                    dataset_summary["docs_processed"] += 1
                    save_doc_checkpoint(doc_index, doc_id, doc)
                    continue
                if args.dry_run:
                    if args.consolidate_monthly:
                        if exists_and_matches:
                            log_event(
                                "DRY_RUN",
                                dataset=dataset_id,
                                action="consolidate_existing",
                                doc_id=doc_id,
                                source=destination,
                                target=monthly_path,
                            )
                        else:
                            log_event(
                                "DRY_RUN",
                                dataset=dataset_id,
                                action="download_and_consolidate",
                                doc_id=doc_id,
                                destination=destination,
                                target=monthly_path,
                            )
                    else:
                        log_event(
                            "DRY_RUN",
                            dataset=dataset_id,
                            action="download",
                            doc_id=doc_id,
                            destination=destination,
                        )
                    dataset_summary["docs_processed"] += 1
                    save_doc_checkpoint(doc_index, doc_id, doc)
                    continue
                try:
                    source_path = destination
                    downloaded_now = False
                    if not (args.consolidate_monthly and exists_and_matches):
                        if doc_id in bulk_written_doc_ids:
                            bulk_written_doc_ids.remove(doc_id)
                        else:
                            client.download_doc(dataset_id, doc_id, destination, doc)
                        downloaded_now = True
                    elif doc_id in bulk_written_doc_ids:
                        bulk_written_doc_ids.remove(doc_id)
                        downloaded_now = True
                    if args.consolidate_monthly:
                        post_dt = str(doc.get("postDatetime", "")).strip()
                        appended_rows = append_doc_to_monthly_csv(source_path, monthly_path, post_datetime=post_dt)
                        if appended_rows > 0:
                            stats.consolidated_updates += 1
                        touched_monthly_paths.add(monthly_path)
                        known_doc_ids = marker_cache.setdefault(marker_path, set())
                        if doc_id not in known_doc_ids:
                            append_marker_doc_id(marker_path, doc_id)
                            known_doc_ids.add(doc_id)
                        if args.delete_source_after_consolidation and source_path.exists():
                            source_path.unlink()
                    elif args.extract_zips:
                        maybe_extract_zip(destination)
                    if downloaded_now:
                        stats.downloaded += 1
                        dataset_summary["docs_downloaded"] += 1
                    dataset_summary["docs_processed"] += 1
                    consecutive_network_failures = 0
                except Exception as exc:  # noqa: BLE001
                    stats.failures += 1
                    dataset_summary["docs_failed"] += 1
                    dataset_summary["status"] = "running_with_failures"
                    record_failure(
                        dataset_id=dataset_id,
                        stage="download",
                        error=str(exc),
                        doc_id=doc_id,
                        page=_safe_int(doc.get("__archive_page"), 0),
                    )
                    if args.resume_state:
                        dataset_state["status"] = "running_with_failures"
                        dataset_state["last_failed_doc_id"] = doc_id
                        dataset_state["last_failed_error"] = str(exc)
                        save_dataset_state(dataset_state_path, dataset_state)
                    log_event("DOWNLOAD_ERROR", dataset=dataset_id, doc_id=doc_id, error=str(exc))
                    if is_name_resolution_failure(exc):
                        consecutive_network_failures += 1
                        if args.network_failure_cooldown_seconds > 0:
                            time.sleep(args.network_failure_cooldown_seconds)
                        if (
                            args.max_consecutive_network_failures > 0
                            and consecutive_network_failures >= args.max_consecutive_network_failures
                        ):
                            raise SystemExit(
                                "Stopping download due to repeated DNS/network resolution failures "
                                f"({consecutive_network_failures} consecutive). "
                                "Check internet/DNS and rerun; completed docs are resumable via .docids/state."
                            ) from exc
                    else:
                        consecutive_network_failures = 0
                    continue

                save_doc_checkpoint(doc_index, doc_id, doc)

                if args.file_timing_frequency != "off":
                    completed_at = datetime.now().astimezone().isoformat(timespec="seconds")
                    elapsed_seconds = time.monotonic() - doc_started_at
                    stampdate = str(doc.get("postDatetime") or "-")
                    parsed_stampdate = parse_api_datetime(stampdate)
                    if parsed_stampdate is not None:
                        stampdate_date = parsed_stampdate.date().isoformat()
                        stampdate_day = parsed_stampdate.day
                    elif "T" in stampdate:
                        stampdate_date = stampdate.split("T", 1)[0]
                        try:
                            stampdate_day = int(stampdate_date.split("-")[2])
                        except Exception:  # noqa: BLE001
                            stampdate_day = -1
                    else:
                        stampdate_date = "-"
                        stampdate_day = -1
                    if args.consolidate_monthly:
                        action = "download+consolidate" if downloaded_now else "consolidate-existing"
                        output_file = monthly_path
                        year = monthly_path.parent.parent.name if monthly_path.parent.parent.name.isdigit() else "-"
                        month = monthly_path.parent.name if monthly_path.parent.name.isdigit() else "-"
                    else:
                        action = "download"
                        output_file = destination
                        year = dataset_subdir.parts[0] if len(dataset_subdir.parts) >= 2 else "-"
                        month = dataset_subdir.parts[1] if len(dataset_subdir.parts) >= 2 else "-"
                    month_key = f"{year}-{month}" if year != "-" and month != "-" else "-"

                    if args.file_timing_frequency == "every-file":
                        log_event(
                            "FILE_COMPLETE",
                            action=action,
                            dataset=dataset_id,
                            doc_id=doc_id,
                            file=output_file,
                            stampdate=stampdate,
                            date=stampdate_date,
                            year=year,
                            month=month,
                            elapsed_seconds=f"{elapsed_seconds:.2f}",
                            completed_at=completed_at,
                        )
                    elif args.file_timing_frequency in stampdate_thresholds:
                        threshold = stampdate_thresholds[args.file_timing_frequency]
                        if current_stampdate is None:
                            current_stampdate = stampdate
                            current_stampdate_files = 1
                            current_stampdate_year = year
                            current_stampdate_month = month
                        elif stampdate == current_stampdate:
                            current_stampdate_files += 1
                        else:
                            completed_stampdates += 1
                            if completed_stampdates % threshold == 0:
                                log_event(
                                    "STAMPDATE_COMPLETE",
                                    dataset=dataset_id,
                                    stampdate=current_stampdate,
                                    year=current_stampdate_year,
                                    month=current_stampdate_month,
                                    files=current_stampdate_files,
                                    completed_at=completed_at,
                                )
                            current_stampdate = stampdate
                            current_stampdate_files = 1
                            current_stampdate_year = year
                            current_stampdate_month = month
                    elif args.file_timing_frequency == "daily":
                        date_key = stampdate_date if stampdate_date != "-" else stampdate
                        if current_date_key is None:
                            current_date_key = date_key
                            current_date_files = 1
                            current_date_year = year
                            current_date_month = month
                        elif date_key == current_date_key:
                            current_date_files += 1
                        else:
                            log_event(
                                "DAY_COMPLETE",
                                dataset=dataset_id,
                                date=current_date_key,
                                year=current_date_year,
                                month=current_date_month,
                                files=current_date_files,
                                completed_at=completed_at,
                            )
                            current_date_key = date_key
                            current_date_files = 1
                            current_date_year = year
                            current_date_month = month
                    elif args.file_timing_frequency in calendar_day_schedules:
                        schedule_days = calendar_day_schedules[args.file_timing_frequency]
                        if stampdate_day in schedule_days and stampdate_date not in printed_calendar_dates:
                            log_event(
                                "DATE_SCHEDULE_HIT",
                                schedule=args.file_timing_frequency,
                                dataset=dataset_id,
                                date=stampdate_date,
                                day=stampdate_day,
                                year=year,
                                month=month,
                                doc_id=doc_id,
                                completed_at=completed_at,
                            )
                            printed_calendar_dates.add(stampdate_date)
                    elif args.file_timing_frequency == "1-month":
                        if current_month_key is None:
                            current_month_key = month_key
                            current_month_files = 1
                        elif month_key == current_month_key:
                            current_month_files += 1
                        else:
                            log_event(
                                "MONTH_COMPLETE",
                                dataset=dataset_id,
                                month=current_month_key,
                                files=current_month_files,
                                completed_at=completed_at,
                            )
                            current_month_key = month_key
                            current_month_files = 1

                if args.write_manifest:
                    manifest_rows.append(
                        {
                            "dataset_id": dataset_id,
                            "title": DATASETS.get(dataset_id, {}).get("title"),
                            "report_name": product_title,
                            "doc_id": doc_id,
                            "postDateTime": doc.get("postDatetime"),
                            "post_datetime": doc.get("postDatetime"),
                            "filename": filename,
                            "destination": str(destination),
                            "consolidated_destination": str(monthly_path) if args.consolidate_monthly else None,
                            "size": doc.get("size"),
                        }
                    )

            if args.file_timing_frequency in stampdate_thresholds and current_stampdate is not None:
                completed_stampdates += 1
                threshold = stampdate_thresholds[args.file_timing_frequency]
                if completed_stampdates % threshold == 0:
                    completed_at = datetime.now().astimezone().isoformat(timespec="seconds")
                    log_event(
                        "STAMPDATE_COMPLETE",
                        dataset=dataset_id,
                        stampdate=current_stampdate,
                        year=current_stampdate_year,
                        month=current_stampdate_month,
                        files=current_stampdate_files,
                        completed_at=completed_at,
                    )
            if args.file_timing_frequency == "daily" and current_date_key is not None:
                completed_at = datetime.now().astimezone().isoformat(timespec="seconds")
                log_event(
                    "DAY_COMPLETE",
                    dataset=dataset_id,
                    date=current_date_key,
                    year=current_date_year,
                    month=current_date_month,
                    files=current_date_files,
                    completed_at=completed_at,
                )
            if args.file_timing_frequency == "1-month" and current_month_key is not None:
                completed_at = datetime.now().astimezone().isoformat(timespec="seconds")
                log_event(
                    "MONTH_COMPLETE",
                    dataset=dataset_id,
                    month=current_month_key,
                    files=current_month_files,
                    completed_at=completed_at,
                )

            monthly_paths_to_sort: Set[Path] = set()
            dataset_root = outdir / dataset_id
            if args.consolidate_monthly:
                monthly_paths_to_sort.update(touched_monthly_paths)
            if args.sort_existing_monthly:
                monthly_paths_to_sort.update(
                    path
                    for path in dataset_root.glob("**/*.csv")
                    if path.is_file()
                    and monthly_csv_in_window(path, dataset_root, dataset_from_date, args.to_date)
                )
            if monthly_sort_order and monthly_paths_to_sort:
                log_event(
                    "MONTHLY_SORT_PLAN",
                    dataset=dataset_id,
                    files=len(monthly_paths_to_sort),
                    order=monthly_sort_order,
                    strategy=args.monthly_sort_strategy,
                )
                for monthly_path in sorted(monthly_paths_to_sort):
                    try:
                        sort_status = sort_monthly_csv(
                            monthly_path,
                            monthly_sort_order,
                            args.monthly_sort_strategy,
                        )
                    except Exception as exc:  # noqa: BLE001
                        stats.monthly_sort_failures += 1
                        record_failure(
                            dataset_id=dataset_id,
                            stage="monthly-sort",
                            error=str(exc),
                        )
                        log_event("MONTHLY_SORT_ERROR", dataset=dataset_id, file=monthly_path, error=str(exc))
                        continue
                    if sort_status == "sorted":
                        stats.monthly_sorted += 1
                    elif sort_status == "already":
                        stats.monthly_already_sorted += 1
                    else:
                        stats.monthly_sort_skipped += 1
                    log_event(
                        "MONTHLY_SORT_DONE",
                        dataset=dataset_id,
                        status=sort_status,
                        file=monthly_path,
                        order=monthly_sort_order,
                        strategy=args.monthly_sort_strategy,
                    )

            dataset_summary["last_completed_page"] = _safe_int(dataset_state.get("last_completed_page"), 0)
            dataset_summary["last_completed_doc_id"] = dataset_state.get("last_completed_doc_id")
            dataset_summary["last_completed_stampdate"] = dataset_state.get("last_completed_stampdate")
            if dataset_summary["status"] == "running":
                dataset_summary["status"] = "completed"
            if args.resume_state:
                dataset_state["status"] = dataset_summary["status"]
                save_dataset_state(dataset_state_path, dataset_state)
            log_event(
                "DATASET_DONE",
                dataset=dataset_id,
                status=dataset_summary["status"],
                listed=dataset_summary["docs_listed"],
                processed=dataset_summary["docs_processed"],
                downloaded=dataset_summary["docs_downloaded"],
                failed=dataset_summary["docs_failed"],
            )

        summary_fields: Dict[str, object] = {
            "downloaded": stats.downloaded,
            "skipped_existing": stats.skipped_existing,
            "skipped_missing_doc_id": stats.skipped_missing_doc_id,
            "skipped_unavailable_dataset": stats.skipped_unavailable_dataset,
            "failures": stats.failures,
        }
        if args.consolidate_monthly:
            summary_fields["monthly_files_updated"] = stats.consolidated_updates
        if monthly_sort_order and (args.consolidate_monthly or args.sort_existing_monthly):
            summary_fields["monthly_files_sorted"] = stats.monthly_sorted
            summary_fields["monthly_files_already_sorted"] = stats.monthly_already_sorted
            summary_fields["monthly_files_sort_skipped"] = stats.monthly_sort_skipped
            summary_fields["monthly_files_sort_failures"] = stats.monthly_sort_failures
        log_event("RUN_SUMMARY", **summary_fields)

        if args.write_manifest and manifest_rows:
            manifest_path = outdir / "download_manifest.json"
            with open(manifest_path, "w", encoding="utf-8") as handle:
                json.dump(manifest_rows, handle, indent=2)
            log_event("MANIFEST_WRITTEN", path=manifest_path)
    except SystemExit as exc:
        summary_status = "failed"
        fatal_error = str(exc)
        record_failure(dataset_id="RUN", stage="fatal", error=fatal_error)
        raise
    except Exception as exc:  # noqa: BLE001
        summary_status = "failed"
        fatal_error = f"{type(exc).__name__}: {exc}"
        record_failure(dataset_id="RUN", stage="fatal", error=fatal_error)
        raise
    finally:
        elapsed_seconds = time.monotonic() - run_started_monotonic
        safe_args: Dict[str, Any] = {}
        for key, value in vars(args).items():
            if key in {"username", "password", "subscription_key"}:
                continue
            if isinstance(value, date):
                safe_args[key] = value.isoformat()
            else:
                safe_args[key] = value
        run_summary = {
            "started_at": run_started_at,
            "finished_at": utc_now_iso(),
            "status": summary_status,
            "fatal_error": fatal_error,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "run_dir": str(run_dir),
            "run_log": str(run_log_path),
            "failures_csv": str(failures_csv_path),
            "summary_json": str(summary_json_path),
            "selected_datasets": selected_ids,
            "args": safe_args,
            "stats": stats_as_dict(stats),
            "datasets": dataset_summaries,
        }
        try:
            summary_json_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
            log_event("SUMMARY_WRITTEN", path=summary_json_path)
        except Exception as exc:  # noqa: BLE001
            log_event("SUMMARY_WRITE_WARN", error=str(exc))
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            failures_handle.close()
            run_log_handle.close()


if __name__ == "__main__":
    main()
