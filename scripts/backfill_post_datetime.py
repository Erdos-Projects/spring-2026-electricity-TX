#!/usr/bin/env python3
"""Backfill postDateTime into consolidated ERCOT monthly CSV files."""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import io
import json
import os
import re
import shutil
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional progress bar
    def tqdm(iterable, **_):  # type: ignore[misc]
        return iterable

from download_ercot_public_reports import (
    API_BASE_URL,
    DEFAULT_CLIENT_ID,
    DEFAULT_SCOPE,
    MONTHLY_SORT_CACHE_VERSION,
    POST_DATETIME_COLUMN,
    POST_DATETIME_COLUMN_ALIASES,
    TOKEN_URL,
    ErcotPublicReportsClient,
    _monthly_sort_cache_path,
    _monthly_sort_file_signature,
    authenticate,
    parse_api_datetime,
    parse_retry_after_seconds,
    read_doc_csv_text,
    read_text_fallback,
    to_end_iso,
    to_start_iso,
)

# POST_DATETIME_COLUMN, POST_DATETIME_COLUMN_ALIASES, and
# MONTHLY_SORT_CACHE_VERSION are imported from download_ercot_public_reports.
POST_DATETIME_ALIASES = POST_DATETIME_COLUMN_ALIASES  # local alias kept for clarity
POST_DATETIME_ALIAS_LOWER = {name.lower() for name in POST_DATETIME_ALIASES}
DOC_ID_SUFFIX_RE = re.compile(r"__(\d+)(?:\.[A-Za-z0-9._-]+)?$")
SECONDARY_SORT_DATE_FIELDS = ("Date", "DeliveryDate", "OperDay", "OperatingDay", "MarketDay")
SECONDARY_SORT_HOUR_FIELDS = ("HourEnding", "Hour_Ending", "DeliveryHour", "HE")
MONTHLY_SORT_STRATEGY = "postdatetime"
# Maximum number of doc IDs sent per bulk POST request.  The ERCOT API enforces
# a hard limit of 256 per request.
BULK_DOWNLOAD_CHUNK_SIZE = 256


@dataclass
class DatasetSummary:
    dataset_id: str
    monthly_files: int = 0
    monthly_rebuilt: int = 0
    rows_written: int = 0
    cells_filled: int = 0
    docs_total: int = 0
    docs_with_post_datetime: int = 0
    docs_missing_post_datetime: int = 0
    downloaded_sources: int = 0
    missing_sources: int = 0
    sources_deleted: int = 0
    sources_archived: int = 0
    verified_months: int = 0
    verified_rows_total: int = 0
    verified_rows_filled: int = 0
    verified_rows_missing: int = 0


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def normalized_dataset_ids(values: Sequence[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in values:
        dataset_id = value.strip().upper()
        if not dataset_id or dataset_id in seen:
            continue
        seen.add(dataset_id)
        ordered.append(dataset_id)
    return ordered


def monthly_csv_paths(dataset_root: Path, from_date: Optional[date], to_date: Optional[date]) -> List[Path]:
    paths: List[Path] = []
    for path in sorted(dataset_root.glob("**/*.csv")):
        if not path.is_file():
            continue
        if DOC_ID_SUFFIX_RE.search(path.name):
            continue
        if from_date is not None and to_date is not None:
            month_start, month_end = month_bounds_from_path(path, dataset_root)
            if month_start is None or month_end is None:
                continue
            if month_end < from_date or month_start > to_date:
                continue
        paths.append(path)
    return paths


def month_bounds_from_path(path: Path, dataset_root: Path) -> Tuple[Optional[date], Optional[date]]:
    try:
        rel = path.relative_to(dataset_root)
    except ValueError:
        return None, None
    if len(rel.parts) < 3:
        return None, None
    year_text, month_text = rel.parts[0], rel.parts[1]
    if not (year_text.isdigit() and month_text.isdigit()):
        return None, None
    year = int(year_text)
    month = int(month_text)
    if month < 1 or month > 12:
        return None, None
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start, end


# _monthly_sort_cache_path and _monthly_sort_file_signature are imported from
# download_ercot_public_reports.

def _sort_cache_classification_for_order(order: str) -> str:
    if order in {"ascending", "descending"}:
        return "sorted"
    return "skipped"


def write_monthly_sort_cache(path: Path, order: str) -> None:
    classification = _sort_cache_classification_for_order(order)
    try:
        size_bytes, mtime_ns = _monthly_sort_file_signature(path)
    except OSError:
        return
    payload = {
        "version": MONTHLY_SORT_CACHE_VERSION,
        "sort_order": order,
        "sort_strategy": MONTHLY_SORT_STRATEGY,
        "classification": classification,
        "size_bytes": size_bytes,
        "mtime_ns": mtime_ns,
        "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    try:
        _monthly_sort_cache_path(path).write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            encoding="utf-8",
        )
    except Exception:  # noqa: BLE001
        return


def read_monthly_sort_cache_classification(path: Path) -> str:
    cache_path = _monthly_sort_cache_path(path)
    if not cache_path.exists():
        return "missing"
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return "invalid"
    if not isinstance(payload, dict):
        return "invalid"
    classification = str(payload.get("classification") or "").strip()
    if classification in {"sorted", "skipped"}:
        return classification
    return "invalid"


def load_doc_ids(marker_path: Path) -> List[str]:
    doc_ids: List[str] = []
    if not marker_path.exists():
        return doc_ids
    for line in marker_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        doc_id = line.strip()
        if doc_id:
            doc_ids.append(doc_id)
    return doc_ids


def load_post_datetime_map_from_state(
    state_dir: Path,
    dataset_id: str,
    doc_id_filter: Optional[Set[str]] = None,
) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    path = state_dir / f"{dataset_id}.archive_docs.jsonl"
    if not path.exists():
        return mapping
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            doc = payload.get("doc") if isinstance(payload, dict) else None
            if not isinstance(doc, dict):
                continue
            doc_id = str(doc.get("docId") or "").strip()
            if not doc_id or doc_id in mapping:
                continue
            if doc_id_filter is not None and doc_id not in doc_id_filter:
                continue
            post_datetime = str(doc.get("postDatetime") or "").strip()
            if post_datetime:
                mapping[doc_id] = post_datetime
    return mapping


def load_archive_doc_map_from_state(
    state_dir: Path,
    dataset_id: str,
    doc_id_filter: Optional[Set[str]] = None,
) -> Dict[str, Dict[str, object]]:
    mapping: Dict[str, Dict[str, object]] = {}
    path = state_dir / f"{dataset_id}.archive_docs.jsonl"
    if not path.exists():
        return mapping
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            doc = payload.get("doc") if isinstance(payload, dict) else None
            if not isinstance(doc, dict):
                continue
            doc_id = str(doc.get("docId") or "").strip()
            if not doc_id or doc_id in mapping:
                continue
            if doc_id_filter is not None and doc_id not in doc_id_filter:
                continue
            mapping[doc_id] = doc
    return mapping


def load_post_datetime_map_from_manifest(manifest_path: Path, dataset_id: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not manifest_path.exists():
        return mapping
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return mapping
    if not isinstance(payload, list):
        return mapping
    for row in payload:
        if not isinstance(row, dict):
            continue
        if str(row.get("dataset_id") or "").strip().upper() != dataset_id:
            continue
        doc_id = str(row.get("doc_id") or "").strip()
        post_datetime = str(row.get("postDateTime") or row.get("post_datetime") or "").strip()
        if doc_id and post_datetime and doc_id not in mapping:
            mapping[doc_id] = post_datetime
    return mapping


def find_source_file_for_doc(month_dir: Path, doc_id: str) -> Optional[Path]:
    for path in iter_source_files_for_doc(month_dir, doc_id):
        if source_has_usable_csv_rows(path):
            return path
    return None


def iter_source_files_for_doc(month_dir: Path, doc_id: str) -> List[Path]:
    matches = sorted(month_dir.glob(f"*__{doc_id}"))
    matches.extend(sorted(month_dir.glob(f"*__{doc_id}.*")))
    valid: List[Path] = []
    for path in matches:
        if not path.is_file():
            continue
        if path.name.endswith(".csv.sortcache.json"):
            continue
        if path.name.endswith(".csv.docids"):
            continue
        if path.name.endswith(".csv") and not DOC_ID_SUFFIX_RE.search(path.name):
            continue
        valid.append(path)
    return valid


def cleaned_source_fieldnames(fieldnames: Iterable[str]) -> List[str]:
    ordered: List[str] = []
    seen: Set[str] = set()
    for name in fieldnames:
        normalized = name.strip()
        if not normalized:
            continue
        if normalized.lower() in POST_DATETIME_ALIAS_LOWER:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def detect_post_datetime_field(fieldnames: Sequence[str]) -> Optional[str]:
    for name in fieldnames:
        if name.strip().lower() in POST_DATETIME_ALIAS_LOWER:
            return name
    return None


def source_row_count(source_path: Path) -> int:
    csv_text = read_doc_csv_text(source_path)
    stripped = csv_text.lstrip()
    if not stripped:
        return 0
    if stripped.startswith("{") or stripped.startswith("["):
        return 0
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return 0
    return sum(1 for _ in reader)


def source_has_usable_csv_rows(source_path: Path) -> bool:
    try:
        return source_row_count(source_path) > 0
    except Exception:  # noqa: BLE001
        return False


def _count_rows_from_csv_text(csv_text: str) -> int:
    """Fast row count using line splitting instead of csv.DictReader iteration.

    ERCOT CSVs never contain multi-line field values, so counting non-empty
    lines is safe and ~10× faster than iterating a DictReader.  Returns 0 for
    empty, JSON, or header-only content.  Trailing blank lines are excluded so
    the result matches csv.DictReader's behaviour.
    """
    stripped = csv_text.lstrip()
    if not stripped or stripped[0] in "{[":
        return 0
    lines = stripped.splitlines()
    if not lines:
        return 0
    # [0] is the header; count non-empty data lines only
    return sum(1 for line in lines[1:] if line.strip())


def _find_source_file_quick(month_dir: Path, doc_id: str) -> Optional[Path]:
    """Return the first source file path for *doc_id* that exists on disk.

    Unlike :func:`find_source_file_for_doc` this function does NOT read file
    content — it only checks that the file exists and has a non-zero size.
    Content validation is deferred to the caller (phase 3 of the source-loading
    pipeline) so each file is read at most once.
    """
    for path in iter_source_files_for_doc(month_dir, doc_id):
        try:
            if path.is_file() and path.stat().st_size > 0:
                return path
        except OSError:
            continue
    return None


def bulk_fetch_source_texts(
    client: ErcotPublicReportsClient,
    dataset_id: str,
    doc_ids: List[str],
    chunk_size: int = BULK_DOWNLOAD_CHUNK_SIZE,
) -> Tuple[Dict[str, str], int]:
    """Download source CSV text for *doc_ids* via the bulk POST endpoint.

    Issues one POST per *chunk_size* doc IDs (max 256).  Uses lenient mode so
    a partial API response does not abort the whole run — failed chunks are
    logged and skipped.

    Returns ``({doc_id: csv_text}, total_downloaded_count)``.
    """
    if not doc_ids:
        return {}, 0
    result: Dict[str, str] = {}
    downloaded = 0
    for offset in range(0, len(doc_ids), chunk_size):
        chunk = doc_ids[offset : offset + chunk_size]
        try:
            raw_map = client.download_docs(dataset_id.lower(), chunk, strict_count=False)
        except Exception as exc:  # noqa: BLE001
            print(
                f"BULK_SOURCE_WARN dataset={dataset_id} offset={offset} "
                f"chunk_size={len(chunk)} error={exc!r}"
            )
            continue
        for doc_id, csv_bytes in raw_map.items():
            result[doc_id] = read_text_fallback(csv_bytes)
            downloaded += 1
    return result, downloaded


def monthly_post_datetime_coverage(monthly_path: Path) -> Tuple[int, int, bool]:
    with open(monthly_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return 0, 0, False
        post_field = detect_post_datetime_field(reader.fieldnames)
        total_rows = 0
        filled_rows = 0
        for row in reader:
            total_rows += 1
            if post_field and str(row.get(post_field) or "").strip():
                filled_rows += 1
    return total_rows, filled_rows, post_field is not None


def monthly_post_datetime_malformed_count(monthly_path: Path) -> int:
    """Return the number of rows whose postDateTime is non-empty but not a
    parseable ISO timestamp.  Zero means every filled cell looks like a real
    API timestamp; a positive count indicates estimated/corrupt values that
    should block source-file deletion or archival."""
    with open(monthly_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return 0
        post_field = detect_post_datetime_field(reader.fieldnames)
        if post_field is None:
            return 0
        malformed = 0
        for row in reader:
            value = str(row.get(post_field) or "").strip()
            if value and parse_api_datetime(value) is None:
                malformed += 1
    return malformed


def month_source_files(monthly_path: Path) -> List[Path]:
    doc_ids = load_doc_ids(monthly_path.with_suffix(monthly_path.suffix + ".docids"))
    month_dir = monthly_path.parent
    files: List[Path] = []
    seen: Set[Path] = set()
    for doc_id in doc_ids:
        for source_path in iter_source_files_for_doc(month_dir, doc_id):
            if source_path in seen:
                continue
            seen.add(source_path)
            files.append(source_path)
    return files


def delete_month_source_files(monthly_path: Path) -> int:
    deleted = 0
    for source_path in month_source_files(monthly_path):
        try:
            source_path.unlink()
            deleted += 1
        except FileNotFoundError:
            continue
    return deleted


def _unique_archive_path(path: Path) -> Path:
    if not path.exists():
        return path
    suffix = 1
    while True:
        candidate = path.with_name(f"{path.name}.dup{suffix}")
        if not candidate.exists():
            return candidate
        suffix += 1


def archive_month_source_files(monthly_path: Path, dataset_id: str, archive_root: Path) -> int:
    month_dir = monthly_path.parent
    year = month_dir.parent.name
    month = month_dir.name
    archive_month_dir = archive_root / dataset_id / year / month
    archive_month_dir.mkdir(parents=True, exist_ok=True)

    archived = 0
    for source_path in month_source_files(monthly_path):
        target_path = archive_month_dir / source_path.name
        if target_path.exists():
            try:
                if target_path.stat().st_size == source_path.stat().st_size:
                    source_path.unlink()
                    archived += 1
                    continue
            except FileNotFoundError:
                pass
            target_path = _unique_archive_path(target_path)
        try:
            source_path.replace(target_path)
        except OSError:
            shutil.move(str(source_path), str(target_path))
        archived += 1
    return archived


def archive_doc_for_download(
    dataset_id: str,
    doc_id: str,
    archive_doc_map: Dict[str, Dict[str, object]],
) -> Dict[str, object]:
    archive_doc = archive_doc_map.get(doc_id)
    if archive_doc:
        return archive_doc
    return {
        "_links": {
            "endpoint": {
                "href": f"{API_BASE_URL}/archive/{dataset_id.lower()}?download={doc_id}",
            }
        }
    }


def resolve_source_path_for_doc(
    *,
    dataset_id: str,
    doc_id: str,
    month_dir: Path,
    archive_doc_map: Dict[str, Dict[str, object]],
    client: Optional[ErcotPublicReportsClient],
    download_missing_sources: bool,
    dry_run: bool,
) -> Tuple[Optional[Path], int]:
    source_path = find_source_file_for_doc(month_dir, doc_id)
    downloaded_sources = 0

    if source_path is not None and source_path.exists() and not source_has_usable_csv_rows(source_path):
        source_path = None

    if source_path is None and client is not None and download_missing_sources:
        candidate_path = month_dir / f"{dataset_id}__{doc_id}"
        if not dry_run:
            try:
                client.download_doc(
                    dataset_id,
                    doc_id,
                    candidate_path,
                    archive_doc_for_download(dataset_id, doc_id, archive_doc_map),
                )
                downloaded_sources += 1
                source_path = candidate_path
            except Exception:  # noqa: BLE001
                source_path = None

    if source_path is None or not source_path.exists():
        return None, downloaded_sources
    if not source_has_usable_csv_rows(source_path):
        return None, downloaded_sources
    return source_path, downloaded_sources


def resolve_post_datetime_for_doc(
    doc_id: str,
    source_path: Optional[Path],
    post_datetime_map: Dict[str, str],
) -> str:
    post_datetime = str(post_datetime_map.get(doc_id) or "").strip()
    if post_datetime:
        return post_datetime
    return ""


def cleaned_csv_row_values(row: Dict[str, object]) -> Dict[str, str]:
    cleaned: Dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        key_clean = str(key).strip()
        if not key_clean or key_clean.lower() in POST_DATETIME_ALIAS_LOWER:
            continue
        cleaned[key_clean] = "" if value is None else str(value)
    return cleaned


def row_fingerprint(values: Dict[str, str], key_fields: Sequence[str]) -> Tuple[str, ...]:
    return tuple(str(values.get(field) or "").strip() for field in key_fields)


def fill_rows_by_source_fingerprint(
    *,
    rows: List[Dict[str, str]],
    key_fields: Sequence[str],
    post_field: str,
    doc_plan: Sequence[Tuple[str, int, str, Optional[Path]]],
    source_text_cache: Optional[Dict[str, str]] = None,
    overwrite_mode: bool = False,
) -> int:
    """Fill *post_field* in *rows* by matching row fingerprints against source docs.

    *source_text_cache* maps ``doc_id → csv_text`` for docs whose source files
    were fetched into memory (bulk download or pre-read disk files).  When a
    doc's source_path is None but its doc_id is in the cache, the cached text
    is used instead of a disk read.

    *overwrite_mode* should be True when the caller cleared existing
    postDateTime values before calling this function.  In that case fingerprint
    collisions are escalated from WARN to ERROR and ambiguous rows are left
    empty rather than being assigned a potentially-wrong value.
    """
    source_fingerprints: Dict[Tuple[str, ...], List[str]] = defaultdict(list)

    for doc_id, _, post_datetime, source_path in doc_plan:
        # Prefer cached text (already read in Phase 3) over re-reading disk.
        if source_text_cache is not None and doc_id in source_text_cache:
            csv_text = source_text_cache[doc_id]
        elif source_path is not None:
            csv_text = read_doc_csv_text(source_path)
        else:
            continue
        if not csv_text.strip():
            continue
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            continue
        for source_row in reader:
            cleaned_source_row = cleaned_csv_row_values(source_row)
            fingerprint = row_fingerprint(cleaned_source_row, key_fields)
            source_fingerprints[fingerprint].append(post_datetime)

    # Detect fingerprint collisions: multiple source rows share identical key
    # field values, making the postDateTime assignment ambiguous.
    collision_count = sum(1 for v in source_fingerprints.values() if len(v) > 1)
    # In overwrite mode the caller deliberately cleared existing values, so
    # assigning a wrong postDateTime is worse than leaving the cell empty.
    # Escalate to ERROR and mark ambiguous fingerprints so they are skipped.
    ambiguous: Set[Tuple[str, ...]] = set()
    if collision_count:
        severity = "ERROR" if overwrite_mode else "WARN"
        print(
            f"FINGERPRINT_COLLISION_{severity} "
            f"colliding_fingerprints={collision_count} "
            f"total_fingerprints={len(source_fingerprints)} "
            f"overwrite_mode={str(overwrite_mode).lower()} "
            f"note=multiple_source_rows_share_same_key_field_values"
        )
        if overwrite_mode:
            ambiguous = {fp for fp, vals in source_fingerprints.items() if len(vals) > 1}

    cells_filled = 0
    ambiguous_rows_skipped = 0
    for row in rows:
        current = str(row.get(post_field) or "").strip()
        if current:
            continue
        fingerprint = row_fingerprint({k: "" if v is None else str(v) for k, v in row.items()}, key_fields)
        if fingerprint in ambiguous:
            # Leave empty: cannot determine which doc owns this row.
            ambiguous_rows_skipped += 1
            continue
        available = source_fingerprints.get(fingerprint)
        if not available:
            continue
        post_datetime = available.pop()
        if post_datetime:
            row[post_field] = post_datetime
            cells_filled += 1
    if ambiguous_rows_skipped:
        print(
            f"FINGERPRINT_AMBIGUOUS_ROWS_SKIPPED "
            f"ambiguous_rows={ambiguous_rows_skipped} "
            f"note=overwrite_mode_left_these_rows_empty_to_avoid_wrong_assignment"
        )
    return cells_filled


def normalize_sort_datetime(value: str) -> Optional[datetime]:
    parsed = parse_api_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_sort_date(value: str) -> Optional[date]:
    raw = value.strip()
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    parsed = parse_api_datetime(raw)
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.date()


def parse_sort_hour_ending(value: str) -> Optional[int]:
    raw = value.strip()
    if not raw:
        return None
    if ":" in raw:
        left = "".join(ch for ch in raw.split(":", 1)[0] if ch.isdigit())
    else:
        left = "".join(ch for ch in raw if ch.isdigit())
    if not left:
        return None
    hour = int(left)
    if hour < 0 or hour > 24:
        return None
    return hour


def first_row_value(
    row: Dict[str, str],
    field_lookup: Dict[str, str],
    candidate_names: Sequence[str],
) -> str:
    for candidate in candidate_names:
        key = field_lookup.get(candidate.lower())
        if key is None:
            continue
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def sorted_rows_by_post_datetime(rows: List[Dict[str, str]], order: str) -> List[Dict[str, str]]:
    if order == "none":
        return rows
    field_lookup: Dict[str, str] = {}
    if rows:
        for key in rows[0].keys():
            if key is None:
                continue
            key_clean = str(key).strip()
            if key_clean:
                field_lookup[key_clean.lower()] = key_clean
    decorated = []
    for index, row in enumerate(rows):
        raw_post_datetime = first_row_value(row, field_lookup, POST_DATETIME_ALIASES)
        raw_sort_date = first_row_value(row, field_lookup, SECONDARY_SORT_DATE_FIELDS)
        raw_sort_hour = first_row_value(row, field_lookup, SECONDARY_SORT_HOUR_FIELDS)
        parsed_post_datetime = normalize_sort_datetime(raw_post_datetime)
        parsed_date = parse_sort_date(raw_sort_date)
        parsed_hour = parse_sort_hour_ending(raw_sort_hour)
        decorated.append((index, parsed_post_datetime, parsed_date, parsed_hour, row))
    if order == "ascending":
        decorated.sort(
            key=lambda item: (
                item[1] is None,
                item[1] or datetime.max,
                item[2] is None,
                item[2] or date.max,
                item[3] is None,
                item[3] if item[3] is not None else 99,
                item[0],
            )
        )
    elif order == "descending":
        decorated.sort(
            key=lambda item: (
                item[1] is not None,
                item[1] or datetime.min,
                item[2] is not None,
                item[2] or date.min,
                item[3] is not None,
                item[3] if item[3] is not None else -1,
                -item[0],
            ),
            reverse=True,
        )
    else:
        raise ValueError(f"Unknown order '{order}'.")
    return [item[4] for item in decorated]


def resolve_credentials(args: argparse.Namespace) -> Tuple[str, str, str]:
    username = args.username or os.getenv("ERCOT_API_USERNAME", "")
    password = args.password or os.getenv("ERCOT_API_PASSWORD", "")
    subscription_key = args.subscription_key or os.getenv("ERCOT_SUBSCRIPTION_KEY", "")
    return username.strip(), password.strip(), subscription_key.strip()


def build_api_client(args: argparse.Namespace) -> ErcotPublicReportsClient:
    username, password, subscription_key = resolve_credentials(args)
    if not username or not password or not subscription_key:
        raise SystemExit(
            "Missing credentials for API operations. Set --username/--password/--subscription-key "
            "or env vars ERCOT_API_USERNAME, ERCOT_API_PASSWORD, ERCOT_SUBSCRIPTION_KEY."
        )
    token = authenticate(
        username=username,
        password=password,
        client_id=args.client_id,
        scope=args.scope,
        token_url=args.token_url,
        timeout_seconds=args.timeout_seconds,
    )
    return ErcotPublicReportsClient(
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


def fetch_post_datetimes_from_api(
    client: ErcotPublicReportsClient,
    dataset_id: str,
    monthly_paths: List[Path],
    dataset_root: Path,
    *,
    page_size: int,
    archive_listing_retries: int,
    retry_sleep_seconds: float,
    progress_every_pages: int,
) -> Dict[str, str]:
    if not monthly_paths:
        return {}
    starts: List[date] = []
    ends: List[date] = []
    for path in monthly_paths:
        start, end = month_bounds_from_path(path, dataset_root)
        if start is None or end is None:
            continue
        starts.append(start)
        ends.append(end)
    if not starts or not ends:
        return {}
    window_start = min(starts)
    window_end = max(ends)
    archive_url = f"{API_BASE_URL}/archive/{dataset_id.lower()}"
    post_datetime_from = to_start_iso(window_start)
    post_datetime_to = to_end_iso(window_end)
    mapping: Dict[str, str] = {}
    page = 1
    docs_scanned = 0
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
                        retry_sleep_seconds * (2**listing_attempt),
                    )
                    print(
                        f"DATASET_METADATA_FETCH_RETRY dataset={dataset_id} "
                        f"page={page} attempt={listing_attempt}/{archive_listing_retries} "
                        f"sleep_seconds={cooldown_seconds:.1f} reason=http_429"
                    )
                    time.sleep(cooldown_seconds)
                    continue
                raise

        if not rows:
            break

        docs_scanned += len(rows)
        for row in rows:
            doc_id = str(row.get("docId") or "").strip()
            post_datetime = str(row.get("postDatetime") or "").strip()
            if doc_id and post_datetime and doc_id not in mapping:
                mapping[doc_id] = post_datetime

        if progress_every_pages > 0 and (page == 1 or page % progress_every_pages == 0):
            print(
                f"DATASET_METADATA_FETCH_PROGRESS dataset={dataset_id} "
                f"page={page} docs_scanned={docs_scanned} unique_doc_ids={len(mapping)}"
            )

        if len(rows) < page_size:
            break
        page += 1
    return mapping


def rebuild_monthly_file(
    *,
    dataset_id: str,
    monthly_path: Path,
    post_datetime_map: Dict[str, str],
    archive_doc_map: Dict[str, Dict[str, object]],
    client: Optional[ErcotPublicReportsClient],
    download_missing_sources: bool,
    order: str,
    dry_run: bool,
    bulk_chunk_size: int = BULK_DOWNLOAD_CHUNK_SIZE,
) -> Tuple[int, int, int, int, int, int, str]:
    marker_path = monthly_path.with_suffix(monthly_path.suffix + ".docids")
    doc_ids = load_doc_ids(marker_path)
    if not doc_ids:
        return 0, 0, 0, 0, 0, 0, "missing_docids"

    month_dir = monthly_path.parent
    output_rows: List[Dict[str, str]] = []
    output_fieldnames: List[str] = [POST_DATETIME_COLUMN]
    missing_sources = 0
    downloaded_sources = 0
    docs_with_post_datetime = 0
    docs_missing_post_datetime = 0

    # ---- Phase 1: identify local source files (existence only, no content read) ----
    local_source_map: Dict[str, Optional[Path]] = {
        doc_id: _find_source_file_quick(month_dir, doc_id) for doc_id in doc_ids
    }
    missing_doc_ids = [did for did in doc_ids if local_source_map[did] is None]

    # ---- Phase 2: bulk pre-download missing sources ----
    source_text_cache: Dict[str, str] = {}
    if missing_doc_ids and client is not None and download_missing_sources and not dry_run:
        fetched, downloaded_sources = bulk_fetch_source_texts(
            client, dataset_id, missing_doc_ids, chunk_size=bulk_chunk_size
        )
        source_text_cache.update(fetched)

    # ---- Phase 3: assemble output rows — read each source file ONCE ----
    for doc_id in doc_ids:
        source_path = local_source_map[doc_id]
        if source_path is not None:
            csv_text = read_doc_csv_text(source_path)
        elif doc_id in source_text_cache:
            csv_text = source_text_cache[doc_id]
            source_path = None
        else:
            missing_sources += 1
            continue

        if not csv_text.strip():
            continue
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            continue
        source_fieldnames = cleaned_source_fieldnames(reader.fieldnames)
        for field in source_fieldnames:
            if field not in output_fieldnames:
                output_fieldnames.append(field)

        post_datetime = resolve_post_datetime_for_doc(doc_id, source_path, post_datetime_map)

        if post_datetime:
            docs_with_post_datetime += 1
        else:
            docs_missing_post_datetime += 1

        for row in reader:
            cleaned = cleaned_csv_row_values(row)
            cleaned[POST_DATETIME_COLUMN] = post_datetime
            output_rows.append(cleaned)

    output_rows = sorted_rows_by_post_datetime(output_rows, order)

    if missing_sources > 0:
        # Avoid partial rewrites. Keep original monthly CSV untouched unless all
        # source docs for this month are available.
        return (
            len(doc_ids),
            len(output_rows),
            0,
            downloaded_sources,
            missing_sources,
            docs_missing_post_datetime,
            "skipped_missing_sources",
        )
    if not output_rows:
        return (
            len(doc_ids),
            len(output_rows),
            0,
            downloaded_sources,
            missing_sources,
            docs_missing_post_datetime,
            "skipped_no_rows",
        )

    if dry_run:
        return (
            len(doc_ids),
            len(output_rows),
            0,
            downloaded_sources,
            missing_sources,
            docs_missing_post_datetime,
            "planned",
        )

    tmp_path = monthly_path.with_suffix(monthly_path.suffix + ".postdatetime.tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=output_fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(output_rows)
    tmp_path.replace(monthly_path)
    write_monthly_sort_cache(monthly_path, order)

    return (
        len(doc_ids),
        len(output_rows),
        0,
        downloaded_sources,
        missing_sources,
        docs_missing_post_datetime,
        "rebuilt",
    )


def add_missing_post_datetime_in_place(
    *,
    dataset_id: str,
    monthly_path: Path,
    post_datetime_map: Dict[str, str],
    archive_doc_map: Dict[str, Dict[str, object]],
    client: Optional[ErcotPublicReportsClient],
    download_missing_sources: bool,
    overwrite_post_datetime: bool,
    order: str,
    dry_run: bool,
    bulk_chunk_size: int = BULK_DOWNLOAD_CHUNK_SIZE,
) -> Tuple[int, int, int, int, int, int, str]:
    marker_path = monthly_path.with_suffix(monthly_path.suffix + ".docids")
    doc_ids = load_doc_ids(marker_path)
    if not doc_ids:
        return 0, 0, 0, 0, 0, 0, "missing_docids"

    with open(monthly_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return len(doc_ids), 0, 0, 0, 0, 0, "skipped_no_header"
        fieldnames = list(reader.fieldnames)
        rows = list(reader)
    if not rows:
        return len(doc_ids), 0, 0, 0, 0, 0, "skipped_no_rows"

    existing_post_field = detect_post_datetime_field(fieldnames)
    if existing_post_field is not None and not overwrite_post_datetime:
        missing_rows = sum(1 for row in rows if not str(row.get(existing_post_field) or "").strip())
        if missing_rows == 0:
            rows_to_write = rows
            sorted_changed = False
            if order != "none":
                sorted_rows = sorted_rows_by_post_datetime(rows, order)
                sorted_changed = sorted_rows != rows
                rows_to_write = sorted_rows
            if dry_run:
                return len(doc_ids), len(rows_to_write), 0, 0, 0, 0, "planned_no_missing_rows"
            if not sorted_changed:
                write_monthly_sort_cache(monthly_path, order)
                return len(doc_ids), len(rows_to_write), 0, 0, 0, 0, "unchanged"
            tmp_path = monthly_path.with_suffix(monthly_path.suffix + ".postdatetime.tmp")
            with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(rows_to_write)
            tmp_path.replace(monthly_path)
            write_monthly_sort_cache(monthly_path, order)
            return len(doc_ids), len(rows_to_write), 0, 0, 0, 0, "updated_sorted_only"

    month_dir = monthly_path.parent
    downloaded_sources = 0
    missing_sources = 0
    docs_missing_post_datetime = 0
    doc_plan: List[Tuple[str, int, str, Optional[Path]]] = []

    # ---- Phase 1: identify local source files (existence only, no content read) ----
    local_source_map: Dict[str, Optional[Path]] = {
        doc_id: _find_source_file_quick(month_dir, doc_id) for doc_id in doc_ids
    }
    missing_doc_ids = [did for did in doc_ids if local_source_map[did] is None]

    # ---- Phase 2: bulk pre-download missing sources in one batch per chunk ----
    # source_text_cache maps doc_id → csv_text for both bulk-fetched and
    # pre-read disk files so that Phase 3 and fingerprint fill each read every
    # source file at most once.
    source_text_cache: Dict[str, str] = {}
    if missing_doc_ids and client is not None and download_missing_sources and not dry_run:
        fetched, downloaded_sources = bulk_fetch_source_texts(
            client, dataset_id, missing_doc_ids, chunk_size=bulk_chunk_size
        )
        source_text_cache.update(fetched)

    # ---- Phase 3: build doc_plan — read each local source file ONCE ----
    for doc_id in doc_ids:
        source_path = local_source_map[doc_id]
        if source_path is not None:
            csv_text = read_doc_csv_text(source_path)
            row_count = _count_rows_from_csv_text(csv_text)
            if row_count == 0:
                missing_sources += 1
                continue
            source_text_cache[doc_id] = csv_text  # reuse in fingerprint fill
        elif doc_id in source_text_cache:
            csv_text = source_text_cache[doc_id]
            row_count = _count_rows_from_csv_text(csv_text)
            if row_count == 0:
                missing_sources += 1
                continue
            source_path = None  # bulk-downloaded; no disk path
        else:
            missing_sources += 1
            continue

        post_datetime = resolve_post_datetime_for_doc(doc_id, source_path, post_datetime_map)
        if not post_datetime:
            docs_missing_post_datetime += 1
        doc_plan.append((doc_id, row_count, post_datetime, source_path))

    post_field = detect_post_datetime_field(fieldnames)
    has_post_field = post_field is not None
    if post_field is None:
        post_field = POST_DATETIME_COLUMN
        output_fieldnames = [post_field] + fieldnames
    else:
        output_fieldnames = fieldnames

    if overwrite_post_datetime:
        for row in rows:
            row[post_field] = ""

    expected_rows = sum(item[1] for item in doc_plan)
    row_count_mismatch = expected_rows != len(rows)

    sort_cache_classification = read_monthly_sort_cache_classification(monthly_path)

    # Use fingerprint fill unless the sort cache *explicitly* confirms the file
    # is in original doc-insertion order ("skipped" = sort was never applied).
    # "sorted", "missing", and "invalid" all trigger fingerprint fill because
    # row order cannot be assumed to match .docids insertion order in those
    # cases.  Sequential fill is only safe when we have a cache entry that
    # proves no reordering has occurred.
    use_fingerprint_fill = (
        row_count_mismatch
        or overwrite_post_datetime
        or has_post_field
        or sort_cache_classification != "skipped"
    )

    cells_filled = 0
    if not use_fingerprint_fill:
        row_index = 0
        for _, row_count, post_datetime, _ in doc_plan:
            for _ in range(row_count):
                row = rows[row_index]
                row_index += 1
                current = str(row.get(post_field) or "").strip()
                if not current and post_datetime:
                    row[post_field] = post_datetime
                    cells_filled += 1
    else:
        key_fields = [name for name in output_fieldnames if name != post_field]
        cells_filled = fill_rows_by_source_fingerprint(
            rows=rows,
            key_fields=key_fields,
            post_field=post_field,
            doc_plan=doc_plan,
            source_text_cache=source_text_cache,
            overwrite_mode=overwrite_post_datetime,
        )

    rows_to_write = rows
    sorted_changed = False
    if order != "none":
        sorted_rows = sorted_rows_by_post_datetime(rows, order)
        sorted_changed = sorted_rows != rows
        rows_to_write = sorted_rows

    status = "planned" if not row_count_mismatch else "planned_row_count_mismatch"
    if dry_run:
        return (
            len(doc_ids),
            len(rows_to_write),
            cells_filled,
            downloaded_sources,
            missing_sources,
            docs_missing_post_datetime,
            status,
        )

    if has_post_field and cells_filled == 0 and not sorted_changed:
        unchanged_status = "unchanged" if not row_count_mismatch else "unchanged_row_count_mismatch"
        write_monthly_sort_cache(monthly_path, order)
        return (
            len(doc_ids),
            len(rows_to_write),
            0,
            downloaded_sources,
            missing_sources,
            docs_missing_post_datetime,
            unchanged_status,
        )

    tmp_path = monthly_path.with_suffix(monthly_path.suffix + ".postdatetime.tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=output_fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows_to_write)
    tmp_path.replace(monthly_path)
    write_monthly_sort_cache(monthly_path, order)

    updated_status = "updated" if not row_count_mismatch else "updated_row_count_mismatch"
    return (
        len(doc_ids),
        len(rows_to_write),
        cells_filled,
        downloaded_sources,
        missing_sources,
        docs_missing_post_datetime,
        updated_status,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", action="append", required=True, help="Dataset ID to repair (repeatable).")
    parser.add_argument("--data-root", default="data/raw/ercot", help="Root directory with dataset folders.")
    parser.add_argument("--state-dir", default="state", help="Directory containing *.archive_docs.jsonl cache files.")
    parser.add_argument(
        "--manifest-path",
        default="data/raw/ercot/download_manifest.json",
        help="Path to download manifest JSON used as extra postDateTime source.",
    )
    parser.add_argument("--from-date", type=parse_date, help="Optional inclusive lower month bound (YYYY-MM-DD).")
    parser.add_argument("--to-date", type=parse_date, help="Optional inclusive upper month bound (YYYY-MM-DD).")
    parser.add_argument(
        "--mode",
        choices=("add-missing", "rebuild"),
        default="add-missing",
        help="add-missing fills missing postDateTime in current monthly files; rebuild reconstructs monthly files.",
    )
    parser.add_argument(
        "--order",
        choices=("ascending", "descending", "none"),
        default="ascending",
        help="Final row order by postDateTime (default ascending).",
    )
    parser.add_argument(
        "--download-missing-sources",
        action="store_true",
        help="Download missing per-doc source files before rebuilding monthly CSVs.",
    )
    parser.add_argument(
        "--overwrite-post-datetime",
        action="store_true",
        help=(
            "In add-missing mode, clear existing postDateTime values and refill from doc-level metadata "
            "resolved from source files + state/manifest/API mappings."
        ),
    )
    parser.add_argument(
        "--fetch-missing-post-datetime",
        action="store_true",
        help="Fetch archive metadata from API to fill missing docId -> postDateTime mappings.",
    )
    parser.add_argument("--username", help="ERCOT username (or ERCOT_API_USERNAME).")
    parser.add_argument("--password", help="ERCOT password (or ERCOT_API_PASSWORD).")
    parser.add_argument("--subscription-key", help="API key (or ERCOT_SUBSCRIPTION_KEY).")
    parser.add_argument("--token-url", default=TOKEN_URL)
    parser.add_argument("--client-id", default=DEFAULT_CLIENT_ID)
    parser.add_argument("--scope", default=DEFAULT_SCOPE)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--max-retries", type=int, default=6)
    parser.add_argument("--retry-sleep-seconds", type=float, default=1.5)
    parser.add_argument("--request-interval-seconds", type=float, default=0.35)
    parser.add_argument(
        "--archive-page-size",
        type=int,
        default=1000,
        help="Archive metadata API page size for postDateTime backfill fetches.",
    )
    parser.add_argument(
        "--archive-listing-retries",
        type=int,
        default=8,
        help="Extra retries per archive metadata page when API returns HTTP 429.",
    )
    parser.add_argument(
        "--archive-progress-pages",
        type=int,
        default=10,
        help="Print archive metadata fetch progress every N pages (0 to disable).",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Print per-month postDateTime coverage after processing.",
    )
    parser.add_argument(
        "--delete-redundant-sources",
        action="store_true",
        help=(
            "Delete per-doc source files for a month after processing when postDateTime "
            "coverage is complete for that monthly CSV."
        ),
    )
    parser.add_argument(
        "--archive-redundant-sources-dir",
        help=(
            "Move redundant per-doc source files into this directory (keeps backup) "
            "when postDateTime coverage is complete."
        ),
    )
    parser.add_argument(
        "--bulk-chunk-size",
        type=int,
        default=BULK_DOWNLOAD_CHUNK_SIZE,
        help=(
            f"Number of doc IDs per bulk-download POST when fetching missing source files "
            f"(default {BULK_DOWNLOAD_CHUNK_SIZE}; API maximum is 256).  "
            "Only relevant with --download-missing-sources."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if (args.from_date is None) != (args.to_date is None):
        raise SystemExit("Use both --from-date and --to-date together, or omit both.")
    if args.from_date is not None and args.to_date is not None and args.from_date > args.to_date:
        raise SystemExit("--from-date must be on or before --to-date.")
    if args.delete_redundant_sources and args.archive_redundant_sources_dir:
        raise SystemExit("Use either --delete-redundant-sources or --archive-redundant-sources-dir, not both.")
    return args


def main() -> None:
    args = parse_args()
    dataset_ids = normalized_dataset_ids(args.dataset)
    data_root = Path(args.data_root)
    state_dir = Path(args.state_dir)
    manifest_path = Path(args.manifest_path)
    archive_redundant_sources_dir = (
        Path(args.archive_redundant_sources_dir).expanduser()
        if args.archive_redundant_sources_dir
        else None
    )

    if not data_root.exists():
        raise SystemExit(f"Data root not found: {data_root}")

    client: Optional[ErcotPublicReportsClient] = None
    summaries: List[DatasetSummary] = []

    for dataset_id in tqdm(dataset_ids, desc="Datasets", unit="dataset"):
        dataset_root = data_root / dataset_id
        if not dataset_root.is_dir():
            print(f"DATASET_SKIP dataset={dataset_id} reason=missing_dir")
            continue

        monthly_paths = monthly_csv_paths(dataset_root, args.from_date, args.to_date)
        summary = DatasetSummary(dataset_id=dataset_id, monthly_files=len(monthly_paths))
        summaries.append(summary)

        if not monthly_paths:
            print(f"DATASET_SKIP dataset={dataset_id} reason=no_monthly_files")
            continue

        all_doc_ids: Set[str] = set()
        for monthly_path in monthly_paths:
            all_doc_ids.update(load_doc_ids(monthly_path.with_suffix(monthly_path.suffix + ".docids")))

        post_datetime_map = load_post_datetime_map_from_state(state_dir, dataset_id, doc_id_filter=all_doc_ids)
        archive_doc_map = load_archive_doc_map_from_state(state_dir, dataset_id, doc_id_filter=all_doc_ids)
        manifest_map = load_post_datetime_map_from_manifest(manifest_path, dataset_id)
        for doc_id, value in manifest_map.items():
            if doc_id in all_doc_ids:
                post_datetime_map.setdefault(doc_id, value)

        missing_before_fetch = sum(1 for doc_id in all_doc_ids if doc_id not in post_datetime_map)
        print(
            f"DATASET_PLAN dataset={dataset_id} monthly_files={len(monthly_paths)} "
            f"doc_ids={len(all_doc_ids)} post_datetime_mapped={len(post_datetime_map)} "
            f"missing_post_datetime_mapping={missing_before_fetch}"
        )

        need_api = args.download_missing_sources or (args.fetch_missing_post_datetime and missing_before_fetch > 0)
        if need_api and client is None:
            client = build_api_client(args)

        if args.fetch_missing_post_datetime and missing_before_fetch > 0 and client is not None:
            fetched_map = fetch_post_datetimes_from_api(
                client,
                dataset_id,
                monthly_paths,
                dataset_root,
                page_size=max(1, args.archive_page_size),
                archive_listing_retries=max(0, args.archive_listing_retries),
                retry_sleep_seconds=max(0.0, args.retry_sleep_seconds),
                progress_every_pages=max(0, args.archive_progress_pages),
            )
            for doc_id, value in fetched_map.items():
                post_datetime_map.setdefault(doc_id, value)
            missing_after_fetch = sum(1 for doc_id in all_doc_ids if doc_id not in post_datetime_map)
            print(
                f"DATASET_METADATA_FETCH dataset={dataset_id} fetched={len(fetched_map)} "
                f"missing_post_datetime={missing_after_fetch}"
            )

        for monthly_path in tqdm(monthly_paths, desc=dataset_id, unit="month", leave=False):
            if args.mode == "add-missing":
                (
                    docs_total,
                    rows_written,
                    cells_filled,
                    downloaded_sources,
                    missing_sources,
                    docs_missing_post_datetime,
                    status,
                ) = add_missing_post_datetime_in_place(
                    dataset_id=dataset_id,
                    monthly_path=monthly_path,
                    post_datetime_map=post_datetime_map,
                    archive_doc_map=archive_doc_map,
                    client=client,
                    download_missing_sources=args.download_missing_sources,
                    overwrite_post_datetime=args.overwrite_post_datetime,
                    order=args.order,
                    dry_run=args.dry_run,
                    bulk_chunk_size=args.bulk_chunk_size,
                )
            else:
                (
                    docs_total,
                    rows_written,
                    cells_filled,
                    downloaded_sources,
                    missing_sources,
                    docs_missing_post_datetime,
                    status,
                ) = rebuild_monthly_file(
                    dataset_id=dataset_id,
                    monthly_path=monthly_path,
                    post_datetime_map=post_datetime_map,
                    archive_doc_map=archive_doc_map,
                    client=client,
                    download_missing_sources=args.download_missing_sources,
                    order=args.order,
                    dry_run=args.dry_run,
                    bulk_chunk_size=args.bulk_chunk_size,
                )
            if docs_total == 0:
                print(f"MONTH_SKIP dataset={dataset_id} file={monthly_path} reason=missing_docids")
                continue

            if status == "rebuilt" or status.startswith("updated"):
                summary.monthly_rebuilt += 1
                summary.rows_written += rows_written
                summary.cells_filled += cells_filled
            summary.docs_total += docs_total
            summary.downloaded_sources += downloaded_sources
            summary.missing_sources += missing_sources
            summary.docs_missing_post_datetime += docs_missing_post_datetime
            docs_with_post_datetime = docs_total - docs_missing_post_datetime - missing_sources
            summary.docs_with_post_datetime += max(0, docs_with_post_datetime)

            print(
                f"MONTH_DONE dataset={dataset_id} file={monthly_path} "
                f"docs={docs_total} rows={rows_written} downloaded_sources={downloaded_sources} "
                f"missing_sources={missing_sources} missing_post_datetime={docs_missing_post_datetime} "
                f"cells_filled={cells_filled} status={status}"
            )
            sort_cache_classification = read_monthly_sort_cache_classification(monthly_path)
            print(
                f"MONTH_SORT_CACHE dataset={dataset_id} file={monthly_path} "
                f"classification={sort_cache_classification} order={args.order}"
            )

            if args.verify or args.delete_redundant_sources or archive_redundant_sources_dir is not None:
                total_rows, filled_rows, has_post_field = monthly_post_datetime_coverage(monthly_path)
                missing_rows = max(0, total_rows - filled_rows)
                summary.verified_months += 1
                summary.verified_rows_total += total_rows
                summary.verified_rows_filled += filled_rows
                summary.verified_rows_missing += missing_rows
                if args.verify:
                    print(
                        f"MONTH_VERIFY dataset={dataset_id} file={monthly_path} "
                        f"has_postDateTime={str(has_post_field).lower()} "
                        f"filled_rows={filled_rows} total_rows={total_rows} missing_rows={missing_rows}"
                    )
                cleanup_mode = (
                    "archive"
                    if archive_redundant_sources_dir is not None
                    else ("delete" if args.delete_redundant_sources else "")
                )
                if cleanup_mode:
                    malformed_rows = (
                        monthly_post_datetime_malformed_count(monthly_path)
                        if has_post_field and missing_rows == 0
                        else 0
                    )
                    cleanup_eligible = has_post_field and missing_rows == 0 and malformed_rows == 0
                    if cleanup_eligible:
                        if args.dry_run:
                            if cleanup_mode == "archive":
                                print(
                                    f"MONTH_CLEANUP dataset={dataset_id} file={monthly_path} "
                                    f"archived_sources=0 status=planned_archive "
                                    f"archive_dir={archive_redundant_sources_dir}"
                                )
                            else:
                                print(
                                    f"MONTH_CLEANUP dataset={dataset_id} file={monthly_path} "
                                    f"deleted_sources=0 status=planned_delete"
                                )
                        elif cleanup_mode == "archive":
                            archived_sources = archive_month_source_files(
                                monthly_path,
                                dataset_id,
                                archive_redundant_sources_dir,
                            )
                            summary.sources_archived += archived_sources
                            print(
                                f"MONTH_CLEANUP dataset={dataset_id} file={monthly_path} "
                                f"archived_sources={archived_sources} status=archived "
                                f"archive_dir={archive_redundant_sources_dir}"
                            )
                        else:
                            deleted_sources = delete_month_source_files(monthly_path)
                            summary.sources_deleted += deleted_sources
                            print(
                                f"MONTH_CLEANUP dataset={dataset_id} file={monthly_path} "
                                f"deleted_sources={deleted_sources} status=deleted"
                            )
                    elif has_post_field and missing_rows == 0 and malformed_rows > 0:
                        print(
                            f"MONTH_CLEANUP dataset={dataset_id} file={monthly_path} "
                            f"deleted_sources=0 archived_sources=0 malformed_rows={malformed_rows} "
                            f"status=skipped_malformed_post_datetime"
                        )
                    else:
                        print(
                            f"MONTH_CLEANUP dataset={dataset_id} file={monthly_path} "
                            f"deleted_sources=0 archived_sources=0 status=skipped_incomplete_coverage"
                        )

    for summary in summaries:
        print(
            f"DATASET_SUMMARY dataset={summary.dataset_id} monthly_files={summary.monthly_files} "
            f"monthly_rebuilt={summary.monthly_rebuilt} docs_total={summary.docs_total} "
            f"docs_with_post_datetime={summary.docs_with_post_datetime} "
            f"docs_missing_post_datetime={summary.docs_missing_post_datetime} "
            f"missing_post_datetime_docs={summary.docs_missing_post_datetime} "
            f"downloaded_sources={summary.downloaded_sources} missing_sources={summary.missing_sources} "
            f"rows_written={summary.rows_written} cells_filled={summary.cells_filled} "
            f"sources_deleted={summary.sources_deleted} "
            f"sources_archived={summary.sources_archived} "
            f"verified_months={summary.verified_months} "
            f"rows_with_post_datetime={summary.verified_rows_filled} "
            f"rows_missing_post_datetime={summary.verified_rows_missing} "
            f"rows_total_checked={summary.verified_rows_total}"
        )


if __name__ == "__main__":
    main()
