#!/usr/bin/env python3
"""Download ERCOT public report data by dataset ID and date range."""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

import requests

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


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


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
        # HTTP-date form is rare here; fallback to default backoff path.
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


def append_doc_to_monthly_csv(source_path: Path, monthly_path: Path) -> int:
    csv_text = read_doc_csv_text(source_path)
    if not csv_text.strip():
        return 0
    lines = csv_text.splitlines()
    if not lines:
        return 0
    monthly_path.parent.mkdir(parents=True, exist_ok=True)
    has_existing = monthly_path.exists() and monthly_path.stat().st_size > 0
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

    # Typical empty archive response includes only metadata + product details.
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
        # No count fields present; still treat this shape as empty archive rather than an error.
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
        self.session.headers.update(
            {
                "Authorization": f"Bearer {bearer_token}",
                "Ocp-Apim-Subscription-Key": subscription_key,
                "Accept": "application/json",
                "User-Agent": "spring-2026-electricity-TX/ercot-downloader",
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
                # Try next candidate on common lookup-path misses or throttling.
                if status in (400, 404, 429, 500, 502, 503, 504) and index < len(candidates) - 1:
                    continue
                raise
        raise RuntimeError("All download URL candidates failed.")


def choose_filename(doc: Dict[str, object]) -> str:
    for key in ("constructedName", "friendlyName"):
        raw = doc.get(key)
        if isinstance(raw, str) and raw.strip():
            return safe_filename(raw)
    doc_id = str(doc.get("docId", "")).strip()
    return safe_filename(f"{doc_id}.bin" if doc_id else "ercot_document.bin")


def with_doc_id_suffix(filename: str, doc_id: str) -> str:
    # Archive often contains multiple docs sharing the same constructed filename.
    # Suffix with doc ID to prevent silent overwrite within each month folder.
    base, ext = os.path.splitext(filename)
    return f"{base}__{doc_id}{ext}" if doc_id else filename


def monthly_csv_path(outdir: Path, dataset_id: str, dataset_subdir: Path) -> Path:
    parts = dataset_subdir.parts
    if len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
        year = parts[-2]
        month = parts[-1]
        return outdir / dataset_id / year / month / f"{dataset_id}_{year}{month}.csv"
    return outdir / dataset_id / dataset_subdir / f"{dataset_id}_undated.csv"


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
    with zipfile.ZipFile(path, "r") as archive:
        archive.extractall(path.parent)


def list_selected_datasets(dataset_ids: Iterable[str]) -> None:
    print("Selected datasets")
    print("=================")
    for dataset_id in dataset_ids:
        metadata = DATASETS.get(dataset_id, {})
        title = metadata.get("title", "Unknown dataset")
        reason = metadata.get("reason", "No reason in catalog.")
        print(f"- {dataset_id}: {title}")
        print(f"  reason: {reason}")


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
) -> List[Dict[str, object]]:
    docs: List[Dict[str, object]] = []
    page = 1
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
                    print(
                        "Archive listing 429 for "
                        f"{dataset_id} page {page} (attempt {listing_attempt}/{archive_listing_retries}). "
                        f"Sleeping {cooldown_seconds:.1f}s before retry."
                    )
                    time.sleep(cooldown_seconds)
                    continue
                raise

        if not rows:
            break
        docs.extend(rows)
        if progress_every_pages > 0 and (page == 1 or page % progress_every_pages == 0):
            print(
                "Archive listing progress "
                f"{dataset_id}: page={page} docs_collected={len(docs)}"
            )
        if len(rows) < page_size:
            break
        page += 1
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
                print(
                    "Archive probe 429 for "
                    f"{dataset_id} (attempt {listing_attempt}/{archive_listing_retries}). "
                    f"Sleeping {cooldown_seconds:.1f}s before retry."
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

    # Coarse-to-fine probe: year -> month -> day.
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

            # Fallback: month has docs but day-level probing found none.
            return month_start

        # Fallback: year has docs but month-level probing found none.
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


def _parse_csv_date(value: str) -> Optional[datetime]:
    raw = value.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _parse_hour_ending(value: str) -> Optional[Tuple[int, int]]:
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
    if hour == 24:
        return 23, 59
    return hour, 0


def _parse_csv_datetime(value: str) -> Optional[datetime]:
    raw = value.strip()
    if not raw:
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


def _csv_row_timestamp(row: Dict[str, str], lower_to_name: Dict[str, str]) -> Optional[datetime]:
    def get(name: str) -> str:
        actual = lower_to_name.get(name.lower())
        if actual is None:
            return ""
        return str(row.get(actual, "") or "")

    # Single timestamp columns.
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

    # Older wind files: HOUR_ENDING already has full datetime.
    parsed_hour_ending_dt = _parse_csv_datetime(get("hour_ending"))
    if parsed_hour_ending_dt is not None:
        return parsed_hour_ending_dt

    # Date + hour pairs.
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


def sort_monthly_csv(path: Path, sort_order: str) -> str:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return "skipped"
        fieldnames = list(reader.fieldnames)
        lower_to_name = {name.lower(): name for name in fieldnames}
        raw_rows = list(reader)
    if not raw_rows:
        return "already"

    parsed_rows: List[Tuple[datetime, int, Dict[str, str]]] = []
    unparsed_rows: List[Tuple[int, Dict[str, str]]] = []
    for index, row in enumerate(raw_rows):
        timestamp = _csv_row_timestamp(row, lower_to_name)
        if timestamp is None:
            unparsed_rows.append((index, row))
            continue
        parsed_rows.append((timestamp, index, row))

    if not parsed_rows:
        return "skipped"

    ordered_parsed = sorted(parsed_rows, key=lambda item: (item[0], item[1]))
    if sort_order == "descending":
        ordered_parsed = list(reversed(ordered_parsed))
    elif sort_order != "ascending":
        raise ValueError(f"Unknown sort order '{sort_order}'.")

    ordered_rows = [item[2] for item in ordered_parsed] + [item[1] for item in unparsed_rows]
    original_rows = raw_rows
    if ordered_rows == original_rows:
        return "already"

    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered_rows)
    return "sorted"


def parse_args() -> argparse.Namespace:
    today = date.today()
    default_from = today - timedelta(days=30)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", help="ERCOT API portal username. Falls back to ERCOT_API_USERNAME.")
    parser.add_argument("--password", help="ERCOT API portal password. Falls back to ERCOT_API_PASSWORD.")
    parser.add_argument(
        "--subscription-key",
        help="API subscription key. Falls back to ERCOT_SUBSCRIPTION_KEY.",
    )
    parser.add_argument("--from-date", type=parse_date, default=default_from, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--to-date", type=parse_date, default=today, help="End date (YYYY-MM-DD).")
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
        help="Use only --dataset IDs (do not include default core profile when --profile is omitted).",
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
        default="match-download-order",
        help=(
            "Post-sort each touched monthly CSV by timestamp after dataset processing. "
            "'match-download-order' uses descending for newest-first downloads, ascending otherwise."
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
        default=0.35,
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
        print(f"Using earliest-available mode: --from-date set to {args.from_date.isoformat()}")
    if args.from_date > args.to_date:
        raise SystemExit("--from-date must be on or before --to-date.")
    if args.page_size <= 0:
        raise SystemExit("--page-size must be greater than 0.")
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
    if selected_profiles is None:
        selected_profiles = [] if args.datasets_only else ["core"]
    selected_ids = resolve_dataset_ids(selected_profiles, args.dataset)
    excluded_ids = set(normalize_dataset_ids(args.exclude_dataset or []))
    if excluded_ids:
        selected_ids = [dataset_id for dataset_id in selected_ids if dataset_id not in excluded_ids]
        print(f"Excluded datasets: {', '.join(sorted(excluded_ids))}")
    if not selected_ids:
        raise SystemExit("No datasets selected after exclusions.")
    list_selected_datasets(selected_ids)

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
        print(f"Warning: unable to list public reports catalog, continuing: {exc}")
        public_reports = []
    product_by_id: Dict[str, Dict[str, object]] = {}
    for product in public_reports:
        report_id = str(product.get("emilId", "")).upper().strip()
        if report_id:
            product_by_id[report_id] = product

    if args.list_api_products:
        print("")
        print("API products")
        print("============")
        for report_id in sorted(product_by_id):
            title = str(product_by_id[report_id].get("reportName", ""))
            print(f"- {report_id}: {title}")
        return

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stats = DownloadStats()
    manifest_rows: List[Dict[str, object]] = []
    marker_cache: Dict[Path, Set[str]] = {}
    consecutive_network_failures = 0

    for dataset_id in selected_ids:
        product = product_by_id.get(dataset_id)
        if product_by_id and product is None:
            print("")
            print(f"[{dataset_id}]")
            print(
                "Skipped: dataset is not present in current ERCOT public-reports catalog "
                "for this account/subscription."
            )
            print("Tip: run with --list-api-products and use one of the listed EMIL IDs.")
            stats.skipped_unavailable_dataset += 1
            continue
        product = product or {}
        product_title = str(
            product.get("reportName") or product.get("name") or DATASETS.get(dataset_id, {}).get("title", "")
        ).strip()
        archive_url = maybe_product_archive_href(product) or f"{API_BASE_URL}/archive/{dataset_id.lower()}"
        print("")
        print(f"[{dataset_id}] {product_title}")
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
                print(f"Earliest-date detection failed for {dataset_id}: {exc}")
                continue
            if detected_from_date is None:
                print(
                    "No archive docs found for this dataset between "
                    f"{args.from_date.isoformat()} and {args.to_date.isoformat()}."
                )
                continue
            dataset_from_date = detected_from_date
            print(f"Auto-detected earliest available date: {dataset_from_date.isoformat()}")

        dataset_post_datetime_from = to_start_iso(dataset_from_date)
        dataset_post_datetime_to = to_end_iso(args.to_date)
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
            )
        except Exception as exc:  # noqa: BLE001
            stats.failures += 1
            print(f"Archive listing failed for {dataset_id}: {exc}")
            continue
        print(
            "Archive documents found "
            f"({dataset_from_date.isoformat()} to {args.to_date.isoformat()}): {len(docs)}"
        )
        if not docs:
            print("No archive docs in this date window.")
            continue
        if args.download_order != "api":
            print(
                f"Sorting {len(docs)} archive docs for download order: {args.download_order} ..."
            )
        docs = order_archive_docs(docs, args.download_order)
        if args.download_order != "api":
            print(f"Applying download order: {args.download_order}")
        if args.max_docs_per_dataset > 0:
            docs = docs[: args.max_docs_per_dataset]
            print(f"Applying --max-docs-per-dataset: {len(docs)} docs")

        for doc in docs:
            doc_id = extract_doc_id(doc)
            if not doc_id:
                stats.skipped_missing_doc_id += 1
                continue
            doc_started_at = time.monotonic()
            filename = choose_filename(doc)
            filename = with_doc_id_suffix(filename, doc_id)
            dataset_subdir = dataset_subdir_from_doc(doc)
            monthly_path = monthly_csv_path(outdir, dataset_id, dataset_subdir)
            marker_path = marker_path_for_monthly(monthly_path)
            if args.consolidate_monthly:
                known_doc_ids = marker_cache.get(marker_path)
                if known_doc_ids is None:
                    known_doc_ids = load_marker_doc_ids(marker_path)
                    marker_cache[marker_path] = known_doc_ids
                if doc_id in known_doc_ids:
                    stats.skipped_existing += 1
                    continue
            destination = outdir / dataset_id / dataset_subdir / filename
            wanted_size = expected_size(doc)
            exists_and_matches = (
                destination.exists()
                and (wanted_size < 0 or destination.stat().st_size == wanted_size)
            )
            if exists_and_matches and not args.consolidate_monthly:
                stats.skipped_existing += 1
                continue
            if args.dry_run:
                if args.consolidate_monthly:
                    if exists_and_matches:
                        print(
                            "DRY RUN consolidate-existing: "
                            f"{dataset_id} docId={doc_id} {destination} -> {monthly_path}"
                        )
                    else:
                        print(
                            "DRY RUN download+consolidate: "
                            f"{dataset_id} docId={doc_id} -> {destination} -> {monthly_path}"
                        )
                else:
                    print(f"DRY RUN download: {dataset_id} docId={doc_id} -> {destination}")
                continue
            try:
                source_path = destination
                downloaded_now = False
                if not (args.consolidate_monthly and exists_and_matches):
                    client.download_doc(dataset_id, doc_id, destination, doc)
                    downloaded_now = True
                if args.consolidate_monthly:
                    appended_rows = append_doc_to_monthly_csv(source_path, monthly_path)
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
                consecutive_network_failures = 0
            except Exception as exc:  # noqa: BLE001
                stats.failures += 1
                print(f"Download failed for {dataset_id} docId={doc_id}: {exc}")
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
                            "Check internet/DNS and rerun; completed docs are resumable via .docids."
                        ) from exc
                else:
                    consecutive_network_failures = 0
                continue

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
                    print(
                        "FILE COMPLETE "
                        f"{action} dataset={dataset_id} docId={doc_id} "
                        f"file={output_file} stampdate={stampdate} date={stampdate_date} year={year} month={month} "
                        f"elapsed={elapsed_seconds:.2f}s completed_at={completed_at}"
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
                            print(
                                "STAMPDATE COMPLETE "
                                f"dataset={dataset_id} stampdate={current_stampdate} "
                                f"year={current_stampdate_year} month={current_stampdate_month} "
                                f"files={current_stampdate_files} completed_at={completed_at}"
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
                        print(
                            "DAY COMPLETE "
                            f"dataset={dataset_id} date={current_date_key} "
                            f"year={current_date_year} month={current_date_month} "
                            f"files={current_date_files} completed_at={completed_at}"
                        )
                        current_date_key = date_key
                        current_date_files = 1
                        current_date_year = year
                        current_date_month = month
                elif args.file_timing_frequency in calendar_day_schedules:
                    schedule_days = calendar_day_schedules[args.file_timing_frequency]
                    if stampdate_day in schedule_days and stampdate_date not in printed_calendar_dates:
                        print(
                            "DATE SCHEDULE HIT "
                            f"schedule={args.file_timing_frequency} dataset={dataset_id} "
                            f"date={stampdate_date} day={stampdate_day} year={year} month={month} "
                            f"docId={doc_id} completed_at={completed_at}"
                        )
                        printed_calendar_dates.add(stampdate_date)
                elif args.file_timing_frequency == "1-month":
                    if current_month_key is None:
                        current_month_key = month_key
                        current_month_files = 1
                    elif month_key == current_month_key:
                        current_month_files += 1
                    else:
                        print(
                            "MONTH COMPLETE "
                            f"dataset={dataset_id} month={current_month_key} "
                            f"files={current_month_files} completed_at={completed_at}"
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
                print(
                    "STAMPDATE COMPLETE "
                    f"dataset={dataset_id} stampdate={current_stampdate} "
                    f"year={current_stampdate_year} month={current_stampdate_month} "
                    f"files={current_stampdate_files} completed_at={completed_at}"
                )
        if args.file_timing_frequency == "daily" and current_date_key is not None:
            completed_at = datetime.now().astimezone().isoformat(timespec="seconds")
            print(
                "DAY COMPLETE "
                f"dataset={dataset_id} date={current_date_key} "
                f"year={current_date_year} month={current_date_month} "
                f"files={current_date_files} completed_at={completed_at}"
            )
        if args.file_timing_frequency == "1-month" and current_month_key is not None:
            completed_at = datetime.now().astimezone().isoformat(timespec="seconds")
            print(
                "MONTH COMPLETE "
                f"dataset={dataset_id} month={current_month_key} "
                f"files={current_month_files} completed_at={completed_at}"
            )

        monthly_paths_to_sort: Set[Path] = set()
        if args.consolidate_monthly:
            monthly_paths_to_sort.update(touched_monthly_paths)
        if args.sort_existing_monthly:
            monthly_paths_to_sort.update(
                path for path in (outdir / dataset_id).glob("**/*.csv") if path.is_file()
            )
        if monthly_sort_order and monthly_paths_to_sort:
            print(
                "Post-sorting monthly CSV files "
                f"({len(monthly_paths_to_sort)}) in {monthly_sort_order} order..."
            )
            for monthly_path in sorted(monthly_paths_to_sort):
                try:
                    sort_status = sort_monthly_csv(monthly_path, monthly_sort_order)
                except Exception as exc:  # noqa: BLE001
                    stats.monthly_sort_failures += 1
                    print(f"Monthly sort failed for {monthly_path}: {exc}")
                    continue
                if sort_status == "sorted":
                    stats.monthly_sorted += 1
                elif sort_status == "already":
                    stats.monthly_already_sorted += 1
                else:
                    stats.monthly_sort_skipped += 1
                print(
                    f"Monthly sort {sort_status}: {monthly_path} "
                    f"(order={monthly_sort_order})"
                )

    print("")
    print("Download summary")
    print("================")
    print(f"Downloaded: {stats.downloaded}")
    print(f"Skipped existing: {stats.skipped_existing}")
    print(f"Skipped missing docId: {stats.skipped_missing_doc_id}")
    print(f"Skipped unavailable dataset: {stats.skipped_unavailable_dataset}")
    if args.consolidate_monthly:
        print(f"Monthly files updated: {stats.consolidated_updates}")
    if monthly_sort_order and (args.consolidate_monthly or args.sort_existing_monthly):
        print(f"Monthly files sorted: {stats.monthly_sorted}")
        print(f"Monthly files already sorted: {stats.monthly_already_sorted}")
        print(f"Monthly files sort skipped: {stats.monthly_sort_skipped}")
        print(f"Monthly files sort failures: {stats.monthly_sort_failures}")
    print(f"Failures: {stats.failures}")

    if args.write_manifest and manifest_rows:
        manifest_path = outdir / "download_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest_rows, handle, indent=2)
        print(f"Manifest written: {manifest_path}")


if __name__ == "__main__":
    main()
