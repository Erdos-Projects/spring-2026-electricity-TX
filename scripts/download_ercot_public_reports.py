#!/usr/bin/env python3
"""Download ERCOT public report data by dataset ID and date range."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

import requests

from ercot_dataset_catalog import DATASETS, available_profiles, resolve_dataset_ids

TOKEN_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
)
API_BASE_URL = "https://api.ercot.com/api/public-reports"
DEFAULT_CLIENT_ID = "fec253ea-0d06-4272-a5e6-b478baeecd70"
DEFAULT_SCOPE = f"openid {DEFAULT_CLIENT_ID} offline_access"


@dataclass
class DownloadStats:
    downloaded: int = 0
    skipped_existing: int = 0
    skipped_missing_doc_id: int = 0
    consolidated_updates: int = 0
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


def coerce_list(payload: object) -> List[Dict[str, object]]:
    rows = _find_first_list_of_dicts(payload)
    if rows is not None:
        return rows

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
            rows = coerce_list(response.json())
            if not rows:
                break
            for row in rows:
                yield row
            if len(rows) < page_size:
                break
            page += 1

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
    if args.from_date > args.to_date:
        raise SystemExit("--from-date must be on or before --to-date.")
    if args.page_size <= 0:
        raise SystemExit("--page-size must be greater than 0.")
    if args.delete_source_after_consolidation and not args.consolidate_monthly:
        raise SystemExit("--delete-source-after-consolidation requires --consolidate-monthly.")

    selected_ids = resolve_dataset_ids(args.profile or ["core"], args.dataset)
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

    post_datetime_from = to_start_iso(args.from_date)
    post_datetime_to = to_end_iso(args.to_date)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stats = DownloadStats()
    manifest_rows: List[Dict[str, object]] = []
    marker_cache: Dict[Path, Set[str]] = {}

    for dataset_id in selected_ids:
        product = product_by_id.get(dataset_id, {})
        product_title = str(
            product.get("reportName") or product.get("name") or DATASETS.get(dataset_id, {}).get("title", "")
        ).strip()
        archive_url = maybe_product_archive_href(product) or f"{API_BASE_URL}/archive/{dataset_id.lower()}"
        print("")
        print(f"[{dataset_id}] {product_title}")
        try:
            docs = list(
                client.iter_archive_docs(
                    archive_url=archive_url,
                    post_datetime_from=post_datetime_from,
                    post_datetime_to=post_datetime_to,
                    page_size=args.page_size,
                )
            )
        except Exception as exc:  # noqa: BLE001
            stats.failures += 1
            print(f"Archive listing failed for {dataset_id}: {exc}")
            continue
        print(f"Archive documents found: {len(docs)}")
        if args.max_docs_per_dataset > 0:
            docs = docs[: args.max_docs_per_dataset]
            print(f"Applying --max-docs-per-dataset: {len(docs)} docs")

        for doc in docs:
            doc_id = extract_doc_id(doc)
            if not doc_id:
                stats.skipped_missing_doc_id += 1
                continue
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
            except Exception as exc:  # noqa: BLE001
                stats.failures += 1
                print(f"Download failed for {dataset_id} docId={doc_id}: {exc}")
                continue

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

    print("")
    print("Download summary")
    print("================")
    print(f"Downloaded: {stats.downloaded}")
    print(f"Skipped existing: {stats.skipped_existing}")
    print(f"Skipped missing docId: {stats.skipped_missing_doc_id}")
    if args.consolidate_monthly:
        print(f"Monthly files updated: {stats.consolidated_updates}")
    print(f"Failures: {stats.failures}")

    if args.write_manifest and manifest_rows:
        manifest_path = outdir / "download_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest_rows, handle, indent=2)
        print(f"Manifest written: {manifest_path}")


if __name__ == "__main__":
    main()
