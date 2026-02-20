#!/usr/bin/env python3
"""
ERCOT batch downloader (scrapes links from a page, filters by keywords & year, downloads and unzips).
Save as ercot_batch_downloader.py

Usage:
    python ercot_batch_downloader.py --base-url "https://www.ercot.com/content/cdr/..." --start-year 2010 --end-year 2025

If you do not know the exact file-list URL, point base-url to:
https://www.ercot.com/mktinfo
and manually find the 'Prices' or 'Market Information' listing page, then point the script there.
"""

import argparse
import os
import re
import sys
import time
import logging
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import zipfile

# ------- CONFIG (edit or override via CLI) -------
BASE_URL = "https://www.ercot.com/mktinfo/prices"  # <-- REPLACE with the ERCOT page that lists historical files
OUTDIR = "data/raw/prices"
KEYWORDS = ["lmp", "price", "prices", "settlement", "rtm", "real-time", "real_time", "realtime", "day-ahead", "dam"]
FOLLOW_LINKS = True   # whether to follow one level of links from BASE_URL
USER_AGENT = "ercot-downloader/1.0 (+https://your.email.or.project)"
MAX_RETRIES = 5
SLEEP_BETWEEN_REQUESTS = 0.5  # seconds
# -------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def get_soup(url, timeout=20):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            logging.warning("Error fetching %s (%s). Retry %d/%d", url, e, attempt + 1, MAX_RETRIES)
            time.sleep(1 + attempt * 2)
    raise RuntimeError(f"Failed to fetch {url}")


def find_links_on_page(url):
    soup = get_soup(url)
    anchors = soup.find_all("a", href=True)
    links = []
    for a in anchors:
        href = a["href"].strip()
        text = (a.get_text() or "").strip()
        full = urljoin(url, href)
        links.append({"href": full, "text": text})
    return links


def looks_like_file_link(href):
    lower = href.lower()
    # common file extensions
    return any(lower.endswith(ext) for ext in [".zip", ".csv", ".gz", ".tgz", ".tar", ".xlsx", ".xls"])


def filter_links(links, keywords, start_year, end_year):
    filtered = []
    year_pattern = re.compile(r"(20\d{2})")  # captures years 2000-2099
    for L in links:
        href = L["href"]
        text = L["text"]
        candidate = href + " " + text
        candidate_lower = candidate.lower()
        # must contain at least one keyword
        if not any(k in candidate_lower for k in keywords):
            continue
        # must mention a year within range (in either href or text). If no year found, still accept but warn.
        years = [int(m) for m in year_pattern.findall(candidate)]
        if years:
            if not any(start_year <= y <= end_year for y in years):
                continue
        # prefer explicit file links but also keep links to pages that may contain files
        filtered.append(L)
    return filtered


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def download_with_resume(url, dest_path):
    """
    Download file with resume support.
    """
    temp_path = dest_path + ".part"
    headers = {}
    pos = 0
    if os.path.exists(temp_path):
        pos = os.path.getsize(temp_path)
        headers["Range"] = f"bytes={pos}-"
    for attempt in range(MAX_RETRIES):
        try:
            with session.get(url, stream=True, headers=headers, timeout=30) as r:
                if r.status_code in (403, 404):
                    raise RuntimeError(f"HTTP {r.status_code} for {url}")
                r.raise_for_status()
                total = r.headers.get("Content-Length")
                if total is not None:
                    total = int(total) + pos
                # stream write
                mode = "ab" if pos else "wb"
                with open(temp_path, mode) as f, tqdm(total=total, unit="B", unit_scale=True, desc=os.path.basename(dest_path), initial=pos) as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            # Move temp file to final dest
            os.replace(temp_path, dest_path)
            return True
        except Exception as e:
            logging.warning("Download %s failed attempt %d/%d: %s", url, attempt + 1, MAX_RETRIES, e)
            time.sleep(1 + attempt * 2)
    return False


def extract_if_zip(path, outdir):
    if not path.lower().endswith(".zip"):
        return
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(outdir)
        logging.info("Extracted %s -> %s", path, outdir)
    except Exception as e:
        logging.warning("Failed to extract %s: %s", path, e)


def normalize_filename_from_url(url):
    p = urlparse(url)
    name = os.path.basename(p.path)
    if not name:
        # fallback: use cleaned netloc+query
        name = (p.netloc + "_" + re.sub(r"[^\w\d]+", "_", p.path))[:200]
    # strip query strings or long params
    name = name.split("?")[0]
    return name


def main(args):
    base_url = args.base_url or BASE_URL
    outdir = args.outdir or OUTDIR
    start_year = args.start_year
    end_year = args.end_year
    keywords = [k.lower() for k in (args.keywords or KEYWORDS)]

    logging.info("Base URL: %s", base_url)
    logging.info("Year range: %d - %d", start_year, end_year)
    logging.info("Keywords: %s", keywords)

    ensure_dir(outdir)

    # Step 1: find links on base page
    logging.info("Fetching links from base URL...")
    top_links = find_links_on_page(base_url)

    candidate_links = []
    # include direct file-like links from base page
    for L in top_links:
        if looks_like_file_link(L["href"]):
            candidate_links.append(L)

    # also filter by keywords & year
    filtered = filter_links(top_links, keywords, start_year, end_year)
    for L in filtered:
        if L not in candidate_links:
            candidate_links.append(L)

    # if FOLLOW_LINKS: follow each filtered link and collect file links from there
    if FOLLOW_LINKS:
        logging.info("Following %d candidate page links to find files...", len(filtered))
        for idx, L in enumerate(filtered, 1):
            href = L["href"]
            try:
                links = find_links_on_page(href)
                # take file-like links on that page
                for sub in links:
                    if looks_like_file_link(sub["href"]):
                        candidate_links.append(sub)
                # also filter by keywords/year on sub-pages
                sub_filtered = filter_links(links, keywords, start_year, end_year)
                for s in sub_filtered:
                    if s not in candidate_links and looks_like_file_link(s["href"]):
                        candidate_links.append(s)
            except Exception as e:
                logging.warning("Could not follow %s: %s", href, e)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    # deduplicate by href
    seen = set()
    unique_links = []
    for L in candidate_links:
        if L["href"] not in seen:
            unique_links.append(L)
            seen.add(L["href"])

    logging.info("Found %d unique candidate file links", len(unique_links))

    # Filter again for year mention and download
    year_regex = re.compile(r"(20\d{2})")
    to_download = []
    for L in unique_links:
        href = L["href"]
        text = L["text"]
        yrs = [int(m) for m in year_regex.findall(href + " " + text)]
        if yrs:
            if not any(start_year <= y <= end_year for y in yrs):
                continue
        # Accept link
        to_download.append(L)

    logging.info("After year filtering: %d links to consider for download", len(to_download))

    # Download loop
    for L in to_download:
        url = L["href"]
        fname = normalize_filename_from_url(url)
        # try to extract year from filename
        years = year_regex.findall(fname)
        year = years[0] if years else "unknown"
        # create output path by year
        dest_dir = os.path.join(outdir, str(year))
        ensure_dir(dest_dir)
        dest_path = os.path.join(dest_dir, fname)
        if os.path.exists(dest_path):
            logging.info("Already have %s -- skipping", dest_path)
            continue
        logging.info("Downloading %s -> %s", url, dest_path)
        success = download_with_resume(url, dest_path)
        if not success:
            logging.error("Failed to download %s", url)
            continue
        # if zip, extract
        try:
            extract_if_zip(dest_path, dest_dir)
        except Exception as e:
            logging.warning("Extraction error for %s: %s", dest_path, e)
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    logging.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ERCOT batch downloader (scrape & download files from a listing page).")
    parser.add_argument("--base-url", type=str, help="Page URL that lists files (ERCOT 'Prices' or 'Market Info' page).")
    parser.add_argument("--outdir", type=str, help="Output directory root.")
    parser.add_argument("--start-year", type=int, default=2010, help="Start year (inclusive).")
    parser.add_argument("--end-year", type=int, default=2025, help="End year (inclusive).")
    parser.add_argument("--keywords", nargs="+", help="Keywords to filter candidate files.")
    args = parser.parse_args()
    main(args)