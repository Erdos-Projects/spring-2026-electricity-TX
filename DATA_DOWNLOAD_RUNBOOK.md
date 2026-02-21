# ERCOT Data Download Runbook

Use this runbook as the single source of truth for ERCOT API downloads in this repo.

Goal:
- Run downloads reliably from terminal.
- Reuse one canonical command template.
- Keep dataset choices, frequencies, and earliest-date references in one place.

## Table of Contents

1. [Prepare Environment](#1-prepare-environment)
2. [Run Canonical Download Command (Window-Based)](#2-run-canonical-download-command-window-based)
3. [Tune Logging and Progress](#3-tune-logging-and-progress)
4. [Select Datasets (Priority + Usage)](#4-select-datasets-priority--usage)
5. [Resume Failed Runs and Handle Errors](#5-resume-failed-runs-and-handle-errors)
6. [Diagnose DNS with Verbose Logging](#6-diagnose-dns-with-verbose-logging)
7. [Apply Git LFS After Dataset Completion](#7-apply-git-lfs-after-dataset-completion)

## 1. Prepare Environment

Run from project root:

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
```

Create/activate Python environment and install required package:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install requests
python3 -c "import requests; print('requests ok:', requests.__version__)"
```

Create ERCOT API access:
1. Sign in/register at ERCOT API portal.
2. Allow pop-ups/redirects for `ercotb2c.b2clogin.com`.
3. Subscribe to `public-reports` API product.
4. Copy your subscription key.

Export credentials:

```bash
export ERCOT_API_USERNAME="your_username"
export ERCOT_API_PASSWORD="your_password"
export ERCOT_SUBSCRIPTION_KEY="your_subscription_key"
```

Optionally list available API product IDs for your account.

```bash
python3 scripts/download_ercot_public_reports.py --list-api-products
```

## 2. Run Canonical Download Command (Window-Based)

Use this template for all runs.

Change only:
- Set `START_DATE` (`YYYY-MM-DD`).
- Set `MONTHS` (`1`, `3`, `6`, `12`).
- Edit dataset lines (`--dataset ...`).

`END_DATE` is auto-calculated and capped at `NEWEST_DATE` (yesterday).

```bash
START_DATE="2025-11-01"
MONTHS=3
NEWEST_DATE="$(python3 - <<'PY'
from datetime import date, timedelta
print((date.today() - timedelta(days=1)).isoformat())
PY
)"

if [[ ! "$MONTHS" =~ ^(1|3|6|12)$ ]]; then
  echo "MONTHS must be one of: 1, 3, 6, 12"
  exit 1
fi

if [[ "$START_DATE" > "$NEWEST_DATE" ]]; then
  echo "START_DATE (${START_DATE}) is after NEWEST_DATE (${NEWEST_DATE})"
  exit 1
fi

END_DATE="$(python3 - "$START_DATE" "$MONTHS" "$NEWEST_DATE" <<'PY'
import sys
import calendar
from datetime import date, timedelta

y, m, d = map(int, sys.argv[1].split("-"))
months = int(sys.argv[2])
newest = date.fromisoformat(sys.argv[3])
start = date(y, m, d)

month_index = (start.year * 12 + start.month - 1) + months
end_year = month_index // 12
end_month = month_index % 12 + 1
end_day = min(start.day, calendar.monthrange(end_year, end_month)[1])
end_exclusive = date(end_year, end_month, end_day)
window_end = end_exclusive - timedelta(days=1)
if window_end > newest:
    window_end = newest
print(window_end.isoformat())
PY
)"

echo "Downloading ${MONTHS}-month window: ${START_DATE} to ${END_DATE} (newest cap: ${NEWEST_DATE})"

python3 scripts/download_ercot_public_reports.py \
  --datasets-only \
  --dataset NP6-346-CD \
  --dataset NP6-905-CD \
  --dataset NP4-732-CD \
  --dataset NP4-745-CD \
  --dataset NP3-233-CD \
  --dataset NP3-565-CD \
  --dataset NP4-523-CD \
  --dataset NP6-788-CD \
  --dataset NP6-331-CD \
  --dataset NP4-188-CD \
  --dataset NP3-911-ER \
  --from-date "${START_DATE}" \
  --to-date "${END_DATE}" \
  --outdir data/raw/ercot \
  --consolidate-monthly \
  --delete-source-after-consolidation \
  --download-order newest-first \
  --sort-monthly-output descending \
  --sort-existing-monthly \
  --archive-progress-pages 100 \
  --request-interval-seconds 2.0 \
  --max-retries 10 \
  --retry-sleep-seconds 4 \
  --archive-listing-retries 20 \
  --max-consecutive-network-failures 8 \
  --network-failure-cooldown-seconds 30 \
  --file-timing-frequency daily \
  --write-manifest
```

Expect this behavior:
- `--download-order newest-first`: download backward in time.
- `--sort-monthly-output descending`: keep monthly CSV rows newest-to-oldest by timestamp.
- `.docids` sidecars prevent duplicate appends on rerun.

## 3. Tune Logging and Progress

### File timing frequency

Set `--file-timing-frequency` to:
- `off`
- `every-file`
- `1-stampdate`
- `12-stampdates`
- `24-stampdates`
- `daily`
- `1-month`
- `bi-month` (days `1`, `15`)
- `tri-month` (days `1`, `10`, `20`)
- `quad-month` (days `1`, `7`, `15`, `22`)

### Archive listing progress

Set `--archive-progress-pages` to:
- `1` (most verbose)
- `10`
- `100`
- `1000` (least verbose)

## 4. Select Datasets (Priority + Usage)

`NP3-912-ER` note:
- Treat this dataset as unresolved in API archive (`archive/np3-912-er` returned `404` in current checks).
- Keep it out of default runs until ERCOT confirms the correct EMIL ID or endpoint.

| Tier | Dataset ID | Type | Observed frequency | Earliest date (online reference) | Primary usage |
|---|---|---|---|---|---|
| 1 | `NP6-346-CD` | Load (actual) | Hourly | 2014-06-19 | Demand baseline, peak analysis |
| 1 | `NP6-905-CD` | Price (settlement) | 15-minute | 2014-05-01 | Main RT market outcome prices |
| 1 | `NP4-732-CD` | Renewable (wind) | Hourly | 2014-08-17 | Wind variability and forecast impact |
| 1 | `NP4-745-CD` | Renewable (solar) | Hourly | 2016-03-12 | Solar ramp and net-load impact |
| 1 | `NP3-233-CD` | Reliability | Hourly | 2014-08-08 | Outage and scarcity context |
| 2 | `NP3-565-CD` | Load forecast | Hourly | 2014-08-08 | Forecast error and planning context |
| 2 | `NP4-523-CD` | Price (DA) | Hourly | 2015-03-26 | DA benchmark vs RT |
| 2 | `NP6-788-CD` | Price (LMP detail) | Interval/market detail | 2015-03-26 | Extra spatial price detail |
| 3 | `NP6-331-CD` | Ancillary prices (RT) | 15-minute | 2015-03-26 | RT reserve/scarcity pricing |
| 3 | `NP4-188-CD` | Ancillary prices (DA) | Hourly/market detail | 2015-03-26 | DA ancillary pricing |
| 3 | `NP3-911-ER` | Renewable detail | Hourly/report-specific | 2025-04-04 | Resource capability vs output |
| pending | `NP3-912-ER` | Weather/load forecast | unresolved | unresolved (API 404) | Weather-load sensitivity |

Earliest-date reference source:
- ERCOT Market Participants data-product details (`First Run Date`), checked 2026-02-21.
- URL pattern: `https://www.ercot.com/mp/data-products/data-product-details?id=<DATASET_ID>`.

## 5. Resume Failed Runs and Handle Errors

When a run fails:
- Rerun the same command after interruptions.
- Deduplication uses:
  - `data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv.docids`

Do not delete `.docids` unless you intentionally want to rebuild monthly files.

Common issues:
1. `429 Too Many Requests`
- Increase `--request-interval-seconds`.
- Keep `--archive-listing-retries` high for large datasets.

2. DNS errors (`Failed to resolve`, `NameResolutionError`)
- Wait and rerun.
- Optional precheck:

```bash
while ! nslookup api.ercot.com >/dev/null 2>&1 || ! nslookup ercotb2c.b2clogin.com >/dev/null 2>&1; do
  echo "DNS not ready, retrying in 30s..."
  sleep 30
done
```

3. `401 Unauthorized`
- Re-export credentials.
- Retry after DNS is stable.

4. Dataset endpoint `404`
- The dataset may not be in current public-reports catalog for your account.
- Check available IDs with `--list-api-products`.

## 6. Diagnose DNS with Verbose Logging

Use this loop to diagnose unstable API connectivity.

```bash
mkdir -p logs
DNS_LOG="logs/dns_health_$(date +%Y%m%d_%H%M%S).log"
echo "DNS health log: $DNS_LOG"

while true; do
  TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "[$TS] nslookup api.ercot.com" | tee -a "$DNS_LOG"
  nslookup api.ercot.com 2>&1 | tee -a "$DNS_LOG"

  echo "[$TS] nslookup ercotb2c.b2clogin.com" | tee -a "$DNS_LOG"
  nslookup ercotb2c.b2clogin.com 2>&1 | tee -a "$DNS_LOG"

  if nslookup api.ercot.com >/dev/null 2>&1 && nslookup ercotb2c.b2clogin.com >/dev/null 2>&1; then
    echo "[$TS] DNS healthy" | tee -a "$DNS_LOG"
    break
  fi

  echo "[$TS] DNS unstable, retrying in 30s" | tee -a "$DNS_LOG"
  sleep 30
done
```

Share the latest DNS log quickly:

```bash
tail -n 40 "$(ls -1t logs/dns_health_*.log | head -n 1)"
```

## 7. Apply Git LFS After Dataset Completion

Install and initialize:

```bash
brew install git-lfs
git lfs install
git lfs version
```

Track and migrate completed dataset files (example: `NP6-346-CD`):

```bash
git lfs track "data/raw/ercot/NP6-346-CD/**/*.csv"
git add .gitattributes
git add --renormalize data/raw/ercot/NP6-346-CD
git lfs ls-files | rg "NP6-346-CD" | head
```

Verify before push:

```bash
git lfs ls-files | rg "NP6-346-CD" | head -n 20
rg "NP6-346-CD" .gitattributes
```

Important:
- LFS usage is based on uploaded object size per version.
- Re-migrating/re-uploading incomplete datasets can consume quota quickly.
- Migrate to LFS only after that dataset download is complete.
