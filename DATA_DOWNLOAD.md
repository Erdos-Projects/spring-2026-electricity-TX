# ERCOT Data Download Runbook

Use this runbook as the single source of truth for ERCOT API downloads in this repo.

Goal:
- Run downloads reliably from terminal.
- Reuse one canonical command template.
- Keep dataset choices, frequencies, and earliest-date references in one place.
- Use config + checkpoints + structured logs so reruns are automatic.

## Table of Contents

1. [Prepare Environment](#1-prepare-environment)
2. [Run Canonical Download Command (Window-Based)](#2-run-canonical-download-command-window-based)
3. [Tune Logging and Progress](#3-tune-logging-and-progress)
4. [Select Datasets (Priority + Usage)](#4-select-datasets-priority--usage)
5. [Makefile Shortcuts](#5-makefile-shortcuts)
6. [Resume Failed Runs and Handle Errors](#6-resume-failed-runs-and-handle-errors)
7. [Diagnose DNS with Verbose Logging](#7-diagnose-dns-with-verbose-logging)
8. [Apply Git LFS After Dataset Completion](#8-apply-git-lfs-after-dataset-completion)
9. [Coverage Audit + Day-Based Time Estimate](#9-coverage-audit--day-based-time-estimate-as-of-2026-02-21)

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
python3 -m pip install requests pyyaml
python3 -c "import requests, yaml; print('requests ok:', requests.__version__); print('pyyaml ok:', yaml.__version__)"
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
# after creating config/download.yaml in Section 2:
make download DOWNLOAD_ARGS="--list-api-products"
```

## 2. Run Canonical Download Command (Window-Based)

Recommended flow (make-first):
1. Copy sample config.
2. Edit `config/download.yaml`.
3. Run `make download`.

Copy sample config:

```bash
mkdir -p config
cp config/download.sample.yaml config/download.yaml
```

Edit `config/download.yaml`, then run:

```bash
make download
```

Override config file path:

```bash
make download CONFIG=config/download.sample.yaml
```

One-off overrides without editing config:

```bash
make download DOWNLOAD_ARGS="--from-date 2025-11-01 --window-months 3"
```

Run status tools after download:

```bash
make last-run
make resume-status
```

Expect this behavior:
- `--window-months`: computes end date inside the script (inclusive window) and caps at yesterday if `--to-date` is omitted.
- `--download-order newest-first`: download backward in time.
- `--sort-monthly-output descending`: keep monthly CSV rows newest-to-oldest by timestamp.
- `--sort-existing-monthly`: sorts only existing monthly CSV files within the active dataset date window.
- `.docids` sidecars prevent duplicate appends on rerun.
- `--state-dir` + `--resume-state` (default on): writes per-dataset checkpoints in `state/<DATASET>.json`.
- `--logs-dir`: writes one folder per run with `run.log`, `failures.csv`, and `summary.json`.

Sample override command (from `2025-09-01`, `2` months, daily prints, archive progress every `10` pages):

```bash
make download DOWNLOAD_ARGS="--from-date 2025-09-01 --window-months 2 --archive-progress-pages 10 --file-timing-frequency daily"
```

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

## 5. Makefile Shortcuts

Use these as your default download operations:

```bash
make help
make download
make last-run
make resume-status
make estimate-time
```

Notes:
- `make download` uses `config/download.yaml` by default.
- Override config path:
`make download CONFIG=config/download.sample.yaml`
- Add extra one-off flags:
`make download DOWNLOAD_ARGS='--dataset NP3-233-CD --from-date 2025-11-01 --window-months 2'`
- View latest run logs and summary:
`make last-run`
- View checkpoint progress:
`make resume-status`
- Compute per-dataset day-based total-time estimate:
`make estimate-time`

## 6. Resume Failed Runs and Handle Errors

When a run fails:
- Rerun the same command after interruptions.
- Deduplication uses `.docids`:
`data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv.docids`
- Checkpoint resume uses:
`state/<DATASET>.json` and `state/<DATASET>.archive_docs.jsonl`
- Structured run artifacts are in:
`logs/downloads/<YYYYMMDD_HHMMSS>/run.log`
`logs/downloads/<YYYYMMDD_HHMMSS>/failures.csv`
`logs/downloads/<YYYYMMDD_HHMMSS>/summary.json`

Do not delete `.docids` unless you intentionally want to rebuild monthly files.
Do not delete `state/*.json` or `state/*.archive_docs.jsonl` unless you intentionally want to restart listing/process progress.

Quick checks:

```bash
make last-run
make resume-status
```

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
- Check available IDs with:
`make download DOWNLOAD_ARGS="--list-api-products"`

## 7. Diagnose DNS with Verbose Logging

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

## 8. Apply Git LFS After Dataset Completion

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

## 9. Coverage Audit + Day-Based Time Estimate (as of 2026-02-21)

### 9.1 Window Check: 2025-11-01 to 2026-02-20

Audit method:
- Checked every dataset ID listed in Section 4.
- Parsed local CSV date/day fields from `data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv`.
- Counted distinct covered dates in `[2025-11-01, 2026-02-20]` (`112` days total).

Result: the full window is **not** completely downloaded for all dataset types.

| Dataset ID | Full window downloaded? | Coverage detail |
|---|---|---|
| `NP6-346-CD` | No | `109/112` days. Missing: `2025-12-04`, `2026-01-16`, `2026-02-07`. |
| `NP6-905-CD` | Yes | `112/112` days. |
| `NP4-732-CD` | Yes | `112/112` days. |
| `NP4-745-CD` | Yes | `112/112` days. |
| `NP3-233-CD` | Yes | `112/112` days. |
| `NP3-565-CD` | Yes | `112/112` days. |
| `NP4-523-CD` | No | `92/112` days. Missing `2025-11-01` and `2026-02-02` through `2026-02-20`. |
| `NP6-788-CD` | No | Dataset directory missing in local data. |
| `NP6-331-CD` | No | Dataset directory missing in local data. |
| `NP4-188-CD` | No | Dataset directory missing in local data. |
| `NP3-911-ER` | No | Dataset directory missing in local data. |
| `NP3-912-ER` | No | Still unresolved in API notes (`404` in Section 4), no local data. |

### 9.2 Day-Based Estimation Method (Per Dataset)

Use per-dataset day timing, not one shared rate across all datasets.

Formula:
- `mean_sec_per_day(dataset) = average(day_completion_delta_seconds)`
- `estimated_total_seconds(dataset) = mean_sec_per_day(dataset) * total_dataset_days`
- `estimated_total_hours = estimated_total_seconds / 3600`

How day deltas are measured:
- Read `DAY COMPLETE dataset=... completed_at=...` lines from `logs/downloads/*/run.log`.
- For each dataset, compute differences between consecutive `completed_at` timestamps.
- Aggregate these per-day intervals per dataset.

### 9.3 Compute the Estimate (Recommended)

Run:

```bash
make estimate-time
```

Optional fixed horizon date:

```bash
make estimate-time AS_OF=2026-02-21
```

The estimator uses:
- Earliest dates listed in Section 4.
- Per-dataset daily intervals from run logs.

Important:
- You need at least a few `DAY COMPLETE` events per dataset for stable estimates.
- Keep `--file-timing-frequency daily` enabled (already in sample config).
- If a dataset has insufficient day samples, estimator prints `N/A` for that dataset.
