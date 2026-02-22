# ERCOT Data Download Runbook

Use this runbook to run and troubleshoot ERCOT API downloads in this repository.

Purpose:
- Run downloads reliably from terminal.
- Reuse one canonical command template.
- Keep the download workflow in one place.
- Use config + checkpoints + structured logs so reruns are automatic.

Scope:
- This file is download-operations only.
- For storage/time planning and dataset-selection estimates, use `DATA_ESTIMATION.md`.
- Current upload policy: raw `2026` CSV data files are ignored by git for now (including `NP6-346-CD`).
- Safety reminder: for shared/team runs, do not download `2026` data (`--to-date` should stay `<= 2025-12-31`).
- If `2026` data already exists locally, keep it local-only and do not stage/push it.

## Table of Contents

1. [Prepare Environment](#1-prepare-environment)
2. [Run Canonical Download Command (Date-Range Based)](#2-run-canonical-download-command-date-range-based)
3. [Tune Logging and Progress](#3-tune-logging-and-progress)
4. [Select Datasets (Priority + Usage)](#4-select-datasets-priority--usage)
5. [Makefile Shortcuts](#5-makefile-shortcuts)
6. [Resume Failed Runs and Handle Errors](#6-resume-failed-runs-and-handle-errors)
7. [Diagnose DNS with Verbose Logging](#7-diagnose-dns-with-verbose-logging)
8. [Apply Git LFS After Dataset Completion](#8-apply-git-lfs-after-dataset-completion)

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
1. Go to `https://apiexplorer.ercot.com/apis` and sign in/register.
2. Allow pop-ups/redirects for `ercotb2c.b2clogin.com`.
3. Go to `https://apiexplorer.ercot.com/products` and subscribe to Public API (`public-reports`).
4. Copy your subscription key (this is your API key).

Credential variables used by the downloader:
- `ERCOT_API_USERNAME` = your ERCOT API portal username
- `ERCOT_API_PASSWORD` = your ERCOT API portal password
- `ERCOT_SUBSCRIPTION_KEY` = your `public-reports` subscription key

Set credentials in terminal (recommended, keeps secrets out of files):

```bash
read -r "ERCOT_API_USERNAME?Username: "
read -rs "ERCOT_API_PASSWORD?Password: "; echo
read -r "ERCOT_SUBSCRIPTION_KEY?Subscription key (API key): "
export ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY
```

Verify variables are set without printing secrets:

```bash
for v in ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY; do
  [ -n "${(P)v}" ] && echo "$v=SET" || echo "$v=NOT_SET"
done
```

Optional Python check in one line (avoids indentation copy/paste issues):

```bash
python3 -c 'import os; ks=("ERCOT_API_USERNAME","ERCOT_API_PASSWORD","ERCOT_SUBSCRIPTION_KEY"); [print("{} set={} len={} trim_ok={}".format(k, bool(os.getenv(k,"")), len(os.getenv(k,"")), os.getenv(k,"")==os.getenv(k,"").strip())) for k in ks]'
```

Security notes:
- Keep credentials blank/missing in `config/download.yaml` so env vars are used.
- Do not pass password/API key in `DOWNLOAD_FLAGS` (can leak to shell history/process list).
- `config/download.yaml` is ignored by git for local-only settings.
- After finishing downloads, clear credentials:

```bash
unset ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY
```

Optional: list available API product IDs for your account.

```bash
# after creating config/download.yaml in Section 2:
make download DOWNLOAD_FLAGS="--list-api-products"
```

## 2. Run Canonical Download Command (Date-Range Based)

For a fresh clone, create local config from the sample:

```bash
mkdir -p config
cp config/download.sample.yaml config/download.yaml
```

Notes:
- The sample file is `config/download.sample.yaml`.
- `config/download.yaml` is local-only and git-ignored, so each person should create their own copy.
- To avoid accidental overwrite, use `cp -i config/download.sample.yaml config/download.yaml`.

After copy, the default date range is:
- `from_date: 2016-01-01` (10-year default range)
- `to_date: 2025-12-31` (script default when `--to-date` is omitted)

If credentials are exported in terminal (`ERCOT_API_USERNAME`, `ERCOT_API_PASSWORD`, `ERCOT_SUBSCRIPTION_KEY`), you do not need to put credentials in `config/download.yaml`.

Shared-run rule:
- Do not include `2026` in shared date ranges.
- Set `--to-date 2025-12-31` when using explicit start/end ranges.

Readable `make download` arguments:
- `DOWNLOAD_CONFIG`: path to config file (default `config/download.yaml`)
- `DOWNLOAD_FLAGS`: downloader CLI flags passed after `--config`

Default run command (recommended). Edit dataset list and date range each time:

```bash
make download DOWNLOAD_FLAGS="--datasets-only \
--dataset NP6-346-CD \
--dataset NP3-565-CD \
--dataset NP4-732-CD \
--dataset NP4-745-CD \
--dataset NP6-905-CD \
--dataset NP4-523-CD \
--dataset NP3-233-CD \
--dataset NP6-788-CD \
--dataset NP6-331-CD \
--dataset NP4-188-CD \
--dataset NP3-911-ER \
--from-date 2016-01-01 \
--to-date 2025-12-31 \
--archive-progress-pages 10 \
--file-timing-frequency daily"
```

Dataset override template (explicit CLI dataset list):

```bash
make download DOWNLOAD_FLAGS="--datasets-only \
--dataset NP6-331-CD \
--dataset NP4-188-CD \
--dataset NP3-911-ER \
--from-date 2025-11-01 \
--to-date 2025-12-31 \
--file-timing-frequency daily"
```

`--datasets-only` selection behavior:
- If CLI `--dataset` flags are present, only those CLI dataset IDs are used.
- If no CLI `--dataset` flags are present, the downloader uses `download.datasets` from `config/download.yaml`.

Date parameters (clear rules):
1. `--from-date` is optional; if omitted, default is `2016-01-01` (10-year default range ending `2025-12-31`).
2. `--to-date` is optional; if omitted, default is `2025-12-31`.
3. Download range is inclusive (`from-date` through `to-date`).

Examples:

```bash
# Mode A: use default end date (2025-12-31)
make download DOWNLOAD_FLAGS="--datasets-only --dataset NP3-233-CD --from-date 2025-11-01"

# Mode B: fixed range (from + to)
make download DOWNLOAD_FLAGS="--datasets-only --dataset NP3-233-CD --from-date 2025-11-01 --to-date 2025-12-31"
```

Add these reliability/network flags only when needed:
- `--request-interval-seconds 2.0`
- `--max-retries 10 --retry-sleep-seconds 4`
- `--archive-listing-retries 20`
- `--max-consecutive-network-failures 8 --network-failure-cooldown-seconds 30`

Expected behavior:
- `--to-date`: if omitted, defaults to `2025-12-31`.
- `--download-order newest-first`: download backward in time.
- `--sort-monthly-output descending`: keep monthly CSV rows newest-to-oldest by timestamp.
- `--sort-existing-monthly`: sorts only existing monthly CSV files within the active dataset date range.
- `.docids` sidecars prevent duplicate appends on rerun.
- `--state-dir` + `--resume-state` (default on): writes per-dataset checkpoints in `state/<DATASET>.json`.
- `--logs-dir`: writes one folder per run with `run.log`, `failures.csv`, and `summary.json`.

Command formatting tips:
- Keep one opening `"` after `DOWNLOAD_FLAGS=` and one closing `"` at the end of the last line.
- If your shell shows `heredoc>` or `dquote>`, press `Ctrl+C` and re-run the command exactly.

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

Use `DATA_ESTIMATION.md` for dataset metadata and planning:
- master dataset catalog (tier/type/frequency/earliest date)
- storage/time estimate tables
- scenario recommendations for `--from-date` decisions (`<=10GB`, `<=50h`)

Quick helpers for choosing dataset IDs:

```bash
python3 scripts/list_ercot_analysis_datasets.py
```

For storage/time estimation and planning (what to download and how far back), use:
- `DATA_ESTIMATION.md`

## 5. Makefile Shortcuts

Use these as your default download operations:

```bash
make help
make download
make last-run
make resume-status
```

Notes:
- Run downloads with the canonical command in Section 2.
- Use `DATA_ESTIMATION.md` for size/time estimate workflows.

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
`make download DOWNLOAD_FLAGS="--list-api-products"`

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
