# ERCOT Data Download Runbook

This guide is for anyone who is new to API-based data downloads.

Goal:
- Download ERCOT data reliably from terminal.
- Understand which Python script to run.
- Understand which datasets matter most for Texas electricity analysis.

## Current Download Status

As of **2026-02-21**:
- ✅ `NP6-346-CD` is complete.
- 🔄 One person is already downloading `NP6-905-CD`.

For now, others should **skip**:
- `NP6-346-CD`
- `NP6-905-CD`

Focus remaining effort on:
- `NP4-732-CD`, `NP4-745-CD`, `NP3-233-CD`, `NP3-565-CD`, `NP4-523-CD`, `NP6-788-CD`, `NP6-331-CD`, `NP4-188-CD`, `NP3-911-ER`, `NP3-912-ER`

## Priority-First Plan (Read This First)

If multiple people are helping, download in this order:

1. **Tier 1 first (most important for analysis)**
- `NP6-346-CD` (actual load baseline)
- `NP6-905-CD` (settlement point prices)
- `NP4-732-CD` (wind actual/forecast)
- `NP4-745-CD` (solar actual/forecast)
- `NP3-233-CD` (outage capacity)

2. **Tier 2 second (high-value context)**
- `NP3-565-CD` (7-day load forecast)
- `NP4-523-CD` (day-ahead system lambda)
- `NP6-788-CD` (extra LMP detail)

3. **Tier 3 last (extended/specialized)**
- `NP6-331-CD`, `NP4-188-CD`, `NP3-911-ER`, `NP3-912-ER`

Assignment suggestion:
- Person A: **already running** `NP6-905-CD` (do not duplicate this dataset).
- Person B: Tier 1 renewables/outages (`NP4-732-CD`, `NP4-745-CD`, `NP3-233-CD`)
- Person C: Tier 2 prices/forecast (`NP6-788-CD`, `NP4-523-CD`, `NP3-565-CD`)
- Person D: Tier 3 ancillary/weather + renewable detail (`NP6-331-CD`, `NP4-188-CD`, `NP3-912-ER`, `NP3-911-ER`)

---

## Canonical Download Command (Use This One)

Use this as the single base command in this runbook.  
For different runs, change only:
- repeated `--dataset` lines
- `--to-date` if needed

```bash
python3 scripts/download_ercot_public_reports.py \
  --datasets-only \
  --dataset <ID_1> \
  --dataset <ID_2> \
  --dataset <ID_3> \
  --from-earliest-available \
  --auto-detect-earliest-per-dataset \
  --to-date 2026-02-20 \
  --outdir data/raw/ercot \
  --consolidate-monthly \
  --delete-source-after-consolidation \
  --download-order newest-first \
  --sort-monthly-output descending \
  --sort-existing-monthly \
  --archive-progress-pages 2 \
  --request-interval-seconds 2.0 \
  --max-retries 10 \
  --retry-sleep-seconds 4 \
  --archive-listing-retries 20 \
  --max-consecutive-network-failures 8 \
  --network-failure-cooldown-seconds 30 \
  --print-file-timing \
  --write-manifest
```

Key flags explained once:
- `--datasets-only`: download only the listed `--dataset` IDs.
- `--from-earliest-available` + `--auto-detect-earliest-per-dataset`: start each dataset from its first available date and avoid empty early loops.
- `--download-order newest-first`: fetch newest docs first (backward in time).
- `--sort-monthly-output descending`: keep each monthly CSV sorted newest-to-oldest by timestamp.
- `--consolidate-monthly`: merge docs into monthly CSV files.
- `.docids` sidecars prevent duplicate appends when rerunning.

---

## Dataset Type Cheat Sheet (What To Monitor -> Which `--dataset` To Use)

Use this section when deciding which data type to download first.

| Data Type | What to Monitor | Dataset IDs to use in script |
|---|---|---|
| Load (actual) | System demand level, peak timing, zone load trends | `NP6-346-CD` |
| Load (forecast) | Forecast error, demand planning, weather-driven demand | `NP3-565-CD`, `NP3-912-ER` |
| Renewable generation | Wind/solar variability and forecast quality | `NP4-732-CD`, `NP4-745-CD`, `NP3-911-ER` |
| Price (real-time/settlement) | Main market outcome prices by node/zone/hub | `NP6-905-CD`, `NP6-788-CD` |
| Price (day-ahead) | Day-ahead benchmark vs real-time spreads | `NP4-523-CD` |
| Ancillary service prices | Reserve/scarcity capacity pricing (RT and DA) | `NP6-331-CD`, `NP4-188-CD` |
| Reliability | Outage-driven stress and scarcity context | `NP3-233-CD` |

### Observed Frequency in Existing Downloads (Local Check)

Checked from current local CSVs (latest 3 monthly files per dataset) on **2026-02-21**.

| Dataset ID | Name (short) | Time columns used | Observed frequency |
|---|---|---|---|
| `NP6-346-CD` | Actual System Load by Forecast Zone | `OperDay` + `HourEnding` | Hourly |
| `NP3-565-CD` | Seven-Day Load Forecast | `DeliveryDate` + `HourEnding` | Hourly |
| `NP4-732-CD` | Wind Power Production | `DELIVERY_DATE` + `HOUR_ENDING` | Hourly |
| `NP4-745-CD` | Solar Power Production | `DELIVERY_DATE` + `HOUR_ENDING` | Hourly |
| `NP6-905-CD` | Settlement Point Prices | `DeliveryDate` + `DeliveryHour` + `DeliveryInterval` | 15-minute (`DeliveryInterval` = 1..4) |

Notes:
- `NP3-565-CD` contains multiple rows per hour because of model/forecast dimensions, but the time granularity is still hourly.
- `NP6-905-CD` is interval-based and much larger; expect heavier storage/compute during analysis.

Quick usage examples (copy and replace IDs as needed):

- Price-focused:
```bash
--datasets-only --dataset NP6-905-CD --dataset NP6-788-CD --dataset NP4-523-CD
```

- Load-focused:
```bash
--datasets-only --dataset NP6-346-CD --dataset NP3-565-CD
```

- Renewable-focused:
```bash
--datasets-only --dataset NP4-732-CD --dataset NP4-745-CD --dataset NP3-911-ER
```

- Reliability-focused:
```bash
--datasets-only --dataset NP3-233-CD
```

---

## 1. What You Need Before Running

1. Open terminal at the project root:

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
```

2. Install required Python packages (first time only).

The downloader script depends on `requests` (plus Python standard library modules).

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install requests
python3 -c "import requests; print('requests ok:', requests.__version__)"
```

3. Create ERCOT API access and get your public API key (`subscription key`).

Registration steps:
1. Go to the ERCOT API portal and sign in or register.
2. If login does not open correctly, allow pop-ups/redirects for:
   - `ercotb2c.b2clogin.com`
3. In the portal, find the `public-reports` API product and subscribe to it.
4. Copy your subscription key (this is your API public key for requests).
5. Keep your portal username/password as well; the script uses all 3 credentials.

4. Export ERCOT API credentials in terminal:

```bash
export ERCOT_API_USERNAME="your_username"
export ERCOT_API_PASSWORD="your_password"
export ERCOT_SUBSCRIPTION_KEY="your_subscription_key"
```

5. Optional but recommended: wait until DNS is healthy before long runs.

```bash
while ! nslookup api.ercot.com >/dev/null 2>&1 || ! nslookup ercotb2c.b2clogin.com >/dev/null 2>&1; do
  echo "DNS not ready, retrying in 30s..."
  sleep 30
done
```

6. Verbose DNS health check with printed output + log file (recommended for troubleshooting):

```bash
mkdir -p logs
DNS_LOG="logs/dns_health_$(date +%Y%m%d_%H%M%S).log"
echo "DNS health log: $DNS_LOG"

while true; do
  TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "[$TS] Checking DNS..." | tee -a "$DNS_LOG"

  echo "[$TS] nslookup api.ercot.com" | tee -a "$DNS_LOG"
  nslookup api.ercot.com 2>&1 | tee -a "$DNS_LOG"

  echo "[$TS] nslookup ercotb2c.b2clogin.com" | tee -a "$DNS_LOG"
  nslookup ercotb2c.b2clogin.com 2>&1 | tee -a "$DNS_LOG"

  if nslookup api.ercot.com >/dev/null 2>&1 && nslookup ercotb2c.b2clogin.com >/dev/null 2>&1; then
    echo "[$TS] DNS is healthy. Starting download commands." | tee -a "$DNS_LOG"
    break
  fi

  echo "[$TS] DNS still unstable. Retrying in 30s..." | tee -a "$DNS_LOG"
  sleep 30
done
```

### What is DNS and why it matters

DNS stands for **Domain Name System**.  
It translates hostnames like:
- `api.ercot.com`
- `ercotb2c.b2clogin.com`

into IP addresses your computer can connect to.

Why this is important:
- Every API request starts with DNS lookup.
- If DNS fails, your script cannot even reach ERCOT endpoints.
- Typical errors are:
  - `Failed to resolve ...`
  - `NameResolutionError`
- This can look like a script freeze, but the issue is network name resolution, not Python logic.

### How to share DNS log

1. Find the latest DNS log:

```bash
ls -1t logs/dns_health_*.log | head -n 1
```

2. Share a quick summary in chat (last 40 lines):

```bash
tail -n 40 "$(ls -1t logs/dns_health_*.log | head -n 1)"
```

3. Share the full file when needed:
- Attach the `.log` file in your project chat tool, or
- Copy it into a GitHub issue comment:

```bash
cat "$(ls -1t logs/dns_health_*.log | head -n 1)"
```

4. Copy/paste status template for project chat:

```text
DNS check update:
- Time:
- Machine/User:
- Command run:
- Result: (healthy / unstable)
- Latest log file: logs/dns_health_YYYYMMDD_HHMMSS.log
- Last error line (if any):
- Next step:
```

---

## 2. Main Script to Use

Use this script for all production downloads:

- `scripts/download_ercot_public_reports.py`

Useful helper script:

- `scripts/list_ercot_analysis_datasets.py` (shows dataset IDs and reasons)

---

## 3. How Resume Works

If a run fails (DNS, 429, auth timeout), rerun the same command.

Why this is safe:
- Monthly CSVs are tracked by sidecar files:
  - `data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv.docids`
- Already-merged `docId`s are skipped automatically.

Do not delete `.docids` unless you intentionally want to rebuild monthly files.

---

## 4. How To Read the Download Code (Quick Map)

File:
- `scripts/download_ercot_public_reports.py`

Read in this order:

1. Argument definitions (`parse_args`)  
   Learn all command flags first.

2. `main()` dataset selection + auth  
   Understand `--datasets-only`, `--dataset`, `--exclude-dataset`, credential use.

3. Archive listing and retries  
   Look at functions handling `429`, page listing, and DNS failures.

4. Consolidation flow  
   Check where docs are appended into monthly CSVs and `.docids` are written.

5. Sort behavior  
   See download order (`api/newest-first/oldest-first`) and monthly sort options.

Key concepts:
- `postDatetime` is used for archive doc grouping.
- `DeliveryDate` inside rows can span future days (especially forecasts like `NP3-565-CD`).
- Row order in monthly files is controlled by sorting options, not only raw API order.

---

## 5. Dataset Priority and Usage (Texas Electricity Analysis)

### Tier 1 (Most Important)

1. `NP6-346-CD` - Actual System Load by Forecast Zone  
   Usage: demand baseline, load trends, peak analysis.

2. `NP6-905-CD` - Settlement Point Prices at Resource Nodes, Hubs and Load Zones  
   Usage: main price outcome variable for market analysis.

3. `NP4-732-CD` - Wind Power Production (Actual + Forecast)  
   Usage: renewable variability and forecast error impact.

4. `NP4-745-CD` - Solar Power Production (Actual + Forecast)  
   Usage: solar ramp effects and net-load behavior.

5. `NP3-233-CD` - Hourly Resource Outage Capacity  
   Usage: reliability stress and scarcity drivers.

### Tier 2 (Strongly Recommended)

6. `NP3-565-CD` - Seven-Day Load Forecast by Model and Weather Zone  
   Usage: forecast error and planning context.

7. `NP4-523-CD` - DAM System Lambda  
   Usage: day-ahead benchmark and DA-vs-RT spreads.

8. `NP6-788-CD` - LMPs by Resource Nodes, Load Zones and Trading Hubs  
   Usage: deeper spatial price detail.

### Tier 3 (Specialized / Extended)

9. `NP6-331-CD` - Real-Time Clearing Prices for Capacity  
   Usage: real-time ancillary service scarcity pricing.

10. `NP4-188-CD` - DAM Clearing Prices for Capacity  
    Usage: day-ahead ancillary service pricing.

11. `NP3-911-ER` - COP HSL and Actual Output for WGRs, PVGRs and ONLRTPF  
    Usage: capability-vs-output diagnostics for renewables.

12. `NP3-912-ER` - Temperature and Weather Zone Load Forecast  
    Usage: weather-load sensitivity modeling.

---

## 6. Common Issues and Quick Fix

1. `429 Too Many Requests`  
- Increase wait: `--request-interval-seconds 2.0` or higher.
- Keep `--archive-listing-retries` high (for big datasets).

2. `Failed to resolve ...` (DNS errors)  
- Wait for DNS recovery and rerun same command.
- Use DNS loop at top of this document before long runs.

3. `401 Unauthorized`  
- Recheck exported credentials.
- Sometimes appears after network instability; rerun after DNS is healthy.

4. Looks frozen after earliest-date detection  
- Use `--archive-progress-pages 1` or `2` to print progress frequently.

---

## 7. Workflow Recommendation

1. One person runs price datasets first (use the canonical command and pick IDs from the dataset type cheat sheet).
2. Another person runs core reliability/load datasets (use the canonical command and pick IDs from the priority section).
3. Keep logs in terminal output and commit script updates separately from data.
4. Do not manually edit raw monthly CSV files.

---

## 8. Git LFS (After Download Is Complete)

Run this section only after the dataset download is finished.

Install Git LFS (required if you will push large data files to GitHub):

```bash
brew install git-lfs
git lfs install
git lfs version
```

One-time migration for current `NP6-346-CD` CSV files to LFS pointers:

```bash
git lfs track "data/raw/ercot/NP6-346-CD/**/*.csv"
git add .gitattributes
git add --renormalize data/raw/ercot/NP6-346-CD
git lfs ls-files | rg "NP6-346-CD" | head
```

Verify migration before commit/push:

```bash
# 1) Confirm git-lfs is active
git lfs version

# 2) Confirm target files are tracked by LFS
git lfs ls-files | rg "NP6-346-CD" | head -n 20

# 3) Confirm .gitattributes contains LFS rule
cat .gitattributes | rg "NP6-346-CD"
```

If `rg` is not installed, use:

```bash
git lfs ls-files | grep "NP6-346-CD" | head -n 20
grep "NP6-346-CD" .gitattributes
```

Important notice about LFS usage:
- LFS usage is based on uploaded object size (file versions), not only line-level diffs.
- Re-migrating or re-uploading frequently-changing large CSV files can consume quota quickly.
- For this reason, do **not** migrate/push a dataset to LFS repeatedly while it is still incomplete.
- Recommended: finish the dataset download first, then run LFS migration/push once.
