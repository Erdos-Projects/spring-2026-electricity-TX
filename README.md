# spring-2026-electricity-TX

Repository for downloading, organizing, and analyzing ERCOT electricity data for Texas.

## Quick Doc Routing

- Run and troubleshoot downloads: `DATA_DOWNLOAD.md` (full) or `DATA_DOWNLOAD_BRIEF.md` (quick-start)
- Plan what to download, expected size, and expected time: `DATA_ESTIMATION.md`
- Clean and validate downloaded data: `DATA_CLEANING.md`

## Current Data Inventory (as of 2026-02-27)

13 datasets downloaded, ~89 GB total on disk.

| Dataset ID | Type | Monthly CSVs | Raw Size | Notes |
|---|---|---:|---|---|
| `NP6-346-CD` | Load (actual, forecast zone) | 104 | 7.3M | postDateTime 100% |
| `NP6-345-CD` | Load (actual, weather zone) | 142 | 13M | from 2014-05 |
| `NP3-565-CD` | Load forecast | 104 | 13G | postDateTime 100% |
| `NP4-732-CD` | Wind actual/forecast | 104 | 2.0G | |
| `NP4-745-CD` | Solar actual/forecast | 45 | 959M | from 2022-06 |
| `NP6-905-CD` | RT settlement prices | 104 | 14G | postDateTime 100% |
| `NP6-788-CD` | RT LMPs (per-SCED) | 25 | 14G | download in progress |
| `NP4-190-CD` | DA settlement prices | 142 | 5.0G | from 2014-05 |
| `NP4-523-CD` | DA system lambda | 104 | 4.7M | |
| `NP4-188-CD` | DA ancillary prices | 104 | 18M | |
| `NP6-331-CD` | RT ancillary prices | 3 | 2.0M | from 2025-12 |
| `NP3-233-CD` | Outage capacity | 104 | 989M | postDateTime 100% |
| `NP3-911-ER` | 2-Day DAM AS reports | 104 | 49M | |

Storage breakdown: raw 50G, archive 30G, compressed 8.4G, sample 725M.

## Documentation Map

- `DATA_DOWNLOAD.md` — Full download runbook: setup, credentials, canonical commands, structured logs, DNS troubleshooting, postDateTime backfill, Git LFS.
- `DATA_DOWNLOAD_BRIEF.md` — Quick-start: shortest end-to-end setup-to-run flow.
- `DATA_ESTIMATION.md` — Dataset-selection planning: coverage snapshot, time estimates from run logs, actual downloaded sizes.
- `DATA_CLEANING.md` — Cleaning and analysis prep: dedupe keys, interval handling, validation checklist, EDA merge template.
- `GIT_TERMINAL.md` — Beginner Git guide: fetch/pull, stage, commit, push, merge, conflict resolution.
- `config/download.sample.yaml` — Starter config. Copy to `config/download.yaml` for local use: `mkdir -p config && cp config/download.sample.yaml config/download.yaml`
- `Makefile` — Shortcut commands. Run `make help` to see available targets.

## Core Scripts

| Script | Purpose |
|---|---|
| `scripts/download_ercot_public_reports.py` | Main ERCOT API downloader with checkpoint resume, bulk download, structured event logging, and tqdm progress bars. |
| `scripts/backfill_post_datetime.py` | Backfill `postDateTime` into monthly CSVs. Supports add-missing/rebuild modes, verify, source cleanup/archive. |
| `scripts/sort_csv.py` | Re-sort existing monthly CSVs without API calls. |
| `scripts/audit_post_datetime_quality.py` | Audit postDateTime fill rate across monthly CSVs. |
| `scripts/ercot_dataset_catalog.py` | Central dataset catalog and profile definitions. |
| `scripts/list_ercot_analysis_datasets.py` | Print recommended dataset IDs by analysis profile. |
| `scripts/show_resume_status.py` | Display download checkpoint/resume status. |
| `scripts/estimate_download_time.py` | Estimate download time from structured logs. |
| `scripts/estimate_dataset_size.py` | Estimate dataset storage from local files. |
| `scripts/check_api_earliest_and_size_estimate.py` | Query ERCOT API for dataset earliest date and doc count. |

Notebooks (root directory):
- `data_cleaning.ipynb` — Data cleaning exploration
- `eda.ipynb` — Exploratory data analysis
- `dataexploration_neeraj.ipynb` — EDA (Neeraj)
- `playing_with_sample_data.ipynb` — Sample data exploration

Legacy script (root): `ercot_batch_downloader.py` — superseded by `scripts/download_ercot_public_reports.py`.

## Typical Workflow

1. Choose datasets for your task:
   - Use `scripts/list_ercot_analysis_datasets.py`, `DATA_DOWNLOAD.md` (dataset selection and run commands), and `DATA_ESTIMATION.md` (size/time planning).
   - Optionally run `make estimate-time` and `make estimate-size` to refresh local planning snapshots.
2. Download raw data:
   - Follow `DATA_DOWNLOAD.md` and use Makefile commands.
   - Start with `make help`, then use `make download`, `make last-run`, `make resume-status`.
   - Use `make sort_csv` only when you want to re-sort existing local monthly CSVs without re-downloading.
3. Clean and merge for analysis:
   - Follow `DATA_CLEANING.md`.
4. Run notebooks/EDA on processed outputs.

## Collaboration Workflow

For all Git-related operations, use `GIT_TERMINAL.md`:
- branch creation and naming
- fetch/pull/rebase choices
- staging, commit, push
- pull request and merge workflow
- conflict resolution

## Data Layout

- Raw downloads:
  - `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/...`
  - Monthly CSVs: `<DATASET_ID>_<YYYYMM>.csv`
  - Sidecar files: `.docids` (dedup), `.sortcache.json` (sort state)
- Archived source files:
  - `data/archive/ercot/<DATASET_ID>/...`
- Compressed snapshots:
  - `data/compressed/`
- Sample data:
  - `data/sample/`
- Processed outputs (not yet created):
  - `data/processed/ercot/<DATASET_ID>/...`
- Run artifacts:
  - `logs/downloads/<YYYYMMDD_HHMMSS>/run.log`
  - `logs/downloads/<YYYYMMDD_HHMMSS>/failures.csv`
  - `logs/downloads/<YYYYMMDD_HHMMSS>/summary.json`
- Resume checkpoints:
  - `state/<DATASET_ID>.json`
  - `state/<DATASET_ID>.archive_docs.jsonl`
