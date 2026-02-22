# spring-2026-electricity-TX

Repository for downloading, organizing, and analyzing ERCOT electricity data for Texas.

## Quick Doc Routing

- Run and troubleshoot downloads: `DATA_DOWNLOAD.md`
- Plan what to download, expected size, and expected time: `DATA_ESTIMATION.md`
- Clean and validate downloaded data: `DATA_CLEANING.md`

## Documentation Map

- `DATA_DOWNLOAD.md`
  - Use for download execution and troubleshooting.
  - Includes setup, credentials, canonical command template, dataset reference, DNS troubleshooting, and Git LFS steps.
- `DATA_ESTIMATION.md`
  - Use for dataset-selection planning before download runs.
  - Includes time/storage estimation methods, latest estimate tables, and scenario recommendations (`<=10GB`, `<=50h`).
- `DATA_CLEANING.md`
  - Use for cleaning and preparing analysis-ready data.
  - Includes dedupe keys, interval handling (`NP6-905-CD`), validation checklist, and EDA merge template.
- `LOCAL_DOWNLOAD_NOTES.md`
  - Use only for personal overrides (`START_DATE`/`WINDOW_MONTHS` values).
  - Keep download instructions in `DATA_DOWNLOAD.md`.
- `GIT_TERMINAL.md`
  - Beginner guide for Git in terminal.
  - Covers fetch/pull, local edits, stage, commit, push, merge workflows, and terminal setup for Codex/Gemini.
- `config/download.sample.yaml`
  - Starter config for downloader runs via `make download`.
  - Copy to `config/download.yaml` for local credentials and run settings.
- `Makefile`
  - Shortcut commands for downloader operations.
  - Run `make help` to see `download`, `last-run`, `resume-status`, `estimate-time`, and `estimate-size`.

Use these runbooks as the default workflow.

## Core Scripts

- `scripts/download_ercot_public_reports.py`
  - Main downloader for ERCOT public reports API.
- `scripts/list_ercot_analysis_datasets.py`
  - Prints recommended dataset IDs by profile with reasons.
- `scripts/ercot_dataset_catalog.py`
  - Central dataset catalog and profile definitions.

## Typical Workflow

1. Choose datasets for your task:
   - Use `scripts/list_ercot_analysis_datasets.py`, `DATA_DOWNLOAD.md` (dataset catalog), and `DATA_ESTIMATION.md` (size/time planning).
   - Optionally run `make estimate-time` and `make estimate-size` before deciding `--from-date`.
2. Download raw data:
   - Follow `DATA_DOWNLOAD.md` and use Makefile commands.
   - Start with `make help`, then use `make download`, `make last-run`, `make resume-status`.
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
- Processed outputs:
  - `data/processed/ercot/<DATASET_ID>/...`

When monthly consolidation is used, `.docids` sidecar files are written next to monthly CSVs for safe resume and deduplication.
