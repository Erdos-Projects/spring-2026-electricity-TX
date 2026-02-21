# spring-2026-electricity-TX

Repository for downloading, organizing, and analyzing ERCOT electricity data for Texas.

## Documentation Map

- `DATA_DOWNLOAD_RUNBOOK.md`
  - Use as the single source of truth for data download.
  - Includes setup, credentials, canonical command template, dataset reference, DNS troubleshooting, and Git LFS guidance.
- `DATA_CLEANING_RUNBOOK.md`
  - Use for cleaning and preparing analysis-ready data.
  - Includes dedupe keys, interval handling (`NP6-905-CD`), validation checklist, and EDA merge template.
- `LOCAL_DOWNLOAD_NOTES.md`
  - Use only for personal overrides (`START_DATE`/`MONTHS` values).
  - Keep operational instructions in `DATA_DOWNLOAD_RUNBOOK.md`.

Use the runbooks above for step-by-step instructions.

## Core Scripts

- `scripts/download_ercot_public_reports.py`
  - Main downloader for ERCOT public reports API.
- `scripts/list_ercot_analysis_datasets.py`
  - Prints recommended dataset IDs by profile with reasons.
- `scripts/ercot_dataset_catalog.py`
  - Central dataset catalog and profile definitions.

## Typical Workflow

1. Choose datasets for your task:
   - Use `scripts/list_ercot_analysis_datasets.py` or the download runbook priority table.
2. Download raw data:
   - Follow `DATA_DOWNLOAD_RUNBOOK.md`.
3. Clean and merge for analysis:
   - Follow `DATA_CLEANING_RUNBOOK.md`.
4. Run notebooks/EDA on processed outputs.

## Data Layout

- Raw downloads:
  - `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/...`
- Processed outputs:
  - `data/processed/ercot/<DATASET_ID>/...`

When monthly consolidation is used, `.docids` sidecar files are written next to monthly CSVs for safe resume and deduplication.
