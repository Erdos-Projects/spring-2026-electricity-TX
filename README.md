# spring-2026-electricity-TX

Scripts and notebooks for collecting and analyzing ERCOT electricity data for Texas.

## ERCOT data scripts

### 1) Determine datasets needed for analysis

Use the profile selector to print recommended ERCOT datasets and reasons:

```bash
python3 scripts/list_ercot_analysis_datasets.py --profile core
```

Available profiles:
- `core`: load, load forecast, wind, solar, real-time prices, day-ahead price, outages.
- `market`: day-ahead/real-time pricing plus ancillary service prices.
- `reliability`: outages, resource capability/output, weather-driven load forecasts.
- `all`: all datasets in the local catalog.

### Dataset ID meaning

ERCOT API products are selected by `dataset ID` (also called report/product ID or EMIL ID), for example `NP6-905-CD`.

- Format pattern: `NP<group>-<number>-<suffix>`
- `NP6-905-CD` means a specific public report ("Settlement Point Prices at Resource Nodes, Hubs and Load Zones")
- Use `--dataset <ID>` to download exactly what you want
- Use `--list-api-products` to see IDs available to your account

### Data types and granularity

Common ERCOT data types used in this repo:

- `Load`: actual system load and load forecasts (hourly and forecast products)
- `Renewables`: wind/solar actual and forecast values (hourly and some 5-minute products)
- `Prices`: real-time and day-ahead market prices (interval-based products)
- `Ancillary services`: capacity clearing prices and reserve-related market products
- `Reliability`: outages and resource capability/availability related reports

Granularity depends on the dataset ID. Some products are hourly, some are 15-minute, and some are 5-minute.

### 2) Download ERCOT public report files

The downloader uses ERCOT's official `public-reports` API and archive endpoint.

Set credentials from your ERCOT API portal account:

```bash
export ERCOT_API_USERNAME="your-ercot-username"
export ERCOT_API_PASSWORD="your-ercot-password"
export ERCOT_SUBSCRIPTION_KEY="your-subscription-key"
```

Download the default `core` profile for a date range:

```bash
python3 scripts/download_ercot_public_reports.py \
  --from-date 2025-01-01 \
  --to-date 2025-12-31 \
  --outdir data/raw/ercot \
  --extract-zips \
  --write-manifest
```

Download only specific dataset IDs (custom selection):

```bash
python3 scripts/download_ercot_public_reports.py \
  --from-date 2024-01-01 \
  --to-date 2024-12-31 \
  --outdir data/raw/ercot_custom \
  --dataset NP6-905-CD \
  --dataset NP4-732-CD \
  --dataset NP4-745-CD \
  --write-manifest
```

Preview what will be downloaded before running:

```bash
python3 scripts/download_ercot_public_reports.py \
  --from-date 2024-01-01 \
  --to-date 2024-01-31 \
  --dataset NP6-905-CD \
  --dry-run
```

For long-range runs with fewer files (recommended for 10-year pulls), consolidate per month:

```bash
python3 scripts/download_ercot_public_reports.py \
  --profile core \
  --from-date 2016-01-01 \
  --to-date 2025-12-31 \
  --outdir data/raw/ercot \
  --consolidate-monthly \
  --request-interval-seconds 0.5 \
  --delete-source-after-consolidation \
  --write-manifest
```

Useful options:
- `--profile market` or `--profile reliability` (repeatable).
- `--dataset NP6-905-CD` to add a specific dataset ID (repeat `--dataset` for multiple IDs).
- `--list-api-products` to list all products available to your API account.
- `--dry-run` to preview downloads without writing files.
- `--max-docs-per-dataset 10` for quick testing.
- `--consolidate-monthly` to store one combined CSV per dataset/month.
- `--delete-source-after-consolidation` to remove per-doc files after append.
- `--request-interval-seconds 0.5` to reduce 429 rate-limit errors on long runs.

## Notes

- The new scripts are targeted downloaders. They avoid broad webpage scraping and focus on the report IDs you actually need.
- Output is organized by `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/`.
- With `--consolidate-monthly`, each month is appended into `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/<DATASET_ID>_<YYYYMM>.csv` and tracked with a `.docids` marker for rerun dedupe.

### What `.docids` files are for

When you use `--consolidate-monthly`, the downloader writes a sidecar file:

- Path pattern: `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/<DATASET_ID>_<YYYYMM>.csv.docids`
- Contents: one `docId` per line for documents already merged into that monthly CSV
- Purpose: prevents duplicate appends on reruns and allows safe resume after interruptions
- Operational note: do not delete `.docids` unless you intentionally want to rebuild that monthly CSV from scratch
