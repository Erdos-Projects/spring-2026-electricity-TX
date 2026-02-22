# ERCOT Data Estimation and Download Planning

Use this document for dataset-selection planning:
- what to download
- how much storage to expect
- how long downloads may take
- what `--from-date` to choose for budget/time targets

Use `DATA_DOWNLOAD.md` for download commands and troubleshooting.

Current snapshot:
- As-of horizon for latest tables: `2025-12-31`
- Last refreshed: `2026-02-22`
- Primary inputs: local consolidated monthly CSV files, `logs/downloads/*/run.log`, dataset earliest-date references
- Shared-run policy still applies: use this file for planning, but keep shared download windows capped at `2025-12-31`.
- Manual download status (`2026-02-22`): completed `NP4-188-CD`, `NP4-523-CD`, `NP6-346-CD`; in progress `NP4-745-CD`.

## Table of Contents

- [Decision Flow](#decision-flow)
- [Inputs and Scope](#1-inputs-and-scope)
- [Coverage Snapshot (Historical Audit)](#2-coverage-snapshot-historical-audit)
- [Time Estimate (Log-and-Fallback Method)](#3-time-estimate-log-and-fallback-method)
- [Size Estimate (Monthly-to-Daily Method)](#4-size-estimate-monthly-to-daily-method)
- [Planning Profiles (`<=10GB`, `<=50h`)](#5-planning-profiles-10gb-50h)
- [Dataset Summary (Capped at End of 2025)](#6-dataset-summary-capped-at-end-of-2025)
- [How to Refresh Estimates](#7-how-to-refresh-estimates)

## Decision Flow

Use this quick flow before final download choices:
1. Pick one primary question and success metric.
2. Fix analysis granularity and date window (`2025-12-31` cap for shared runs).
3. Start with the lean profile in Section 5; add heavier datasets only if needed.
4. Validate expected size/time using Sections 5 and 6 before running downloads.

## 1. Inputs and Scope

Earliest-date reference source:
- ERCOT Market Participants data-product details (`First Run Date`), checked `2026-02-22`.
- URL pattern: `https://www.ercot.com/mp/data-products/data-product-details?id=<DATASET_ID>`.

Dataset metadata used by planning estimates:

| Tier | Dataset ID | Type | Observed frequency | Earliest available date | Primary usage |
|---|---|---|---|---|---|
| 1 | `NP6-346-CD` | Load (actual) | Hourly | 2017-06-29 | Demand baseline, peak analysis |
| 1 | `NP6-905-CD` | Price (settlement) | 15-minute | 2010-11-30 | Main RT market outcome prices |
| 1 | `NP4-732-CD` | Renewable (wind) | Hourly | 2010-11-28 | Wind variability and forecast impact |
| 1 | `NP4-745-CD` | Renewable (solar) | Hourly | 2022-06-30 | Solar ramp and net-load impact |
| 1 | `NP3-233-CD` | Reliability | Hourly | 2013-10-20 | Outage and scarcity context |
| 2 | `NP3-565-CD` | Load forecast | Hourly | 2017-06-28 | Forecast error and planning context |
| 2 | `NP4-523-CD` | Price (DA) | Hourly | 2013-12-11 | DA benchmark vs RT |
| 2 | `NP6-788-CD` | Price (LMP detail) | Interval/market detail | 2010-11-30 | Extra spatial price detail |
| 3 | `NP6-331-CD` | Ancillary prices (RT) | 15-minute | 2025-12-05 | RT reserve/scarcity pricing |
| 3 | `NP4-188-CD` | Ancillary prices (DA) | Hourly/market detail | 2010-11-29 | DA ancillary pricing |
| 3 | `NP3-911-ER` | Renewable detail | Hourly/report-specific | 2011-01-29 | Resource capability vs output |

`NP3-912-ER` note:
- Excluded from tables in this file because archive endpoint is unresolved (`404`/internal error in current checks).

## 2. Coverage Snapshot (Historical Audit)

Audit window: `2025-11-01` to `2026-02-20` (`112` days).

Audit method:
- Checked every dataset ID listed in Section 1.
- Parsed local CSV date/day fields from `data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv`.
- Counted distinct covered dates in the audit window.

Result at the time of that audit: the full window was **not** completely downloaded for all dataset types.
Current local status may differ after later runs.

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

## 3. Time Estimate (Log-and-Fallback Method)

Method (Dec `2025` snapshot):
- Primary: parse `DAY COMPLETE` events from `logs/downloads/*/run.log` and compute per-dataset day-interval seconds.
- Secondary: if primary has too few intervals (`<3`), estimate day time by local CSV mtime span divided by covered day intervals.
- Tertiary: if both primary/secondary fail for a dataset but at least one dataset has an estimate, use global mean seconds/day across estimable datasets.
- Projection:
`estimated_total_hours = mean_sec_per_day * historical_days / 3600`.

Notes:
- This removes most `N/A` time rows when logs are sparse.
- `NP3-912-ER` appears as unresolved when no confirmed earliest date is available.

Latest capped time total (to `2025-12-31`, from Section 6 merged table):
- `1847.27` hours (`76.97` days).
- Global fallback was not used in this capped view.

## 4. Size Estimate (Monthly-to-Daily Method)

Method (Dec `2025` snapshot):
- For each dataset, use local monthly consolidated files:
`data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv`.
- Compute `avg_monthly_size` as the mean monthly CSV file size.
- Convert to daily via month length:
`avg_daily_size = mean(monthly_file_size / days_in_month)`.
- Project total size from earliest available date to `2025-12-31` (inclusive):
`estimated_total_size = avg_daily_size * historical_days`.

Latest capped size total (to `2025-12-31`, from Section 6 merged table):
- `75.73 GB`.

## 5. Planning Profiles (`<=10GB`, `<=50h`)

Use these planning numbers for quick planning.

Note:
- Converting `15-minute` data to hourly helps analysis/storage, but does **not** speed up API download.

Fastest levers for download speed:
- Move `--from-date` forward (shorter history window).
- Exclude high-volume datasets (`NP6-788-CD`, `NP6-905-CD`).
- Lower `--request-interval-seconds` carefully (faster but more `429` risk).

Profile definitions:
- `lean_6_no_rt_price`: `NP6-346-CD`, `NP3-233-CD`, `NP4-732-CD`, `NP4-745-CD`, `NP4-523-CD`, `NP6-331-CD`
- `core_7_with_rt_price`: `lean_6_no_rt_price` + `NP6-905-CD`
- `core_7_with_forecast`: `lean_6_no_rt_price` + `NP3-565-CD`
- `analysis_7_rt_plus_forecast`: `NP6-346-CD`, `NP3-233-CD`, `NP4-732-CD`, `NP4-745-CD`, `NP4-523-CD`, `NP6-905-CD`, `NP3-565-CD`

If using one common earliest date for all selected datasets:
- Common earliest date for these profiles: `2025-12-05` (limited by `NP6-331-CD`).

| Profile | Size from `2025-12-05` to `2025-12-31` | Download time from `2025-12-05` to `2025-12-31` |
|---|---|---|
| `lean_6_no_rt_price` | `32.50 MB` | `3.8 h` |
| `core_7_with_rt_price` | `118.95 MB` | `6.3 h` |
| `core_7_with_forecast` | `150.63 MB` | `4.2 h` |
| `analysis_7_rt_plus_forecast` | `237.07 MB` | `5.3 h` |

To stay near `<=10GB`, use these projected `--from-date` values:

| Profile | Suggested `--from-date` | Projected size to `2025-12-31` | Projected download hours |
|---|---|---|---|
| `lean_6_no_rt_price` | `2010-11-28` (already below `10GB`) | `3.67 GB` | `314.6 h` |
| `core_7_with_rt_price` | `2019-03-10` | `10.00 GB` | `422.5 h` |
| `core_7_with_forecast` | `2020-10-17` | `10.00 GB` | `181.6 h` |
| `analysis_7_rt_plus_forecast` | `2022-10-23` | `10.00 GB` | `229.6 h` |

To stay near `<=50h` download time, use these projected `--from-date` values:

| Profile | Suggested `--from-date` | Projected size to `2025-12-31` | Projected download hours |
|---|---|---|---|
| `lean_6_no_rt_price` | `2024-05-30` | `699.29 MB` | `50.0 h` |
| `core_7_with_rt_price` | `2025-04-06` | `1.16 GB` | `49.8 h` |
| `core_7_with_forecast` | `2024-09-10` | `2.60 GB` | `49.9 h` |
| `analysis_7_rt_plus_forecast` | `2025-04-23` | `2.17 GB` | `49.8 h` |

## 6. Dataset Summary (Capped at End of 2025)

Cap used in this section:
- `2025-12-31` for total estimated download size and total estimated download time.

Method notes:
- `Estimated Avg Monthly File Size` and `Estimated Total Download Size` use currently available local monthly CSV files.
- `Estimated Total Download Time` and `Avg Monthly File Time` use the log-and-fallback time model with horizon capped at `2025-12-31`.
- Rows are sorted by `Priority` ascending, then by `Estimated Avg Monthly File Size` ascending.
- `NP3-912-ER` is excluded until earliest date/endpoint is confirmed.

Time source legend:
- `time_source=log-daily`: estimate comes from `DAY COMPLETE` events in `logs/downloads/*/run.log`.
- `time_source=fallback-mtime`: estimate comes from local CSV file modification-time span divided by covered-day intervals when log-daily samples are insufficient.
- `time_source=global-fallback`: estimate uses the mean sec/day across datasets with valid estimates (only used if both methods above are unavailable).

| Priority | Dataset ID | Data Type | Earliest Date | Estimated Avg Monthly File Size | Avg Monthly File Time | Estimated Total Download Size (to 2025-12-31) | Estimated Total Download Time (to 2025-12-31) | Notes |
|---|---|---|---|---|---|---|---|---|
| 1 | `NP6-346-CD` | Load (actual) | 2017-06-29 | 44.86 KB | 0.20 h | 4.47 MB | 20.85 h | `time_source=fallback-mtime` |
| 1 | `NP3-233-CD` | Reliability | 2013-10-20 | 8.53 MB | 1.25 h | 1.23 GB | 185.80 h | `time_source=fallback-mtime` |
| 1 | `NP4-732-CD` | Renewable (wind) | 2010-11-28 | 10.05 MB | 0.42 h | 1.78 GB | 77.76 h | `time_source=log-daily` |
| 1 | `NP4-745-CD` | Renewable (solar) | 2022-06-30 | 18.20 MB | 0.62 h | 766.27 MB | 26.07 h | `time_source=fallback-mtime` |
| 1 | `NP6-905-CD` | Price (settlement) | 2010-11-30 | 98.83 MB | 2.89 h | 17.56 GB | 526.71 h | `time_source=fallback-mtime` |
| 2 | `NP4-523-CD` | Price (DA) | 2013-12-11 | 19.65 KB | 0.02 h | 2.78 MB | 2.58 h | `time_source=log-daily` |
| 2 | `NP3-565-CD` | Load forecast | 2017-06-28 | 127.77 MB | 0.54 h | 12.77 GB | 55.51 h | `time_source=fallback-mtime` |
| 2 | `NP6-788-CD` | Price (LMP detail) | 2010-11-30 | 237.83 MB | 5.05 h | 41.61 GB | 943.45 h | `time_source=log-daily` |
| 3 | `NP3-911-ER` | Renewable detail | 2011-01-29 | 45.93 KB | 0.02 h | 7.89 MB | 3.18 h | `time_source=log-daily` |
| 3 | `NP4-188-CD` | Ancillary prices (DA) | 2010-11-29 | 90.81 KB | 0.02 h | 16.05 MB | 3.84 h | `time_source=log-daily` |
| 3 | `NP6-331-CD` | Ancillary prices (RT) | 2025-12-05 | 140.38 KB | 1.68 h | 122.30 KB | 1.51 h | `time_source=log-daily` |

Merged table totals:
- Estimated total size across listed datasets: `75.73 GB`.
- Estimated total time across listed datasets: `1847.27 h` (`76.97` days).

Priority totals (from displayed row values):
- Priority `1`: `21.32 GB`, `837.19 h` (`34.88` days)
- Priority `2`: `54.38 GB`, `1001.54 h` (`41.73` days)
- Priority `3`: `24.06 MB`, `8.53 h` (`0.36` days)

## 7. How to Refresh Estimates

Refresh time estimate (capped at end of 2025 for merged table):

```bash
make estimate-time AS_OF=2025-12-31
```

Refresh size estimate (capped at end of 2025 for merged table):

```bash
make estimate-size AS_OF=2025-12-31
```

Optional single-dataset refresh:

```bash
make estimate-time AS_OF=2025-12-31 EST_DATASET=NP6-346-CD
make estimate-size AS_OF=2025-12-31 EST_DATASET=NP6-346-CD
```

After refresh:
- update Section 6 table
- update Section 3/4 totals
- update snapshot metadata at top (`As-of horizon`, `Last refreshed`)
