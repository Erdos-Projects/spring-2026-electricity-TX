# ERCOT Data Estimation and Download Planning

Use this document for dataset-selection planning:
- what to download
- how much storage to expect
- how long downloads may take
- what `--from-date` to choose for budget/time targets

Use `DATA_DOWNLOAD.md` for download commands and troubleshooting.

Current snapshot:
- As-of horizon for latest tables: `2026-02-22`
- Last refreshed: `2026-02-22`
- Primary inputs: local consolidated monthly CSV files, `logs/downloads/*/run.log`, dataset earliest-date references
- Shared-run policy still applies: use this file for planning, but keep shared download windows capped at `2025-12-31`.

## Table of Contents

- [Decision Flow](#decision-flow)
- [Inputs and Scope](#1-inputs-and-scope)
- [Coverage Snapshot (Historical Audit)](#2-coverage-snapshot-historical-audit)
- [Time Estimate (Log-and-Fallback Method)](#3-time-estimate-log-and-fallback-method)
- [Size Estimate (Monthly-to-Daily Method)](#4-size-estimate-monthly-to-daily-method)
- [Planning Scenarios (`<=10GB`, `<=50h`)](#5-planning-scenarios-10gb-50h)
- [Dataset Summary (Capped at End of Jan 2026)](#6-dataset-summary-capped-at-end-of-jan-2026)
- [How to Refresh Estimates](#7-how-to-refresh-estimates)

## Decision Flow

Use this section to align the team before final download decisions.

Discussion checklist:
1. Pick one primary question and one backup question.
2. Fix one analysis interval for first pass (recommended: hourly).
3. Fix start and end date for training.
4. Choose 5-7 datasets that directly support the primary question.
5. Assign parallel non-overlapping downloads by dataset.

Question template:
- Goal: what decision or insight do we want?
- Target variable: what are we predicting/explaining?
- Forecast horizon: same hour, 1-hour ahead, day-ahead, etc.
- Unit of analysis: system-wide, zone, hub, node.
- Success metric: MAE/RMSE (regression) or precision/recall/F1 (events).

Quick scoring rule (1-5 each, choose highest total):
- Business relevance
- Data readiness (coverage + frequency compatibility)
- Modeling feasibility in project timeline
- Interpretability for presentation

Candidate analysis directions:

1. Price spike drivers (explanation-first)
- Main question: Which conditions are associated with high real-time price periods?
- Minimum datasets: `NP6-905-CD`, `NP6-346-CD`, `NP4-732-CD`, `NP4-745-CD`, `NP3-233-CD`
- Optional: `NP4-523-CD`, `NP6-331-CD`

2. Net-load and renewable ramps
- Main question: How do wind/solar ramps change net load and stress periods?
- Minimum datasets: `NP6-346-CD`, `NP4-732-CD`, `NP4-745-CD`
- Optional: `NP6-905-CD`

3. Forecast-error impact
- Main question: How does load forecast error relate to market outcomes?
- Minimum datasets: `NP3-565-CD`, `NP6-346-CD`, `NP4-523-CD`
- Optional: `NP6-905-CD`

4. Reliability stress and outages
- Main question: How does outage capacity relate to scarcity and price pressure?
- Minimum datasets: `NP3-233-CD`, `NP6-346-CD`, `NP4-523-CD`
- Optional: `NP6-331-CD`, `NP6-905-CD`

Recommended first pass:
1. Start with one hourly system-wide question.
2. Use a smaller dataset set first to validate cleaning + merge + baseline model.
3. Add heavier price datasets only after pipeline is stable.

## 1. Inputs and Scope

Earliest-date reference source:
- ERCOT Market Participants data-product details (`First Run Date`), checked `2026-02-21`.
- URL pattern: `https://www.ercot.com/mp/data-products/data-product-details?id=<DATASET_ID>`.

Dataset metadata used by planning estimates:

| Tier | Dataset ID | Type | Observed frequency | Earliest available date | Primary usage |
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

`NP3-912-ER` note:
- Excluded from tables in this file because archive endpoint is unresolved (`404` in current checks).

## 2. Coverage Snapshot (Historical Audit)

Audit window: `2025-11-01` to `2026-02-20` (`112` days).

Audit method:
- Checked every dataset ID listed in Section 1.
- Parsed local CSV date/day fields from `data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv`.
- Counted distinct covered dates in the audit window.

Result at the time of that audit: the full window was **not** completely downloaded for all dataset types. Current local status may differ after later runs.

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

Method (as-of `2026-02-22`):
- Primary: parse `DAY COMPLETE` events from `logs/downloads/*/run.log` and compute per-dataset day-interval seconds.
- Secondary: if primary has too few intervals (`<3`), estimate day time by local CSV mtime span divided by covered day intervals.
- Tertiary: if both primary/secondary fail for a dataset but at least one dataset has an estimate, use global mean seconds/day across estimable datasets.
- Projection:
`estimated_total_hours = mean_sec_per_day * historical_days / 3600`.

Notes:
- This removes most `N/A` time rows when logs are sparse.
- `NP3-912-ER` appears as unresolved when no confirmed earliest date is available.

Latest time estimate snapshot (run on `2026-02-22`, as-of `2026-02-21`):

| Dataset ID | Earliest Date | Historical Days (to 2026-02-21) | Mean Sec/Day | Sample Day Intervals | Source | Estimated Total Hours | Estimated Total Days | Notes |
|---|---|---|---|---|---|---|---|---|
| `NP3-233-CD` | 2014-08-08 | 4,216 | 150.11 (sd 0.00) | 119 | fallback-mtime | 175.79 | 7.32 | covered_days=120, csv_files=4 |
| `NP3-565-CD` | 2014-08-08 | 4,216 | 64.28 (sd 0.00) | 339 | fallback-mtime | 75.28 | 3.14 | covered_days=340, csv_files=12 |
| `NP3-911-ER` | 2025-04-04 | 324 | 2.00 (sd 0.00) | 5 | log-daily | 0.18 | 0.01 | from DAY COMPLETE run logs |
| `NP3-912-ER` | unresolved | N/A | N/A | N/A | N/A | N/A | N/A | earliest date unresolved |
| `NP4-188-CD` | 2015-03-26 | 3,986 | 2.00 (sd 0.25) | 29 | log-daily | 2.21 | 0.09 | from DAY COMPLETE run logs |
| `NP4-523-CD` | 2015-03-26 | 3,986 | 2.00 (sd 0.31) | 18 | log-daily | 2.21 | 0.09 | from DAY COMPLETE run logs |
| `NP4-732-CD` | 2014-08-17 | 4,207 | 681.39 (sd 0.00) | 208 | fallback-mtime | 796.29 | 33.18 | covered_days=209, csv_files=9 |
| `NP4-745-CD` | 2016-03-12 | 3,634 | 42.02 (sd 0.00) | 667 | fallback-mtime | 42.42 | 1.77 | covered_days=668, csv_files=22 |
| `NP6-331-CD` | 2015-03-26 | 3,986 | 201.00 (sd 1.29) | 30 | log-daily | 222.55 | 9.27 | from DAY COMPLETE run logs |
| `NP6-346-CD` | 2014-06-19 | 4,266 | 52.47 (sd 0.00) | 3155 | fallback-mtime | 62.17 | 2.59 | covered_days=3156, csv_files=105 |
| `NP6-788-CD` | 2015-03-26 | 3,986 | 168.44 (sd 0.00) | 32 | fallback-mtime | 186.50 | 7.77 | covered_days=33, csv_files=2 |
| `NP6-905-CD` | 2014-05-01 | 4,315 | 344.06 (sd 0.00) | 181 | fallback-mtime | 412.40 | 17.18 | covered_days=182, csv_files=7 |

Total estimated hours (datasets with enough samples): `1978.01` (`82.42` days).
Global fallback was not used in this snapshot.

## 4. Size Estimate (Monthly-to-Daily Method)

Method (as-of `2026-02-22`):
- For each dataset, use local monthly consolidated files:
`data/raw/ercot/<DATASET>/<YYYY>/<MM>/<DATASET>_<YYYYMM>.csv`.
- Compute `avg_monthly_size` as the mean monthly CSV file size.
- Convert to daily via month length:
`avg_daily_size = mean(monthly_file_size / days_in_month)`.
- Project total size from earliest available date to `2026-02-22` (inclusive):
`estimated_total_size = avg_daily_size * historical_days`.

Latest size estimate snapshot (run on `2026-02-22`):

| Dataset ID | Earliest Date | Historical Days (to 2026-02-22) | Monthly Files Used | Avg Monthly Size | Avg Daily Size (monthly/day) | Estimated Total Size | Notes |
|---|---|---|---|---|---|---|---|
| `NP3-233-CD` | 2014-08-08 | 4,217 | 4 | 8.53 MB | 289.69 KB | 1.17 GB | month-derived files=4 |
| `NP3-565-CD` | 2014-08-08 | 4,217 | 12 | 127.77 MB | 4.21 MB | 17.32 GB | month-derived files=12 |
| `NP3-911-ER` | 2025-04-04 | 325 | 1 | 45.93 KB | 1.48 KB | 481.57 KB | month-derived files=1 |
| `NP4-188-CD` | 2015-03-26 | 3,987 | 1 | 108.70 KB | 3.51 KB | 13.65 MB | month-derived files=1 |
| `NP4-523-CD` | 2015-03-26 | 3,987 | 4 | 18.16 KB | 616.86 B | 2.35 MB | month-derived files=4 |
| `NP4-732-CD` | 2014-08-17 | 4,208 | 9 | 10.75 MB | 362.27 KB | 1.45 GB | month-derived files=9 |
| `NP4-745-CD` | 2016-03-12 | 3,635 | 22 | 18.58 MB | 624.98 KB | 2.17 GB | month-derived files=22 |
| `NP6-331-CD` | 2015-03-26 | 3,987 | 3 | 140.38 KB | 4.53 KB | 17.64 MB | month-derived files=3 |
| `NP6-346-CD` | 2014-06-19 | 4,267 | 105 | 44.16 KB | 1.45 KB | 6.05 MB | month-derived files=105 |
| `NP6-788-CD` | 2015-03-26 | 3,987 | 2 | 179.21 MB | 5.84 MB | 22.74 GB | month-derived files=2 |
| `NP6-905-CD` | 2014-05-01 | 4,316 | 7 | 98.83 MB | 3.26 MB | 13.75 GB | month-derived files=7 |

Total estimated size across estimable datasets: `58.63 GB`.

## 5. Planning Scenarios (`<=10GB`, `<=50h`)

Use these planning numbers for quick planning.

Note:
- Converting `15-minute` data to hourly helps analysis/storage, but does **not** speed up API download.

Fastest levers for download speed:
- Move `--from-date` forward (shorter history window).
- Exclude high-volume datasets (`NP6-788-CD`, `NP6-905-CD`).
- Lower `--request-interval-seconds` carefully (faster but more `429` risk).

Scenario definitions:
- `lean_6_no_rt_price`: `NP6-346-CD`, `NP3-233-CD`, `NP4-732-CD`, `NP4-745-CD`, `NP4-523-CD`, `NP6-331-CD`
- `core_7_with_rt_price`: `lean_6_no_rt_price` + `NP6-905-CD`
- `core_7_with_forecast`: `lean_6_no_rt_price` + `NP3-565-CD`
- `analysis_7_rt_plus_forecast`: `NP6-346-CD`, `NP3-233-CD`, `NP4-732-CD`, `NP4-745-CD`, `NP4-523-CD`, `NP6-905-CD`, `NP3-565-CD`

If using one common earliest date for all selected datasets:
- Common earliest date for these scenarios: `2016-03-12` (limited by `NP4-745-CD`).

| Scenario | Size from `2016-03-12` to `2026-02-21` | Download time from `2016-03-12` to `2026-02-21` |
|---|---|---|
| `lean_6_no_rt_price` | `4.45 GB` | `1139.9 h` |
| `core_7_with_rt_price` | `16.03 GB` | `1487.2 h` |
| `core_7_with_forecast` | `19.37 GB` | `1204.8 h` |
| `analysis_7_rt_plus_forecast` | `30.94 GB` | `1349.1 h` |

To stay near `<=10GB`, use these projected `--from-date` values:

| Scenario | Suggested `--from-date` | Projected size to `2026-02-21` | Projected download hours |
|---|---|---|---|
| `lean_6_no_rt_price` | `2016-03-12` (already below `10GB`) | `4.45 GB` | `1139.9 h` |
| `core_7_with_rt_price` | `2019-12-09` | `10.00 GB` | `927.8 h` |
| `core_7_with_forecast` | `2021-01-04` | `10.00 GB` | `621.6 h` |
| `analysis_7_rt_plus_forecast` | `2022-12-06` | `9.99 GB` | `435.8 h` |

To stay near `<=50h` download time, use these projected `--from-date` values:

| Scenario | Suggested `--from-date` | Projected size to `2026-02-21` | Projected download hours |
|---|---|---|---|
| `lean_6_no_rt_price` | `2025-09-16` | `199.30 MB` | `49.9 h` |
| `core_7_with_rt_price` | `2025-10-23` | `550.98 MB` | `49.9 h` |
| `core_7_with_forecast` | `2025-09-25` | `818.88 MB` | `49.7 h` |
| `analysis_7_rt_plus_forecast` | `2025-10-11` | `1.14 GB` | `49.7 h` |

## 6. Dataset Summary (Capped at End of Jan 2026)

Cap used in this section:
- `2026-01-31` for total estimated download size and total estimated download time.

Method notes:
- `Estimated Avg Monthly File Size` and `Estimated Total Download Size` use local monthly CSV files up to `2026-01`.
- `Estimated Total Download Time` uses the current log-and-fallback time model, with projection capped at `2026-01-31`.
- `NP3-912-ER` is excluded from this table until earliest date/endpoint is confirmed.
- `Estimated Avg Monthly File Download Time` is computed from estimated mean sec/day times average days/month of observed monthly files.

Time source legend:
- `time_source=log-daily`: estimate comes from `DAY COMPLETE` events in `logs/downloads/*/run.log`.
- `time_source=fallback-mtime`: estimate comes from local CSV file modification-time span divided by covered-day intervals when log-daily samples are insufficient.
- `time_source=global-fallback`: estimate uses the mean sec/day across datasets with valid estimates (only used if both methods above are unavailable).

| Dataset ID | Data Type | Priority | Frequency | Earliest Date | Estimated Avg Monthly File Size | Estimated Avg Monthly File Download Time | Estimated Total Download Size (to 2026-01-31) | Estimated Total Download Time (to 2026-01-31) | Notes |
|---|---|---|---|---|---|---|---|---|---|
| `NP3-233-CD` | Reliability | 1 | Hourly | 2014-08-08 | 9.34 MB | 1.25 h | 1.25 GB | 174.92 h | time_source=fallback-mtime |
| `NP3-565-CD` | Load forecast | 2 | Hourly | 2014-08-08 | 132.28 MB | 0.54 h | 17.76 GB | 74.90 h | time_source=fallback-mtime |
| `NP3-911-ER` | Renewable detail | 3 | Hourly/report-specific | 2025-04-04 | 45.93 KB | 0.02 h | 448.97 KB | 0.17 h | time_source=log-daily |
| `NP4-188-CD` | Ancillary prices (DA) | 3 | Hourly/market detail | 2015-03-26 | 108.70 KB | 0.02 h | 13.58 MB | 2.20 h | time_source=log-daily |
| `NP4-523-CD` | Price (DA) | 2 | Hourly | 2015-03-26 | 19.89 KB | 0.02 h | 2.51 MB | 2.20 h | time_source=log-daily |
| `NP4-732-CD` | Renewable (wind) | 1 | Hourly | 2014-08-17 | 10.66 MB | 5.72 h | 1.42 GB | 792.31 h | time_source=fallback-mtime |
| `NP4-745-CD` | Renewable (solar) | 1 | Hourly | 2016-03-12 | 18.88 MB | 0.36 h | 2.18 GB | 42.17 h | time_source=fallback-mtime |
| `NP6-331-CD` | Ancillary prices (RT) | 3 | 15-minute | 2015-03-26 | 209.98 KB | 1.68 h | 26.23 MB | 221.38 h | time_source=log-daily |
| `NP6-346-CD` | Load (actual) | 1 | Hourly | 2014-06-19 | 44.30 KB | 0.44 h | 6.03 MB | 61.87 h | time_source=fallback-mtime |
| `NP6-788-CD` | Price (LMP detail) | 2 | Interval/market detail | 2015-03-26 | 313.25 MB | 1.38 h | 39.13 GB | 185.52 h | time_source=fallback-mtime |
| `NP6-905-CD` | Price (settlement) | 1 | 15-minute | 2014-05-01 | 101.81 MB | 2.93 h | 13.94 GB | 410.39 h | time_source=fallback-mtime |

## 7. How to Refresh Estimates

Refresh time estimate table (logs + fallback):

```bash
make estimate-time AS_OF=2026-02-22
```

Refresh size estimate table:

```bash
make estimate-size AS_OF=2026-02-22
```

Optional single-dataset refresh:

```bash
make estimate-time EST_DATASET=NP6-346-CD
make estimate-size EST_DATASET=NP6-346-CD
```

After refresh:
- paste updated tables into Sections 3/4
- update the snapshot header at top (`As-of horizon`, `Last refreshed`)
