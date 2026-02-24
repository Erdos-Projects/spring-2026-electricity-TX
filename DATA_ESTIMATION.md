# ERCOT Data Estimation and Download Planning

Use this document for current planning based on actual local data and recent run logs.

Use `DATA_DOWNLOAD.md` for download commands and troubleshooting.

Current snapshot:
- As-of horizon: `2025-12-31`
- Last refreshed: `2026-02-23`
- Primary inputs: `data/raw/ercot/*`, `logs/downloads/*/run.log`, `logs/downloads/*/summary.json`
- Shared-run command baseline remains `2017-07-01` to `2024-12-31` (see `DATA_DOWNLOAD.md`)
- `NP3-912-ER` is excluded (unresolved archive endpoint in current checks)

## Table of Contents

- [Inputs and Scope](#1-inputs-and-scope)
- [Coverage Snapshot (2025)](#2-coverage-snapshot-2025)
- [Download Time Snapshot (Chunk-Based)](#3-download-time-snapshot-chunk-based)
- [Dataset Summary (Actual Size Snapshot)](#4-dataset-summary-actual-size-snapshot)
- [How to Refresh](#5-how-to-refresh)

## 1. Inputs and Scope

Planning window in this file:
- start: `2017-07-01`
- end: `2025-12-31`

Observed dataset starts in the current local/API-log snapshot:

| Dataset ID | Type | Observed earliest date in current access/snapshot | Start used in planning window |
|---|---|---|---|
| `NP6-346-CD` | Load (actual) | `2017-06-30` | `2017-07-01` |
| `NP3-565-CD` | Load forecast | `2017-07-01` | `2017-07-01` |
| `NP4-732-CD` | Wind | `2017-06-29` | `2017-07-01` |
| `NP4-745-CD` | Solar | `2022-06-28` | `2022-06-28` |
| `NP6-905-CD` | Settlement prices | `2017-06-30` | `2017-07-01` |
| `NP6-788-CD` | LMP detail | `2024-02-21` | `2024-02-21` |
| `NP4-523-CD` | DAM system lambda | `2017-07-02` | `2017-07-02` |
| `NP6-331-CD` | RT ancillary prices | `2025-12-04` | `2025-12-04` |
| `NP4-188-CD` | DAM ancillary prices | `2017-07-02` | `2017-07-02` |
| `NP3-233-CD` | Outage capacity | `2017-07-01` | `2017-07-01` |
| `NP3-911-ER` | Renewable detail | `2017-06-29` | `2017-07-01` |

## 2. Coverage Snapshot (2025)

Audit window:
- `2025-01-01` to `2025-12-31` (`365` days)

Audit method:
- Parsed local `2025` monthly CSV files under `data/raw/ercot/<DATASET>/2025/<MM>/...`
- Counted distinct covered dates in each dataset's expected 2025 window

Result:
- Full expected-window coverage in `7/11` datasets
- Incomplete datasets: `NP6-346-CD`, `NP4-523-CD`, `NP4-188-CD`, `NP3-911-ER`

| Dataset ID | Full expected 2025 window downloaded? | Coverage detail |
|---|---|---|
| `NP6-346-CD` | No | `363/365` days. Missing: `2025-12-04`, `2025-12-31`. |
| `NP6-905-CD` | Yes | `365/365` days in expected window. |
| `NP4-732-CD` | Yes | `365/365` days in expected window. |
| `NP4-745-CD` | Yes | `365/365` days in expected window (`2025-01-01` to `2025-12-31`). |
| `NP3-233-CD` | Yes | `365/365` days in expected window. |
| `NP3-565-CD` | Yes | `365/365` days in expected window. |
| `NP4-523-CD` | No | `335/365` days. Missing `30` days (`2025-01-01` and `2025-12-02` to `2025-12-31`). |
| `NP6-788-CD` | Yes | `314/314` days in expected window (`2025-02-21` to `2025-12-31`). |
| `NP6-331-CD` | Yes | `28/28` days in expected window (`2025-12-04` to `2025-12-31`). |
| `NP4-188-CD` | No | `337/365` days. Missing `28` days (includes `2025-01-01` and multiple dates in December). |
| `NP3-911-ER` | No | `363/365` days. Missing: `2025-12-30`, `2025-12-31`. |

## 3. Download Time Snapshot (Chunk-Based)

Current time estimate uses chunk-download timing from structured logs:
- Parse `BULK_DONE ... status=ok ... elapsed_seconds=...`
- Convert to monthly average by dataset
- Project into `2017-07` to `2025-12` window (capped by dataset start)
- For `NP3-911-ER`, use `DAY_COMPLETE` timestamp span fallback (no useful bulk-write timing in sampled runs)

Runs used:
- `20260222_184938`
- `20260222_185802`
- `20260222_192334`
- `20260223_023227`

| Dataset ID | Sampled Months | Monthly Avg Seconds | Target Months (`2017-07` to `2025-12`, capped) | Estimated Hours |
|---|---:|---:|---:|---:|
| `NP3-233-CD` | 108 | 14.87 | 102 | 0.42 |
| `NP3-565-CD` | 102 | 27.02 | 102 | 0.77 |
| `NP3-911-ER` | 102 | 91.11 | 102 | 2.58 |
| `NP4-188-CD` | 102 | 0.82 | 102 | 0.02 |
| `NP4-523-CD` | 102 | 0.79 | 102 | 0.02 |
| `NP4-732-CD` | 90 | 22.47 | 102 | 0.64 |
| `NP4-745-CD` | 43 | 19.42 | 43 | 0.23 |
| `NP6-331-CD` | 1 | 65.80 | 1 | 0.02 |
| `NP6-346-CD` | 102 | 0.80 | 102 | 0.02 |
| `NP6-788-CD` | 23 | 237.70 | 23 | 1.52 |
| `NP6-905-CD` | 102 | 59.11 | 102 | 1.67 |

Total chunk-based estimate (`NP3-912-ER` excluded):
- `7.92` hours

Completed-run sanity check:
- `20260222_192334` elapsed: `5.69` h (`2017-07` to `2024-12`)
- `20260222_185802` elapsed: `2.13` h (`2025`)
- Combined elapsed: `7.82` h

## 4. Dataset Summary (Actual Size Snapshot)

Range:
- `2017-07-01` to `2025-12-31`

Method notes:
- Actual-only snapshot (no modeled time columns)
- `Actual Start Date in Window` is `max(2017-07-01, observed_start)`
- `Dates Available` is inclusive from actual start date to `2025-12-31`

| Priority | Dataset ID | Data Type | Observed Start | Actual Start Date in Window | Dates Available (to 2025-12-31) | Monthly Files (local / expected) | Actual Total Downloaded Size (2017-07-01 to 2025-12-31) | Actual Avg Monthly Downloaded Size | Actual Avg Yearly Downloaded Size | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `NP6-346-CD` | Load (actual) | 2017-06-30 | 2017-07-01 | 3,106 | 102/102 | 4.52 MB | 45.43 KB | 514.82 KB | - |
| 1 | `NP3-233-CD` | Outage capacity | 2017-07-01 | 2017-07-01 | 3,106 | 102/102 | 656.63 MB | 6.44 MB | 72.96 MB | - |
| 1 | `NP4-732-CD` | Renewable (wind) | 2017-06-29 | 2017-07-01 | 3,106 | 102/102 | 1.61 GB | 16.19 MB | 183.45 MB | - |
| 1 | `NP4-745-CD` | Renewable (solar) | 2022-06-28 | 2022-06-28 | 1,283 | 43/43 | 774.61 MB | 18.01 MB | 193.65 MB | starts after `2017-07-01` |
| 1 | `NP6-905-CD` | Price (settlement) | 2017-06-30 | 2017-07-01 | 3,106 | 102/102 | 8.68 GB | 87.13 MB | 987.42 MB | - |
| 2 | `NP4-523-CD` | Price (DA) | 2017-07-02 | 2017-07-02 | 3,105 | 102/102 | 1.96 MB | 19.71 KB | 223.38 KB | - |
| 2 | `NP3-565-CD` | Load forecast | 2017-07-01 | 2017-07-01 | 3,106 | 102/102 | 10.71 GB | 107.51 MB | 1.19 GB | - |
| 2 | `NP6-788-CD` | Price (LMP detail) | 2024-02-21 | 2024-02-21 | 680 | 23/23 | 8.52 GB | 379.16 MB | 4.26 GB | current access starts at `2024-02` |
| 3 | `NP4-188-CD` | Ancillary prices (DA) | 2017-07-02 | 2017-07-02 | 3,105 | 102/102 | 9.19 MB | 92.26 KB | 1.02 MB | - |
| 3 | `NP3-911-ER` | Renewable detail | 2017-06-29 | 2017-07-01 | 3,106 | 102/102 | 31.79 MB | 319.15 KB | 3.53 MB | - |
| 3 | `NP6-331-CD` | Ancillary prices (RT) | 2025-12-04 | 2025-12-04 | 28 | 1/1 | 360.21 KB | 360.21 KB | 360.21 KB | starts after `2017-07-01` |

Totals:
- Actual total downloaded size (`2017-07-01` to `2025-12-31`): `30.96 GB`
- Priority `1`: `11.69 GB`
- Priority `2`: `19.23 GB`
- Priority `3`: `41.33 MB`

## 5. How to Refresh

Refresh time model inputs:

```bash
make estimate-time AS_OF=2025-12-31
```

Refresh size snapshot inputs:

```bash
make estimate-size AS_OF=2025-12-31
```

Single-dataset refresh:

```bash
make estimate-time AS_OF=2025-12-31 EST_DATASET=NP6-346-CD
make estimate-size AS_OF=2025-12-31 EST_DATASET=NP6-346-CD
```
