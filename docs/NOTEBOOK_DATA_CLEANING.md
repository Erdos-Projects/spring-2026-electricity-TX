# data_cleaning.ipynb — Component Brief

## Purpose

Converts raw monthly ERCOT CSV files into analysis-ready parquet files covering 10 datasets (load, pricing, forecasts, wind, ancillary services, weather). Handles datetime standardization (including DST transitions), deduplication by `postDateTime` (keep latest), data quality validation, and merges everything into a wide EDA table (`ercot_combined`). Individual parquets are used downstream by `eda.ipynb` for leakage-safe feature engineering.

---

## Section Outline

| Section | Cell ID | Description |
|---|---|---|
| §1 Setup | `s01-setup` | Imports, path constants, helper functions (parse_ts_cst, DST localization) |
| §2 Dataset Inventory | `s02-inv-hdr` | Overview table of 10 datasets loaded |
| §2.1 DST Timestamp Verification | `s02-dst-hdr` | DST reference helpers; validates HourEnding→ts_cst conversion |
| §2.2 One-Time Raw CSV Pre-filter | `kbsde9pf5x` | Pre-filter step (FILTER_RAW_CSVS flag): drops unused rows/cols from source CSVs |
| §3 Load and Clean Each Dataset | `s03-load-hdr` | Per-dataset load cells (NP6-346, NP6-345, NP3-565, NP6-905, NP4-732, NP4-190, NP4-523, NP4-188, NP3-233, Weather) |
| §4 Validation | `s04-validate-hdr` | Cross-dataset gap checks, interval regularity, missing-slot heatmap (13 datasets) |
| §4.1 Coverage Statistics | `ooyal1xck6` | Row counts, date ranges, null summaries for all parquets |
| §5 Build Processed Data | `s05-merge-hdr` | Left-join all datasets on ts_cst into `ercot_combined`; compute derived columns |
| §6 Save Outputs | `s06-save-hdr` | Write all individual parquets + ercot_combined.parquet to data/processed/ercot/ |

---

## Key Outputs

**Main EDA table:**
- `ercot_combined.parquet` — 74,569 rows × 64 cols, wide format, all datasets merged on ts_cst

**Individual parquets (data/processed/ercot/):**

| File | Rows | Description |
|---|---|---|
| `np6_346_houston.parquet` | 74,521 | Actual load — Houston pricing zone |
| `np6_346_total.parquet` | 74,521 | Actual load — system total |
| `np6_345_coast.parquet` | 74,521 | Actual load — Coast weather zone |
| `np6_345_total.parquet` | 74,521 | Actual load — all weather zones total |
| `np3_565_forecast1.parquet` | 74,592 | D+1 load forecast snapshot |
| `np4_732_wind_system.parquet` | 74,519 | Wind actual + STWPF — system-wide |
| `np4_732_wind_houston.parquet` | 74,519 | Wind actual + STWPF — Houston zone |
| `np4_732_forecast1.parquet` | 74,591 | D+1 wind forecast snapshot |
| `np4_190_dam_houston.parquet` | 74,498 | DAM prices — Houston Hub |
| `np4_190_dam_avg.parquet` | 74,498 | DAM prices — 4-hub average |
| `np6_905_rtm_hourly_houston.parquet` | 74,546 | RTM prices — hourly (from 15-min) — HB_HOUSTON |
| `np6_905_rtm_hourly_busavg.parquet` | 74,546 | RTM prices — hourly — HB_BUSAVG |
| `np4_523_system_lambda.parquet` | 74,545 | DAM system lambda (marginal cost) |
| `np4_188_mcpc_ecrs.parquet` | 74,545 | Ancillary: ECRS (starts 2023-06) |
| `np4_188_mcpc_rrs.parquet` | 74,545 | Ancillary: Responsive Reserve |
| `np4_188_mcpc_regup.parquet` | 74,545 | Ancillary: Regulation Up |
| `np4_188_mcpc_regdn.parquet` | 74,545 | Ancillary: Regulation Down |
| `np4_188_mcpc_nspin.parquet` | 74,545 | Ancillary: Non-Spinning Reserve |
| `np3_233_outage_total.parquet` | 74,711 | Outage capacity — system-wide |
| `np3_233_outage_houston.parquet` | 40,470 | Outage capacity — Houston (starts 2021-05) |
| `np3_233_forecast1.parquet` | 74,592 | D+1 outage forecast snapshot |
| `weather_hourly.parquet` | 75,288 | Hourly weather — wide format (21 cols) |
| `weather_daily.parquet` | 3,137 | Daily weather — wide format (481 cols) |
| `train_features.parquet` | 65,712 | Leakage-safe features — train 2017-07 → 2024-12 |
| `test_features.parquet` | 8,760 | Leakage-safe features — test 2025 |

---

## Known Issues / Caveats

1. **DST validation cell (Cell 10, id=`qd27c82x56c`)** — `KeyError: 'OperDay'` diagnostic failure; pre-existing, harmless. Run with `--allow-errors`.
2. **NP4-732-CD spring-forward 2018-03-11** — ERCOT published HE=3 instead of HE=2. Patched in `o6w7vk6pmea` (remaps HE=3→HE=2 for that date).
3. **NP4-190-CD 95h residual nulls** — 48h startup gap (2017-06-29 to 2017-07-01) + 47h partial-day re-settlement amendments across 7 dates. Confirmed unrecoverable.
4. **NP4-732-CD 74h residual nulls** — 9 DST spring-forward hours + 62h Sep 26-28 2024 (raw CSV 99.6% null) + 1h Dec 31 2025. Accepted: wind features have low Lasso importance.
5. **`system_wide_hsl` column** — 85% null; ERCOT only publishes from 2024-09-28. Not usable as a model feature.
6. **NP3-233-CD no DSTFlag** — uses fixed UTC-6 offset; 1h off during CDT months. Documented, accepted.
7. **NP6-346-CD, NP6-345-CD** — Dec 4 + Dec 31 2025 missing from both raw CSV and archive. Confirmed ERCOT non-publication.
8. **`ercot_combined` has no `post_datetime`** — stripped at merge (datasets post at different times, leakage risk). Use individual parquets for leakage-safe filtering.
9. **FILTER_RAW_CSVS = False** (cell `kbsde9pf5x`) — set to True to run once to pre-filter raw CSVs. Not needed for re-runs.

---

## Pending

- None — all cells execute cleanly as of 2026-03-20 (except DST diagnostic, which is intentionally skipped).
