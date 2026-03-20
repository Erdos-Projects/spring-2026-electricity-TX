# spring-2026-electricity-TX

ERCOT electricity price volatility forecasting for the Texas Houston Hub.
**Goal:** predict intra-hour RTM price volatility (`log(rtm_price_std)`) and flag price spikes (>$100/MWh) using day-ahead features available at midnight CST.

**Status:** Modeling complete. Best model: XGBoost v3, R²=0.489, RMSE=0.610 (test 2025).
Remaining: presentation notebook.

---

## Project Directory

### Notebooks (run in this order)

| Notebook | Purpose | Sections |
|---|---|---|
| `data_cleaning.ipynb` | Raw ERCOT CSVs → processed parquets | §1 Setup · §2 Dataset Inventory · §3 Load & Clean · §4 Validation · §5 Build Processed Data · §6 Save Outputs |
| `eda.ipynb` | EDA, feature engineering, leakage-safe design | §1 Setup · §2 Target Variable · §3 Feature EDA · §4 Regime Analysis · §5 Modeling Roadmap · Appendix A–B |
| `modeling.ipynb` | Baseline through XGBoost v3, error analysis, drift detection | §6 Baseline · §7 Model Improvements · §8 Error Analysis · §9 Rolling Forecast & Drift · §10 Ensemble |

> `modeling.ipynb` sections start at §6 (continuation from `eda.ipynb` §5).

### Documentation (`docs/`)

| File | Purpose |
|---|---|
| `docs/DATA_DOWNLOAD.md` | Full download runbook: setup, credentials, commands, postDateTime backfill |
| `docs/DATA_ESTIMATION.md` | Dataset-selection planning: coverage, sizes, time estimates |
| `docs/MODEL_ANALYSIS.md` | Volatility analysis plan: model hierarchy, benchmark design, evaluation framework |
| `docs/houston_weather_USAGE.md` | Weather data fetcher (Open-Meteo API): modes, station registry, examples |
| `docs/GIT_TERMINAL.md` | Beginner Git guide: daily pipeline, conflict resolution, PR workflow |

### Key Supporting Files

| File | Purpose |
|---|---|
| `TO_DO_LIST.md` | Task tracker: completed, in-progress, pending |
| `config/download.sample.yaml` | Starter config — copy to `config/download.yaml` for local use |
| `scripts/` | Download, backfill, audit, and utility scripts (see Scripts section) |
| `figures/` | All saved plots, organized by notebook (`figures/data_cleaning/`, `figures/eda/`, `figures/modeling/`) |
| `compressed/processed_ercot_2026-03-20.tar.gz` | Processed parquets snapshot — extract to skip `data_cleaning.ipynb` |

---

## Quick Start

```bash
# 1. Extract processed data into the correct directory
#    This includes train_features.parquet and test_features.parquet —
#    you can skip eda.ipynb's slow parquet-building step (cell hvtm7kf7wu5).
mkdir -p data/processed/ercot
tar -xzf compressed/processed_ercot_2026-03-20.tar.gz -C data/processed/ercot/

# 2. Activate conda environment
conda activate erdos_ds_environment

# 3. Run notebooks in order
jupyter notebook eda.ipynb        # EDA and feature engineering (parquet build optional if tarball extracted)
jupyter notebook modeling.ipynb   # reads train/test_features.parquet from data/processed/ercot/
```

To download raw data from scratch: see `docs/DATA_DOWNLOAD.md`.

---

## Data Layout

```
data/
├── raw/ercot/<DATASET>/<YYYY>/<MM>/      # Raw monthly CSVs + sidecar files
├── archive/ercot/<DATASET>/              # Per-doc source files (post backfill)
├── processed/ercot/                      # Parquets (gitignored — use compressed/)
└── sample/                               # Small sample files for dev/testing

compressed/
├── processed_ercot_2026-03-20.tar.gz     # All processed parquets (73 MB)
└── raw_<DS>_202602.tar.gz                # Raw monthly CSVs per dataset (9 files)

figures/
├── data_cleaning/                        # Plots from data_cleaning.ipynb
├── eda/                                  # Plots from eda.ipynb
└── modeling/                             # Plots from modeling.ipynb

docs/                                     # Project documentation
scripts/                                  # Python utility scripts
archive/                                  # Legacy exploration notebooks
state/                                    # Download checkpoints (<DATASET>.json)
logs/                                     # Download and backfill logs
```

---

## Datasets (Analysis Window: 2017-07-01 → 2025-12-31)

| Dataset | Content | Granularity | Model Role |
|---|---|---|---|
| `NP6-346-CD` | Actual load — Houston + 3 zones | Hourly | `load_houston_lag48` (D-2 lag) |
| `NP6-345-CD` | Actual load — 8 weather zones | Hourly | `fc_coast` proxy |
| `NP3-565-CD` | Load forecast | Hourly | `fc_coast`, `fc_system_total` |
| `NP6-905-CD` | RTM settlement prices (15-min→hourly) | 15-min | Target + `rtm_*_lag48` |
| `NP4-732-CD` | Wind actual + forecast | Hourly | `wgrpp_lz_south_houston`, `wf_stwpf` |
| `NP4-190-CD` | DAM settlement prices | Hourly | `dam_price_houston` (top feature) |
| `NP4-523-CD` | DAM system lambda | Hourly | `system_lambda` |
| `NP4-188-CD` | DAM ancillary MCPC prices | Hourly | `mcpc_regup/rrs/nspin/regdn` |
| `NP3-233-CD` | Outage capacity | Hourly | `total_resource_mw` |
| Weather | Houston + Texas stations (Open-Meteo) | Hourly | 4 Houston weather features |
| `NP4-745-CD` | Solar actual/forecast | Hourly | **Excluded** — starts 2022-06 |
| `NP6-331-CD` | RT ancillary prices | Hourly | **Excluded** — only 3 months |
| `NP3-911-ER` | 2-Day DAM AS reports | Hourly | **Excluded** — schema changed 4× |

**Prediction cutoff:** midnight CST (00:00 D). DAM/lambda/ancillary are available at cutoff (posted ~12:35 CST D-1). Load/RTM/wind actuals require a D-2 lag (they post after midnight).

---

## Model Results

| Model | Train Window | R² (test 2025) | RMSE |
|---|---|---|---|
| Ridge v1 (22 features) | 2017–2024 | 0.243 | 0.740 |
| XGBoost v2 (30 features) | 2017–2024 | 0.353 | 0.687 |
| **XGBoost v3 (31 feat, best window)** | **2021–2024** | **0.494** | **0.607** |
| XGBoost v3 (full train) | 2017–2024 | 0.485 | 0.612 |
| Spike classifier | 2021–2024 | AUC=0.969, F1=0.506 | — |

Top features: `garch_cond_vol` (25.5%), `dam_price_houston` (15.0%), `abs_dam_rtm_spread` (4.2%).
Bootstrap CI (block=24h, 2000 iters): R² [0.459, 0.508].

**Train/Test/Hold-out split:**
- Train: 2017-07-04 → 2024-12-31 (65,712 hours)
- Test: 2025-01-01 → 2025-12-31 (8,760 hours) — clean holdout, never used for model selection
- Hold-out: 2026 — reserved; do not touch until model is locked

---

## Scripts

| Script | Purpose |
|---|---|
| `scripts/download_ercot_public_reports.py` | Main ERCOT API downloader (checkpoint resume, bulk download, tqdm) |
| `scripts/backfill_post_datetime.py` | Backfill `postDateTime` into monthly CSVs |
| `scripts/archive_raw_sources.py` | Move per-doc source files to archive after download |
| `scripts/weather_data_processing.py` | Process raw weather station CSVs → hourly/daily parquets |
| `scripts/houston_weather.py` / `tx_stations.py` | Fetch weather data from Open-Meteo API + station registry |
| `scripts/sort_csv.py` | Re-sort monthly CSVs without API calls |
| `scripts/audit_post_datetime_quality.py` | Audit postDateTime fill rate across monthly CSVs |
| `scripts/ercot_dataset_catalog.py` | Central dataset catalog and profile definitions |
| `scripts/list_ercot_analysis_datasets.py` | Print recommended dataset IDs by analysis profile |
| `scripts/show_resume_status.py` | Display download checkpoint/resume status |

Legacy one-off scripts: `scripts/archive/` (10 files — horizon analysis, size estimators, one-off fixes).

---

## Collaboration

- All data is local and gitignored (`/data/`). Share processed data via `compressed/processed_ercot_2026-03-20.tar.gz`.
- Git workflow: see `docs/GIT_TERMINAL.md`.
- Environment: `conda activate erdos_ds_environment` (Python 3.12, arch, xgboost, sklearn, matplotlib, seaborn).
- Figures are gitignored. Each notebook saves to `figures/<notebook>/` and auto-creates the subfolder at run time.
