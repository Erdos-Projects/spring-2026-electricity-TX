# spring-2026-electricity-TX

ERCOT electricity price volatility forecasting for the Texas Houston Hub.
**Goal:** predict intra-hour RTM price volatility (`log(rtm_price_std)`) and flag price spikes (>$100/MWh) using day-ahead features available at midnight CST.

**Status:** Complete. Final regression model: XGBoost v3 tuned (31 features, +GARCH D-1 lag, post-Uri 2021–2024 window). Test R²=0.393, RMSE=0.665, MAE=4.033 $/MWh (2025 holdout). Submitted spike classifier: XGB Clf v3, PR-AUC=0.237, F1=0.329. Post-submission improvement: XGB-slim (6 features), PR-AUC=0.302, F1=0.389.

---

## Project Directory

### Notebooks (run in this order)

| Notebook | Purpose | Sections |
|---|---|---|
| `data_cleaning.ipynb` | Raw ERCOT CSVs → processed parquets | §1 Setup · §2 Dataset Inventory · §3 Load & Clean · §4 Validation · §5 Build Processed Data · §6 Save Outputs |
| `eda.ipynb` | EDA, feature engineering, leakage-safe design | §1 Setup · §2 Target Variable · §3 Feature EDA · §4 Regime Analysis · §5 Modeling Roadmap · Appendix A–B |
| `model_regression.ipynb` | Regression: baseline through XGBoost v3 tuned, error analysis, rolling forecast, drift detection | §6 Baseline · §7 Model Improvements · §8 Ensemble · §9 Error Analysis & Leaderboard · §10 Rolling Forecast & Drift |
| `model_classifier.ipynb` | Spike classifier: walk-forward CV + test evaluation for binary spike detection (RTM > $100/MWh) | §9.1 CV Diagnostics · §9.2 PR Curves · §9.3 Test Evaluation |
| `presentation.ipynb` | Erdos showcase: narrative, EDA plots, model leaderboard, error analysis, conclusions | §1–§7 |

> `model_regression.ipynb` sections start at §6 (continuation from `eda.ipynb` §5).
> `model_classifier.ipynb` is self-contained — loads parquet features and trained pkl files directly.

### Documentation (`docs/`)

| File | Purpose |
|---|---|
| `docs/DATA_DOWNLOAD.md` | Full download runbook: setup, credentials, commands, postDateTime backfill |
| `docs/DATA_ESTIMATION.md` | Dataset-selection planning: coverage, sizes, time estimates |
| `docs/MODEL_ANALYSIS.md` | Volatility analysis plan: model hierarchy, benchmark design, evaluation framework |
| `docs/houston_weather_USAGE.md` | Weather data fetcher (Open-Meteo API): modes, station registry, examples |
| `docs/GIT_TERMINAL.md` | Beginner Git guide: daily pipeline, conflict resolution, PR workflow |
| `docs/NOTEBOOK_DATA_CLEANING.md` | `data_cleaning.ipynb` component brief: cell IDs, outputs, design decisions |
| `docs/NOTEBOOK_EDA.md` | `eda.ipynb` component brief: cell IDs, outputs, feature engineering design |
| `docs/NOTEBOOK_MODELING.md` | `model_regression.ipynb` component brief: cell IDs, section outline, key results |

### Key Supporting Files

| File | Purpose |
|---|---|
| `config/download.sample.yaml` | Starter config — copy to `config/download.yaml` for local use |
| `scripts/` | Download, backfill, evaluation, and utility scripts (see Scripts section) |
| `figures/` | All saved plots, organized by notebook (`figures/data_cleaning/`, `figures/eda/`, `figures/modeling/`) |
| `compressed/processed_ercot_2026-03-22.tar.gz` | Processed parquets snapshot — extract to skip `data_cleaning.ipynb` |

---

## Quick Start

```bash
# 1. Extract processed data into the correct directory
#    Includes train_features.parquet and test_features.parquet —
#    skip the slow data_cleaning.ipynb parquet-build step.
mkdir -p data/processed/ercot
tar -xzf compressed/processed_ercot_2026-03-22.tar.gz -C data/processed/ercot/

# 2. Activate conda environment
conda activate erdos_ds_environment

# 3. Run notebooks in order
jupyter notebook eda.ipynb               # EDA and feature engineering (parquet build optional)
jupyter notebook model_regression.ipynb  # reads train/test_features.parquet
jupyter notebook model_classifier.ipynb  # reads same parquets + saves classifier pkls
```

To download raw data from scratch: see `docs/DATA_DOWNLOAD.md`.

---

## Data Layout

```
data/
├── raw/ercot/<DATASET>/<YYYY>/<MM>/      # Raw monthly CSVs + sidecar files
├── archive/ercot/<DATASET>/              # Per-doc source files (post backfill)
├── processed/ercot/                      # Parquets + model pkls (gitignored — use compressed/)
└── sample/                               # Small sample files for dev/testing

compressed/
├── processed_ercot_2026-03-22.tar.gz     # All processed parquets (latest)
└── raw_<DS>_202602.tar.gz                # Raw monthly CSVs per dataset (9 files)

figures/
├── data_cleaning/                        # Plots from data_cleaning.ipynb
├── eda/                                  # Plots from eda.ipynb
├── model_regression/                     # Plots from model_regression.ipynb
├── model_classifier/                     # Plots from model_classifier.ipynb
└── presentation/                         # Plots from presentation.ipynb

docs/                                     # Project documentation (gitignored locally)
scripts/                                  # Python utility scripts
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
| `NP6-905-CD` | RTM settlement prices (15-min→hourly) | 15-min | Target + `rtm_*_lag24` |
| `NP4-732-CD` | Wind actual + forecast | Hourly | `wgrpp_lz_south_houston`, `wf_stwpf` |
| `NP4-190-CD` | DAM settlement prices | Hourly | `dam_price_houston` (top feature) |
| `NP4-523-CD` | DAM system lambda | Hourly | `system_lambda` |
| `NP4-188-CD` | DAM ancillary MCPC prices | Hourly | `mcpc_regup/rrs/nspin/regdn` |
| `NP3-233-CD` | Outage capacity | Hourly | `total_resource_mw` |
| Weather | Houston stations (Open-Meteo) | Hourly | 4 Houston weather features |
| `NP4-745-CD` | Solar actual/forecast | Hourly | **Excluded** — starts 2022-06 |
| `NP6-331-CD` | RT ancillary prices | Hourly | **Excluded** — only 3 months |
| `NP3-911-ER` | 2-Day DAM AS reports | Hourly | **Excluded** — schema changed 4× |

**Prediction cutoff:** 00:05 CST (D). DAM/lambda/ancillary available (posted ~12:35 CST D-1). RTM available as D-1 lag (last interval posts ~00:02 CST D). Load/wind actuals require D-2 lag (post ~05:50/00:31 CST D, after cutoff).

---

## Model Results

Model selection uses **walk-forward CV R²** (val folds 2022/2023/2024). Test set (2025) used only for final reporting on the selected model.

> **Metric note:** AUC-ROC is omitted for the spike classifier. At a 2.2% spike rate, a trivial always-negative classifier scores AUC≈0.98 — meaningless. We use PR-AUC (continuous scores) and F1 (at fixed/optimal threshold) instead.

### Volatility Regression (`model_regression.ipynb`)

| Model | Features | Train Window | CV R² | Test R² | RMSE (log) | MAE ($/MWh) | Selection |
|---|---|---|---|---|---|---|---|
| HAR-Ridge | 3 | 2017–2024 | 0.169 | 0.190 | 0.768 | 4.348 | ✗ |
| Full-Ridge (29 feat) | 29 | 2017–2024 | 0.145 | 0.156 | 0.784 | 4.525 | ✗ |
| HAR+Full-Ridge (32 feat) | 32 | 2017–2024 | 0.259 | 0.261 | 0.733 | 4.291 | ✗ best linear |
| XGBoost v1 | 21 | 2017–2024 | — | 0.348 | 0.689 | 4.173 | ✗ |
| XGBoost v2 | 29 | 2017–2024 | — | 0.369 | 0.678 | 4.101 | ✗ |
| XGBoost v3 (orig params) | 31 | 2017–2024 | 0.286 | 0.372 | 0.676 | 4.102 | ✗ |
| XGBoost v3 tuned (full train) | 31 | 2017–2024 | 0.311 | 0.385 | 0.669 | 4.028 | ✗ |
| **XGBoost v3 tuned (post-Uri)** | **31** | **2021–2024** | **0.311** | **0.386** | **0.669** | **4.027** | **✅ FINAL** |

**Rolling forecast (2025, 53 weeks):** Sliding 4-year window retraining beats fixed model in 43/53 weeks (MAE 0.4925 vs 0.5069).

Top features (v3 tuned): GARCH conditional vol (~25%), Houston Hub DAM price (~15%), absolute DAM–RTM spread (~4%).

### Spike Classifier (`model_classifier.ipynb`)

**CV walk-forward (val 2022/2023/2024, 4.37% spike rate):**

| Model | CV PR-AUC | CV F1 (mean) | Selection |
|---|---|---|---|
| Naive: DAM > $100 | — | 0.471 | ✅ best CV F1 (regime-specific) |
| XGB Clf v1 (21 feat) | **0.363** | 0.433 | ✗ |
| XGB Clf v2 (29 feat) | 0.359 | 0.438 | ✗ |
| **XGB Clf v3 (31 feat)** | **0.351** | **0.426** | **✗** |
| *XGB-slim (6 feat, post-submission)* | *TBD* | *TBD* | *post-submission* |

**Test 2025 (2.24% spike rate, 196/8,760 hours):**

| Model | PR-AUC | F1 | Precision | Recall |
|---|---|---|---|---|
| Naive: DAM > $100 (fixed) | — | 0.294 | 0.366 | 0.245 |
| DAM continuous score | 0.296 | — | — | — |
| XGB Clf v1 | 0.229 | 0.323 | 0.239 | 0.500 |
| XGB Clf v2 | 0.237 | 0.317 | 0.251 | 0.439 |
| **XGB Clf v3** | **0.237** | **0.329** | **0.251** | **0.474** |
| *XGB-slim (6 feat, post-submission)* | *0.302* | *0.389* | *0.340* | *0.454* |

> **Post-submission note:** XGB-slim uses 6 spike-relevant features (`dam_price_houston`, `system_lambda`, `mcpc_regup`, `rtm_std_lag24`, `abs_dam_rtm_spread`, `garch_cond_vol`) with depth=3 and post-Uri (2021–2024) training. It is the first classifier to beat DAM continuous on both PR-AUC and F1. A comprehensive ablation of 25+ variants (feature swaps, interactions, temporal context, isolation forest, calibrated ensembles, quantile regression, multi-model blends) confirmed that no variant consistently improves over the 6-feature model.

**Train/Test/Hold-out split:**
- Train: 2017-07-04 → 2024-12-31 (65,712 hours)
- Test: 2025-01-01 → 2025-12-31 (8,760 hours) — clean holdout, never used for model selection
- Hold-out: 2026 — reserved; do not touch

---

## Scripts

| Script | Purpose |
|---|---|
| `scripts/download_ercot_public_reports.py` | Main ERCOT API downloader (checkpoint resume, bulk download) |
| `scripts/backfill_post_datetime.py` | Backfill `postDateTime` into monthly CSVs |
| `scripts/archive_raw_sources.py` | Move per-doc source files to archive after download |
| `scripts/weather_data_processing.py` | Process raw weather station CSVs → hourly/daily parquets |
| `scripts/houston_weather.py` / `tx_stations.py` | Fetch weather data from Open-Meteo API + station registry |
| `scripts/metrics_core.py` | Core feature engineering + regression metrics (R², RMSE, MAE) |
| `scripts/metrics_rolling.py` | Rolling XGBoost forecast evaluation (Option A vs C) |
| `scripts/metrics_rolling_har.py` | Rolling HAR-Ridge forecast evaluation |
| `scripts/metrics_rolling_arima.py` | Rolling ARIMA/ARIMAX forecast evaluation |
| `scripts/metrics_classifier_ensemble.py` | Classifier ensemble (Ridge + XGB) evaluation |
| `scripts/eval_all_models.py` | Evaluate all XGB model pkl versions on 2025 test set |
| `scripts/run_ts_models.py` | Run ARIMA/ARIMAX/SARIMAX time-series models |
| `scripts/audit_post_datetime_quality.py` | Audit postDateTime fill rate across monthly CSVs |
| `scripts/sort_csv.py` | Re-sort monthly CSVs without API calls |
| `scripts/ercot_dataset_catalog.py` | Central dataset catalog and profile definitions |
| `scripts/show_resume_status.py` | Display download checkpoint/resume status |

---

## Collaboration

- All data is local and gitignored (`/data/`). Share processed data via `compressed/processed_ercot_2026-03-22.tar.gz`.
- Git workflow: see `docs/GIT_TERMINAL.md`.
- Environment: `conda activate erdos_ds_environment` (Python 3.11, arch, xgboost, sklearn, pmdarima, matplotlib, seaborn).
- Figures are gitignored locally. Each notebook saves to `figures/<notebook>/` and auto-creates the subfolder at run time.
