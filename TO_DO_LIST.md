# Project To-Do List
_Last updated: 2026-03-15_

Team: Yun, Eric, Neeraj (+ others)

---

## ✅ Completed

- Downloaded all 9 ERCOT datasets (2017-07 → 2024-12) + weather CSVs
- Backfilled `postDateTime` for all datasets (100% coverage)
- Removed Git LFS — all CSVs are local only (gitignored)
- Built processed parquets: load, DAM, RTM, ancillary, wind, outage, weather
- EDA notebook (`eda.ipynb`): time series plots, target selection, feature correlations, non-linear effects
- Designed leakage-safe `build_features(delivery_date)` using 6PM D-1 cutoff
- Audited `build_features()` — no direct leakage; 3 minor issues fixed
- Added weather features (`temp_f`, `humidity_pct`, `wind_gust_mph`, `precip_in`, `temp_f_texas`) to `build_features()`
- Built `train_features.parquet` (65,712 rows × 31 cols, 2017-07 → 2023-12) and `test_features.parquet` (8,760 rows, 2024-2025)
- Added feature dictionary cell to `eda.ipynb` — all 27 features explained by category
- **Section 6**: Ridge regression baseline (R²=0.231) + XGBoost v1 regressor (R²=0.357) + XGBoost v1 classifier (AUC=0.888)
- **Section 7**: XGBoost v2 — 7 new engineered features, tuned hyperparams, optimal spike threshold (R²=0.378, AUC=0.903)
- **Section 7.1**: HAR-RV baseline + GARCH order selection (AR(1)-GARCH(2,1)-t, BIC=55488.7)
- **Section 7.2**: Seasonal OLS + GARCH on residuals (persistence=1.000, IGARCH); walk-forward CV: Ridge=0.169, Lasso=0.111; Lasso feature selection (7 survivors)
- **Section 7.3**: GARCH conditional vol σ_t as 35th XGBoost feature — CV mean R² 0.285→0.454, final test R² 0.314→0.410
- **Section 7.4**: Rolling Lasso feature selection over 3-year windows — heatmap of regime shifts in feature importance
- **Section 7.5**: Post-Uri training window test — 3 windows (2017–2023, 2021–2023, 2022–2023) compared on 2024–2025 test set

---

## 🔥 High Priority — Remaining (Day 3)

- [ ] **Run Sections 7.3–8** — execute cells in eda.ipynb in order; record final R² from 7.5; update model_results.md with actual numbers
- [ ] **Presentation notebook** — clean summary for Erdos showcase (1 notebook, key plots + model leaderboard)
- [x] **Error analysis** — Section 8 added: MAE by hour/month, spike vs non-spike, 10 worst predictions, PR-AUC + Brier score
- [x] **Bug fix** — Section 7.3 now includes all 34 engineered features (was missing 7 v2 features)
- [x] **all_features.parquet** — saved with all 35 features + split label at data/processed/ercot/

---

## 🟡 Medium Priority

- [ ] **Fix drop_duplicates in data_cleaning.ipynb** — 6 datasets (load346, load345, lam, anc, dam, rtm905) missing `keep='last'` + sort by postDateTime. Add `.sort_values('post_datetime').drop_duplicates(..., keep='last')`. Only affects re-runs; current parquets are correct.
- [ ] **Fix weather_data_processing.py path** — change `../data/raw/weather/` to `data/raw/weather/` so it runs from project root.
- [ ] **Update model leaderboard** with Section 7.5 winner
- [ ] **Spike classifier error analysis** — which spikes are missed? precision/recall by price range
- [ ] **Commit presentation notebook** once drafted

---

## 🔵 Low Priority / Ideas

- [ ] **Zone-level models** — extend targets to NORTH, SOUTH, WEST zones (not just Houston)
- [ ] **Intraday RTM forecasting** — shorter-horizon models using RTM data from earlier in day
- [ ] **Ensemble / stacking** — combine GARCH σ_t forecast with XGBoost for improved vol estimates

---

## ⛔ Blocked / On Hold

- **2026 out-of-sample evaluation** — hold-out until final model is locked
- **NP4-745-CD (SCED prices)** — excluded from analysis (starts 2022-06, no full history)
- **NP3-911-ER (ancillary offers)** — excluded (schema changed 4×, not coherent)

---

## Collaboration Notes

- **Data lives locally** (gitignored) — each person needs their own copy. See `README.md` for setup.
- **Notebooks**: `data_cleaning.ipynb` builds all parquets; `eda.ipynb` has all analysis, features, and models.
- **Train/test split**: Train 2017-07-04 → 2023-12-31 | Test 2024–2025 | Hold-out 2026
- **conda env**: `erdos_ds_environment` — install `arch` with `conda run -n erdos_ds_environment pip install arch`
- **WORKLOG.md** is a detailed local work log (gitignored — ask Yun for current version if needed).
