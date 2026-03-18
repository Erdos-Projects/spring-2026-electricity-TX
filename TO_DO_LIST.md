# Project To-Do List
_Last updated: 2026-03-17 (session 2)_

Team: Yun, Eric, Neeraj (+ others)

---

## ✅ Completed

### Data Pipeline
- Downloaded all 9 ERCOT datasets (2017-07 → 2025-12) + weather CSVs
- Backfilled `postDateTime` for all datasets (100% coverage)
- Built processed parquets: load, DAM, RTM, ancillary, wind, outage, weather (all in `data/processed/ercot/`)
- Compressed shareable archive: `processed_ercot_2026-03-16.tar.gz` (81 MB)

### Feature Engineering
- Designed leakage-safe `build_features(delivery_date)` — 6PM D-1 cutoff, no look-ahead
- Audited `build_features()` — no direct leakage; 3 issues fixed (outage fallback, ECRS fillna, ECRS flag)
- Added 5 weather features (`temp_f`, `humidity_pct`, `wind_gust_mph`, `precip_in`, `temp_f_texas`)
- Built `train_features.parquet` (~65,688 rows, 2017-07 → 2024-12) and `test_features.parquet` (~8,760 rows, 2025)
- Built `all_features.parquet` (~74,448 rows, all 31 features + split label)
- Removed `mcpc_ecrs`, `mcpc_ecrs_available`, `log_mcpc_ecrs`, `mcpc_ecrs_above_50` from all feature sets (launched 2021-06, too short coverage) — feature counts: v1=23, v2=30, v3=31
- Fixed `build_features()` performance: `_day()` now uses vectorized timestamp comparison (was `.dt.date`, caused KeyboardInterrupt on large parquets)
- **RTM std target updated to 5-point**: `rtm_price_std_hb_houston` now includes boundary point p0 of next hour, capturing inter-hour price jump; mean/max/min remain 4-point

### EDA (`eda.ipynb` Sections 1–5)
- Target selection, time series, diurnal/seasonal patterns, spike rate analysis
- Feature EDA: net load, wind error, forecast revision std, DAM price, ancillary prices
- Regime analysis: Winter Storm Uri (Feb 2021), high-volatility hours, autocorrelation
- Modeling roadmap: feature correlations, modeling plan, data design explanation

### Modeling (`eda.ipynb` Sections 6–8)
- **Section 6**: Ridge (R²=0.231) + XGBoost v1 (R²=0.357) + classifier (AUC=0.888)
- **Section 7**: XGBoost v2 — 30 features (after ECRS removal), tuned hyperparams, optimal spike threshold (R²=0.378, AUC=0.903)
- **Section 7.1**: HAR-RV baseline; GARCH order selection — AR(1)-GARCH(2,1)-t wins (BIC=55488.7)
- **Section 7.2**: Seasonal OLS + GARCH on residuals → IGARCH (α+β=1.0); walk-forward CV (Ridge=0.169, Lasso=0.111); Lasso selects 7 features
- **Section 7.3**: GARCH conditional vol as 31st XGBoost feature → **R²=0.468** (test 2025); 3-fold CV (val 2022/2023/2024), mean CV R²=0.451
- **Section 7.4**: Rolling 4-year Lasso windows — regime shift heatmap (5 windows: 2017-20 → 2021-24)
- **Section 7.5**: Post-Uri window test — winner: 2021–2024 post-Uri (R²=0.475 vs 0.468 full train)
- **Section 8**: Error analysis — MAE by hour/month, spike vs non-spike, worst predictions, PR-AUC, Brier score, leaderboard (code added)
- **Metrics reference cell**: R², RMSE, MAE, AUC, PR-AUC, F1, Brier, BSS explained with formulas

### Notebook Quality
- Logical cell order: Setup → EDA (1–5) → Feature Eng → Models (6–8) → Conclusion
- All major sections have markdown headers, transition cells, and subsection numbers (3.1, 4.1, 5.1–5.3)
- Fixed misplaced X.1 headers (3.1, 4.1, 5.1 were appearing after their code blocks; now correctly before). Done 2026-03-17.
- Feature dictionary: all 27 base features explained by category
- Evaluation metrics reference: all 8 metrics defined with LaTeX formulas
- Conclusion cell: results table, 5 key findings, limitations

---

## ✅ Completed (2026-03-17 session 1)

- [x] **Re-run `data_cleaning.ipynb`** — Done. Rebuilt all parquets with 5-point RTM std. DST cell fails harmlessly (pre-existing; run with `--allow-errors`).
- [x] **Re-run `eda.ipynb` end-to-end** — Done. Rebuilt features (56,928/17,544 rows, 31 features) + all models + all PNGs.
- [x] **Section 7.5 results recorded** — Winner: 2021–2023 post-Uri (R²=0.405, old split). Saved as `model_xgb_reg_v3_best.pkl`.
- [x] **Section 8 outputs confirmed** — MAE plots, leaderboard, PR-AUC=0.273, bootstrap CI printed correctly.

## ✅ Completed (2026-03-17 session 2)

- [x] **Updated train/test split** — Train ≤2024-12-31, Test=2025 only, Hold-out=2026. 2024 moved into training (was in test). CV folds: val=2022/2023/2024 (all within training).
- [x] **Fixed CV fold_ends bug** — fold_ends[2] was `2024-12-31` (would cause fold 3 validate on 2025 = test leak). Fixed to `2023-12-31`.
- [x] **Fixed leap-year bug** — `fold_val_end` now uses `pd.Timestamp(f'{fold_name}-12-31 23:00')` instead of `+Timedelta(days=365)`.
- [x] **Changed rolling Lasso to 4-year windows** — 5 windows (2017-2020 → 2021-2024, step=1yr). Updated comment and plot title from "3-year" to "4-year".
- [x] **Audited all pipeline/date leakage issues** — 7 issues found and fixed; no structural leakage in final notebook.
- [x] **Re-run `eda.ipynb` with clean split** — Done (2026-03-18). train≤2024, test=2025. XGB v3 best R²=0.475 (2021–2024 post-Uri window).

---

## 🔥 High Priority — Presentation

- [ ] **Build presentation notebook** — clean 1-notebook summary for Erdos showcase:
  - Problem statement + ERCOT context
  - 3–4 key EDA plots (YoY volatility trend, spike clustering, Uri zoom, feature correlations)
  - Model leaderboard table
  - Error analysis plots (MAE by hour/month, spike vs non-spike)
  - Rolling Lasso heatmap (regime shifts)
  - Conclusion + limitations

---

## 🟡 Medium Priority — Code Fixes

- [x] **Fix `scripts/eda_stats_report.py` line ~264** — "generation capacity" → "outage capacity" (NP3-233-CD naming). Done 2026-03-17.
- [x] **Fix `drop_duplicates` in `data_cleaning.ipynb`** — 8 datasets fixed: added `.sort_values('post_datetime').drop_duplicates(..., keep='last')`. Only affects re-runs; current parquets are correct. Done 2026-03-17.
- [x] **Fix `scripts/weather_data_processing.py` path** — changed `../data/raw/weather/` → `data/raw/weather/` (5 occurrences). Done 2026-03-17.

---

## 🔵 Low Priority / Future Ideas

- [ ] **Zone-level models** — extend to NORTH, SOUTH, WEST zones (same pipeline, different target)
- [ ] **Intraday forecasting** — shorter-horizon models using partial-day RTM data
- [ ] **Ensemble** — blend Ridge + XGBoost v3 (estimated +0.02–0.04 R²)
- [ ] **YoY volatility plot** — annotated bar chart showing 2017→2024 trend (EDA addition)
- [ ] **Spike clustering analysis** — run-length distribution of consecutive spike hours

---

## ⛔ Blocked / Out of Scope

- **2026 out-of-sample evaluation** — hold-out; do not touch until final model locked
- **NP4-745-CD (SCED/solar)** — excluded (starts 2022-06, no pre-2022 coverage)
- **NP3-911-ER (ancillary offers)** — excluded (schema changed 4×, not coherent)

---

## Collaboration Notes

- **Data**: local only (gitignored) — use `processed_ercot_2026-03-16.tar.gz` to share; extract into `data/processed/`
- **Notebooks**: `data_cleaning.ipynb` builds parquets; `eda.ipynb` has all analysis and models
- **Run order**: restart kernel, run `eda.ipynb` top-to-bottom; Section 7.3 must run before 7.4/7.5/8
- **conda env**: `erdos_ds_environment` — run `conda run -n erdos_ds_environment pip install arch` if missing
- **Train/test split**: Train ≤2024-12-31 | Test 2025 | Hold-out 2026
