# Project To-Do List
_Last updated: 2026-03-19 (session 6 continued)_

Team: Yun, Eric, Neeraj (+ others)

---

## ✅ Completed

### Data Pipeline
- Downloaded all 9 ERCOT datasets (2017-07 → 2025-12) + weather CSVs
- Backfilled `postDateTime` for all datasets (100% coverage)
- Built processed parquets: load, DAM, RTM, ancillary, wind, outage, weather (all in `data/processed/ercot/`)
- Compressed shareable archive: `processed_ercot_2026-03-20.tar.gz` (73 MB)

### Feature Engineering
- Designed leakage-safe `build_features(delivery_date)` — 6PM D-1 cutoff, no look-ahead
- Audited `build_features()` — no direct leakage; 3 issues fixed (outage fallback, ECRS fillna, ECRS flag)
- Added 5 weather features (`temp_f`, `humidity_pct`, `wind_gust_mph`, `precip_in`, `temp_f_texas`)
- Built `train_features.parquet` (~65,688 rows, 2017-07 → 2024-12) and `test_features.parquet` (~8,760 rows, 2025)
- Built `all_features.parquet` (74,472 rows, 30 features + 4 targets + split label — created by modeling.ipynb)
- Removed `mcpc_ecrs`, `mcpc_ecrs_available`, `log_mcpc_ecrs`, `mcpc_ecrs_above_50` from all feature sets (launched 2021-06, too short coverage) — feature counts: v1=22, v2=29, v3=30
- Fixed `build_features()` performance: `_day()` now uses vectorized timestamp comparison (was `.dt.date`, caused KeyboardInterrupt on large parquets)
- **RTM std target updated to 5-point**: `rtm_price_std_hb_houston` now includes boundary point p0 of next hour, capturing inter-hour price jump; mean/max/min remain 4-point

### EDA (`analysis.ipynb` Sections 1–5)
- Target selection, time series, diurnal/seasonal patterns, spike rate analysis
- Feature EDA: net load, wind error, forecast revision std, DAM price, ancillary prices
- Regime analysis: Winter Storm Uri (Feb 2021), high-volatility hours, autocorrelation
- Modeling roadmap: feature correlations, modeling plan, data design explanation

### Modeling (`analysis.ipynb` Sections 6–9)
- **Section 6**: Ridge (R²=0.231) + XGBoost v1 (R²=0.357) + classifier (AUC=0.888)
- **Section 7**: XGBoost v2 — 30 features (after ECRS removal), tuned hyperparams, optimal spike threshold (R²=0.378, AUC=0.903)
- **Section 7.1**: HAR-RV baseline; GARCH order selection — AR(1)-GARCH(2,1)-t wins (BIC=55488.7)
- **Section 7.2**: Seasonal OLS + GARCH on residuals → IGARCH (α+β=1.0); walk-forward CV (Ridge=0.169, Lasso=0.111); Lasso selects 5 features
- **Section 7.3**: GARCH conditional vol as 31st XGBoost feature → **R²=0.468** (test 2025); 3-fold CV (val 2022/2023/2024), mean CV R²=0.451
- **Section 7.4**: Rolling 4-year Lasso windows — regime shift heatmap (5 windows: 2017-20 → 2021-24)
- **Section 7.5**: Post-Uri window test — winner: 2021–2024 post-Uri (R²=0.475 vs 0.468 full train)
- **Section 8**: Error analysis — MAE by hour/month, spike vs non-spike, worst predictions, PR-AUC=0.269, Brier score, leaderboard
- **Section 9**: Rolling 24h forecast evaluation — Option A (fixed model, MAE by hour-of-day) vs Option C (sliding 4-year window, 52 weekly retrains); drift score = MAE_A − MAE_C per week; 45/53 weeks C wins (mean drift +0.0114)
- **Section 9.3**: Formal drift tests — Page-Hinkley (5 change points in 2025), KS test per feature per month (all 26 features significant in 10–12/12 months), Jensen-Shannon divergence heatmap
- **Metrics reference cell**: R², RMSE, MAE, AUC, PR-AUC, F1, Brier, BSS explained with formulas

### Notebook Quality
- Logical cell order: Setup → EDA (1–5) → Feature Eng → Models (6–9) → Conclusion
- All major sections have markdown headers, transition cells, and subsection numbers (3.1, 4.1, 5.1–5.3)
- Fixed misplaced X.1 headers (3.1, 4.1, 5.1 were appearing after their code blocks; now correctly before). Done 2026-03-17.
- Feature dictionary: all 27 base features explained by category
- Evaluation metrics reference: all 8 metrics defined with LaTeX formulas
- Conclusion cell: results table, 6 key findings (incl. drift), limitations
- Intro TOC includes sections 1–9; all section headers consistent ("## N. Title" style)

---

## ✅ Completed (2026-03-17 session 1)

- [x] **Re-run `data_cleaning.ipynb`** — Done. Rebuilt all parquets with 5-point RTM std. DST cell fails harmlessly (pre-existing; run with `--allow-errors`).
- [x] **Re-run `analysis.ipynb` end-to-end** — Done. Rebuilt features (56,928/17,544 rows, 31 features) + all models + all PNGs.
- [x] **Section 7.5 results recorded** — Winner: 2021–2023 post-Uri (R²=0.405, old split). Saved as `model_xgb_reg_v3_best.pkl`.
- [x] **Section 8 outputs confirmed** — MAE plots, leaderboard, PR-AUC=0.273, bootstrap CI printed correctly.

## ✅ Completed (2026-03-17 session 2)

- [x] **Updated train/test split** — Train ≤2024-12-31, Test=2025 only, Hold-out=2026. 2024 moved into training (was in test). CV folds: val=2022/2023/2024 (all within training).
- [x] **Fixed CV fold_ends bug** — fold_ends[2] was `2024-12-31` (would cause fold 3 validate on 2025 = test leak). Fixed to `2023-12-31`.
- [x] **Fixed leap-year bug** — `fold_val_end` now uses `pd.Timestamp(f'{fold_name}-12-31 23:00')` instead of `+Timedelta(days=365)`.
- [x] **Changed rolling Lasso to 4-year windows** — 5 windows (2017-2020 → 2021-2024, step=1yr). Updated comment and plot title from "3-year" to "4-year".
- [x] **Audited all pipeline/date leakage issues** — 7 issues found and fixed; no structural leakage in final notebook.
- [x] **Re-run `analysis.ipynb` with clean split** — Done (2026-03-18). train≤2024, test=2025. XGB v3 best R²=0.475 (2021–2024 post-Uri window).

## ✅ Completed (2026-03-18 session 3)

- [x] **Added Section 9: Rolling forecast & drift detection** — Option A (fixed model, MAE by hour-of-day) vs Option C (sliding 4-year window, 52 weekly retrains). Drift score = MAE_A − MAE_C per week; 45/53 weeks C wins, mean drift +0.0114.
- [x] **Added Section 9.3: Formal drift tests** — Page-Hinkley (5 change points: Jan 25, Mar 3, Apr 28, Jul 12, Oct 3 2025); KS test shows all 26 features significant in 10–12/12 months; JS divergence heatmap confirms sustained covariate shift.
- [x] **Fixed intro TOC** — Sections 6–9 added to table of contents in intro markdown cell.
- [x] **Fixed Section 7.5 markdown** — Window names updated (2017-2023/2021-2023 → 2017-2024/2021-2024); test date "2024-2025" → "2025".
- [x] **Updated weather features in Section 3.6 Final Feature Set table** — replaced TBD with actual 5 features.
- [x] **Comprehensive notebook review + consistency pass** — 9 cells corrected:
  - Removed duplicate `### 3.1` header from `eda-s3-hdr`
  - Removed `mcpc_ecrs` row from Sec 3.6 Final Feature Set table; added ECRS exclusion note
  - Fixed Sec 5.2 CV folds: removed "Fold 4: validate held-out test" (test leakage); corrected to 3-fold with proper holdout note
  - Fixed Sec 7.1 (`ebqtn536prt`): "Train ≤2023, Test 2024–2025" → "Train ≤2024, Test 2025"; XGB v2 R²=0.372
  - Filled all TBD values in Sec 7.3 (`8cbr2thdi17`): v2 R²=0.372/RMSE=0.676, v3 R²=0.468/RMSE=0.622; feature importance 21.5%/19.9%; correct CV table values
  - Fixed Sec 7.2 summary table (`9095lbikrwo`): XGB v2 CV R² 0.289→0.273 (correct post-fix value); labeled "pre-GARCH"
  - Fixed Sec 8 header style: "## Section 8 —" → "## 8."
  - Filled Conclusion TBDs: all model R² values, PR-AUC 0.269, F1 0.338, spike ratio 2.5×; added drift finding (6th key finding)
  - Fixed `krj9cpi4w3j` save cell: "≤2023 from test (2024–2025)" → "≤2024 from test (2025)"
- [x] **Final consistency pass** — stale "2024–2025" test date references fixed in 4 additional cells (`lfl8i3ow8mp`, `cdelxch0xcn` ×2, `6fjl1b32fne`); Section 9 added to intro TOC; XGB v2 CV R² 0.289→0.273 corrected; train/test/holdout summary added to intro cell.

---

## ✅ Completed (2026-03-18 session 4)

- [x] **Houston-only weather features** — dropped `temp_f_texas_avg` from all feature sets (build_features, FEAT_V2 lists in 5 code cells, Feature Dictionary, Section 3.6 table). Feature counts: v1=22, v2=29, v3=30.
- [x] **Added XGBoost classifier v3** — added to Section 7.3 (`zqsinbn9kv`) using `FEAT_V3` (30 feats incl. `garch_cond_vol`). Section 8 (`lsctyepdxx`) updated to load `model_xgb_clf_v3.pkl` and use FEAT_V3.
- [x] **Fixed SPIKE_THRESHOLD inconsistency** — `SPIKE_THRESHOLD = 500` in eda-imports is EDA-only (clarified with comment); Section 5.2 plan table now explicitly says `> $100/MWh` instead of `> SPIKE_THRESHOLD`. Model's `spike_flag = (rtm_price_mean > 100)` is unambiguous.
- [x] **Feature count consistency pass** — updated all "23/30/31 features" references to "22/29/30" across 23 cells total. Lasso zeros-out count updated: 23/30 → 24/29 (5 survivors unchanged).

## ✅ Completed (2026-03-18 session 5)

- [x] **Removed git-lfs** — `git lfs uninstall`; no `.gitattributes`; all data gitignored via `/data/`
- [x] **Re-downloaded NP6-346-CD** — 3,105 source docs → 103 real monthly CSVs (replaced LFS pointer stubs); per-doc sources archived to `data/archive/ercot/NP6-346-CD/`
- [x] **Added `scripts/archive_raw_sources.py`** — moves per-doc source files to archive after download; keeps monthly CSVs in raw/
- [x] **Updated all datasets to March 2026** — downloaded March 2026 docs for all 9 active datasets; Feb 2026 verified complete for all
- [x] **Per-dataset compressed archives** — `compressed/raw_<DS>_202602.tar.gz` (9 files); replaced old `processed_ercot_2026-03-16.tar.gz`
- [x] **Fixed `KeyError: 'OperDay'` in Cell 10** — `_dst_detail()` now checks `date_col not in df.columns` after concat; prints `[SKIP]` and returns gracefully
- [x] **data_cleaning.ipynb runs clean** — verified end-to-end with no errors
- [x] **analysis.ipynb runs clean** — verified end-to-end with no errors in any cell

## ✅ Completed (2026-03-19 session 6)

- [x] **Swapped wind features to Houston-specific** — replaced `wgrpp_system_wide`/`wind_error_system`/`np4_732_wind_system` with `wgrpp_lz_south_houston`/`wind_error_houston`/`np4_732_wind_houston` across all 10 affected cells in `analysis.ipynb` (`vv567ec2vqn`, `bg2hicyxe48`, `5qwqlafwv7`, `cdelxch0xcn`, `zqsinbn9kv`, `rnii199t7eo`, `oe8x9u81b9b`, `eda-wind-error`, `eda-uri`, `hvtm7kf7wu5`)
- [x] **Updated markdown cells** — `eda-s3-2-hdr`, `vcl13m4b3hn`, `2k0lwjbibru`, `eda-s5-2-plan`, `9uhfq09mjk` (Feature Dictionary), `eda-corr-summary`
- [x] **Added `wgrpp_lz_south_houston` to `ercot_combined` merge** — `data_cleaning.ipynb` cell `s05-merge-code` updated
- [x] **Fixed 5 cells with partial content** — cells were saved as partial lines in previous session; restored from HEAD and applied correct substitutions
- [x] **All 39 code cells pass syntax check** — zero remaining system-wide wind references in `analysis.ipynb`
- [x] **Rebuilt parquets and re-ran all models** — `ercot_combined` already rebuilt (Mar 18 23:45 run); `train/test/all_features.parquet` rebuilt with `wgrpp_lz_south_houston`/`wind_error_houston`, `temp_f_texas_avg` dropped
- [x] **Fixed FEAT_V3 ordering bug (Sections 8 & 9)** — Section 7.5 redefines `FEAT_V2/FEAT_V3` from DataFrame column order; pkl models have different feature ordering. Fix: `FEAT_V3 = list(m_v3.get_booster().feature_names)` after loading pkl. Applied to `lsctyepdxx` (Sec 8) and `s9_1code0001` (Sec 9.1).
- [x] **Extended bootstrap CI (Section 8.3, cell `6fjl1b32fne`)** — block bootstrap (block=24h, 2000 iterations): R²=0.484 [0.459, 0.508]; MAE=3.773 [3.053, 4.349] $/MWh; paired Δ R²=+0.130 [+0.106, +0.152], p<0.001. Also covers AUC/PR-AUC CIs. Saved `bootstrap_ci.png`.
- [x] **Updated all stale markdown cells** — Section 7.3, Section 7.5, Conclusion with fresh numbers (v3 R²=0.484, v3 best R²=0.489, RMSE=0.610, classifier AUC=0.969/PR-AUC=0.495/F1=0.506)
- [x] **Full analysis.ipynb clean run** — 0 errors across all 39 code cells (Mar 19 03:49 run)

---

## 🔥 High Priority — Presentation

- [ ] **[DataAgent]** Rebuild `ercot_combined.parquet` — Houston wind swap (`wgrpp_lz_south_houston` in merge)
- [ ] **[DataAgent]** Fix weather CSVs — download actual content (currently LFS pointer stubs), rerun `weather_data_processing.py`
- [ ] **[ValidationAgent]** Schema + row count check after rebuild
- [ ] **[EDAAgent]** Re-run `eda.ipynb` §3–§5 after parquet rebuild
- [ ] **[ModelAgent]** Rebuild feature parquets + retrain all models (v2/v3 reg+clf, rolling Lasso, §7.5, §9)
- [ ] **[ValidationAgent]** Verify feature parquets + notebook run after retrain
- [ ] **[PresentationAgent]** Build `presentation.ipynb` — clean 1-notebook summary for Erdos showcase:
  - Problem statement + ERCOT context
  - 3–4 key EDA plots (YoY volatility trend, spike clustering, Uri zoom, feature correlations)
  - Model leaderboard table
  - Error analysis plots (MAE by hour/month, spike vs non-spike)
  - Rolling Lasso heatmap (regime shifts)
  - Conclusion + limitations
- [ ] **[PythonAgent]** Convert notebooks to `src/` Python scripts once notebooks are stable
- [ ] **[GitAgent]** Commit all of the above once each stage is verified

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

- **Data**: local only (gitignored) — use `compressed/processed_ercot_2026-03-20.tar.gz` to share; extract into `data/processed/`
- **Notebooks**: `data_cleaning.ipynb` builds parquets; `eda.ipynb` (§1–5) and `modeling.ipynb` (§6–10) have all analysis and models
- **Run order**: restart kernel, run `eda.ipynb` then `modeling.ipynb`; Section 7.3 must run before 7.4/7.5/8/9; Section 9 is memory-intensive — if kernel crashes at Sec 9.2, run drift scripts standalone in `/tmp/`
- **conda env**: `erdos_ds_environment` — run `conda run -n erdos_ds_environment pip install arch` if missing
- **Train/test split**: Train ≤2024-12-31 | Test 2025 | Hold-out 2026
