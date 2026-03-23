# modeling.ipynb — Component Brief

## Purpose

Train, validate, and evaluate models to predict hourly RTM price volatility (`log_rtm_std`) and detect price spikes (RTM > $100/MWh) at the ERCOT Houston Hub. Sections 6–10 form the modeling continuation from `eda.ipynb`. Best model: XGBoost v3 (30 features including GARCH conditional volatility), R²=0.489, RMSE=0.610 on 2025 test set.

---

## Section Outline

| Section | Cell ID | Description | Key Result |
|---|---|---|---|
| §6 Baseline Models | `bg2hicyxe48` | Ridge (22 feat) + XGBoost v1 (22 feat) + naive baselines | Ridge R²=0.243; XGB v1 R²=0.357; Classifier AUC=0.888 |
| §6.1 Model Evaluation Metrics | (metrics cell) | Regression + classification metrics defined (R², RMSE, AUC, PR-AUC, F1, Brier, BSS) | Reference only |
| §7 Model Improvements | `5qwqlafwv7` | XGBoost v2: 29 features, tuned hyperparams, optimized spike threshold | v2 R²=0.359; AUC=0.905 |
| §7.1 HAR-RV & GARCH Baselines | (harv cell) | HAR (3-lag realized vol) + GARCH as statistical baselines; GARCH inappropriate at 24h horizon | HAR R²=0.110; GARCH R²=−5.3 |
| §7.2 Seasonal OLS + GARCH Residuals | (garch cell) | GARCH(1,1)-t on residual series; walk-forward CV linear models; Lasso feature selection | Ridge CV R²=0.169; IGARCH (α+β=1.0) |
| §7.3 XGBoost v3 — GARCH Vol as Feature | `cdelxch0xcn` | GARCH conditional vol as 30th feature; walk-forward CV v2 vs v3; final model | v3 R²=0.484; +0.130 vs v2 (p<0.001) |
| §7.3.1 Hyperparameter Tuning | (tune cell) | 54-param grid search on 3-fold CV; max_depth=4, lr=0.03 wins CV | CV R²=0.492; test R²=0.477 (overfit) |
| §7.3.2 Lasso Feature Selection — 30 Features | `gq7l7qcrogh` | Lasso identifies 7 robust survivors from 30 features | Survivors: DAM, GARCH, rtm_std_lag48, fc_system_total, total_resource_mw, mcpc_ecrs (pre-2021=0), regup |
| §7.4 Rolling Window Feature Selection | (rolling lasso) | 5 × 4-year Lasso windows (2017–2020 → 2021–2024, step=1yr) | GARCH coeff grows 0.31→0.52; DAM stable ~0.18 |
| §7.5 Post-Uri Training Window Test | (uri window) | Compare 2017–2024 vs 2021–2024 vs 2022–2024 on test 2025 | Post-Uri (2021–2024): R²=0.489; best window |
| §8 Error Analysis & Model Leaderboard | `lsctyepdxx` | MAE by hour/month/spike, regression diagnostics, classification metrics, block bootstrap CI | R² [0.459, 0.508]; spike MAE 1.07 vs non-spike 0.46 |
| §8.1 Regression Diagnostics | `u2zb73mrmz8` | Q-Q plot, residuals vs fitted, predicted vs actual | Slight heteroskedasticity in high-spike regime |
| §8.2 Classification Diagnostics | `sgui53m87g9` | ROC, PR curve, confusion matrix, FPR/FNR by month | Threshold=0.480; F1=0.506 |
| §8.3 Block Bootstrap CI | `6fjl1b32fne` | 2000 reps, block=24h; paired Δ R² v3 vs v2 | Δ R²=+0.130 [+0.106, +0.152], p<0.001 |
| §9. Rolling 24h Forecast & Drift | `s9_1code0001` | Weekly retrain (sliding 4yr); Option A (fixed) vs C (sliding) | Option C beats A 45/53 weeks; mean drift +0.0114 |
| §9.1 Option A — Fixed Model | (opt-a cell) | Fixed XGB v3 (2017–2024), predict all 8,760 test hours; MAE by hour | Peak hours 08–18 hardest (±0.7 log units) |
| §9.2 Option C — Sliding Window | (opt-c cell) | 52 weekly retrains; drift score = MAE_A − MAE_C per week | 45/53 weeks C wins; mean drift +0.0114 |
| §9.3 Formal Drift Tests | (drift cell) | Page-Hinkley CUSUM; KS test per feature per month; JS divergence heatmap | 5 change points in 2025 (Jan, Mar, Apr, Jul, Oct) |
| §10 Ensemble | (ensemble cell) | α·XGB_v3 + (1−α)·Ridge; optimize α on 2024 fold | α=0.94; R²=0.481 (no improvement) |
| Conclusion | (concl cell) | Key results, 8 findings, limitations, next steps | Best: XGB v3 R²=0.484 [0.459, 0.508] |

---

## Model Artifacts

All pkl files saved to `data/processed/ercot/`:

| File | Model | Features | Train | Test R² |
|---|---|---|---|---|
| `model_ridge.pkl` | Ridge (α=1.0, scaled) | 22 | 2017–2024 | 0.243 |
| `model_xgb_reg.pkl` | XGBoost v1 | 22 | 2017–2024 | 0.357 |
| `model_xgb_reg_v2.pkl` | XGBoost v2 | 29 | 2017–2024 | 0.359 |
| `model_xgb_reg_v3.pkl` | XGBoost v3 | 30 | 2017–2024 | 0.484 |
| `model_xgb_reg_v3_best.pkl` | **XGBoost v3 (post-Uri)** | 30 | 2021–2024 | **0.489** |
| `model_xgb_clf.pkl` | XGB Classifier v1 | 22 | 2017–2024 | AUC=0.888 |
| `model_xgb_clf_v2.pkl` | XGB Classifier v2 | 29 | 2017–2024 | AUC=0.905 |
| `model_xgb_clf_v3.pkl` | **XGB Classifier v3** | 30 | 2021–2024 | **AUC=0.969** |

---

## Model Leaderboard

| Model | R² (test 2025) | RMSE | AUC | PR-AUC | F1 |
|---|---|---|---|---|---|
| HAR-OLS | 0.110 | — | — | — | — |
| Ridge v1 | 0.243 | 0.740 | — | — | — |
| XGBoost v1 | 0.357 | 0.684 | 0.888 | — | — |
| XGBoost v2 | 0.359 | 0.683 | 0.905 | — | 0.317 |
| **XGBoost v3 (2021–2024)** | **0.489** | **0.610** | **0.969** | **0.495** | **0.506** |
| XGBoost v3 (full train) | 0.484 | 0.613 | — | — | — |
| Ensemble (0.94·XGB + 0.06·Ridge) | 0.481 | 0.615 | — | — | — |

**Bootstrap 95% CI (block=24h, 2000 iters):** R²=0.484 [0.459, 0.508]; AUC=0.969 [0.961, 0.977]; Δ R² vs v2 = +0.130 [+0.106, +0.152], p<0.001.

---

## Key Findings

1. **DAM price is the strongest predictor** — 26.9% feature importance; day-ahead market price is the best single signal for intra-hour volatility.
2. **GARCH conditional volatility is #2** — 21.5% importance; adds +0.130 R² vs v2 (bootstrap confirmed, p<0.001).
3. **ERCOT exhibits IGARCH dynamics** — α+β=1.0; volatility shocks never decay; consistent with heavy-tailed energy market behaviour.
4. **Spike hours are 2.3× harder to predict** — spike MAE=1.073 vs non-spike MAE=0.458; extreme events driven by unforeseen grid stress.
5. **Post-Uri regime shift** — rolling Lasso shows GARCH coefficient growing (0.31→0.52); pre-Uri features weaken post-2021. Best training window = 2021–2024.
6. **Sustained drift in 2025** — Page-Hinkley detects 5 regime-change points; sliding 4yr window beats fixed model 45/53 weeks.
7. **Grid search overfits with 3 folds** — 54-combo search improved CV R² (+0.008) but worsened test (−0.007); original hyperparams kept.
8. **Ensemble does not improve** — Ridge too weak (r=0.811 error correlation with XGB); optimal α=0.94 collapses to near-pure XGBoost.

---

## Figures (saved to `figures/modeling/`)

`feature_importance.png`, `model_evaluation.png`, `model_v2_evaluation.png`, `model_rf_evaluation.png`, `garch_cond_vol.png`, `lasso_feature_selection.png`, `lasso_feature_selection_v3.png`, `rolling_lasso_heatmap.png`, `error_analysis.png`, `regression_diagnostics.png`, `classification_diagnostics.png`, `bootstrap_ci.png`, `option_a_mae_by_hour.png`, `rolling_forecast_drift.png`, `drift_pagehinkley.png`, `drift_covariate_shift.png`

---

## Pending

- **Presentation notebook** — clean 1-notebook summary for Erdos showcase (high priority).
- **2026 hold-out** — reserved; do not evaluate until model locked.
- **Zone-level extension** — straightforward with same pipeline; different target column.
- **Better ensemble** — would need a second strong model with low error correlation (e.g., different architecture).
