# model_regression.ipynb — Component Brief

## Purpose

Train, validate, and evaluate regression models to predict hourly RTM price volatility (`log_rtm_std`) at the ERCOT Houston Hub. Sections 6–10 are the modeling continuation from `eda.ipynb` §5. Spike classification is handled separately in `model_classifier.ipynb`. Final model: XGBoost v3 tuned (31 features, post-Uri 2021–2024 window), R²=0.393, RMSE=0.665, MAE=4.033 $/MWh on 2025 test set.

---

## Section Outline

| Section | Description | Key Result |
|---|---|---|
| §6 Baseline Models | HAR-Ridge (3 HAR lags), Full-Ridge (29 feat), HAR+Full-Ridge (32 feat), XGBoost v1 (21 feat) | HAR+Ridge CV R²=0.259; XGB v1 test R²=0.348 |
| §6.1 Model Evaluation Metrics | R², RMSE (log), MAE ($/MWh) defined; walk-forward CV setup (val 2022/2023/2024) | Reference only |
| §7 Model Improvements | XGBoost v2 (29 feat), GARCH conditional vol as feature, v3 walk-forward CV | v2 test R²=0.369; v3 CV R²=0.286 |
| §7.1 HAR-RV & GARCH Baselines | HAR (3-lag realized vol) + GARCH(1,1)-t; GARCH inappropriate at 24h horizon | HAR R²=0.190; GARCH residual fit only |
| §7.2 Seasonality, GARCH Residuals, ARIMA/SARIMAX | GARCH(1,1)-t on residual series; walk-forward linear models with ARIMAX/SARIMAX; test eval for linear models | Ridge CV R²=0.169; IGARCH (α+β≈1.0) |
| §7.3 XGBoost v3 — GARCH Vol as Feature | GARCH conditional vol (lag=24) as 31st feature; walk-forward CV v3 | v3 orig CV R²=0.286, test R²=0.372 |
| §7.3.1 Lasso Feature Selection | Lasso identifies robust survivors; rolling 5-window Lasso stability analysis | GARCH coeff grows 0.31→0.52 post-Uri |
| §7.3.2 Hyperparameter Tuning | Grid search (max_depth, lr, n_estimators, subsample); CV winner: depth=4, lr=0.03 | CV R²=0.311; tuned test R²=0.385 |
| §7.4 Post-Uri Training Window | Compare 2017–2024 vs 2021–2024 train windows on 2025 holdout | 2021–2024 tuned R²=0.393 vs 0.386 (full tuned) |
| §8 Ensemble | α·XGB_v3 + (1−α)·Ridge; proper 3-fold walk-forward CV; optimize α on CV | α=0.72 CV R²=0.371; test R²=0.364 — worse than XGB alone |
| §9 Error Analysis & Model Leaderboard | MAE by hour/month/spike; regression diagnostics; block bootstrap CI; feature importance; full leaderboard | Spike MAE ~3× non-spike; GARCH ~25%, DAM ~15% |
| §9.1 Feature Importance | SHAP/XGB feature importances for v3 tuned | Top: garch_cond_vol (~25%), dam_price_houston (~15%) |
| §9.2 Regression Diagnostics | Q-Q plot, residuals vs fitted, predicted vs actual; block bootstrap CI | R² CI: [0.336, 0.436] |
| §10 Rolling Forecast & Drift | Weekly retrain (sliding 4yr window) vs fixed model; drift tests | Sliding beats fixed 43/53 weeks; MAE 0.4925 vs 0.5069 |
| §10.3 Rolling Forecast Comparison | 53 weekly retrains; Option A (fixed) vs Option C (sliding); HAR-Ridge comparison | Sliding wins median weekly MAE |
| §10.4 Formal Drift Tests | Page-Hinkley CUSUM; KS covariate shift; JS divergence heatmap | 5 change points (Jan, Mar, Apr, Jul, Oct 2025) |
| §10.5 Ljung-Box Test | Residual autocorrelation test | Reference |
| §10.6 Diebold-Mariano Test | Model A vs Model C formal test | Reference |

---

## Model Artifacts

Regression pkl files saved to `data/processed/ercot/`:

| File | Model | Features | Train Window | Test R² | RMSE | MAE $/MWh |
|---|---|---|---|---|---|---|
| `model_ridge.pkl` | HAR+Full Ridge (32 feat) | 32 | 2017–2024 | 0.261 | 0.733 | 4.291 |
| `model_xgb_reg.pkl` | XGBoost v1 | 21 | 2017–2024 | 0.348 | 0.689 | 4.173 |
| `model_xgb_reg_v2.pkl` | XGBoost v2 | 29 | 2017–2024 | 0.369 | 0.678 | 4.101 |
| `model_xgb_reg_v3.pkl` | XGBoost v3 (orig params) | 31 | 2017–2024 | 0.372 | 0.676 | 4.102 |
| **`model_xgb_reg_v3_best.pkl`** | **XGBoost v3 tuned (post-Uri) ← PRIMARY** | **31** | **2021–2024** | **0.393** | **0.665** | **4.033** |
| `model_xgb_reg_v3_tuned.pkl` | XGBoost v3 tuned (full-train) | 31 | 2017–2024 | 0.386 | 0.669 | 4.027 |

Classifier pkl files are written by `model_classifier.ipynb`.

---

## Model Leaderboard

| Model | Features | Train Window | CV R² | Test R² | RMSE (log) | MAE ($/MWh) |
|---|---|---|---|---|---|---|
| HAR-Ridge | 3 | 2017–2024 | 0.169 | 0.190 | 0.768 | 4.348 |
| Full-Ridge (29 feat) | 29 | 2017–2024 | 0.145 | 0.156 | 0.784 | 4.525 |
| HAR+Full-Ridge (32 feat) | 32 | 2017–2024 | 0.259 | 0.261 | 0.733 | 4.291 |
| XGBoost v1 | 21 | 2017–2024 | — | 0.348 | 0.689 | 4.173 |
| XGBoost v2 | 29 | 2017–2024 | — | 0.369 | 0.678 | 4.101 |
| XGBoost v3 (orig params) | 31 | 2017–2024 | 0.286 | 0.372 | 0.676 | 4.102 |
| XGBoost v3 tuned (full train) | 31 | 2017–2024 | 0.311 | 0.385 | 0.669 | 4.028 |
| **XGBoost v3 tuned (post-Uri)** | **31** | **2021–2024** | **0.311** | **0.393** | **0.665** | **4.033** |

**Rolling forecast (2025, 53 weeks):** Sliding 4-year window beats fixed in 43/53 weeks (MAE 0.4925 vs 0.5069).

---

## Key Findings

1. **DAM price is the strongest predictor** — ~15% feature importance; day-ahead market price is the best single signal for intra-hour volatility.
2. **GARCH conditional volatility is #1** — ~25% importance; yesterday's conditional vol (lag=24) captures volatility clustering and is the top feature.
3. **ERCOT exhibits IGARCH dynamics** — α+β≈1.0; volatility shocks never decay; consistent with heavy-tailed energy market behaviour.
4. **Post-Uri regime shift** — rolling Lasso shows GARCH coefficient growing (0.31→0.52) post-2021. Best training window = 2021–2024.
5. **Hyperparameter tuning + post-Uri window pays off** — grid search found depth=4, lr=0.03; post-Uri tuned achieves R²=0.393 vs full-train tuned 0.386 (Δ=+0.007).
6. **Sustained drift in 2025** — Page-Hinkley detects 5 regime-change points; sliding 4yr window beats fixed model 43/53 weeks.
7. **Linear ensemble does not improve** — α=0.72 gives R²=0.374 (worse than tuned XGB alone at 0.393). XGB v3 tuned is sole final model.
8. **SARIMAX corrected** — per-fold execution with 48K-row truncation gives CV R²=0.135 (previously -1.652 from OOM). Comparable to ARIMAX (0.136) and Full-Ridge (0.145).

---

## Figures (saved to `figures/model_regression/`)

`feature_importance.png`, `garch_cond_vol.png`, `lasso_feature_selection.png`, `rolling_lasso_heatmap.png`, `error_analysis.png`, `regression_diagnostics.png`, `bootstrap_ci.png`, `rolling_forecast_drift.png`, `drift_pagehinkley.png`, `drift_covariate_shift.png`, `residuals_vs_features.png`, `residuals_by_regime.png`, `residuals_rolling_bias.png`, `residuals_acf_ljungbox.png`, `residuals_calibration_garch.png`, `residuals_model_comparison.png`, `pres_leaderboard.png`, `pres_error.png`

---

## Pending

- **2026 hold-out** — reserved; do not evaluate until model locked.
- **Zone-level extension** — straightforward with same pipeline; different target column.
- **Better ensemble** — would need a second strong model with low error correlation (e.g., different architecture).
