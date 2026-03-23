# model_regression.ipynb — Component Brief

## Purpose

Train, validate, and evaluate regression models to predict hourly RTM price volatility (`log_rtm_std`) at the ERCOT Houston Hub. Sections 6–10 are the modeling continuation from `eda.ipynb` §5. Spike classification is handled separately in `model_classifier.ipynb`. Final model: XGBoost v3 tuned (31 features, post-Uri 2021–2024 window), R²=0.386, RMSE=0.669, MAE=4.027 $/MWh on 2025 test set.

---

## Section Outline

| Section | Description | Key Result |
|---|---|---|
| §6 Baseline Models | HAR-Ridge (3 HAR lags), Full-Ridge (29 feat), HAR+Full-Ridge (32 feat), XGBoost v1 (21 feat) | HAR+Ridge CV R²=0.259; XGB v1 test R²=0.348 |
| §6.1 Model Evaluation Metrics | R², RMSE (log), MAE ($/MWh) defined; walk-forward CV setup (val 2022/2023/2024) | Reference only |
| §7 Model Improvements | XGBoost v2 (29 feat), GARCH conditional vol as feature, v3 walk-forward CV | v2 test R²=0.369; v3 CV R²=0.286 |
| §7.1 HAR-RV & GARCH Baselines | HAR (3-lag realized vol) + GARCH(1,1)-t; GARCH inappropriate at 24h horizon | HAR R²=0.190; GARCH residual fit only |
| §7.2 Seasonal OLS + GARCH Residuals | GARCH(1,1)-t on residual series; walk-forward linear models with ARIMAX/SARIMAX | Ridge CV R²=0.169; IGARCH (α+β≈1.0) |
| §7.3 XGBoost v3 — GARCH Vol as Feature | GARCH conditional vol (lag=24) as 31st feature; walk-forward CV v3; hyperparameter tuning | v3 orig CV R²=0.286, test R²=0.372 |
| §7.3.1 Hyperparameter Tuning | Grid search (max_depth, lr, n_estimators, subsample); CV winner: depth=4, lr=0.03 | CV R²=0.311; tuned test R²=0.385 |
| §7.3.2 Lasso Feature Selection | Lasso identifies robust survivors; rolling 5-window Lasso stability analysis | GARCH coeff grows 0.31→0.52 post-Uri |
| §7.4 Post-Uri Training Window | Compare 2017–2024 vs 2021–2024 train windows on 2025 holdout | 2021–2024 test R²=0.386 vs 0.385 (full) |
| §8 Model Leaderboard | Summary table: HAR-Ridge through XGB v3 tuned (post-Uri) | **FINAL: R²=0.386, RMSE=0.669, MAE=4.027** |
| §9 Error Analysis | MAE by hour/month/spike; regression diagnostics; block bootstrap CI; feature importance | Spike MAE ~3× non-spike; GARCH ~25%, DAM ~15% |
| §9.1 Regression Diagnostics | Q-Q plot, residuals vs fitted, predicted vs actual | Moderate heteroskedasticity in high-spike regime |
| §9.2 Block Bootstrap CI | 2000 reps, block=24h; 95% CI on test R²; paired Δ vs linear baseline | R² CI: [0.336, 0.436]; Δ vs HAR+Ridge significant |
| §10 Rolling Forecast & Drift | Weekly retrain (sliding 4yr window) vs fixed model; drift detection | Sliding beats fixed 43/53 weeks; MAE 0.4925 vs 0.5069 |
| §10.1 Rolling Forecast | 53 weekly retrains on 2025; sliding-window Option C vs fixed Option A | Median weekly MAE improvement: +0.015 |
| §10.2 Drift Detection | Page-Hinkley CUSUM; KS covariate shift; JS divergence heatmap | 5 change points detected (Jan, Mar, Apr, Jul, Oct 2025) |

---

## Model Artifacts

Regression pkl files saved to `data/processed/ercot/`:

| File | Model | Features | Train Window | Test R² | RMSE | MAE $/MWh |
|---|---|---|---|---|---|---|
| `model_ridge.pkl` | HAR+Full Ridge (32 feat) | 32 | 2017–2024 | 0.261 | 0.733 | 4.291 |
| `model_xgb_reg.pkl` | XGBoost v1 | 21 | 2017–2024 | 0.348 | 0.689 | 4.173 |
| `model_xgb_reg_v2.pkl` | XGBoost v2 | 29 | 2017–2024 | 0.369 | 0.678 | 4.101 |
| `model_xgb_reg_v3.pkl` | XGBoost v3 (orig params) | 31 | 2017–2024 | 0.372 | 0.676 | 4.102 |
| `model_xgb_reg_v3_best.pkl` | XGBoost v3 (orig params, post-Uri) | 31 | 2021–2024 | 0.378 | 0.673 | 4.09 |
| **`model_xgb_reg_v3_tuned.pkl`** | **XGBoost v3 tuned (post-Uri) ← PRIMARY** | **31** | **2021–2024** | **0.386** | **0.669** | **4.027** |

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
| **XGBoost v3 tuned (post-Uri)** | **31** | **2021–2024** | **0.311** | **0.386** | **0.669** | **4.027** |

**Rolling forecast (2025, 53 weeks):** Sliding 4-year window beats fixed in 43/53 weeks (MAE 0.4925 vs 0.5069).

---

## Key Findings

1. **DAM price is the strongest predictor** — ~15% feature importance; day-ahead market price is the best single signal for intra-hour volatility.
2. **GARCH conditional volatility is #1** — ~25% importance; yesterday's conditional vol (lag=24) captures volatility clustering and is the top feature.
3. **ERCOT exhibits IGARCH dynamics** — α+β≈1.0; volatility shocks never decay; consistent with heavy-tailed energy market behaviour.
4. **Post-Uri regime shift** — rolling Lasso shows GARCH coefficient growing (0.31→0.52) post-2021. Best training window = 2021–2024.
5. **Hyperparameter tuning improves CV but not test** — grid search improved CV R² by +0.025 on full-train, +0.000 on post-Uri (same winner), but test improvement is marginal (+0.001). Depth=4, lr=0.03 selected.
6. **Sustained drift in 2025** — Page-Hinkley detects 5 regime-change points; sliding 4yr window beats fixed model 43/53 weeks.
7. **Linear ensemble does not improve** — α=0.72 gives R²=0.364, MAE=4.082 (worse than tuned XGB alone). XGB v3 tuned is sole final model.

---

## Figures (saved to `figures/modeling/`)

`feature_importance.png`, `garch_cond_vol.png`, `lasso_feature_selection.png`, `rolling_lasso_heatmap.png`, `error_analysis.png`, `regression_diagnostics.png`, `bootstrap_ci.png`, `rolling_forecast_drift.png`, `drift_pagehinkley.png`, `drift_covariate_shift.png`

---

## Pending

- **2026 hold-out** — reserved; do not evaluate until model locked.
- **Zone-level extension** — straightforward with same pipeline; different target column.
- **Better ensemble** — would need a second strong model with low error correlation (e.g., different architecture).
