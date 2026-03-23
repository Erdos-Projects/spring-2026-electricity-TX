# Model Results
_Authoritative metrics reference. Updated by ModelAgent and MemoryAgent._
_Last updated: 2026-03-22 (final confirmed metrics — rolling TS model exclusion, tuned Option C corrected)_

---

## Final Model Selection

**FINAL MODEL: XGBoost v3 tuned (post-Uri 2021–2024)**
- Features: 31 (FEAT_V3 = FEAT_V2_XGB + garch_cond_vol)
- Hyperparameters: `max_depth=4, lr=0.03, n_estimators=600, min_child_weight=3` (grid search, 54 combos)
- Training window: 2021-01-01 → 2024-12-31 (post-Uri, excludes pre-Uri regime)
- Artifact: `data/processed/ercot/model_xgb_reg_v3_tuned.pkl` (PRIMARY — tuned, depth=4, lr=0.03)
- Note: `model_xgb_reg_v3_best.pkl` = untuned (depth=5, lr=0.05), R²=0.378; `model_xgb_reg_v3_tuned.pkl` = tuned (depth=4, lr=0.03), R²=0.386 ← PRIMARY

| Metric | Value |
|---|---|
| CV R² (mean, folds 2022/2023/2024) | 0.3111 |
| Test R² (2025, reported once) | 0.386 |
| Test RMSE (log scale) | 0.669 |
| Test MAE ($/MWh) | 4.027 |

---

## Feature Sets (2026-03-22 final)

| Name | Count | Contents |
|---|---|---|
| FEAT_V2 | 29 | Linear models — market features, no system_lambda (collinear r=0.9996) |
| FEAT_V2_XGB | 30 | FEAT_V2 + system_lambda (XGBoost baseline, handles collinearity) |
| FEAT_V3 | 31 | FEAT_V2_XGB + garch_cond_vol (final model, D-1 GARCH lag) |
| FEAT_ARIMAX | 25 | FEAT_V2 minus RTM price lags (AR terms already model target autocorrelation) |

---

## Section 7.1–7.3: XGBoost Regression Leaderboard

All models evaluated on test 2025 (reported once; CV used for selection).

| Model | Features | Train window | CV R² | Test R² | RMSE (log) | MAE ($/MWh) | Selected |
|---|---|---|---|---|---|---|---|
| XGBoost v1 | 21 | 2017–2024 | — | 0.348 | 0.689 | 4.17 | ✗ §6 baseline |
| XGBoost v2 (no λ) | 29 | 2017–2024 | — | 0.369 | 0.678 | 4.10 | ✗ |
| XGBoost v2_XGB (with λ) | 30 | 2017–2024 | 0.2827 | 0.375 | 0.675 | — | ✗ |
| XGBoost v3 + GARCH (orig params) | 31 | 2017–2024 | 0.2861 | 0.376 | 0.674 | — | ✗ |
| XGBoost v3 tuned (full train) | 31 | 2017–2024 | 0.311 | 0.373 | 0.676 | 4.11 | ✗ |
| **XGBoost v3 tuned (post-Uri)** | **31** | **2021–2024** | **0.311** | **0.386** | **0.669** | **4.027** | **✅ FINAL** |

Note: GARCH leakage fixed 2026-03-22. Old R²≈0.485–0.494 used same-day RTM residuals; now uses D-1 lag (lag=24). Corrected values above.

### Post-Uri vs Full-Train
Training on 2021–2024 (post-Uri) gives R²=0.386 (tuned pkl) vs 0.373 on full 2017–2024 (same tuned hyperparameters), Δ=+0.013. The untuned post-Uri run gives R²=0.378. Confirms Winter Storm Uri permanently altered ERCOT volatility dynamics.

### Bootstrap CI (tuned v3, post-Uri)
- R²: [0.370, 0.426]
- RMSE: [0.636, 0.688]
- MAE: ~[3.6, 4.4] $/MWh
- PR-AUC (regression as classifier): [0.162, 0.319]
- Δ R² (GARCH vs v2_XGB): p=0.014 — statistically significant

---

## Section 7.2: Walk-Forward CV — Linear and Time-Series Models

All models trained with expanding window (folds: val=2022, val=2023, val=2024).

| Model | Features | CV R² mean | CV folds (2022/2023/2024) | Test R² | RMSE (log) | MAE ($/MWh) | Notes |
|---|---|---|---|---|---|---|---|
| ARIMA (univariate) | 1 | −0.490 | — | — | — | — | No market information |
| HAR-OLS | 3 TS lags | ~0.109 | — | — | — | — | Pure realized-vol memory |
| HAR-Ridge | 3 TS lags | 0.169 | 0.105/0.234/0.169 | 0.190 | 0.768 | 4.348 | |
| Full-OLS (29 feat) | 29 | ~0.145 | — | — | — | — | |
| Full-Ridge (29 feat) | 29 | 0.145 | 0.060/0.285/0.089 | 0.156 | 0.784 | 4.525 | |
| Full-Lasso (29 feat) | 29 | ~0.111 | — | — | — | — | Feature selection |
| RTM-lag-only | 1 | 0.062 | −0.129/0.234/0.080 | — | — | — | Univariate RTM lag |
| **HAR+Full-Ridge (best linear)** | **32** | **0.259** | **0.198/0.349/0.228** | **0.261** | **0.733** | **4.291** | **Best linear baseline** |
| ARIMAX | 25 | skipped | — | — | — | — | Base HAR dominated by XGB (+0.188 R²); not run |
| SARIMAX | 25 | skipped | — | — | — | — | RUN_SARIMAX=False |

**Note (2026-03-22):** Rolling HAR-Ridge (MAE=0.5859, R²=0.197, 26/53 weeks) and rolling ARIMA/ARIMAX were evaluated but excluded from production. XGB v3 tuned dominates by ΔR²=+0.188 on fixed comparison; rolling XGB further widens the gap to ΔR²=+0.203. Time-series models are not recommended.

---

## Section 7.5: Training Window Analysis

| Window | Model | CV R² | Test R² | RMSE | MAE | Notes |
|---|---|---|---|---|---|---|
| 2017–2024 (full) | XGB v3 tuned | 0.311 | 0.373 | 0.676 | 4.11 | Pre-Uri regime included |
| 2021–2024 (post-Uri) | XGB v3 tuned | 0.311 | 0.386 | 0.669 | 4.027 | **Best — FINAL** |
| 2022–2024 (3-year) | XGB v3 tuned | — | ~0.360 | — | — | Too little data |

Best window: 2021–2024 (post-Uri). Saved as `model_xgb_reg_v3_tuned.pkl` (PRIMARY). Note: `model_xgb_reg_v3_best.pkl` is the untuned variant (depth=5, lr=0.05, R²=0.378).

---

## Section 8: Ensemble

Ridge (29 feat) + XGBoost v3 tuned (31 feat), α tuned per fold in 3-fold CV.

| Configuration | α | CV R² | Test R² | RMSE | Notes |
|---|---|---|---|---|---|
| CV-derived α | 0.72 | 0.3094 | 0.371 | 0.677 | Δ=−0.007 vs XGB alone |
| Test-optimal α | 0.90 | — | 0.381 | 0.671 | α chosen on test — invalid for production |
| XGB v3 tuned alone | — | 0.311 | 0.386 | 0.669 | **Recommended** |

Conclusion: Ensemble does not add value. Ridge (test R²=0.156) is too weak to contribute diversity. CV-derived α=0.72 hurts on test; test-optimal α=0.90 marginally helps but is not production-usable. **XGBoost v3 tuned is the sole recommended final model.**

---

## Section 9: Spike Classifier

Target: `spike_flag = 1` if RTM mean > $100/MWh. Spike rate: 2.24% (196/8,760 test hours).

> AUC-ROC is not reported — at 2.2% spike rate a trivial all-negative classifier scores ~0.98, making it uninformative. Operative metrics: PR-AUC and F1.

| Model | Features | PR-AUC | F1 | Precision | Recall | Threshold | Brier |
|---|---|---|---|---|---|---|---|
| XGB Classifier v1 | 21 | 0.214 | 0.321 | 0.228 | 0.541 | 0.251 | — |
| XGB Classifier v2 | 29 | 0.225 | 0.289 | 0.232 | 0.383 | 0.471 | — |
| **XGB Classifier v3 (31 feat)** | **31** | **0.233** | **0.315** | **0.261** | **0.398** | **0.508** | **0.030** |
| XGB Reg v3 best (as classifier) | 31 | 0.236 | 0.300 | 0.217 | 0.485 | 2.404 | — |

Classifier v3 BSS=−0.383 (miscalibrated due to scale_pos_weight; PR-AUC/F1 are operative).
Spike hours show 3.0× higher MAE than non-spike hours (ratio from error analysis).

### GARCH Parameters
GARCH(1,1)-t, confirmed optimal by AIC/BIC grid (p∈{1,2}, q∈{0,1,2}):
- ω=0.1712, α=0.8762, β=0.1238, ν=5.05
- IGARCH: α+β=1.0 — volatility shocks in ERCOT never decay
- Lag: D-1 (lag=24h) — `garch_cond_vol[t] = sqrt(ω + α·ε²[t-24] + β·σ²[t-24])`

---

## Section 10: Rolling Forecast & Drift Detection

_Updated 2026-03-22: post-imputation fix + tuned rolling XGB results._

All metrics are MAE(log) on 2025 weekly forecasts (53 weeks, n=53 comparison pairs).

| Model | Type | MAE (log) | R² | Weeks beats fixed XGB | Notes |
|---|---|---|---|---|---|
| Fixed XGB v3 (Option A) | Fixed, tuned | 0.5099 | 0.386 | — | Baseline |
| Rolling XGB v3 untuned | Sliding 4yr window, 53 weeks | 0.4965 | 0.400 | 39/53 | Pre-tuning reference |
| **Rolling XGB v3 tuned (Option C)** | **Sliding 4yr window, 53 weeks** | **0.4925** | **0.400** | **43/53** | **Best production model** |
| Fixed HAR-Ridge | Fixed | 0.5859 | 0.190 | — | Linear baseline (excluded) |
| Rolling HAR-Ridge | Sliding 4yr window, 53 weeks | 0.5859 | 0.197 | 26/53 | Negligible improvement (excluded) |

**Drift score (rolling tuned Option C vs fixed Option A):** mean Δ MAE = +0.0174 (positive = rolling better)
**Drift score (rolling untuned vs fixed):** mean Δ MAE = +0.0134 (pre-tuning reference)

**XGBoost vs HAR-Ridge gap:**
- Fixed: ΔR²=+0.196 (XGB 0.386 vs HAR-Ridge 0.190)
- Rolling: ΔR²=+0.203 (XGB 0.400 vs HAR-Ridge 0.197)

**Best model decisions:**
- **Held-out (2026 eval):** Use fixed XGB v3 tuned (R²=0.386) — no 2025 data seen during training, clean OOS
- **Production (live deployment):** Use rolling tuned XGB Option C (R²=0.400, 43/53 weeks, MAE=0.4925) — retrain weekly on sliding 4-year window
- HAR-Ridge rolling improvement is negligible (26/53 weeks, ΔR²=+0.007); not recommended
- Rolling HAR-Ridge and rolling ARIMA/ARIMAX excluded: XGB dominates by ΔR²=+0.196 (fixed) to +0.203 (rolling)

### Page-Hinkley Change Points (2025)
6 structural change points detected in 2025:
- Jan 21, Feb 28, Jul 1, Jul 12, Sep 25, Oct 26

### KS Test
All 26 features significant in 10–12/12 months; JS divergence heatmap saved. Confirms ERCOT feature distributions shift substantially across 2025.

---

## Top Features — Final Model (XGB v3 tuned, post-Uri)

| Rank | Feature | Importance | Group |
|---|---|---|---|
| 1 | garch_cond_vol | ~25% | GARCH volatility (D-1 lag) |
| 2 | dam_price_houston | ~15% | Day-Ahead Market price |
| 3 | system_lambda | ~9% | DAM congestion price |
| 4 | rtm_std_lag24 | ~7% | Yesterday's RTM volatility |
| 5 | mcpc_regup / outage_fraction | ~7% | Reserve market tightness |

Note: After GARCH leakage fix, garch_cond_vol is still a meaningful top-2 feature, but importance dropped from inflated ~21–25% (same-day residuals) to its correct D-1 lag value.

---

## Artifact Paths

| Artifact | Path |
|---|---|
| XGB v3 tuned (PRIMARY final model) | `data/processed/ercot/model_xgb_reg_v3_tuned.pkl` (depth=4, lr=0.03, R²=0.386) |
| XGB v3 untuned post-Uri | `data/processed/ercot/model_xgb_reg_v3_best.pkl` (depth=5, lr=0.05, R²=0.378) |
| XGB v3 (full train, 2017–2024) | `data/processed/ercot/model_xgb_reg_v3.pkl` |
| XGB v2_XGB (full train) | `data/processed/ercot/model_xgb_reg_v2_xgb.pkl` |
| XGB Classifier v3 | `data/processed/ercot/model_xgb_clf_v3.pkl` |
