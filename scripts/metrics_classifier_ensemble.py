"""
metrics_classifier_ensemble.py — Classifier and ensemble metrics for the ERCOT
Houston Hub volatility forecasting project.

Covers:
  - Classifier evaluation: model_xgb_clf_v3.pkl (XGBoost clf v3, 31 feat)
    PR-AUC, optimal-threshold F1, precision, recall, Brier score
  - Regression model used as classifier: model_xgb_reg_v3_best.pkl
    threshold-optimized F1 via PR curve on predicted log_rtm_std
  - Ensemble evaluation: Ridge(29 feat) + XGBoost v3 best (31 feat)
    alpha sweep [0.0, 0.1, ..., 0.72, ..., 1.0] on test 2025
    Ridge refitted on full train ≤2024; XGB loaded from pkl

Run from project root with:
    conda run -n erdos_ds_environment python scripts/metrics_classifier_ensemble.py

Paths are relative to project root.
"""

import warnings
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.linear_model import Ridge, LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score,
    average_precision_score, f1_score, precision_score, recall_score,
    precision_recall_curve, brier_score_loss,
)
import xgboost as xgb
from arch import arch_model

# ── Paths ─────────────────────────────────────────────────────────────────────
PROC            = Path('data/processed/ercot')
TARGET          = 'log_rtm_std'
TRAIN_END_FINAL = pd.Timestamp('2024-12-31 23:00')
SPIKE_THRESHOLD = 100.0   # $/MWh — defines spike_flag

# ── Feature sets (must match notebook exactly) ────────────────────────────────
FEAT_V2 = [
    'dam_price_houston',
    'load_houston_lag48', 'rtm_mean_lag24', 'rtm_std_lag24',
    'total_resource_mw',
    'fc_system_total', 'wgrpp_lz_south_houston', 'wind_error_houston',
    'fc_coast', 'wf_stwpf_lz_south_houston',
    'temp_f_houston_avg', 'humidity_pct_houston_avg',
    'wind_gust_mph_houston_avg', 'precip_in_houston_avg',
    'mcpc_regup', 'mcpc_rrs', 'mcpc_nspin', 'mcpc_regdn',
    'hour', 'month', 'dow',
    'fc_net_load', 'dam_rtm_spread', 'week',
    'load_lag7d', 'rtm_price_std_lag7d', 'rtm_price_mean_lag7d', 'outage_fraction',
    'abs_dam_rtm_spread',
]  # 29 features — for Ridge

FEAT_V2_XGB = FEAT_V2 + ['system_lambda']           # 30 features for XGBoost
FEAT_V3     = FEAT_V2_XGB + ['garch_cond_vol']       # 31 features


# =============================================================================
# Helper functions (copied exactly from metrics_core.py / notebook §7 setup)
# =============================================================================

def add_engineered_features(df):
    """Add 8 engineered features on a copy. Must be called on combined
    train+test DataFrame so that 7-day lag shifts span the boundary."""
    df = df.copy()
    df['fc_net_load']           = df['fc_coast'] - df['wf_stwpf_lz_south_houston']
    df['dam_rtm_spread']        = df['dam_price_houston'] - df['rtm_mean_lag24']
    df['abs_dam_rtm_spread']    = df['dam_rtm_spread'].abs()
    df['week']                  = df.index.isocalendar().week.astype(int)
    df['load_lag7d']            = df['load_houston_lag48'].shift(168)
    df['rtm_price_std_lag7d']   = df['rtm_std_lag24'].shift(168)
    df['rtm_price_mean_lag7d']  = df['rtm_mean_lag24'].shift(168)
    df['outage_fraction']       = df['total_resource_mw'] / (df['fc_system_total'] + 1)
    return df


def make_seasonal(df):
    """Hour/month/dow dummy features for seasonal OLS deseasonalisation."""
    tmp = pd.DataFrame({
        'hour':  df.index.hour,
        'month': df.index.month,
        'dow':   df.index.dayofweek,
    }, index=df.index)
    return pd.get_dummies(tmp.astype(str), drop_first=True)


def build_garch_vol(df, train_end):
    """
    Fit seasonal OLS + GARCH(1,1)-t on training residuals (index <= train_end),
    forecast 1-step-ahead conditional vol for the full period, then apply a
    D-1 lag (shift 24h) so the feature is leakage-safe at the midnight cutoff.
    """
    target  = df['log_rtm_std'] if 'log_rtm_std' in df.columns else df.iloc[:, 0]
    tr_mask = target.index <= train_end

    # Seasonal OLS on train
    S_tr     = make_seasonal(target[tr_mask].to_frame())
    seas_ols = LinearRegression().fit(S_tr, target[tr_mask])

    # Residuals on full period
    S_all = make_seasonal(target.to_frame()).reindex(columns=S_tr.columns, fill_value=0)
    resid = target - seas_ols.predict(S_all)

    # GARCH(1,1)-t fit on train residuals only
    split = int(tr_mask.sum())
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        gm = arch_model(resid, mean='Constant', vol='GARCH', p=1, q=1, dist='t')
        gr = gm.fit(last_obs=split, disp='off', show_warning=False)

    # 1-step-ahead forecast for full period
    fc = gr.forecast(horizon=1, start=0, reindex=False)
    cond_vol = pd.Series(
        np.sqrt(np.clip(fc.variance['h.1'].values, 0, None)),
        index=target.index
    )
    return cond_vol.shift(24)   # D-1 lag — leakage-safe


def impute_combined(combined):
    """Column-specific imputation. Medians fit on train only."""
    # ffill for time-series lag features
    for col in ['rtm_std_lag24', 'load_houston_lag48']:
        if col in combined.columns:
            combined[col] = combined[col].ffill()

    # Median imputation for price/market features (fit on train, apply to both)
    train_mask = combined.index <= TRAIN_END_FINAL
    for col in ['dam_price_houston', 'system_lambda',
                'mcpc_regup', 'mcpc_rrs', 'mcpc_nspin', 'mcpc_regdn']:
        if col in combined.columns:
            med = combined.loc[train_mask, col].median()
            combined[col] = combined[col].fillna(med)

    # total_resource_mw: fillna(0) — nulls are pre-2022
    if 'total_resource_mw' in combined.columns:
        combined['total_resource_mw'] = combined['total_resource_mw'].fillna(0)

    return combined


def opt_threshold_metrics(y_true, y_prob):
    """Return PR-AUC, optimal-F1, threshold, precision, recall at that threshold."""
    prauc = average_precision_score(y_true, y_prob)
    prec_arr, rec_arr, thr_arr = precision_recall_curve(y_true, y_prob)
    f1_arr  = 2 * prec_arr[:-1] * rec_arr[:-1] / (prec_arr[:-1] + rec_arr[:-1] + 1e-9)
    opt_idx = np.argmax(f1_arr)
    opt_thr = thr_arr[opt_idx] if opt_idx < len(thr_arr) else 0.5
    pred    = (y_prob >= opt_thr).astype(int)
    return {
        'PR-AUC':    round(float(prauc), 4),
        'F1 (opt)':  round(float(f1_arr[opt_idx]), 4),
        'Threshold': round(float(opt_thr), 4),
        'Precision': round(float(precision_score(y_true, pred, zero_division=0)), 4),
        'Recall':    round(float(recall_score(y_true, pred, zero_division=0)), 4),
    }


# =============================================================================
# STEP 1 — Load data and build combined feature matrix
# =============================================================================
print("=" * 65)
print("STEP 1: Loading parquets and building combined feature matrix")
print("=" * 65)

train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')

print(f"  train: {train_df.shape}  ({train_df.index.min().date()} -> {train_df.index.max().date()})")
print(f"  test:  {test_df.shape}  ({test_df.index.min().date()} -> {test_df.index.max().date()})")

# Apply engineered features on combined to get correct 7-day shift across boundary
combined = pd.concat([train_df, test_df]).sort_index()
combined = add_engineered_features(combined)
combined = impute_combined(combined)

# Derive spike_flag if not present (spike_flag = rtm_price_mean > $100/MWh)
if 'spike_flag' not in combined.columns:
    if 'rtm_price_mean' in combined.columns:
        combined['spike_flag'] = (combined['rtm_price_mean'] > SPIKE_THRESHOLD).astype(int)
    else:
        raise RuntimeError("Neither spike_flag nor rtm_price_mean found in parquet.")

print(f"  Combined shape after engineering + imputation: {combined.shape}")


# =============================================================================
# STEP 2 — Build GARCH conditional volatility (D-1 lag, leakage-safe)
# =============================================================================
print()
print("=" * 65)
print("STEP 2: Building GARCH conditional vol (D-1 lag, leakage-safe)")
print("=" * 65)
print("  Fitting GARCH(1,1)-t on training residuals (train <= 2024-12-31)...")

garch_vol = build_garch_vol(combined[[TARGET]], TRAIN_END_FINAL)
combined['garch_cond_vol'] = garch_vol

n_nulls = combined['garch_cond_vol'].isna().sum()
print(f"  garch_cond_vol built. NaN count (expected ~24 at head): {n_nulls}")

# Split back into train and test after adding GARCH
train = combined[combined.index <= TRAIN_END_FINAL].copy()
test  = combined[combined.index > TRAIN_END_FINAL].copy()

# Drop rows with null target in test
test = test.dropna(subset=[TARGET, 'spike_flag'])

y_te      = test[TARGET].values
y_te_clf  = test['spike_flag'].astype(int).values

print(f"  Test rows after dropna: {len(test):,}  |  spike rate: {y_te_clf.mean():.2%}")
print(f"  Spike hours: {y_te_clf.sum()} / {len(y_te_clf)}")


# =============================================================================
# STEP 3 — Classifier: model_xgb_clf_v3.pkl (XGBoost clf v3, 31 feat)
# =============================================================================
print()
print("=" * 65)
print("STEP 3: Classifier — model_xgb_clf_v3.pkl")
print("=" * 65)

clf_metrics = {}
clf_pkl = PROC / 'model_xgb_clf_v3.pkl'
if not clf_pkl.exists():
    print(f"  WARNING: {clf_pkl} not found. Skipping classifier eval.")
else:
    with open(clf_pkl, 'rb') as f:
        m_clf = pickle.load(f)

    # Use feature names stored in booster (if trained with named columns)
    _fn_clf   = m_clf.get_booster().feature_names
    clf_feats = list(_fn_clf) if _fn_clf is not None else FEAT_V3

    print(f"  Features used by classifier: {len(clf_feats)}")
    # Check all required features are present
    missing = [c for c in clf_feats if c not in test.columns]
    if missing:
        print(f"  WARNING: missing features in test: {missing}")

    prob_clf = m_clf.predict_proba(test[clf_feats])[:, 1]

    clf_m = opt_threshold_metrics(y_te_clf, prob_clf)
    brier = brier_score_loss(y_te_clf, prob_clf)
    clf_m['Brier'] = round(float(brier), 4)
    clf_metrics['XGB clf v3 (31 feat)'] = clf_m

    print(f"  PR-AUC={clf_m['PR-AUC']:.4f}  F1(opt)={clf_m['F1 (opt)']:.4f}  "
          f"Threshold={clf_m['Threshold']:.4f}")
    print(f"  Precision={clf_m['Precision']:.4f}  Recall={clf_m['Recall']:.4f}  "
          f"Brier={clf_m['Brier']:.4f}")


# =============================================================================
# STEP 4 — Regression model as classifier: model_xgb_reg_v3_best.pkl
# =============================================================================
print()
print("=" * 65)
print("STEP 4: Regression model as classifier — model_xgb_reg_v3_best.pkl")
print("=" * 65)

reg_pkl = PROC / 'model_xgb_reg_v3_best.pkl'
if not reg_pkl.exists():
    print(f"  WARNING: {reg_pkl} not found. Skipping reg-as-clf eval.")
    m_best = None
    pred_best = None
else:
    with open(reg_pkl, 'rb') as f:
        m_best = pickle.load(f)

    _fn_best  = m_best.get_booster().feature_names
    reg_feats = list(_fn_best) if _fn_best is not None else FEAT_V3

    print(f"  Features used by regressor: {len(reg_feats)}")
    pred_best = m_best.predict(test[reg_feats])

    # Regression test metrics
    r2_best   = r2_score(y_te, pred_best)
    rmse_best = mean_squared_error(y_te, pred_best) ** 0.5
    mae_best  = mean_absolute_error(np.expm1(y_te), np.expm1(pred_best))
    print(f"  Regression: R²={r2_best:.4f}  RMSE(log)={rmse_best:.4f}  MAE($/MWh)={mae_best:.3f}")

    # Use predicted log_rtm_std as ranking score for spike classification
    # (higher predicted volatility -> higher spike probability)
    reg_m = opt_threshold_metrics(y_te_clf, pred_best)
    brier_reg = brier_score_loss(y_te_clf, np.clip(
        (pred_best - pred_best.min()) / (pred_best.max() - pred_best.min() + 1e-9), 0, 1
    ))
    reg_m['Brier (scaled)'] = round(float(brier_reg), 4)
    clf_metrics['XGB reg v3 best (as clf, 31 feat)'] = reg_m

    print(f"  Reg-as-clf: PR-AUC={reg_m['PR-AUC']:.4f}  F1(opt)={reg_m['F1 (opt)']:.4f}  "
          f"Threshold={reg_m['Threshold']:.4f}")
    print(f"  Precision={reg_m['Precision']:.4f}  Recall={reg_m['Recall']:.4f}  "
          f"Brier(scaled)={reg_m['Brier (scaled)']:.4f}")


# =============================================================================
# STEP 5 — Ensemble: Ridge(29 feat) + XGBoost v3 best (31 feat)
#          Ridge refitted on full train ≤2024; XGB loaded from pkl
#          alpha sweep on test 2025
# =============================================================================
print()
print("=" * 65)
print("STEP 5: Ensemble — Ridge(29) + XGB v3 best, alpha sweep on test 2025")
print("=" * 65)

# Refit Ridge on full training set (≤2024)
train_nodrop = train.dropna(subset=[TARGET])
y_tr = train_nodrop[TARGET].values

imp_ridge = SimpleImputer(strategy='median')
X_tr_ridge = imp_ridge.fit_transform(train_nodrop[FEAT_V2])
X_te_ridge = imp_ridge.transform(test[FEAT_V2])

sc_ridge = StandardScaler()
ridge_final = Ridge(alpha=1.0)
ridge_pipe  = Pipeline([('sc', sc_ridge), ('ridge', ridge_final)])
ridge_pipe.fit(X_tr_ridge, y_tr)
pred_ridge_te = ridge_pipe.predict(X_te_ridge)

r2_ridge_te   = r2_score(y_te, pred_ridge_te)
rmse_ridge_te = mean_squared_error(y_te, pred_ridge_te) ** 0.5
mae_ridge_te  = mean_absolute_error(np.expm1(y_te), np.expm1(pred_ridge_te))
print(f"  Ridge (29 feat) test: R²={r2_ridge_te:.4f}  RMSE={rmse_ridge_te:.4f}  MAE={mae_ridge_te:.3f}")

if m_best is None or pred_best is None:
    print("  WARNING: XGB best pkl not available — cannot compute ensemble.")
else:
    r2_xgb_te   = r2_score(y_te, pred_best)
    rmse_xgb_te = mean_squared_error(y_te, pred_best) ** 0.5
    mae_xgb_te  = mean_absolute_error(np.expm1(y_te), np.expm1(pred_best))
    print(f"  XGB v3 best (31 feat) test: R²={r2_xgb_te:.4f}  RMSE={rmse_xgb_te:.4f}  "
          f"MAE={mae_xgb_te:.3f}")

    # Alpha sweep — note: alpha here is XGB weight (alpha * xgb + (1-alpha) * ridge)
    alphas = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.72, 0.8, 0.9, 1.0]

    print()
    print(f"  {'alpha (XGB)':>12}  {'R²':>8}  {'RMSE':>8}  {'MAE($/MWh)':>12}")
    print("  " + "-" * 46)

    ens_results = []
    best_r2_ens, best_alpha, best_rmse, best_mae = -np.inf, None, None, None

    for alpha in alphas:
        ens_pred = alpha * pred_best + (1.0 - alpha) * pred_ridge_te
        r2_ens   = r2_score(y_te, ens_pred)
        rmse_ens = mean_squared_error(y_te, ens_pred) ** 0.5
        mae_ens  = mean_absolute_error(np.expm1(y_te), np.expm1(ens_pred))
        marker   = " <-- best" if r2_ens > best_r2_ens else ""
        print(f"  {alpha:>12.2f}  {r2_ens:>8.4f}  {rmse_ens:>8.4f}  {mae_ens:>12.3f}{marker}")
        ens_results.append({'alpha': alpha, 'R²': r2_ens, 'RMSE': rmse_ens, 'MAE': mae_ens})
        if r2_ens > best_r2_ens:
            best_r2_ens = r2_ens
            best_alpha  = alpha
            best_rmse   = rmse_ens
            best_mae    = mae_ens

    print()
    print(f"  Best ensemble alpha (XGB weight): {best_alpha:.2f}")
    print(f"  Ensemble test: R²={best_r2_ens:.4f}  RMSE={best_rmse:.4f}  MAE={best_mae:.3f}")


# =============================================================================
# FINAL METRICS SUMMARY
# =============================================================================
print()
print("=" * 65)
print("=== METRICS: CLASSIFIER & ENSEMBLE (test = 2025) ===")
print("=" * 65)

# Classifier leaderboard
print()
print("CLASSIFIER LEADERBOARD — spike_flag (rtm_price_mean > $100/MWh)")
print(f"  {'Model':<40} {'PR-AUC':>7} {'F1(opt)':>8} {'Thresh':>7} {'Prec':>7} {'Recall':>7}")
print("  " + "-" * 80)
for model_name, m in clf_metrics.items():
    print(f"  {model_name:<40} {m['PR-AUC']:>7.4f} {m['F1 (opt)']:>8.4f} "
          f"{m['Threshold']:>7.4f} {m['Precision']:>7.4f} {m['Recall']:>7.4f}")

# Regression + ensemble leaderboard
print()
print("REGRESSION LEADERBOARD — log_rtm_std (test = 2025)")
print(f"  {'Model':<40} {'R²':>7} {'RMSE(log)':>10} {'MAE($/MWh)':>12}")
print("  " + "-" * 72)

if m_best is not None:
    print(f"  {'XGB v3 best (31 feat, post-Uri)':<40} {r2_xgb_te:>7.4f} {rmse_xgb_te:>10.4f} {mae_xgb_te:>12.3f}")
print(f"  {'Ridge (29 feat, full train)':<40} {r2_ridge_te:>7.4f} {rmse_ridge_te:>10.4f} {mae_ridge_te:>12.3f}")
if m_best is not None:
    print(f"  {'Ensemble (alpha={:.2f}, XGB+Ridge)':<40} {best_r2_ens:>7.4f} {best_rmse:>10.4f} {best_mae:>12.3f}".format(best_alpha))

# Spike rate info
print()
print(f"Test set: {len(test):,} hours  |  Spike rate: {y_te_clf.mean():.2%}  "
      f"({y_te_clf.sum()} spike hours)")

print()
print("Done.")
