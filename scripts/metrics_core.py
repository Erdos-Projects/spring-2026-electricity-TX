"""
metrics_core.py — Core model metrics for the ERCOT Houston Hub volatility project.

Covers:
  - Setup: load train/test parquets, define all feature sets and constants
  - Engineered features (add_engineered_features)
  - Column-specific imputation
  - GARCH conditional volatility (build_garch_vol)
  - §7.2 walk-forward CV + final test eval: HAR-Ridge, Full-Ridge, HAR+Full-Ridge
  - §7.5 best model (post-Uri XGB v3 tuned): load pkl, eval on test 2025
  - Final leaderboard

Run from project root with:
    conda run -n erdos_ds_environment python scripts/metrics_core.py

All paths are relative to project root.
"""

import warnings
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.linear_model import Ridge, RidgeCV, LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score,
    average_precision_score, f1_score, precision_score, recall_score, precision_recall_curve,
)
import xgboost as xgb
from arch import arch_model

# ── Paths ─────────────────────────────────────────────────────────────────────
PROC = Path('data/processed/ercot')
TARGET = 'log_rtm_std'
TRAIN_END_FINAL = pd.Timestamp('2024-12-31 23:00')

# ── Feature sets ──────────────────────────────────────────────────────────────
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
]  # 29 features — linear models (mcpc_ecrs excluded; system_lambda excluded)

FEAT_V2_XGB = FEAT_V2 + ['system_lambda']   # 30 features for XGBoost
FEAT_V3     = FEAT_V2_XGB + ['garch_cond_vol']  # 31 features

# Features for ARIMAX (drop RTM lags — would be circular for time-series models)
_RTM_LAG_COLS = {'rtm_mean_lag24', 'rtm_std_lag24', 'rtm_price_std_lag7d', 'rtm_price_mean_lag7d'}
FEAT_ARIMAX = [f for f in FEAT_V2 if f not in _RTM_LAG_COLS]  # 25 features

# CV fold configuration
FOLD_ENDS  = [pd.Timestamp('2021-12-31 23:00'),
              pd.Timestamp('2022-12-31 23:00'),
              pd.Timestamp('2023-12-31 23:00')]
FOLD_NAMES = ['2022', '2023', '2024']

# XGBoost base params (pre-tuning)
XGB_PARAMS = dict(
    n_estimators=600, learning_rate=0.05, max_depth=5,
    subsample=0.8, colsample_bytree=0.8,
    min_child_weight=3, reg_alpha=0.1, reg_lambda=1.0,
    random_state=42, n_jobs=-1,
)

# ── Helper: engineered features ───────────────────────────────────────────────

def add_engineered_features(df):
    """Add 7 engineered features in-place on a copy. Must be called on combined
    train+test DataFrame so that 7-day lag shifts span the boundary correctly."""
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

    Parameters
    ----------
    df        : DataFrame with a 'log_rtm_std' column (or single-column),
                hourly index covering both train and val/test periods.
    train_end : pd.Timestamp — last training observation for GARCH fit.

    Returns
    -------
    pd.Series — conditional vol, D-1 lagged, same index as df.
    """
    target  = df['log_rtm_std'] if 'log_rtm_std' in df.columns else df.iloc[:, 0]
    tr_mask = target.index <= train_end

    # Seasonal OLS on train
    S_tr = make_seasonal(target[tr_mask].to_frame())
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
    return cond_vol.shift(24)  # D-1 lag — leakage-safe


def impute_combined(combined):
    """Apply column-specific imputation in-place on the combined DataFrame.
    Medians are fit on the training portion only (index <= TRAIN_END_FINAL)."""
    # ffill for time-series lag features (preserves temporal structure)
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

    # total_resource_mw: fillna(0) — nulls are pre-2022, final model unaffected
    if 'total_resource_mw' in combined.columns:
        combined['total_resource_mw'] = combined['total_resource_mw'].fillna(0)

    return combined


def reg_metrics(y_true, y_pred, label=''):
    r2   = r2_score(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred) ** 0.5
    mae  = mean_absolute_error(np.expm1(y_true), np.expm1(y_pred))
    if label:
        print(f"  {label:<40} R²={r2:.4f}  RMSE(log)={rmse:.4f}  MAE($/MWh)={mae:.3f}")
    return {'R² test': round(r2, 3), 'RMSE': round(rmse, 3), 'MAE': round(mae, 3)}


# =============================================================================
# STEP 1 — Load data and build combined feature matrix
# =============================================================================
print("=" * 60)
print("STEP 1: Loading parquets and building combined feature matrix")
print("=" * 60)

train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')

print(f"  train_features: {train_df.shape}  ({train_df.index.min().date()} → {train_df.index.max().date()})")
print(f"  test_features:  {test_df.shape}  ({test_df.index.min().date()} → {test_df.index.max().date()})")

# Apply engineered features on combined to get correct 7-day shift across boundary
combined = pd.concat([train_df, test_df]).sort_index()
combined = add_engineered_features(combined)
combined = impute_combined(combined)

print(f"  combined shape after engineering + imputation: {combined.shape}")

# Build rtm_full for HAR lags (independent of combined)
rtm_full = combined[[TARGET]].copy()
rtm_full['rv_d1'] = rtm_full[TARGET].shift(24)
rtm_full['rv_w']  = rtm_full[TARGET].rolling(24 * 7).mean().shift(24)
rtm_full['rv_m']  = rtm_full[TARGET].rolling(24 * 30).mean().shift(24)


# =============================================================================
# STEP 2 — §7.2 Walk-forward CV: fast linear models (HAR, Ridge, HAR+Full-Ridge)
# =============================================================================
print()
print("=" * 60)
print("STEP 2: §7.2 Walk-forward CV — linear/statistical models")
print("=" * 60)

results = {
    'Ridge': [], 'Ridge-seasonal': [],
    'HAR': [], 'HAR+Full-Ridge': [],
    'RTM-lag-only': [], 'Seasonal-naive': [],
}
HAR_COLS = ['rv_d1', 'rv_w', 'rv_m']

for fold_train_end, val_year in zip(FOLD_ENDS, FOLD_NAMES):
    fold_val_start = fold_train_end + pd.Timedelta(hours=1)
    fold_val_end   = pd.Timestamp(f'{val_year}-12-31 23:00')

    tr_mask  = combined.index <= fold_train_end
    val_mask = (combined.index >= fold_val_start) & (combined.index <= fold_val_end)

    tr  = combined.loc[tr_mask].copy()
    va  = combined.loc[val_mask].copy()
    y_tr  = tr[TARGET].dropna()
    tr    = tr.loc[y_tr.index]
    y_val = va[TARGET].dropna()
    va    = va.loc[y_val.index]

    print(f"\n  Fold {val_year}: train={len(tr):,}  val={len(va):,}")

    # Ridge (29 feat)
    imp_r = SimpleImputer(strategy='median')
    X_tr_r = pd.DataFrame(imp_r.fit_transform(tr[FEAT_V2]), columns=FEAT_V2, index=tr.index)
    X_va_r = pd.DataFrame(imp_r.transform(va[FEAT_V2]),     columns=FEAT_V2, index=va.index)
    sc = StandardScaler()
    m = Ridge(alpha=10.0).fit(sc.fit_transform(X_tr_r), y_tr)
    results['Ridge'].append(r2_score(y_val, m.predict(sc.transform(X_va_r))))

    # Ridge + seasonal dummies
    _Xtr_s = make_seasonal(X_tr_r)
    _Xva_s = make_seasonal(X_va_r)
    sc_s = StandardScaler()
    m_s = Ridge(alpha=10.0).fit(sc_s.fit_transform(_Xtr_s), y_tr)
    results['Ridge-seasonal'].append(r2_score(y_val, m_s.predict(sc_s.transform(_Xva_s))))

    # HAR only
    _htr = rtm_full[HAR_COLS].reindex(tr.index).fillna(0)
    _hva = rtm_full[HAR_COLS].reindex(va.index).fillna(0)
    sc_h = StandardScaler()
    m_h = Ridge(alpha=1.0).fit(sc_h.fit_transform(_htr), y_tr)
    results['HAR'].append(r2_score(y_val, m_h.predict(sc_h.transform(_hva))))

    # HAR + Full Ridge
    _hf_Xtr = np.hstack([X_tr_r.values, _htr.values])
    _hf_Xva = np.hstack([X_va_r.values, _hva.values])
    sc_hf = StandardScaler()
    m_hf = Ridge(alpha=10.0).fit(sc_hf.fit_transform(_hf_Xtr), y_tr)
    results['HAR+Full-Ridge'].append(r2_score(y_val, m_hf.predict(sc_hf.transform(_hf_Xva))))

    # RTM-lag-only baseline
    lag_cols = [c for c in FEAT_V2 if 'lag' in c or 'rtm' in c]
    imp_l = SimpleImputer(strategy='median')
    X_tr_l = pd.DataFrame(imp_l.fit_transform(tr[lag_cols]), columns=lag_cols, index=tr.index)
    X_va_l = pd.DataFrame(imp_l.transform(va[lag_cols]),     columns=lag_cols, index=va.index)
    sc_l = StandardScaler()
    m_l = Ridge(alpha=1.0).fit(sc_l.fit_transform(X_tr_l), y_tr)
    results['RTM-lag-only'].append(r2_score(y_val, m_l.predict(sc_l.transform(X_va_l))))

    # Seasonal-naive baseline
    _naive = tr[TARGET].reindex(va.index - pd.DateOffset(weeks=1)).values
    _naive = np.where(np.isnan(_naive), tr[TARGET].mean(), _naive)
    results['Seasonal-naive'].append(r2_score(y_val, _naive))

    # Print fold summary
    for model_name, scores in results.items():
        print(f"    {model_name:<22}: fold R²={scores[-1]:.4f}")

print()
print("  Walk-forward CV mean R²:")
for model_name, scores in sorted(results.items(), key=lambda x: -np.mean(x[1])):
    print(f"    {model_name:<22}: {np.mean(scores):.4f}  folds={[f'{s:.3f}' for s in scores]}")


# =============================================================================
# STEP 3 — §7.2 Final test evaluation: fit on full train ≤2024, eval on 2025
# =============================================================================
print()
print("=" * 60)
print("STEP 3: §7.2 Final test evaluation — train ≤2024, test=2025")
print("=" * 60)

_tr_final = combined[combined.index <= '2024-12-31']
_te_final = combined[combined.index >= '2025-01-01']
_y_tr     = _tr_final[TARGET].dropna().values
_y_te     = _te_final[TARGET].dropna().values
# Align indices to non-null target rows
_tr_idx = _tr_final[TARGET].dropna().index
_te_idx = _te_final[TARGET].dropna().index

_har_tr = rtm_full[HAR_COLS].reindex(_tr_idx).fillna(0)
_har_te = rtm_full[HAR_COLS].reindex(_te_idx).fillna(0)

leaderboard_linear = []

# HAR-Ridge (3 TS lags)
print("  Fitting HAR-Ridge...")
_sc_har = StandardScaler()
_m_har = Ridge(alpha=1.0).fit(_sc_har.fit_transform(_har_tr.values), _y_tr)
_p_har = _m_har.predict(_sc_har.transform(_har_te.values))
leaderboard_linear.append({
    'Model': 'HAR-Ridge (3 lags)',
    'CV R² mean': round(np.mean(results.get('HAR', [float('nan')])), 3),
    **reg_metrics(_y_te, _p_har, 'HAR-Ridge (3 lags)'),
})

# Full-Ridge (29 market features)
print("  Fitting Full-Ridge (29 feat)...")
_imp_fr = SimpleImputer(strategy='median')
_X_tr_fr = _imp_fr.fit_transform(_tr_final.reindex(_tr_idx)[FEAT_V2])
_X_te_fr = _imp_fr.transform(_te_final.reindex(_te_idx)[FEAT_V2])
_sc_fr   = StandardScaler()
_m_fr    = RidgeCV(alphas=[0.1, 1, 10, 100, 1000]).fit(_sc_fr.fit_transform(_X_tr_fr), _y_tr)
_p_fr    = _m_fr.predict(_sc_fr.transform(_X_te_fr))
leaderboard_linear.append({
    'Model': 'Full-Ridge (29 feat)',
    'CV R² mean': round(np.mean(results.get('Ridge', [float('nan')])), 3),
    **reg_metrics(_y_te, _p_fr, 'Full-Ridge (29 feat)'),
})

# HAR + Full Ridge (32 features)
print("  Fitting HAR+Full-Ridge (32 feat)...")
_hf_tr = np.hstack([_X_tr_fr, _har_tr.values])
_hf_te = np.hstack([_X_te_fr, _har_te.values])
_sc_hf = StandardScaler()
_m_hf  = RidgeCV(alphas=[0.1, 1, 10, 100, 1000]).fit(_sc_hf.fit_transform(_hf_tr), _y_tr)
_p_hf  = _m_hf.predict(_sc_hf.transform(_hf_te))
leaderboard_linear.append({
    'Model': 'HAR+Full-Ridge (32 feat)',
    'CV R² mean': round(np.mean(results.get('HAR+Full-Ridge', [float('nan')])), 3),
    **reg_metrics(_y_te, _p_hf, 'HAR+Full-Ridge (32 feat)'),
})

print()
print("  §7.2 Linear/Statistical Model Leaderboard (test = 2025):")
print(f"  {'Model':<36} {'CV R²':>7} {'Test R²':>8} {'RMSE':>7} {'MAE($/MWh)':>11}")
print("  " + "-" * 72)
for row in sorted(leaderboard_linear, key=lambda x: x['R² test'], reverse=True):
    print(f"  {row['Model']:<36} {row['CV R² mean']:>7.3f} {row['R² test']:>8.3f} "
          f"{row['RMSE']:>7.3f} {row['MAE']:>11.3f}")


# =============================================================================
# STEP 4 — Build GARCH conditional volatility for full combined window
# =============================================================================
print()
print("=" * 60)
print("STEP 4: Building GARCH conditional vol (D-1 lag, leakage-safe)")
print("=" * 60)
print("  Fitting GARCH(1,1)-t on training residuals (train ≤2024-12-31)...")

garch_vol_full = build_garch_vol(combined[[TARGET]], TRAIN_END_FINAL)
combined_v3 = combined.copy()
combined_v3['garch_cond_vol'] = garch_vol_full

n_garch_nulls = combined_v3['garch_cond_vol'].isna().sum()
print(f"  garch_cond_vol built. NaN count (expected ~24 at head): {n_garch_nulls}")


# =============================================================================
# STEP 5 — §7.5 Post-Uri XGB v3 best model: load pkl and eval on test 2025
# =============================================================================
print()
print("=" * 60)
print("STEP 5: §7.5 Best model — load model_xgb_reg_v3_best.pkl, eval 2025")
print("=" * 60)

_pkl_path = PROC / 'model_xgb_reg_v3_best.pkl'
if not _pkl_path.exists():
    print(f"  WARNING: {_pkl_path} not found. Skipping §7.5 eval.")
    r2_best = rmse_best = mae_best = float('nan')
    xgb_best_row = None
else:
    with open(_pkl_path, 'rb') as f:
        m_best = pickle.load(f)

    # Use feature names from booster if available; fall back to FEAT_V3
    _fn_best = m_best.get_booster().feature_names
    _feat_best = list(_fn_best) if _fn_best is not None else FEAT_V3

    # The saved model was trained on post-Uri window (2021-01 → 2024-12-31)
    # but evaluation is always on 2025 test set
    test_mask_v3 = combined_v3.index > TRAIN_END_FINAL
    X_te_v3 = combined_v3.loc[test_mask_v3, _feat_best]
    y_te_v3 = combined_v3.loc[test_mask_v3, TARGET]

    pred_best   = m_best.predict(X_te_v3)
    r2_best     = r2_score(y_te_v3, pred_best)
    rmse_best   = mean_squared_error(y_te_v3, pred_best) ** 0.5
    mae_best    = mean_absolute_error(np.expm1(y_te_v3), np.expm1(pred_best))

    print(f"  Features used: {len(_feat_best)}  (model booster names)")
    print(f"  Test rows (2025): {len(y_te_v3):,}")
    reg_metrics(y_te_v3.values, pred_best, 'XGB v3 best (post-Uri 2021–2024)')

    xgb_best_row = {
        'Model': 'XGB v3 untuned best (post-Uri)',
        'CV R² mean': float('nan'),
        'R² test': round(r2_best, 3),
        'RMSE': round(rmse_best, 3),
        'MAE': round(mae_best, 3),
    }

# STEP 5b — model_xgb_reg_v3_tuned.pkl (max_depth=4, lr=0.03)
_pkl_tuned = PROC / 'model_xgb_reg_v3_tuned.pkl'
xgb_tuned_row = None
if _pkl_tuned.exists():
    with open(_pkl_tuned, 'rb') as f:
        m_tuned = pickle.load(f)
    _fn_tuned = m_tuned.get_booster().feature_names
    _feat_tuned = list(_fn_tuned) if _fn_tuned is not None else FEAT_V3
    test_mask_t = combined_v3.index > TRAIN_END_FINAL
    X_te_t = combined_v3.loc[test_mask_t, _feat_tuned]
    y_te_t = combined_v3.loc[test_mask_t, TARGET]
    pred_t = m_tuned.predict(X_te_t)
    r2_t   = r2_score(y_te_t, pred_t)
    rmse_t = mean_squared_error(y_te_t, pred_t) ** 0.5
    mae_t  = mean_absolute_error(np.expm1(y_te_t), np.expm1(pred_t))
    reg_metrics(y_te_t.values, pred_t, 'XGB v3 tuned (max_depth=4, lr=0.03, post-Uri)')
    xgb_tuned_row = {
        'Model': 'XGB v3 tuned (max_depth=4, lr=0.03)',
        'CV R² mean': 0.3111,
        'R² test': round(r2_t, 3),
        'RMSE': round(rmse_t, 3),
        'MAE': round(mae_t, 3),
    }


# STEP 5c — Ensemble (Ridge 29 feat + XGB tuned, alpha=0.72) MAE
# Ridge is fit on post-Uri train (2021+) with median imputation
print()
print("=" * 60)
print("STEP 5c: Ensemble alpha=0.72 (Ridge + XGB tuned)")
print("=" * 60)
_ens_row = None
if xgb_tuned_row is not None:
    from sklearn.pipeline import Pipeline
    _pu_tr_mask = (combined_v3.index >= '2021-01-01') & (combined_v3.index <= TRAIN_END_FINAL)
    _te_mask_ens = combined_v3.index > TRAIN_END_FINAL
    _ridge_ens = Pipeline([
        ('imp', SimpleImputer(strategy='median')),
        ('sc', StandardScaler()),
        ('r', RidgeCV(alphas=[0.1, 1, 10, 100])),
    ])
    _ridge_ens.fit(combined_v3.loc[_pu_tr_mask, FEAT_V2], combined_v3.loc[_pu_tr_mask, TARGET])
    _pred_ridge_ens = _ridge_ens.predict(combined_v3.loc[_te_mask_ens, FEAT_V2])
    _pred_xgb_ens   = m_tuned.predict(combined_v3.loc[_te_mask_ens, _feat_tuned])
    _y_te_ens = combined_v3.loc[_te_mask_ens, TARGET]
    _alpha = 0.72
    _pred_ens = _alpha * _pred_xgb_ens + (1 - _alpha) * _pred_ridge_ens
    _r2_ens   = r2_score(_y_te_ens, _pred_ens)
    _rmse_ens = mean_squared_error(_y_te_ens, _pred_ens) ** 0.5
    _mae_ens  = mean_absolute_error(np.expm1(_y_te_ens), np.expm1(_pred_ens))
    print(f"  Ensemble alpha=0.72: R²={_r2_ens:.4f}  RMSE={_rmse_ens:.4f}  MAE($/MWh)={_mae_ens:.3f}")
    _ens_row = {'Model': 'Ensemble Ridge+XGB (α=0.72 CV)', 'CV R² mean': 0.309,
                'R² test': round(_r2_ens, 3), 'RMSE': round(_rmse_ens, 3), 'MAE': round(_mae_ens, 3)}


# STEP 5d — Classifier metrics (uses combined_v3 with proper garch_cond_vol)
print()
print("=" * 60)
print("STEP 5d: Classifier metrics (XGB clf v3 + Reg-as-clf)")
print("=" * 60)
_te_clf_mask = combined_v3.index > TRAIN_END_FINAL
_y_clf = combined_v3.loc[_te_clf_mask, 'spike_flag']

def _eval_clf(proba, y_true, label, threshold=None):
    from sklearn.metrics import precision_recall_curve
    prec_arr, rec_arr, thr_arr = precision_recall_curve(y_true, proba)
    f1_arr = 2 * prec_arr * rec_arr / (prec_arr + rec_arr + 1e-9)
    if threshold is None:
        best_i = np.argmax(f1_arr[:-1])
        threshold = float(thr_arr[best_i])
    pred_bin = (proba >= threshold).astype(int)
    prauc = average_precision_score(y_true, proba)
    f1    = f1_score(y_true, pred_bin)
    prec  = precision_score(y_true, pred_bin, zero_division=0)
    rec   = recall_score(y_true, pred_bin, zero_division=0)
    print(f"  {label}:")
    print(f"    PR-AUC={prauc:.3f}  F1={f1:.3f}  Precision={prec:.3f}  Recall={rec:.3f}  threshold={threshold:.3f}")
    return {'prauc': prauc, 'f1': f1, 'prec': prec, 'rec': rec, 'thr': threshold}

_clf_pkl = PROC / 'model_xgb_clf_v3.pkl'
if _clf_pkl.exists():
    with open(_clf_pkl, 'rb') as f: _m_clf = pickle.load(f)
    _fn_clf = list(_m_clf.get_booster().feature_names)
    _X_clf = combined_v3.loc[_te_clf_mask, _fn_clf]
    _proba_clf = _m_clf.predict_proba(_X_clf)[:, 1]
    _clf_metrics = _eval_clf(_proba_clf, _y_clf, 'XGB Classifier v3 (31 feat)', threshold=0.508)
else:
    print("  model_xgb_clf_v3.pkl not found — skipping")
    _clf_metrics = None

if xgb_tuned_row is not None:
    _pred_reg_te = pred_t  # from STEP 5b
    _reg_clf_metrics = _eval_clf(_pred_reg_te, _y_clf, 'Reg v3 tuned as classifier', threshold=2.404)
else:
    _reg_clf_metrics = None


# =============================================================================
# FINAL LEADERBOARD
# =============================================================================
print()
print("=" * 60)
print("=== METRICS: FINAL LEADERBOARD (test = 2025) ===")
print("=" * 60)

all_rows = sorted(leaderboard_linear, key=lambda x: x['R² test'], reverse=True)
xgb_rows = [r for r in [xgb_tuned_row, xgb_best_row] if r is not None]
if xgb_rows:
    all_rows = xgb_rows + all_rows

print(f"{'Model':<40} {'CV R²':>7} {'Test R²':>8} {'RMSE':>7} {'MAE($/MWh)':>11}")
print("-" * 78)
for row in all_rows:
    cv_r2 = row.get('CV R² mean', float('nan'))
    cv_str = f"{cv_r2:.3f}" if not np.isnan(cv_r2) else "  —  "
    print(f"  {row['Model']:<38} {cv_str:>7} {row['R² test']:>8.3f} "
          f"{row['RMSE']:>7.3f} {row['MAE']:>11.3f}")

print()
if xgb_best_row is not None:
    print(f"Final model: XGB v3 tuned best (post-Uri 2021–2024)")
    print(f"  Test R²={r2_best:.4f}  RMSE(log)={rmse_best:.4f}  MAE($/MWh)={mae_best:.3f}")
print()
print("Done.")
