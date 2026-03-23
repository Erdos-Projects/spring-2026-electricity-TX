"""
metrics_rolling.py — Rolling forecast and drift analysis for ERCOT Houston Hub.

Covers:
  - Same setup/feature engineering/imputation/GARCH as metrics_core.py (self-contained)
  - §10.1 Option A — fixed model (post-Uri 2021–2024), MAE by hour of day
  - §10.2 Option C — sliding 4-year window, 52 weekly retrains; weekly MAE + drift score
  - Option A vs C summary: overall MAE, R², weeks C beats A

Drift tests (KS, Page-Hinkley) are skipped to keep runtime manageable.

Run from project root with:
    conda run -n erdos_ds_environment python scripts/metrics_rolling.py

All paths are relative to project root.
"""

import warnings
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.linear_model import Ridge, LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
import xgboost as xgb
from arch import arch_model

# ── Paths and constants ───────────────────────────────────────────────────────
PROC           = Path('data/processed/ercot')
TARGET         = 'log_rtm_std'
TRAIN_END_FULL = pd.Timestamp('2024-12-31 23:00')

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
]  # 29 features — linear models

FEAT_V2_XGB = FEAT_V2 + ['system_lambda']   # 30 features for XGBoost
FEAT_V3     = FEAT_V2_XGB + ['garch_cond_vol']  # 31 features

# XGBoost params — tuned hyperparameters (§7.3.1 grid search, matches model_xgb_reg_v3_best.pkl)
XGB_PARAMS = dict(
    n_estimators=600, learning_rate=0.03, max_depth=4,
    subsample=0.8, colsample_bytree=0.8,
    min_child_weight=3, reg_alpha=0.1, reg_lambda=1.0,
    random_state=42, n_jobs=-1,
)

# ── Helper functions ──────────────────────────────────────────────────────────

def add_engineered_features(df):
    """Add 7 engineered features. Must be called on combined train+test DataFrame
    so that 7-day lag shifts span the train/test boundary correctly."""
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


def impute_combined(combined, train_end):
    """Apply column-specific imputation. Medians fit on train (index <= train_end)."""
    for col in ['rtm_std_lag24', 'load_houston_lag48']:
        if col in combined.columns:
            combined[col] = combined[col].ffill()

    train_mask = combined.index <= train_end
    for col in ['dam_price_houston', 'system_lambda',
                'mcpc_regup', 'mcpc_rrs', 'mcpc_nspin', 'mcpc_regdn']:
        if col in combined.columns:
            med = combined.loc[train_mask, col].median()
            combined[col] = combined[col].fillna(med)

    if 'total_resource_mw' in combined.columns:
        combined['total_resource_mw'] = combined['total_resource_mw'].fillna(0)

    return combined


# =============================================================================
# STEP 1 — Load data and build combined feature matrix with GARCH vol
# =============================================================================
print("=" * 60)
print("STEP 1: Loading data and building GARCH feature matrix")
print("=" * 60)

train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')

print(f"  train_features: {train_df.shape}  ({train_df.index.min().date()} → {train_df.index.max().date()})")
print(f"  test_features:  {test_df.shape}  ({test_df.index.min().date()} → {test_df.index.max().date()})")

# Combine, engineer features, impute
combined9 = pd.concat([train_df, test_df]).sort_index()
combined9  = add_engineered_features(combined9)
combined9  = impute_combined(combined9, TRAIN_END_FULL)

print(f"  combined shape after engineering + imputation: {combined9.shape}")
print("  Building GARCH conditional vol (D-1 lag)... ", end='', flush=True)
garch_vol9 = build_garch_vol(combined9[[TARGET]], TRAIN_END_FULL)
combined9['garch_cond_vol'] = garch_vol9
print("done.")

# Rebuild FEAT_V3 explicitly here (do not inherit from any global state)
FEAT_V3 = FEAT_V2_XGB + ['garch_cond_vol']  # 31 features

# Split back into train and test
train9 = combined9[combined9.index <= TRAIN_END_FULL].copy()
test9  = combined9[combined9.index >  TRAIN_END_FULL].copy()

print(f"  train9: {len(train9):,} rows  |  test9: {len(test9):,} rows")


# =============================================================================
# STEP 2 — Load fixed model (Option A) and evaluate on test 2025
# =============================================================================
print()
print("=" * 60)
print("STEP 2: §10.1 Option A — fixed model (post-Uri 2021–2024)")
print("=" * 60)

_pkl_path = PROC / 'model_xgb_reg_v3_best.pkl'
if not _pkl_path.exists():
    raise FileNotFoundError(
        f"{_pkl_path} not found. Run the notebook §7.5 cell first to save the model."
    )

with open(_pkl_path, 'rb') as f:
    model_a = pickle.load(f)

# Use feature names from booster to ensure order matches training
_fn_a = model_a.get_booster().feature_names
_feat_a = list(_fn_a) if _fn_a is not None else FEAT_V3

X_te9 = test9[_feat_a]
y_te9 = test9[TARGET]

pred_a_series = pd.Series(model_a.predict(X_te9), index=test9.index)
mae_a_overall = mean_absolute_error(y_te9, pred_a_series)
r2_a_overall  = r2_score(y_te9, pred_a_series)

print(f"  Features used by model_a: {len(_feat_a)}")
print(f"  Option A — overall MAE(log): {mae_a_overall:.4f}")
print(f"  Option A — overall R²:       {r2_a_overall:.4f}")

# Attach predictions to test9 for joint analysis
test9 = test9.copy()
test9['pred_a'] = pred_a_series
test9['err_a']  = (test9[TARGET] - pred_a_series).abs()

# MAE by hour of day
by_hour_a = (test9.groupby(test9.index.hour)['err_a']
             .agg(['mean', 'median'])
             .rename(columns={'mean': 'MAE', 'median': 'MedAE'}))

print(f"  Best hour  (UTC): {by_hour_a['MAE'].idxmin():02d}:00  MAE={by_hour_a['MAE'].min():.4f}")
print(f"  Worst hour (UTC): {by_hour_a['MAE'].idxmax():02d}:00  MAE={by_hour_a['MAE'].max():.4f}")
print(f"  Peak/off-peak ratio: {by_hour_a['MAE'].max() / by_hour_a['MAE'].min():.2f}x")
print()
print("  MAE by hour of day (UTC):")
print(f"  {'Hour':>5}  {'MAE':>7}  {'MedAE':>7}")
for hr, row_h in by_hour_a.iterrows():
    print(f"  {hr:>5}  {row_h['MAE']:>7.4f}  {row_h['MedAE']:>7.4f}")


# =============================================================================
# STEP 3 — §10.2 Option C: sliding 4-year window, 52 weekly retrains
# =============================================================================
print()
print("=" * 60)
print("STEP 3: §10.2 Option C — sliding 4-year window, weekly retraining")
print("=" * 60)

WINDOW_DAYS = 4 * 365   # 4-year window — matches best training window (§7.5)
week_starts = pd.date_range('2025-01-01', '2025-12-25', freq='7D')  # 52 retrains
pred_c = pd.Series(np.nan, index=test9.index, dtype=float)

print(f"  {len(week_starts)} weekly retrains x 4-year window (this takes ~10-20 min)...")
t0 = time.time()

for i, ws in enumerate(week_starts):
    we       = min(ws + pd.Timedelta(days=6), pd.Timestamp('2025-12-31'))
    tr_end   = ws - pd.Timedelta(hours=1)
    tr_start = tr_end - pd.Timedelta(days=WINDOW_DAYS)

    mask_tr = (combined9.index >= tr_start) & (combined9.index <= tr_end)
    mask_te = (test9.index >= ws) & (test9.index <= pd.Timestamp(f'{we.date()} 23:00'))

    X_tr_c = combined9.loc[mask_tr, FEAT_V3]
    y_tr_c = combined9.loc[mask_tr, TARGET]
    X_te_c = test9.loc[mask_te, FEAT_V3]

    if len(X_tr_c) < 500 or len(X_te_c) == 0:
        print(f"    Week {i+1}: skipped (train={len(X_tr_c)}, test={len(X_te_c)})")
        continue

    m_c = xgb.XGBRegressor(**XGB_PARAMS)
    m_c.fit(X_tr_c, y_tr_c, verbose=False)
    pred_c.loc[mask_te] = m_c.predict(X_te_c)

    # Progress update every 13 weeks
    if (i + 1) % 13 == 0:
        elapsed = (time.time() - t0) / 60
        valid_so_far  = pred_c.notna()
        mae_so_far    = mean_absolute_error(test9.loc[valid_so_far, TARGET], pred_c[valid_so_far])
        print(f"    Week {i+1}/{len(week_starts)}  ({elapsed:.1f} min)  "
              f"running MAE(log)={mae_so_far:.4f}")

elapsed_total = (time.time() - t0) / 60
print(f"  Option C complete in {elapsed_total:.1f} min.")

valid_c       = pred_c.notna()
mae_c_overall = mean_absolute_error(y_te9[valid_c], pred_c[valid_c])
r2_c_overall  = r2_score(y_te9[valid_c], pred_c[valid_c])

print(f"  Option C — overall MAE(log): {mae_c_overall:.4f}")
print(f"  Option C — overall R²:       {r2_c_overall:.4f}")
print(f"  Delta MAE (C - A): {mae_c_overall - mae_a_overall:+.4f}  "
      f"({'C wins' if mae_c_overall < mae_a_overall else 'A wins'})")

# Attach Option C predictions and errors
test9['pred_c'] = pred_c
test9['err_c']  = (test9[TARGET] - pred_c).abs()
by_hour_c = test9.groupby(test9.index.hour)['err_c'].mean()


# =============================================================================
# STEP 4 — Drift analysis: weekly MAE and drift score
# =============================================================================
print()
print("=" * 60)
print("STEP 4: Weekly drift analysis")
print("=" * 60)

weekly = (test9.resample('W')
          .agg({'err_a': 'mean', 'err_c': 'mean'})
          .rename(columns={'err_a': 'MAE_A', 'err_c': 'MAE_C'})
          .dropna())
weekly['drift_score'] = weekly['MAE_A'] - weekly['MAE_C']  # positive → C wins

n_c_wins    = (weekly['drift_score'] > 0).sum()
mean_drift  = weekly['drift_score'].mean()
best_wk     = weekly['drift_score'].idxmax()
worst_wk    = weekly['drift_score'].idxmin()

print(f"  Weeks C beats A : {n_c_wins}/{len(weekly)}")
print(f"  Mean drift score: {mean_drift:+.4f}")
print(f"  Largest C win   : week of {best_wk.date()}  score={weekly['drift_score'].max():+.4f}")
print(f"  Largest A win   : week of {worst_wk.date()}  score={weekly['drift_score'].min():+.4f}")

print()
print("  Weekly MAE summary (first 10 and last 10 weeks):")
print(f"  {'Week ending':>12}  {'MAE_A':>7}  {'MAE_C':>7}  {'Drift':>8}  {'Winner':>6}")
weeks_to_show = list(weekly.head(10).iterrows()) + list(weekly.tail(10).iterrows())
for wk_end, wk_row in weeks_to_show:
    winner = 'C' if wk_row['drift_score'] > 0 else 'A'
    print(f"  {wk_end.date()!s:>12}  {wk_row['MAE_A']:>7.4f}  {wk_row['MAE_C']:>7.4f}  "
          f"{wk_row['drift_score']:>+8.4f}  {winner:>6}")


# =============================================================================
# FINAL METRICS BLOCK
# =============================================================================
print()
print("=" * 60)
print("=== METRICS: OPTION A vs OPTION C (test = 2025) ===")
print("=" * 60)
print()
print(f"  {'Metric':<35} {'Option A':>10}  {'Option C':>10}  {'Delta (C-A)':>12}")
print("  " + "-" * 72)
print(f"  {'MAE (log scale, overall)':<35} {mae_a_overall:>10.4f}  {mae_c_overall:>10.4f}  "
      f"{mae_c_overall - mae_a_overall:>+12.4f}")
print(f"  {'R² (test 2025)':<35} {r2_a_overall:>10.4f}  {r2_c_overall:>10.4f}  "
      f"{r2_c_overall - r2_a_overall:>+12.4f}")
print(f"  {'Weeks C beats A':<35} {'—':>10}  {n_c_wins:>10}  "
      f"{'(of ' + str(len(weekly)) + ' total)':>12}")
print(f"  {'Mean weekly drift score':<35} {'—':>10}  {mean_drift:>+10.4f}  {'':>12}")
print()
print(f"  MAE by hour of day (UTC) — Option A vs C:")
print(f"  {'Hour':>5}  {'MAE_A':>7}  {'MAE_C':>7}  {'Diff':>8}")
for hr in range(24):
    mae_a_hr = by_hour_a.loc[hr, 'MAE'] if hr in by_hour_a.index else float('nan')
    mae_c_hr = by_hour_c.loc[hr] if hr in by_hour_c.index else float('nan')
    diff = mae_c_hr - mae_a_hr
    print(f"  {hr:>5}  {mae_a_hr:>7.4f}  {mae_c_hr:>7.4f}  {diff:>+8.4f}")
print()
print("Done.")
