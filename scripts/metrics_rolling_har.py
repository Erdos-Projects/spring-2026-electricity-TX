"""
metrics_rolling_har.py — Adds rolling HAR-Ridge to the Option A vs C comparison.

Reuses the same setup as metrics_rolling.py:
  - Load train/test parquets, add engineered features, impute, build GARCH vol.

Then:
  Option A_har — Fixed HAR-Ridge on full train (≤2024), predict all of test 2025.
  Option C_har — Sliding 4-year window, 52 weekly retrains, HAR-Ridge per window.

HAR features: rv_d1 (lag 24), rv_w (7-day rolling mean lag 24), rv_m (30-day rolling mean lag 24).
Ridge(alpha=1.0) with StandardScaler — same as §7.2 walk-forward CV.

XGB Option A metrics are computed from the saved pkl (fast).
XGB Option C metrics (52 retrains, ~15 min) can be provided via --xgb-c-mae and
--xgb-c-r2 flags to skip recomputing. If not provided the XGB rolling loop runs.

Final block prints 4-row comparison table:
  Fixed XGB (A), Rolling XGB (C), Fixed HAR-Ridge, Rolling HAR-Ridge.

Run from project root with:
    conda run -n erdos_ds_environment python scripts/metrics_rolling_har.py

To skip XGB rolling (use known values from a prior metrics_rolling.py run):
    conda run -n erdos_ds_environment python scripts/metrics_rolling_har.py \\
        --xgb-c-mae 0.4965 --xgb-c-r2 0.400 --xgb-c-weeks 39 --xgb-n-weeks 53
"""

import argparse
import warnings
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.linear_model import Ridge, LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
import xgboost as xgb
from arch import arch_model

# ── Paths and constants ────────────────────────────────────────────────────────
PROC           = Path('data/processed/ercot')
TARGET         = 'log_rtm_std'
TRAIN_END_FULL = pd.Timestamp('2024-12-31 23:00')

# ── Feature sets (same as metrics_rolling.py) ─────────────────────────────────
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
]  # 29 features

FEAT_V2_XGB = FEAT_V2 + ['system_lambda']   # 30 features
FEAT_V3     = FEAT_V2_XGB + ['garch_cond_vol']  # 31 features

# XGB params (§10 original v3, pre-tuning — matches metrics_rolling.py)
XGB_PARAMS = dict(
    n_estimators=600, learning_rate=0.05, max_depth=5,
    subsample=0.8, colsample_bytree=0.8,
    min_child_weight=3, reg_alpha=0.1, reg_lambda=1.0,
    random_state=42, n_jobs=-1,
)

HAR_FEATURES = ['rv_d1', 'rv_w', 'rv_m']

# ── CLI args ───────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description='Rolling HAR-Ridge vs XGB comparison')
parser.add_argument('--xgb-c-mae',   type=float, default=None,
                    help='Pre-computed Option C XGB MAE(log) — skips rolling retrains')
parser.add_argument('--xgb-c-r2',    type=float, default=None,
                    help='Pre-computed Option C XGB R²')
parser.add_argument('--xgb-c-weeks', type=int,   default=None,
                    help='Weeks Option C beats Option A (XGB)')
parser.add_argument('--xgb-n-weeks', type=int,   default=None,
                    help='Total weeks in Option C comparison (XGB)')
args = parser.parse_args()

skip_xgb_rolling = (args.xgb_c_mae is not None)


# ── Helper functions ───────────────────────────────────────────────────────────

def add_engineered_features(df):
    """Add 7 engineered features. Call on combined train+test so 7-day lags
    span the train/test boundary correctly."""
    df = df.copy()
    df['fc_net_load']          = df['fc_coast'] - df['wf_stwpf_lz_south_houston']
    df['dam_rtm_spread']       = df['dam_price_houston'] - df['rtm_mean_lag24']
    df['abs_dam_rtm_spread']   = df['dam_rtm_spread'].abs()
    df['week']                 = df.index.isocalendar().week.astype(int)
    df['load_lag7d']           = df['load_houston_lag48'].shift(168)
    df['rtm_price_std_lag7d']  = df['rtm_std_lag24'].shift(168)
    df['rtm_price_mean_lag7d'] = df['rtm_mean_lag24'].shift(168)
    df['outage_fraction']      = df['total_resource_mw'] / (df['fc_system_total'] + 1)
    return df


def add_har_features(df):
    """
    Build HAR features from log_rtm_std — all lagged by 24h (D-1) so they are
    leakage-safe at the midnight cutoff.

      rv_d1 : lag-24 of log_rtm_std                (daily RV, D-1)
      rv_w  : 7-day  rolling mean of y, then lag-24 (weekly RV, D-1)
      rv_m  : 30-day rolling mean of y, then lag-24 (monthly RV, D-1)

    Same construction as §7.2 HAR walk-forward CV cells.
    """
    df = df.copy()
    y = df[TARGET]
    df['rv_d1'] = y.shift(24)
    df['rv_w']  = y.rolling(7  * 24, min_periods=1).mean().shift(24)
    df['rv_m']  = y.rolling(30 * 24, min_periods=1).mean().shift(24)
    return df


def make_seasonal(df):
    """Hour/month/dow dummy features for seasonal OLS de-seasonalisation."""
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

    S_tr     = make_seasonal(target[tr_mask].to_frame())
    seas_ols = LinearRegression().fit(S_tr, target[tr_mask])

    S_all = make_seasonal(target.to_frame()).reindex(columns=S_tr.columns, fill_value=0)
    resid  = target - seas_ols.predict(S_all)

    split = int(tr_mask.sum())
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        gm = arch_model(resid, mean='Constant', vol='GARCH', p=1, q=1, dist='t')
        gr = gm.fit(last_obs=split, disp='off', show_warning=False)

    fc = gr.forecast(horizon=1, start=0, reindex=False)
    cond_vol = pd.Series(
        np.sqrt(np.clip(fc.variance['h.1'].values, 0, None)),
        index=target.index
    )
    return cond_vol.shift(24)  # D-1 lag — leakage-safe


def impute_combined(combined, train_end):
    """Column-specific imputation. Medians fit on train (index <= train_end)."""
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


def make_har_pipeline():
    """Ridge(alpha=1.0) with StandardScaler — same as §7.2."""
    return Pipeline([
        ('scaler', StandardScaler()),
        ('ridge',  Ridge(alpha=1.0)),
    ])


# =============================================================================
# STEP 1 — Load data, engineer features, impute, build GARCH vol
# =============================================================================
print("=" * 60)
print("STEP 1: Loading data and building feature matrix")
print("=" * 60)

train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')

print(f"  train_features: {train_df.shape}  ({train_df.index.min().date()} → {train_df.index.max().date()})")
print(f"  test_features:  {test_df.shape}  ({test_df.index.min().date()} → {test_df.index.max().date()})")

combined = pd.concat([train_df, test_df]).sort_index()
combined = add_engineered_features(combined)
combined = impute_combined(combined, TRAIN_END_FULL)

print(f"  combined shape after engineering + imputation: {combined.shape}")
print("  Building GARCH conditional vol (D-1 lag)... ", end='', flush=True)
garch_vol = build_garch_vol(combined[[TARGET]], TRAIN_END_FULL)
combined['garch_cond_vol'] = garch_vol
print("done.")

# Build HAR features on full combined frame (lags span train/test boundary correctly)
combined = add_har_features(combined)

FEAT_V3 = FEAT_V2_XGB + ['garch_cond_vol']  # 31 features (explicit)

train9 = combined[combined.index <= TRAIN_END_FULL].copy()
test9  = combined[combined.index >  TRAIN_END_FULL].copy()

print(f"  train9: {len(train9):,} rows  |  test9: {len(test9):,} rows")
y_te9 = test9[TARGET]


# =============================================================================
# STEP 2 — Option A XGB: load fixed model, evaluate on test 2025
# =============================================================================
print()
print("=" * 60)
print("STEP 2: Option A (XGB) — fixed model (post-Uri 2021–2024)")
print("=" * 60)

_pkl_path = PROC / 'model_xgb_reg_v3_best.pkl'
if not _pkl_path.exists():
    raise FileNotFoundError(
        f"{_pkl_path} not found. Run notebook §7.5 cell first to save the model."
    )

with open(_pkl_path, 'rb') as f:
    model_a = pickle.load(f)

_fn_a   = model_a.get_booster().feature_names
_feat_a = list(_fn_a) if _fn_a is not None else FEAT_V3

X_te9         = test9[_feat_a]
pred_a_series = pd.Series(model_a.predict(X_te9), index=test9.index)
mae_a_overall = mean_absolute_error(y_te9, pred_a_series)
r2_a_overall  = r2_score(y_te9, pred_a_series)

print(f"  Features used by model_a: {len(_feat_a)}")
print(f"  Option A (XGB) — overall MAE(log): {mae_a_overall:.4f}")
print(f"  Option A (XGB) — overall R²:       {r2_a_overall:.4f}")

test9 = test9.copy()
test9['pred_a'] = pred_a_series
test9['err_a']  = (test9[TARGET] - pred_a_series).abs()


# =============================================================================
# STEP 3 — Option C XGB: either use pre-supplied values or run rolling retrains
# =============================================================================
print()
print("=" * 60)
print("STEP 3: Option C (XGB) — sliding 4-year window, weekly retraining")
print("=" * 60)

WINDOW_DAYS = 4 * 365
week_starts  = pd.date_range('2025-01-01', '2025-12-25', freq='7D')  # 52 windows

if skip_xgb_rolling:
    mae_c_overall  = args.xgb_c_mae
    r2_c_overall   = args.xgb_c_r2
    n_c_wins_xgb   = args.xgb_c_weeks if args.xgb_c_weeks is not None else float('nan')
    n_weeks_xgb    = args.xgb_n_weeks if args.xgb_n_weeks is not None else 53
    # We don't have week-level predictions — set err_c to NaN so weekly table is skipped
    test9['err_c'] = np.nan
    print(f"  Skipping XGB rolling retrains — using supplied metrics.")
    print(f"  Option C (XGB) — overall MAE(log): {mae_c_overall:.4f}  (pre-supplied)")
    print(f"  Option C (XGB) — overall R²:       {r2_c_overall:.4f}  (pre-supplied)")
    print(f"  Weeks C beats A: {n_c_wins_xgb}/{n_weeks_xgb}  (pre-supplied)")
else:
    pred_c = pd.Series(np.nan, index=test9.index, dtype=float)
    print(f"  {len(week_starts)} weekly retrains x 4-year window (slow ~15-20 min)...")
    t0 = time.time()

    for i, ws in enumerate(week_starts):
        we       = min(ws + pd.Timedelta(days=6), pd.Timestamp('2025-12-31'))
        tr_end   = ws - pd.Timedelta(hours=1)
        tr_start = tr_end - pd.Timedelta(days=WINDOW_DAYS)

        mask_tr = (combined.index >= tr_start) & (combined.index <= tr_end)
        mask_te = (test9.index >= ws) & (test9.index <= pd.Timestamp(f'{we.date()} 23:00'))

        X_tr_c = combined.loc[mask_tr, FEAT_V3]
        y_tr_c = combined.loc[mask_tr, TARGET]
        X_te_c = test9.loc[mask_te, FEAT_V3]

        if len(X_tr_c) < 500 or len(X_te_c) == 0:
            print(f"    Week {i+1}: skipped (train={len(X_tr_c)}, test={len(X_te_c)})")
            continue

        m_c = xgb.XGBRegressor(**XGB_PARAMS)
        m_c.fit(X_tr_c, y_tr_c, verbose=False)
        pred_c.loc[mask_te] = m_c.predict(X_te_c)

        if (i + 1) % 13 == 0:
            elapsed      = (time.time() - t0) / 60
            valid_so_far = pred_c.notna()
            mae_so_far   = mean_absolute_error(
                test9.loc[valid_so_far, TARGET], pred_c[valid_so_far]
            )
            print(f"    Week {i+1}/{len(week_starts)}  ({elapsed:.1f} min)  "
                  f"running MAE(log)={mae_so_far:.4f}")

    elapsed_total = (time.time() - t0) / 60
    print(f"  Option C (XGB) complete in {elapsed_total:.1f} min.")

    valid_c       = pred_c.notna()
    mae_c_overall = mean_absolute_error(y_te9[valid_c], pred_c[valid_c])
    r2_c_overall  = r2_score(y_te9[valid_c], pred_c[valid_c])
    print(f"  Option C (XGB) — overall MAE(log): {mae_c_overall:.4f}")
    print(f"  Option C (XGB) — overall R²:       {r2_c_overall:.4f}")

    test9['pred_c'] = pred_c
    test9['err_c']  = (test9[TARGET] - pred_c).abs()

    weekly_xgb   = (test9.resample('W')
                    .agg({'err_a': 'mean', 'err_c': 'mean'})
                    .rename(columns={'err_a': 'MAE_A', 'err_c': 'MAE_C'})
                    .dropna())
    n_c_wins_xgb = (weekly_xgb['MAE_A'] > weekly_xgb['MAE_C']).sum()
    n_weeks_xgb  = len(weekly_xgb)


# =============================================================================
# STEP 4 — Option A_har: fixed HAR-Ridge on full train, predict test 2025
# =============================================================================
print()
print("=" * 60)
print("STEP 4: Option A_har — fixed HAR-Ridge (train ≤2024)")
print("=" * 60)

har_train_mask = (
    (train9.index <= TRAIN_END_FULL) &
    train9[HAR_FEATURES].notna().all(axis=1)
)
X_tr_har_a = train9.loc[har_train_mask, HAR_FEATURES]
y_tr_har_a = train9.loc[har_train_mask, TARGET]

har_test_mask = test9[HAR_FEATURES].notna().all(axis=1)
X_te_har_a    = test9.loc[har_test_mask, HAR_FEATURES]
y_te_har_a    = test9.loc[har_test_mask, TARGET]

model_a_har = make_har_pipeline()
model_a_har.fit(X_tr_har_a, y_tr_har_a)

pred_a_har_series = pd.Series(
    model_a_har.predict(X_te_har_a),
    index=X_te_har_a.index
)
mae_a_har = mean_absolute_error(y_te_har_a, pred_a_har_series)
r2_a_har  = r2_score(y_te_har_a, pred_a_har_series)

print(f"  Train rows used: {len(X_tr_har_a):,}  |  Test rows used: {len(X_te_har_a):,}")
print(f"  Ridge coefficients: rv_d1={model_a_har['ridge'].coef_[0]:.4f}  "
      f"rv_w={model_a_har['ridge'].coef_[1]:.4f}  "
      f"rv_m={model_a_har['ridge'].coef_[2]:.4f}  "
      f"intercept={model_a_har['ridge'].intercept_:.4f}")
print(f"  Option A_har — overall MAE(log): {mae_a_har:.4f}")
print(f"  Option A_har — overall R²:       {r2_a_har:.4f}")

test9['pred_a_har'] = pred_a_har_series
test9['err_a_har']  = (test9[TARGET] - pred_a_har_series).abs()


# =============================================================================
# STEP 5 — Option C_har: rolling HAR-Ridge, 52 weekly retrains
# =============================================================================
print()
print("=" * 60)
print("STEP 5: Option C_har — rolling HAR-Ridge (52 weekly retrains)")
print("=" * 60)

pred_c_har = pd.Series(np.nan, index=test9.index, dtype=float)

print(f"  {len(week_starts)} weekly retrains x 4-year window (fast — Ridge fits)...")
t0_har = time.time()

for i, ws in enumerate(week_starts):
    we       = min(ws + pd.Timedelta(days=6), pd.Timestamp('2025-12-31'))
    tr_end   = ws - pd.Timedelta(hours=1)
    tr_start = tr_end - pd.Timedelta(days=WINDOW_DAYS)

    mask_tr_h = (
        (combined.index >= tr_start) &
        (combined.index <= tr_end) &
        combined[HAR_FEATURES].notna().all(axis=1)
    )
    mask_te_h = (
        (test9.index >= ws) &
        (test9.index <= pd.Timestamp(f'{we.date()} 23:00')) &
        test9[HAR_FEATURES].notna().all(axis=1)
    )

    X_tr_h = combined.loc[mask_tr_h, HAR_FEATURES]
    y_tr_h = combined.loc[mask_tr_h, TARGET]
    X_te_h = test9.loc[mask_te_h, HAR_FEATURES]

    if len(X_tr_h) < 100 or len(X_te_h) == 0:
        print(f"    Week {i+1}: skipped (train={len(X_tr_h)}, test={len(X_te_h)})")
        continue

    m_h = make_har_pipeline()
    m_h.fit(X_tr_h, y_tr_h)
    pred_c_har.loc[mask_te_h] = m_h.predict(X_te_h)

    if (i + 1) % 13 == 0:
        elapsed      = (time.time() - t0_har) / 60
        valid_h      = pred_c_har.notna()
        mae_h_so_far = mean_absolute_error(
            test9.loc[valid_h, TARGET], pred_c_har[valid_h]
        )
        print(f"    Week {i+1}/{len(week_starts)}  ({elapsed:.1f} min)  "
              f"running MAE(log)={mae_h_so_far:.4f}")

elapsed_har = (time.time() - t0_har) / 60
print(f"  Option C_har complete in {elapsed_har:.1f} min.")

valid_c_har = pred_c_har.notna()
mae_c_har   = mean_absolute_error(y_te9[valid_c_har], pred_c_har[valid_c_har])
r2_c_har    = r2_score(y_te9[valid_c_har], pred_c_har[valid_c_har])

print(f"  Option C_har — overall MAE(log): {mae_c_har:.4f}")
print(f"  Option C_har — overall R²:       {r2_c_har:.4f}")

test9['pred_c_har'] = pred_c_har
test9['err_c_har']  = (test9[TARGET] - pred_c_har).abs()

# Weekly stats for HAR
weekly_har = (test9.resample('W')
              .agg({'err_a_har': 'mean', 'err_c_har': 'mean'})
              .rename(columns={'err_a_har': 'MAE_A_har', 'err_c_har': 'MAE_C_har'})
              .dropna())
weekly_har['drift_har'] = weekly_har['MAE_A_har'] - weekly_har['MAE_C_har']
n_c_wins_har = (weekly_har['drift_har'] > 0).sum()
n_weeks_har  = len(weekly_har)
mean_drift_har = weekly_har['drift_har'].mean()


# =============================================================================
# STEP 6 — Weekly MAE table for rolling HAR-Ridge
# =============================================================================
print()
print("=" * 60)
print("STEP 6: Weekly MAE — Rolling HAR-Ridge (Option C_har)")
print("=" * 60)
print()
print(f"  {'Week ending':>12}  {'MAE_A_har':>10}  {'MAE_C_har':>10}  {'Drift':>9}  {'Winner':>6}")

weeks_to_show = list(weekly_har.head(10).iterrows()) + list(weekly_har.tail(10).iterrows())
for wk_end, wk_row in weeks_to_show:
    winner = 'C_har' if wk_row['drift_har'] > 0 else 'A_har'
    print(f"  {wk_end.date()!s:>12}  {wk_row['MAE_A_har']:>10.4f}  {wk_row['MAE_C_har']:>10.4f}  "
          f"{wk_row['drift_har']:>+9.4f}  {winner:>6}")

print()
print(f"  Weeks C_har beats A_har: {n_c_wins_har}/{n_weeks_har}")
print(f"  Mean drift score (HAR):  {mean_drift_har:+.4f}")


# =============================================================================
# FINAL COMPARISON TABLE
# =============================================================================
print()
print("=" * 70)
print("=== ROLLING COMPARISON: Fixed vs Rolling, XGB vs HAR-Ridge ===")
print("=" * 70)
print()
print(f"  {'Model':<30}  {'MAE(log)':>9}  {'R²':>7}  {'Weeks beats fixed':>18}")
print("  " + "-" * 70)
print(f"  {'Fixed XGB (Option A)':<30}  {mae_a_overall:>9.4f}  {r2_a_overall:>7.3f}  {'—':>18}")

if skip_xgb_rolling:
    xgb_c_weeks_str = f"{n_c_wins_xgb}/{n_weeks_xgb} (pre-supplied)"
else:
    xgb_c_weeks_str = f"{n_c_wins_xgb}/{n_weeks_xgb}"
print(f"  {'Rolling XGB (Option C)':<30}  {mae_c_overall:>9.4f}  {r2_c_overall:>7.3f}  "
      f"{xgb_c_weeks_str:>18}")
print(f"  {'Fixed HAR-Ridge':<30}  {mae_a_har:>9.4f}  {r2_a_har:>7.3f}  {'—':>18}")
print(f"  {'Rolling HAR-Ridge':<30}  {mae_c_har:>9.4f}  {r2_c_har:>7.3f}  "
      f"{str(n_c_wins_har) + '/' + str(n_weeks_har):>18}")
print()
print(f"  XGB vs HAR-Ridge (fixed):   "
      f"MAE delta = {mae_a_overall - mae_a_har:+.4f}  "
      f"R² delta = {r2_a_overall - r2_a_har:+.3f}  (positive = XGB better)")
print(f"  XGB vs HAR-Ridge (rolling): "
      f"MAE delta = {mae_c_overall - mae_c_har:+.4f}  "
      f"R² delta = {r2_c_overall - r2_c_har:+.3f}  (positive = XGB better)")
print()
print("Done.")
