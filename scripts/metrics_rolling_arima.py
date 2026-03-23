"""
metrics_rolling_arima.py — Rolling ARIMA and ARIMAX with 52 weekly retrains.

Same setup/feature engineering/imputation/GARCH as metrics_rolling.py.
SARIMAX is skipped (too slow for weekly retrains).

Run from project root with:
    conda run -n erdos_ds_environment python scripts/metrics_rolling_arima.py

Output redirected to /tmp/rolling_arima_out.txt when run in background.
"""

import warnings
import time
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from arch import arch_model
from pmdarima import auto_arima

# ── Paths and constants ───────────────────────────────────────────────────────
PROC           = Path('data/processed/ercot')
TARGET         = 'log_rtm_std'
TRAIN_END_FULL = pd.Timestamp('2024-12-31 23:00')

# ── Feature sets ──────────────────────────────────────────────────────────────
FEAT_COLS = [
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
]  # 29 features — FEAT_V2 (linear models)

_RTM_LAG_COLS = {'rtm_mean_lag24', 'rtm_std_lag24', 'rtm_price_std_lag7d', 'rtm_price_mean_lag7d'}
FEAT_ARIMAX = [f for f in FEAT_COLS if f not in _RTM_LAG_COLS]  # 25 features


# ── Helper functions ──────────────────────────────────────────────────────────

def add_engineered_features(df):
    """Add 7 engineered features. Called on combined train+test DataFrame."""
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
# STEP 1 — Load data and build combined feature matrix
# =============================================================================
print("=" * 60)
print("STEP 1: Loading data and building feature matrix")
print("=" * 60)

train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')

print(f"  train_features: {train_df.shape}  ({train_df.index.min().date()} -> {train_df.index.max().date()})")
print(f"  test_features:  {test_df.shape}  ({test_df.index.min().date()} -> {test_df.index.max().date()})")

# Combine, engineer features, impute
combined = pd.concat([train_df, test_df]).sort_index()
combined  = add_engineered_features(combined)
combined  = impute_combined(combined, TRAIN_END_FULL)

print(f"  combined shape after engineering + imputation: {combined.shape}")
print(f"  FEAT_ARIMAX: {len(FEAT_ARIMAX)} features")
print(f"  Excluded RTM lag cols: {sorted(_RTM_LAG_COLS)}")

# Split back into train and test
train_full = combined[combined.index <= TRAIN_END_FULL].copy()
test_full  = combined[combined.index >  TRAIN_END_FULL].copy()

print(f"  train_full: {len(train_full):,} rows  |  test_full: {len(test_full):,} rows")


# =============================================================================
# STEP 2 — Fixed ARIMA and ARIMAX baselines (fit once on full train)
# =============================================================================
print()
print("=" * 60)
print("STEP 2: Fixed ARIMA and ARIMAX — fit on train <=2024, predict 2025")
print("=" * 60)

y_tr_full = train_full[TARGET].dropna()
y_te_full = test_full[TARGET].dropna()

# Fixed ARIMA
print("  Fitting fixed ARIMA on full train (may take ~1-3 min)...")
t_start = time.time()
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    fixed_arima = auto_arima(
        y_tr_full, d=0, max_p=3, max_q=2, start_p=0, start_q=0,
        information_criterion='aic', stepwise=True,
        error_action='ignore', suppress_warnings=True
    )
print(f"  Fixed ARIMA order: {fixed_arima.order}  ({(time.time()-t_start)/60:.1f} min)")

pred_fixed_arima = fixed_arima.predict(n_periods=len(y_te_full))
mae_fixed_arima  = mean_absolute_error(y_te_full.values, pred_fixed_arima)
r2_fixed_arima   = r2_score(y_te_full.values, pred_fixed_arima)
print(f"  Fixed ARIMA — MAE(log): {mae_fixed_arima:.4f}  R²: {r2_fixed_arima:.4f}")

# Fixed ARIMAX
print("  Fitting fixed ARIMAX on full train (may take ~1-3 min)...")
imp_fixed = SimpleImputer(strategy='median')
X_tr_ax_full = imp_fixed.fit_transform(train_full.loc[y_tr_full.index, FEAT_ARIMAX])
X_te_ax_full = imp_fixed.transform(test_full.loc[y_te_full.index, FEAT_ARIMAX])

t_start = time.time()
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    fixed_arimax = auto_arima(
        y_tr_full, X=X_tr_ax_full, d=0, max_p=3, max_q=2, start_p=0, start_q=0,
        information_criterion='aic', stepwise=True,
        error_action='ignore', suppress_warnings=True
    )
print(f"  Fixed ARIMAX order: {fixed_arimax.order}  ({(time.time()-t_start)/60:.1f} min)")

pred_fixed_arimax = fixed_arimax.predict(n_periods=len(y_te_full), X=X_te_ax_full)
mae_fixed_arimax  = mean_absolute_error(y_te_full.values, pred_fixed_arimax)
r2_fixed_arimax   = r2_score(y_te_full.values, pred_fixed_arimax)
print(f"  Fixed ARIMAX — MAE(log): {mae_fixed_arimax:.4f}  R²: {r2_fixed_arimax:.4f}")


# =============================================================================
# STEP 3 — Rolling ARIMA: 52 weekly retrains (4-year sliding window)
# =============================================================================
print()
print("=" * 60)
print("STEP 3: Rolling ARIMA — 52 weekly retrains (4-year window)")
print("=" * 60)

WINDOW_DAYS = 4 * 365
week_starts = pd.date_range('2025-01-01', '2025-12-25', freq='7D')
pred_rolling_arima = pd.Series(np.nan, index=test_full.index, dtype=float)

print(f"  {len(week_starts)} weekly retrains x 4-year window...")
t0 = time.time()

for i, ws in enumerate(week_starts):
    we       = min(ws + pd.Timedelta(days=6), pd.Timestamp('2025-12-31'))
    tr_end   = ws - pd.Timedelta(hours=1)
    tr_start = tr_end - pd.Timedelta(days=WINDOW_DAYS)

    mask_tr = (combined.index >= tr_start) & (combined.index <= tr_end)
    mask_te = (test_full.index >= ws) & (test_full.index <= pd.Timestamp(f'{we.date()} 23:00'))

    y_tr_w = combined.loc[mask_tr, TARGET].dropna()
    y_te_w_idx = test_full.loc[mask_te].index
    y_te_w = test_full.loc[mask_te, TARGET].dropna()

    if len(y_tr_w) < 500 or len(y_te_w) == 0:
        print(f"    Week {i+1}: skipped (train={len(y_tr_w)}, test={len(y_te_w)})")
        continue

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        m_ar = auto_arima(
            y_tr_w, d=0, max_p=3, max_q=2, start_p=0, start_q=0,
            information_criterion='aic', stepwise=True,
            error_action='ignore', suppress_warnings=True
        )

    preds = m_ar.predict(n_periods=len(y_te_w))
    pred_rolling_arima.loc[y_te_w.index] = preds

    # Progress every 13 weeks
    if (i + 1) % 13 == 0:
        elapsed = (time.time() - t0) / 60
        valid_so_far = pred_rolling_arima.notna()
        mae_so_far = mean_absolute_error(
            test_full.loc[valid_so_far, TARGET], pred_rolling_arima[valid_so_far]
        )
        print(f"    Week {i+1}/{len(week_starts)}  ({elapsed:.1f} min)  "
              f"running MAE(log)={mae_so_far:.4f}")

elapsed_total = (time.time() - t0) / 60
print(f"  Rolling ARIMA complete in {elapsed_total:.1f} min.")

valid_ra = pred_rolling_arima.notna()
mae_rolling_arima = mean_absolute_error(
    test_full.loc[valid_ra, TARGET], pred_rolling_arima[valid_ra]
)
r2_rolling_arima = r2_score(
    test_full.loc[valid_ra, TARGET], pred_rolling_arima[valid_ra]
)
print(f"  Rolling ARIMA — MAE(log): {mae_rolling_arima:.4f}  R²: {r2_rolling_arima:.4f}")

# Per-week MAE for rolling ARIMA vs fixed ARIMA
test_full_ra = test_full.copy()
test_full_ra['pred_ra'] = pred_rolling_arima
test_full_ra['err_fa']  = (test_full_ra[TARGET] - mae_fixed_arima).abs()  # placeholder
# Use actual fixed predictions for weekly comparison
pred_fixed_arima_series = pd.Series(
    fixed_arima.predict(n_periods=len(y_te_full)),
    index=y_te_full.index
)
test_full_ra['err_fa'] = (test_full_ra[TARGET] - pred_fixed_arima_series).abs()
test_full_ra['err_ra'] = (test_full_ra[TARGET] - pred_rolling_arima).abs()

weekly_arima = (test_full_ra.resample('W')
                .agg({'err_fa': 'mean', 'err_ra': 'mean'})
                .rename(columns={'err_fa': 'MAE_Fixed', 'err_ra': 'MAE_Rolling'})
                .dropna())
weekly_arima['beats_fixed'] = weekly_arima['MAE_Rolling'] < weekly_arima['MAE_Fixed']
n_beats_arima = weekly_arima['beats_fixed'].sum()


# =============================================================================
# STEP 4 — Rolling ARIMAX: 52 weekly retrains (4-year sliding window)
# =============================================================================
print()
print("=" * 60)
print("STEP 4: Rolling ARIMAX — 52 weekly retrains (4-year window)")
print("=" * 60)

pred_rolling_arimax = pd.Series(np.nan, index=test_full.index, dtype=float)

print(f"  {len(week_starts)} weekly retrains x 4-year window...")
t0 = time.time()

for i, ws in enumerate(week_starts):
    we       = min(ws + pd.Timedelta(days=6), pd.Timestamp('2025-12-31'))
    tr_end   = ws - pd.Timedelta(hours=1)
    tr_start = tr_end - pd.Timedelta(days=WINDOW_DAYS)

    mask_tr = (combined.index >= tr_start) & (combined.index <= tr_end)
    mask_te = (test_full.index >= ws) & (test_full.index <= pd.Timestamp(f'{we.date()} 23:00'))

    y_tr_w = combined.loc[mask_tr, TARGET].dropna()
    tr_w   = combined.loc[mask_tr].loc[y_tr_w.index]
    y_te_w = test_full.loc[mask_te, TARGET].dropna()
    te_w   = test_full.loc[mask_te].loc[y_te_w.index]

    if len(y_tr_w) < 500 or len(y_te_w) == 0:
        print(f"    Week {i+1}: skipped (train={len(y_tr_w)}, test={len(y_te_w)})")
        continue

    imp_ax = SimpleImputer(strategy='median')
    X_tr_ax = imp_ax.fit_transform(tr_w[FEAT_ARIMAX])
    X_te_ax = imp_ax.transform(te_w[FEAT_ARIMAX])

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        m_ax = auto_arima(
            y_tr_w, X=X_tr_ax, d=0, max_p=3, max_q=2, start_p=0, start_q=0,
            information_criterion='aic', stepwise=True,
            error_action='ignore', suppress_warnings=True
        )

    preds = m_ax.predict(n_periods=len(y_te_w), X=X_te_ax)
    pred_rolling_arimax.loc[y_te_w.index] = preds

    # Progress every 13 weeks
    if (i + 1) % 13 == 0:
        elapsed = (time.time() - t0) / 60
        valid_so_far = pred_rolling_arimax.notna()
        mae_so_far = mean_absolute_error(
            test_full.loc[valid_so_far, TARGET], pred_rolling_arimax[valid_so_far]
        )
        print(f"    Week {i+1}/{len(week_starts)}  ({elapsed:.1f} min)  "
              f"running MAE(log)={mae_so_far:.4f}")

elapsed_total = (time.time() - t0) / 60
print(f"  Rolling ARIMAX complete in {elapsed_total:.1f} min.")

valid_rx = pred_rolling_arimax.notna()
mae_rolling_arimax = mean_absolute_error(
    test_full.loc[valid_rx, TARGET], pred_rolling_arimax[valid_rx]
)
r2_rolling_arimax = r2_score(
    test_full.loc[valid_rx, TARGET], pred_rolling_arimax[valid_rx]
)
print(f"  Rolling ARIMAX — MAE(log): {mae_rolling_arimax:.4f}  R²: {r2_rolling_arimax:.4f}")

# Per-week MAE for rolling ARIMAX vs fixed ARIMAX
pred_fixed_arimax_series = pd.Series(
    fixed_arimax.predict(n_periods=len(y_te_full), X=X_te_ax_full),
    index=y_te_full.index
)
test_full_rx = test_full.copy()
test_full_rx['err_fx'] = (test_full_rx[TARGET] - pred_fixed_arimax_series).abs()
test_full_rx['err_rx'] = (test_full_rx[TARGET] - pred_rolling_arimax).abs()

weekly_arimax = (test_full_rx.resample('W')
                 .agg({'err_fx': 'mean', 'err_rx': 'mean'})
                 .rename(columns={'err_fx': 'MAE_Fixed', 'err_rx': 'MAE_Rolling'})
                 .dropna())
weekly_arimax['beats_fixed'] = weekly_arimax['MAE_Rolling'] < weekly_arimax['MAE_Fixed']
n_beats_arimax = weekly_arimax['beats_fixed'].sum()


# =============================================================================
# STEP 5 — Per-week MAE tables
# =============================================================================
print()
print("=" * 60)
print("STEP 5: Per-week MAE tables")
print("=" * 60)

print()
print("  ARIMA — Weekly MAE (Fixed vs Rolling):")
print(f"  {'Week ending':>12}  {'Fixed':>8}  {'Rolling':>8}  {'Delta':>8}  {'Winner':>7}")
for wk_end, wk_row in weekly_arima.iterrows():
    delta  = wk_row['MAE_Rolling'] - wk_row['MAE_Fixed']
    winner = 'Rolling' if wk_row['beats_fixed'] else 'Fixed'
    print(f"  {wk_end.date()!s:>12}  {wk_row['MAE_Fixed']:>8.4f}  {wk_row['MAE_Rolling']:>8.4f}  "
          f"{delta:>+8.4f}  {winner:>7}")

print()
print("  ARIMAX — Weekly MAE (Fixed vs Rolling):")
print(f"  {'Week ending':>12}  {'Fixed':>8}  {'Rolling':>8}  {'Delta':>8}  {'Winner':>7}")
for wk_end, wk_row in weekly_arimax.iterrows():
    delta  = wk_row['MAE_Rolling'] - wk_row['MAE_Fixed']
    winner = 'Rolling' if wk_row['beats_fixed'] else 'Fixed'
    print(f"  {wk_end.date()!s:>12}  {wk_row['MAE_Fixed']:>8.4f}  {wk_row['MAE_Rolling']:>8.4f}  "
          f"{delta:>+8.4f}  {winner:>7}")


# =============================================================================
# FINAL SUMMARY
# =============================================================================
print()
print("=" * 60)
print("=== ROLLING COMPARISON ===")
print("=" * 60)
print()
print(f"  {'Model':<30}  {'MAE(log)':>9}  {'R²':>7}  {'Weeks beats fixed'}")
print("  " + "-" * 65)
print(f"  {'Fixed ARIMA':<30}  {mae_fixed_arima:>9.4f}  {r2_fixed_arima:>7.3f}  —")
print(f"  {'Rolling ARIMA':<30}  {mae_rolling_arima:>9.4f}  {r2_rolling_arima:>7.3f}  "
      f"{n_beats_arima}/{len(weekly_arima)}")
print(f"  {'Fixed ARIMAX':<30}  {mae_fixed_arimax:>9.4f}  {r2_fixed_arimax:>7.3f}  —")
print(f"  {'Rolling ARIMAX':<30}  {mae_rolling_arimax:>9.4f}  {r2_rolling_arimax:>7.3f}  "
      f"{n_beats_arimax}/{len(weekly_arimax)}")
print()
print("Done.")
