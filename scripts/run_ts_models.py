#!/usr/bin/env python3
"""
Run ARIMA, ARIMAX, SARIMAX walk-forward CV + final test evaluation.
Model-by-model order: complete all folds for each model before moving on.
Saves pkl artifacts and prints results for updating presentation.

Usage: python3 scripts/run_ts_models.py
"""
import numpy as np
import pandas as pd
import pickle
import warnings
from pathlib import Path
from pmdarima import auto_arima
from statsmodels.tsa.statespace.sarimax import SARIMAX as _SARIMAX
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

warnings.filterwarnings('ignore')

PROC   = Path('data/processed/ercot')
TARGET = 'log_rtm_std'

def add_engineered_features(df):
    df = df.copy()
    df['fc_net_load']     = df['fc_coast'] - df['wf_stwpf_lz_south_houston']
    df['dam_rtm_spread']  = df['dam_price_houston'] - df['rtm_mean_lag24']
    df['abs_dam_rtm_spread'] = df['dam_rtm_spread'].abs()
    df['week']            = df.index.isocalendar().week.astype(int)
    df['load_lag7d']         = df['load_houston_lag48'].shift(168)
    df['rtm_price_std_lag7d']   = df['rtm_std_lag24'].shift(168)
    df['rtm_price_mean_lag7d']  = df['rtm_mean_lag24'].shift(168)
    df['outage_fraction'] = df['total_resource_mw'] / (df['fc_system_total'] + 1)
    return df

def _reg_metrics(y_true, y_pred):
    return {
        'R² test': round(r2_score(y_true, y_pred), 3),
        'RMSE':    round(mean_squared_error(y_true, y_pred)**0.5, 3),
        'MAE':     round(mean_absolute_error(np.expm1(y_true), np.expm1(y_pred)), 3),
    }

# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading data...", flush=True)
train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')
combined = pd.concat([train_df, test_df]).sort_index()
combined = add_engineered_features(combined)

TRAIN_END = pd.Timestamp('2024-12-31 23:00')
_train_mask = combined.index <= TRAIN_END

for _col in ['rtm_std_lag24', 'load_houston_lag48']:
    if _col in combined.columns:
        combined[_col] = combined[_col].ffill()
for _col in ['dam_price_houston', 'system_lambda', 'mcpc_regup', 'mcpc_rrs', 'mcpc_nspin', 'mcpc_regdn']:
    if _col in combined.columns:
        _med = combined.loc[_train_mask, _col].median()
        combined[_col] = combined[_col].fillna(_med)
if 'total_resource_mw' in combined.columns:
    combined['total_resource_mw'] = combined['total_resource_mw'].fillna(0)

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
]
_RTM_LAG_COLS = {'rtm_mean_lag24', 'rtm_std_lag24', 'rtm_price_std_lag7d', 'rtm_price_mean_lag7d'}
FEAT_ARIMAX = [f for f in FEAT_COLS if f not in _RTM_LAG_COLS]

fold_ends  = [pd.Timestamp('2021-12-31 23:00'),
              pd.Timestamp('2022-12-31 23:00'),
              pd.Timestamp('2023-12-31 23:00')]
fold_names = ['2022', '2023', '2024']

# ── Pre-build fold data ───────────────────────────────────────────────────────
folds = []
for fold_train_end, val_year in zip(fold_ends, fold_names):
    fold_val_start = fold_train_end + pd.Timedelta(hours=1)
    fold_val_end   = pd.Timestamp(f'{val_year}-12-31 23:00')
    tr = combined.loc[combined.index <= fold_train_end].copy()
    va = combined.loc[(combined.index >= fold_val_start) & (combined.index <= fold_val_end)].copy()
    y_tr = tr[TARGET].dropna()
    y_val = va[TARGET].dropna()
    tr = tr.loc[y_tr.index]
    va = va.loc[y_val.index]
    folds.append((val_year, tr, va, y_tr, y_val))

results = {}
leaderboard = []

# ══════════════════════════════════════════════════════════════════════════════
# 1. ARIMA (univariate) — fastest
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60, flush=True)
print("1. ARIMA (univariate)", flush=True)
print("="*60, flush=True)
results['ARIMA'] = []
for val_year, tr, va, y_tr, y_val in folds:
    print(f"  Fold {val_year} (train={len(tr)}, val={len(va)})...", end=" ", flush=True)
    m = auto_arima(y_tr, d=0, max_p=3, max_q=2, seasonal=False,
                   information_criterion='aic', stepwise=True,
                   error_action='ignore', suppress_warnings=True)
    p = m.predict(n_periods=len(y_val))
    r2 = r2_score(y_val.values, p)
    results['ARIMA'].append(r2)
    print(f"ARIMA{m.order} R²={r2:.3f}", flush=True)
print(f"  CV mean: {np.mean(results['ARIMA']):.4f}", flush=True)

# Final test
print("  Final test eval...", end=" ", flush=True)
_tr = combined.loc[combined.index <= TRAIN_END]
_te = combined.loc[combined.index > TRAIN_END]
_y_tr = _tr[TARGET].dropna(); _y_te = _te[TARGET].dropna()
_arima_f = auto_arima(_y_tr, d=0, max_p=3, max_q=2, seasonal=False,
                      information_criterion='aic', stepwise=True,
                      error_action='ignore', suppress_warnings=True)
_p = _arima_f.predict(n_periods=len(_y_te))
metrics = _reg_metrics(_y_te.values, _p)
metrics['CV R²'] = round(np.mean(results['ARIMA']), 3)
metrics['Model'] = f'ARIMA{_arima_f.order}'
leaderboard.append(metrics)
print(f"ARIMA{_arima_f.order} test R²={metrics['R² test']}", flush=True)
with open(PROC / 'model_arima.pkl', 'wb') as f:
    pickle.dump(_arima_f, f)
print(f"  Saved model_arima.pkl\n", flush=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. Linear models (HAR-Ridge, Full-Ridge, HAR+Full-Ridge) — fast
# ══════════════════════════════════════════════════════════════════════════════
print("="*60, flush=True)
print("2. Linear models (Ridge variants)", flush=True)
print("="*60, flush=True)
_har_feats = ['rtm_std_lag24', 'rtm_std_lag168', 'rtm_std_lag24_7d']
results['HAR'] = []; results['Ridge'] = []; results['HAR+Full-Ridge'] = []
for val_year, tr, va, y_tr, y_val in folds:
    print(f"  Fold {val_year}...", end=" ", flush=True)
    _imp = SimpleImputer(strategy='median')
    # HAR
    _h_tr = tr[[c for c in _har_feats if c in tr.columns]].fillna(0)
    _h_va = va[[c for c in _har_feats if c in va.columns]].fillna(0)
    sc = StandardScaler(); _Xh_tr = sc.fit_transform(_h_tr); _Xh_va = sc.transform(_h_va)
    _haronly = Ridge(alpha=10.0).fit(_Xh_tr, y_tr)
    results['HAR'].append(r2_score(y_val, _haronly.predict(_Xh_va)))
    # Ridge
    _Xr_tr = _imp.fit_transform(tr[FEAT_COLS]); _Xr_va = _imp.transform(va[FEAT_COLS])
    sc2 = StandardScaler(); _Xr_tr = sc2.fit_transform(_Xr_tr); _Xr_va = sc2.transform(_Xr_va)
    _ridge = Ridge(alpha=10.0).fit(_Xr_tr, y_tr)
    results['Ridge'].append(r2_score(y_val, _ridge.predict(_Xr_va)))
    # HAR+Full
    _hf_tr = np.hstack([_Xr_tr, _Xh_tr]); _hf_va = np.hstack([_Xr_va, _Xh_va])
    _hfr = Ridge(alpha=10.0).fit(_hf_tr, y_tr)
    results['HAR+Full-Ridge'].append(r2_score(y_val, _hfr.predict(_hf_va)))
    print(f"HAR={results['HAR'][-1]:.3f} Ridge={results['Ridge'][-1]:.3f} HAR+Full={results['HAR+Full-Ridge'][-1]:.3f}", flush=True)

for name in ['HAR', 'Ridge', 'HAR+Full-Ridge']:
    print(f"  {name} CV mean: {np.mean(results[name]):.4f}", flush=True)

# Final test + pkl
print("  Final test eval + pkl save...", flush=True)
_tr_f = combined.loc[combined.index <= TRAIN_END].copy()
_te_f = combined.loc[combined.index > TRAIN_END].copy()
_tr_f = add_engineered_features(_tr_f) if 'fc_net_load' not in _tr_f.columns else _tr_f
_te_f = add_engineered_features(_te_f) if 'fc_net_load' not in _te_f.columns else _te_f
_y_tr_f = _tr_f[TARGET].dropna(); _y_te_f = _te_f[TARGET].dropna()
_tr_f = _tr_f.loc[_y_tr_f.index]; _te_f = _te_f.loc[_y_te_f.index]

# HAR-Ridge
_h_tr = _tr_f[[c for c in _har_feats if c in _tr_f.columns]].fillna(0)
_h_te = _te_f[[c for c in _har_feats if c in _te_f.columns]].fillna(0)
_har_pipe = Pipeline([('sc', StandardScaler()), ('ridge', Ridge(alpha=10.0))])
_har_pipe.fit(_h_tr.values, _y_tr_f)
_p = _har_pipe.predict(_h_te.values)
m = _reg_metrics(_y_te_f.values, _p); m['CV R²'] = round(np.mean(results['HAR']), 3); m['Model'] = 'HAR-Ridge (3 lags)'
leaderboard.append(m)
with open(PROC / 'model_har_ridge.pkl', 'wb') as f: pickle.dump(_har_pipe, f)

# Full-Ridge
_imp_f = SimpleImputer(strategy='median')
_Xr_tr = _imp_f.fit_transform(_tr_f[FEAT_COLS]); _Xr_te = _imp_f.transform(_te_f[FEAT_COLS])
_full_pipe = Pipeline([('sc', StandardScaler()), ('ridge', Ridge(alpha=10.0))])
_full_pipe.fit(_Xr_tr, _y_tr_f)
_p = _full_pipe.predict(_Xr_te)
m = _reg_metrics(_y_te_f.values, _p); m['CV R²'] = round(np.mean(results['Ridge']), 3); m['Model'] = 'Full-Ridge (29 feat)'
leaderboard.append(m)
with open(PROC / 'model_full_ridge.pkl', 'wb') as f: pickle.dump({'pipeline': _full_pipe, 'imputer': _imp_f, 'features': FEAT_COLS}, f)

# HAR+Full-Ridge
_hf_tr = np.hstack([_Xr_tr, _h_tr.values]); _hf_te = np.hstack([_Xr_te, _h_te.values])
_hf_pipe = Pipeline([('sc', StandardScaler()), ('ridge', Ridge(alpha=10.0))])
_hf_pipe.fit(_hf_tr, _y_tr_f)
_p = _hf_pipe.predict(_hf_te)
m = _reg_metrics(_y_te_f.values, _p); m['CV R²'] = round(np.mean(results['HAR+Full-Ridge']), 3); m['Model'] = 'HAR+Full-Ridge (32 feat)'
leaderboard.append(m)
with open(PROC / 'model_har_full_ridge.pkl', 'wb') as f: pickle.dump({'pipeline': _hf_pipe, 'imputer': _imp_f}, f)

for row in leaderboard[-3:]:
    print(f"    {row['Model']:<30} test R²={row['R² test']}  RMSE={row['RMSE']}  MAE={row['MAE']}", flush=True)
print(f"  Saved model_har_ridge.pkl, model_full_ridge.pkl, model_har_full_ridge.pkl\n", flush=True)

# ══════════════════════════════════════════════════════════════════════════════
# 3. ARIMAX (25 exog features) — slow
# ══════════════════════════════════════════════════════════════════════════════
print("="*60, flush=True)
print("3. ARIMAX (25 exog features)", flush=True)
print("="*60, flush=True)
results['ARIMAX'] = []
for val_year, tr, va, y_tr, y_val in folds:
    print(f"  Fold {val_year} (train={len(tr)})...", end=" ", flush=True)
    _imp = SimpleImputer(strategy='median')
    _X_tr = _imp.fit_transform(tr[FEAT_ARIMAX]); _X_va = _imp.transform(va[FEAT_ARIMAX])
    m = auto_arima(y_tr, X=_X_tr, d=0, start_p=0, max_p=3, start_q=0, max_q=2,
                   seasonal=False, information_criterion='aic', stepwise=True,
                   error_action='ignore', suppress_warnings=True)
    p = m.predict(n_periods=len(y_val), X=_X_va)
    r2 = r2_score(y_val.values, p)
    results['ARIMAX'].append(r2)
    print(f"ARIMAX{m.order} R²={r2:.3f}", flush=True)
print(f"  CV mean: {np.mean(results['ARIMAX']):.4f}", flush=True)

# Final test
print("  Final test eval...", end=" ", flush=True)
_imp_ax = SimpleImputer(strategy='median')
_X_tr_ax = _imp_ax.fit_transform(_tr_f[FEAT_ARIMAX]); _X_te_ax = _imp_ax.transform(_te_f[FEAT_ARIMAX])
_arimax_f = auto_arima(_y_tr_f, X=_X_tr_ax, d=0, start_p=0, max_p=3, start_q=0, max_q=2,
                       seasonal=False, information_criterion='aic', stepwise=True,
                       error_action='ignore', suppress_warnings=True)
_p = _arimax_f.predict(n_periods=len(_y_te_f), X=_X_te_ax)
m = _reg_metrics(_y_te_f.values, _p); m['CV R²'] = round(np.mean(results['ARIMAX']), 3)
m['Model'] = f'ARIMAX{_arimax_f.order} (25 feat)'
leaderboard.append(m)
print(f"{m['Model']} test R²={m['R² test']}", flush=True)
with open(PROC / 'model_arimax.pkl', 'wb') as f:
    pickle.dump({'model': _arimax_f, 'imputer': _imp_ax, 'features': FEAT_ARIMAX}, f)
print(f"  Saved model_arimax.pkl\n", flush=True)

# ══════════════════════════════════════════════════════════════════════════════
# 4. SARIMAX (m=24) — slowest
# ══════════════════════════════════════════════════════════════════════════════
print("="*60, flush=True)
print("4. SARIMAX(1,0,1)(1,0,1,24) — 25 exog features", flush=True)
print("="*60, flush=True)
results['SARIMAX'] = []
for val_year, tr, va, y_tr, y_val in folds:
    print(f"  Fold {val_year} (train={len(tr)})...", end=" ", flush=True)
    _imp = SimpleImputer(strategy='median')
    _X_tr = _imp.fit_transform(tr[FEAT_ARIMAX]); _X_va = _imp.transform(va[FEAT_ARIMAX])
    try:
        _sm = _SARIMAX(y_tr, exog=_X_tr, order=(1,0,1), seasonal_order=(1,0,1,24),
                       enforce_stationarity=False, enforce_invertibility=False)
        _sr = _sm.fit(disp=False, maxiter=200, start_params=np.zeros(_sm.k_params))
        _sp = _sr.forecast(steps=len(y_val), exog=_X_va)
        r2 = r2_score(y_val.values, _sp)
        results['SARIMAX'].append(r2)
        print(f"R²={r2:.3f}", flush=True)
    except Exception as e:
        results['SARIMAX'].append(float('nan'))
        print(f"FAILED: {e}", flush=True)
print(f"  CV mean: {np.nanmean(results['SARIMAX']):.4f}", flush=True)

# Final test
print("  Final test eval...", end=" ", flush=True)
try:
    _sm_f = _SARIMAX(_y_tr_f, exog=_X_tr_ax, order=(1,0,1), seasonal_order=(1,0,1,24),
                     enforce_stationarity=False, enforce_invertibility=False)
    _sr_f = _sm_f.fit(disp=False, maxiter=200, start_params=np.zeros(_sm_f.k_params))
    _p = _sr_f.forecast(steps=len(_y_te_f), exog=_X_te_ax)
    m = _reg_metrics(_y_te_f.values, _p); m['CV R²'] = round(np.nanmean(results['SARIMAX']), 3)
    m['Model'] = 'SARIMAX(1,0,1)(1,0,1,24)'
    leaderboard.append(m)
    print(f"test R²={m['R² test']}", flush=True)
    with open(PROC / 'model_sarimax.pkl', 'wb') as f:
        pickle.dump({'model': _sr_f, 'imputer': _imp_ax, 'features': FEAT_ARIMAX}, f)
    print(f"  Saved model_sarimax.pkl\n", flush=True)
except Exception as e:
    print(f"FAILED: {e}\n", flush=True)

# ══════════════════════════════════════════════════════════════════════════════
# FINAL LEADERBOARD
# ══════════════════════════════════════════════════════════════════════════════
print("="*72, flush=True)
print(f"{'Model':<36} {'CV R²':>7} {'Test R²':>8} {'RMSE':>7} {'MAE($/MWh)':>11}", flush=True)
print("-" * 72, flush=True)
for row in sorted(leaderboard, key=lambda x: x['R² test'], reverse=True):
    print(f"  {row['Model']:<34} {row['CV R²']:>7.3f} {row['R² test']:>8.3f} {row['RMSE']:>7.3f} {row['MAE']:>11.3f}", flush=True)
print("\nDone. Update presentation.ipynb with these results.", flush=True)
