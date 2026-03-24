#!/usr/bin/env python3
"""§7.2 Final Test Evaluation — all linear/statistical models on 2025 test set.
Standalone script. Injects output into notebook cell 26."""
import json
import gc
import os
import numpy as np
import pandas as pd
import pickle
import nbformat
from pathlib import Path
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

os.chdir("/Users/cielo69/github/spring-2026-electricity-TX")

PROC = Path('data/processed/ercot')
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

# Load data
train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')
combined = pd.concat([train_df, test_df]).sort_index()
combined = add_engineered_features(combined)
tr_final = combined.loc[train_df.index].copy()
te_final = combined.loc[test_df.index].copy()
del train_df, test_df, combined; gc.collect()

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

y_tr = tr_final[TARGET].dropna()
y_te = te_final[TARGET].dropna()
tr_final = tr_final.loc[y_tr.index]
te_final = te_final.loc[y_te.index]

# Load CV results
all_results = {}
with open('/tmp/72_fast.json') as f:
    all_results.update(json.load(f))
with open('/tmp/72_arima.json') as f:
    all_results['ARIMA'] = json.load(f)
with open('/tmp/72_arimax.json') as f:
    all_results['ARIMAX'] = json.load(f)
all_results['SARIMAX'] = [0.065, 0.257, 0.084]

def _ridge_pipe():
    return Pipeline([('sc', StandardScaler()), ('ridge', Ridge(alpha=10.0))])

def _reg_metrics(y_true, y_pred):
    return {
        'R2_test': round(r2_score(y_true, y_pred), 3),
        'RMSE':    round(mean_squared_error(y_true, y_pred)**0.5, 3),
        'MAE':     round(mean_absolute_error(np.expm1(y_true), np.expm1(y_pred)), 3),
    }

leaderboard = []

# ── HAR-Ridge (3 TS lags) ──
_har_feats = ['rtm_std_lag24', 'rtm_std_lag168', 'rtm_std_lag24_7d']
_har_tr = tr_final[[c for c in _har_feats if c in tr_final.columns]].fillna(0)
_har_te = te_final[[c for c in _har_feats if c in te_final.columns]].fillna(0)
m = _ridge_pipe().fit(_har_tr.values, y_tr.values)
p = m.predict(_har_te.values)
leaderboard.append({'Model': 'HAR-Ridge (3 lags)',
    **_reg_metrics(y_te.values, p), 'CV_R2': round(np.mean(all_results.get('HAR', [float('nan')])), 3)})
with open(PROC / 'model_har_ridge.pkl', 'wb') as f: pickle.dump(m, f)
print("Saved model_har_ridge.pkl")

# ── Full-Ridge (29 features) ──
imp = SimpleImputer(strategy='median')
X_tr = imp.fit_transform(tr_final[FEAT_COLS])
X_te = imp.transform(te_final[FEAT_COLS])
m = _ridge_pipe().fit(X_tr, y_tr.values)
p = m.predict(X_te)
leaderboard.append({'Model': 'Full-Ridge (29 feat)',
    **_reg_metrics(y_te.values, p), 'CV_R2': round(np.mean(all_results.get('Ridge', [float('nan')])), 3)})
with open(PROC / 'model_full_ridge.pkl', 'wb') as f: pickle.dump((m, imp), f)
print("Saved model_full_ridge.pkl")

# ── HAR+Full-Ridge (32 features) ──
_hf_tr = np.hstack([X_tr, _har_tr.values])
_hf_te = np.hstack([X_te, _har_te.values])
m = _ridge_pipe().fit(_hf_tr, y_tr.values)
p = m.predict(_hf_te)
leaderboard.append({'Model': 'HAR+Full-Ridge (32 feat)',
    **_reg_metrics(y_te.values, p), 'CV_R2': round(np.mean(all_results.get('HAR+Full-Ridge', [float('nan')])), 3)})
with open(PROC / 'model_har_full_ridge.pkl', 'wb') as f: pickle.dump((m, imp), f)
print("Saved model_har_full_ridge.pkl")

# ── ARIMA ──
print("Fitting ARIMA on full train...")
from pmdarima import auto_arima
y_tr_ts = y_tr.values.copy()
y_te_ts = y_te.values.copy()

arima = auto_arima(y_tr_ts, d=0, start_p=0, max_p=3, start_q=0, max_q=2,
    seasonal=False, information_criterion='aic', stepwise=True,
    error_action='ignore', suppress_warnings=True)
p = arima.predict(n_periods=len(y_te_ts))
leaderboard.append({'Model': f'ARIMA{arima.order}',
    **_reg_metrics(y_te_ts, p), 'CV_R2': round(np.mean(all_results['ARIMA']), 3)})
with open(PROC / 'model_arima.pkl', 'wb') as f: pickle.dump(arima, f)
print(f"Saved model_arima.pkl (order={arima.order})")
del arima; gc.collect()

# ── ARIMAX ──
print("Fitting ARIMAX on full train...")
imp_ax = SimpleImputer(strategy='median')
X_tr_ax = imp_ax.fit_transform(tr_final[FEAT_ARIMAX])
X_te_ax = imp_ax.transform(te_final[FEAT_ARIMAX])
arimax = auto_arima(y_tr_ts, X=X_tr_ax, d=0, start_p=0, max_p=3, start_q=0, max_q=2,
    seasonal=False, information_criterion='aic', stepwise=True,
    error_action='ignore', suppress_warnings=True)
p = arimax.predict(n_periods=len(y_te_ts), X=X_te_ax)
leaderboard.append({'Model': f'ARIMAX{arimax.order} (25 feat)',
    **_reg_metrics(y_te_ts, p), 'CV_R2': round(np.mean(all_results['ARIMAX']), 3)})
with open(PROC / 'model_arimax.pkl', 'wb') as f: pickle.dump((arimax, imp_ax), f)
print(f"Saved model_arimax.pkl (order={arimax.order})")
del arimax; gc.collect()

# ── SARIMAX — use pre-computed test results ──
leaderboard.append({'Model': 'SARIMAX(1,0,1)(1,0,1,24)',
    'R2_test': 0.154, 'RMSE': 0.785, 'MAE': 4.521,
    'CV_R2': round(np.mean(all_results['SARIMAX']), 3)})
print("SARIMAX test metrics from pre-computed run (R²=0.154)")

# ── Print leaderboard ──
print(f"\n{'='*72}")
print("§7.2 Linear/Statistical Model Leaderboard (test = 2025)")
print(f"{'='*72}")
print(f"{'Model':<36} {'CV R²':>7} {'Test R²':>8} {'RMSE':>7} {'MAE($/MWh)':>11}")
print("-" * 72)
for row in sorted(leaderboard, key=lambda x: x['R2_test'], reverse=True):
    print(f"  {row['Model']:<34} {row['CV_R2']:>7.3f} {row['R2_test']:>8.3f} {row['RMSE']:>7.3f} {row['MAE']:>11.3f}")
print("\n-> Best linear model feeds into unified leaderboard alongside XGBoost v3.")

# Save leaderboard JSON
json.dump(leaderboard, open('/tmp/72_test_leaderboard.json', 'w'), default=str)
