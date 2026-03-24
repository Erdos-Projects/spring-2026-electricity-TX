#!/usr/bin/env python3
"""Run a single SARIMAX fold with minimal memory footprint."""
import sys
import json
import gc
import os

os.chdir("/Users/cielo69/github/spring-2026-electricity-TX")

fold_idx = int(sys.argv[1])  # 0, 1, or 2
fold_years = ['2022', '2023', '2024']
val_year = fold_years[fold_idx]

import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import r2_score

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

# Load and prepare data
train_df = pd.read_parquet(PROC / 'train_features.parquet')
test_df  = pd.read_parquet(PROC / 'test_features.parquet')
combined = pd.concat([train_df, test_df]).sort_index()
combined = add_engineered_features(combined)
del train_df, test_df; gc.collect()

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

fold_ends = [pd.Timestamp('2021-12-31 23:00'),
             pd.Timestamp('2022-12-31 23:00'),
             pd.Timestamp('2023-12-31 23:00')]

fold_train_end = fold_ends[fold_idx]
fold_val_start = fold_train_end + pd.Timedelta(hours=1)
fold_val_end   = pd.Timestamp(f'{val_year}-12-31 23:00')

tr = combined.loc[combined.index <= fold_train_end].copy()
va = combined.loc[(combined.index >= fold_val_start) & (combined.index <= fold_val_end)].copy()

y_tr = tr[TARGET].dropna()
y_val = va[TARGET].dropna()
tr = tr.loc[y_tr.index]
va = va.loc[y_val.index]

# Extract exogenous arrays before deleting combined
X_tr = tr[FEAT_ARIMAX].fillna(0).values.copy()
X_va = va[FEAT_ARIMAX].fillna(0).values.copy()
y_tr_arr = y_tr.values.copy()
y_val_arr = y_val.values.copy()

# Free all DataFrames
del combined, tr, va, y_tr, y_val
gc.collect()

print(f"Fold {val_year}: train={len(y_tr_arr)} val={len(y_val_arr)}", flush=True)
print(f"  fitting SARIMAX (m=24)...", flush=True)

from pmdarima import auto_arima

mdl = auto_arima(
    y_tr_arr, X=X_tr, d=0,
    max_p=2, max_q=1, max_P=1, max_Q=1, D=0,
    start_p=0, start_q=0, start_P=0, start_Q=0,
    seasonal=True, m=24,
    information_criterion='aic', stepwise=True,
    error_action='ignore', suppress_warnings=True)

pred = mdl.predict(n_periods=len(y_val_arr), X=X_va)
r2 = r2_score(y_val_arr, pred)

print(f"  {val_year} SARIMAX order: {mdl.order} x {mdl.seasonal_order}  R²={r2:.3f}", flush=True)

json.dump({"r2": r2, "order": str(mdl.order), "seasonal": str(mdl.seasonal_order)},
          open(f'/tmp/72_sarimax_fold{fold_idx}.json', 'w'))

print("Done.", flush=True)
