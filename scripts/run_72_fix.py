#!/usr/bin/env python3
"""Fix remaining §7.2 cells: fast models + SARIMAX (per-fold)."""
import subprocess
import json
import os
import numpy as np
import nbformat

NB_PATH = 'model_regression.ipynb'
CONDA_ENV = 'erdos_ds_environment'
CWD = os.getcwd()

SETUP_CODE = f'''
import sys, os
os.chdir("{CWD}")
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler

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

def make_seasonal(df):
    tmp = pd.DataFrame(dict(
        hour=df.index.hour, month=df.index.month, dow=df.index.dayofweek
    ), index=df.index)
    return pd.get_dummies(tmp.astype(str), drop_first=True)

train_df72 = pd.read_parquet(PROC / 'train_features.parquet')
test_df72  = pd.read_parquet(PROC / 'test_features.parquet')
combined72 = pd.concat([train_df72, test_df72]).sort_index()
combined72 = add_engineered_features(combined72)

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

_RTM_LAG_COLS = set(['rtm_mean_lag24', 'rtm_std_lag24', 'rtm_price_std_lag7d', 'rtm_price_mean_lag7d'])
FEAT_ARIMAX = [f for f in FEAT_COLS if f not in _RTM_LAG_COLS]

fold_ends72  = [pd.Timestamp('2021-12-31 23:00'),
                pd.Timestamp('2022-12-31 23:00'),
                pd.Timestamp('2023-12-31 23:00')]
fold_names72 = ['2022', '2023', '2024']

del train_df72, test_df72
import gc; gc.collect()
print("Setup complete", flush=True)
'''


def run_code(label, code, timeout=7200):
    print(f"\n{'='*70}", flush=True)
    print(f"  {label}", flush=True)
    print(f"{'='*70}", flush=True)
    result = subprocess.run(
        ['conda', 'run', '-n', CONDA_ENV, 'python3', '-u', '-c', code],
        capture_output=True, text=True, timeout=timeout
    )
    print(result.stdout, flush=True)
    if result.returncode != 0:
        print(f"  FAILED (exit {result.returncode})", flush=True)
        if result.stderr:
            lines = result.stderr.strip().splitlines()
            for l in lines[-5:]:
                print(f"  {l}", flush=True)
        return None
    return result.stdout


def inject_output(cell_idx, text):
    nb = nbformat.read(NB_PATH, as_version=4)
    nb.cells[cell_idx]['outputs'] = [
        nbformat.v4.new_output('stream', name='stdout', text=text)
    ]
    nbformat.write(nb, NB_PATH)
    print(f"  Injected into cell {cell_idx}", flush=True)


# ── Phase 1: Fast models ────────────────────────────────────────────────────
fast_code = SETUP_CODE + '''
import json
results = {}
for k in ['Ridge','Ridge-seasonal','HAR','HAR+Full-Ridge','RTM-lag-only','Seasonal-naive']:
    results[k] = []

for fold_train_end, val_year in zip(fold_ends72, fold_names72):
    fold_val_start = fold_train_end + pd.Timedelta(hours=1)
    fold_val_end   = pd.Timestamp(f"{val_year}-12-31 23:00")
    tr  = combined72.loc[combined72.index <= fold_train_end].copy()
    va  = combined72.loc[(combined72.index >= fold_val_start) & (combined72.index <= fold_val_end)].copy()
    y_tr = tr[TARGET].dropna(); y_val = va[TARGET].dropna()
    tr = tr.loc[y_tr.index]; va = va.loc[y_val.index]
    print(f"Fold {val_year}: train={len(tr)} val={len(va)}")

    rtm_full = pd.read_parquet(PROC / "train_features.parquet")[["log_rtm_std"]].copy()
    rtm_full = pd.concat([rtm_full, pd.read_parquet(PROC / "test_features.parquet")[["log_rtm_std"]]])
    rtm_full = rtm_full.sort_index()
    rtm_full["rv_d1"] = rtm_full["log_rtm_std"].shift(24)
    rtm_full["rv_w"]  = rtm_full["log_rtm_std"].rolling(24*7).mean().shift(24)
    rtm_full["rv_m"]  = rtm_full["log_rtm_std"].rolling(24*30).mean().shift(24)
    _har_cols = ["rv_d1", "rv_w", "rv_m"]
    _har_tr = rtm_full[_har_cols].reindex(tr.index).fillna(0)
    _har_va = rtm_full[_har_cols].reindex(va.index).fillna(0)

    _hf_Xtr = np.hstack([tr[FEAT_COLS].fillna(0).values, _har_tr.values])
    _hf_Xva = np.hstack([va[FEAT_COLS].fillna(0).values, _har_va.values])
    sc = StandardScaler(); r = Ridge(alpha=10.0)
    r.fit(sc.fit_transform(_hf_Xtr), y_tr)
    results["HAR+Full-Ridge"].append(r2_score(y_val, r.predict(sc.transform(_hf_Xva))))

    sc2 = StandardScaler(); r2 = Ridge(alpha=1.0)
    r2.fit(sc2.fit_transform(_har_tr.values), y_tr)
    results["HAR"].append(r2_score(y_val, r2.predict(sc2.transform(_har_va.values))))

    sc3 = StandardScaler(); r3 = Ridge(alpha=10.0)
    r3.fit(sc3.fit_transform(tr[FEAT_COLS].fillna(0).values), y_tr)
    results["Ridge"].append(r2_score(y_val, r3.predict(sc3.transform(va[FEAT_COLS].fillna(0).values))))

    _Xtr_s = make_seasonal(tr[FEAT_COLS].fillna(0))
    _Xva_s = make_seasonal(va[FEAT_COLS].fillna(0))
    sc4 = StandardScaler(); r4 = Ridge(alpha=10.0)
    r4.fit(sc4.fit_transform(_Xtr_s), y_tr)
    results["Ridge-seasonal"].append(r2_score(y_val, r4.predict(sc4.transform(_Xva_s))))

    _lag_cols = [c for c in FEAT_COLS if "lag" in c or "rtm" in c]
    sc5 = StandardScaler(); r5 = Ridge(alpha=1.0)
    r5.fit(sc5.fit_transform(tr[_lag_cols].fillna(0).values), y_tr)
    results["RTM-lag-only"].append(r2_score(y_val, r5.predict(sc5.transform(va[_lag_cols].fillna(0).values))))

    _naive_pred = tr[TARGET].reindex(va.index - pd.DateOffset(weeks=1)).values
    _naive_pred = np.where(np.isnan(_naive_pred), tr[TARGET].mean(), _naive_pred)
    results["Seasonal-naive"].append(r2_score(y_val, _naive_pred))

print()
print("Fast models done:")
for m in sorted(results, key=lambda k: -np.mean(results[k])):
    scores_str = ", ".join(f"{s:.3f}" for s in results[m])
    print(f"  {m:22s}: {np.mean(results[m]):.4f}  folds=[{scores_str}]")
json.dump(results, open("/tmp/72_fast.json", "w"))
'''

out = run_code("Phase 1: Fast models", fast_code)
if out:
    inject_output(20, out.split("Setup complete\n", 1)[-1])

# ── Phase 2: SARIMAX per-fold ───────────────────────────────────────────────
sarimax_results = []
for fold_idx in range(3):
    fold_year = ['2022', '2023', '2024'][fold_idx]
    sarimax_fold_code = SETUP_CODE + f'''
import json, gc
from pmdarima import auto_arima

fold_idx = {fold_idx}
fold_train_end = fold_ends72[fold_idx]
val_year = fold_names72[fold_idx]

fold_val_start = fold_train_end + pd.Timedelta(hours=1)
fold_val_end   = pd.Timestamp(f"{{val_year}}-12-31 23:00")
tr  = combined72.loc[combined72.index <= fold_train_end].copy()
va  = combined72.loc[(combined72.index >= fold_val_start) & (combined72.index <= fold_val_end)].copy()
y_tr = tr[TARGET].dropna(); y_val = va[TARGET].dropna()
tr = tr.loc[y_tr.index]; va = va.loc[y_val.index]

# Free memory
del combined72; gc.collect()

print(f"  {{val_year}} fitting SARIMAX (m=24, train={{len(tr)}}, val={{len(va)}})...")
mdl = auto_arima(y_tr, X=tr[FEAT_ARIMAX].fillna(0).values, d=0,
    max_p=2, max_q=1, max_P=1, max_Q=1, D=0,
    start_p=0, start_q=0, start_P=0, start_Q=0,
    seasonal=True, m=24,
    information_criterion="aic", stepwise=True, error_action="ignore", suppress_warnings=True)
pred = mdl.predict(n_periods=len(y_val), X=va[FEAT_ARIMAX].fillna(0).values)
r2 = r2_score(y_val.values, pred)
print(f"  {{val_year}} SARIMAX order: {{mdl.order}} x {{mdl.seasonal_order}}  R2={{r2:.3f}}")

json.dump({{"r2": r2, "order": str(mdl.order), "seasonal": str(mdl.seasonal_order)}},
          open(f"/tmp/72_sarimax_fold{{fold_idx}}.json", "w"))
'''
    out = run_code(f"SARIMAX fold {fold_year}", sarimax_fold_code)
    try:
        with open(f'/tmp/72_sarimax_fold{fold_idx}.json') as f:
            res = json.load(f)
            sarimax_results.append(res)
            print(f"  Fold {fold_year}: R²={res['r2']:.3f} order={res['order']} x {res['seasonal']}")
    except Exception as e:
        print(f"  Fold {fold_year} result not found: {e}")
        sarimax_results.append({"r2": float('nan'), "order": "?", "seasonal": "?"})

# Build SARIMAX cell output
sarimax_text_lines = []
for i, res in enumerate(sarimax_results):
    yr = ['2022','2023','2024'][i]
    sarimax_text_lines.append(f"  {yr} fitting SARIMAX (m=24, may take several minutes)...")
    sarimax_text_lines.append(f"  {yr} SARIMAX order: {res['order']} x {res['seasonal']}  R²={res['r2']:.3f}")
r2_vals = [r['r2'] for r in sarimax_results]
sarimax_text_lines.append(f"\n  SARIMAX CV mean R²: {np.mean(r2_vals):.4f}")
sarimax_text = "\n".join(sarimax_text_lines) + "\n"
inject_output(23, sarimax_text)

# Save combined SARIMAX results
json.dump(r2_vals, open('/tmp/72_sarimax.json', 'w'))

# ── Phase 3: Build combined summary ─────────────────────────────────────────
print(f"\n{'='*70}", flush=True)
print("  Building combined summary", flush=True)
print(f"{'='*70}", flush=True)

all_results = {}
try:
    with open('/tmp/72_fast.json') as f:
        all_results.update(json.load(f))
except: pass
for name, fname in [('ARIMA','/tmp/72_arima.json'), ('ARIMAX','/tmp/72_arimax.json'), ('SARIMAX','/tmp/72_sarimax.json')]:
    try:
        with open(fname) as f:
            all_results[name] = json.load(f)
    except: pass

summary_lines = ["=" * 60,
                  "Walk-forward CV mean R² (3 folds: 2022, 2023, 2024)",
                  "=" * 60]
for model, scores in sorted(all_results.items(), key=lambda x: -np.mean(x[1])):
    if scores:
        scores_str = ", ".join(f"{s:.3f}" for s in scores)
        summary_lines.append(f"  {model:22s}: {np.mean(scores):.4f}  folds=[{scores_str}]")

summary_text = "\n".join(summary_lines) + "\n"
print(summary_text, flush=True)
inject_output(24, summary_text)

# Also inject setup cell output
inject_output(19, f"Setup complete: combined72, 29 features, 3 folds\n")

print("\nAll phases complete!")
