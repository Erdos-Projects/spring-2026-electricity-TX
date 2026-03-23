"""
eval_all_models.py — Evaluate all XGB model versions (v1, v2, v3 variants) on 2025 test set.
Run from project root: conda run -n erdos_ds_environment python scripts/eval_all_models.py
"""
import pickle, warnings
import numpy as np, pandas as pd
from pathlib import Path
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

warnings.filterwarnings('ignore')

PROC = Path('data/processed/ercot')
TARGET = 'log_rtm_std'
TRAIN_END = pd.Timestamp('2024-12-31 23:00')

# ── Feature engineering (minimal — same as metrics_core.py) ──────────────────
def add_engineered_features(df):
    df = df.copy()
    df['fc_net_load']          = df['fc_coast'] - df['wf_stwpf_lz_south_houston']
    df['dam_rtm_spread']       = df['dam_price_houston'] - df['rtm_mean_lag24']
    df['abs_dam_rtm_spread']   = df['dam_rtm_spread'].abs()
    df['week']                 = df.index.isocalendar().week.astype(int)
    df['load_lag7d']           = df['load_houston_lag48'].shift(168)
    df['rtm_price_std_lag7d']  = df['rtm_std_lag24'].shift(168)
    df['rtm_price_mean_lag7d'] = df['rtm_mean_lag24'].shift(168)
    df['outage_fraction']      = df['total_resource_mw'] / (df['fc_system_total'] + 1)
    df['hour']                 = df.index.hour
    df['month']                = df.index.month
    df['dow']                  = df.index.dayofweek
    return df

# ── Load data ─────────────────────────────────────────────────────────────────
train = pd.read_parquet(PROC / 'train_features.parquet')
test  = pd.read_parquet(PROC / 'test_features.parquet')
combined = pd.concat([train, test]).sort_index()
combined = add_engineered_features(combined)
test_mask = combined.index > TRAIN_END

y_te = combined.loc[test_mask, TARGET]
print(f"Test rows (2025): {test_mask.sum():,}  |  all cols: {len(combined.columns)}")
print()

# ── Evaluate each pkl ─────────────────────────────────────────────────────────
pkls = [
    ('model_xgb_reg.pkl',         'XGB v1'),
    ('model_xgb_reg_v2.pkl',      'XGB v2'),
    ('model_xgb_reg_v2_xgb.pkl',  'XGB v2_xgb'),
    ('model_xgb_reg_v3.pkl',      'XGB v3 (depth=5, lr=0.05)'),
    ('model_xgb_reg_v3_best.pkl', 'XGB v3 best (depth=5, lr=0.05)'),
    ('model_xgb_reg_v3_tuned.pkl','XGB v3 tuned (depth=4, lr=0.03) ← PRIMARY'),
]

print(f"{'Model':<45} {'depth':>5} {'lr':>5} {'nfeat':>6} {'R²':>7} {'RMSE':>7} {'MAE$/MWh':>9}")
print("-" * 90)
for fname, label in pkls:
    p = PROC / fname
    if not p.exists():
        print(f"  {label:<43} — NOT FOUND")
        continue
    with open(p, 'rb') as f:
        m = pickle.load(f)
    params = m.get_params()
    depth  = params.get('max_depth', '?')
    lr     = params.get('learning_rate', '?')

    fn = m.get_booster().feature_names
    if fn is None:
        # old models without stored feature names — skip or use what's available
        print(f"  {label:<43} {str(depth):>5} {str(lr):>5}   N/A  (no feature names stored)")
        continue

    avail   = [c for c in fn if c in combined.columns]
    missing = [c for c in fn if c not in combined.columns]
    if missing:
        print(f"  {label:<43} {str(depth):>5} {str(lr):>5} {len(avail):>6}  SKIP — {len(missing)} features not in pipeline: {missing[:3]}")
        continue
    X_te = combined.loc[test_mask, avail]
    pred = m.predict(X_te)
    r2   = r2_score(y_te, pred)
    rmse = mean_squared_error(y_te, pred) ** 0.5
    mae  = mean_absolute_error(np.expm1(y_te), np.expm1(pred))
    print(f"  {label:<43} {str(depth):>5} {str(lr):>5} {len(avail):>6} {r2:>7.4f} {rmse:>7.4f} {mae:>9.3f}")

print()
print("Done.")
