#!/usr/bin/env python3
"""
Detail investigation of suspicious negative horizons in flagged datasets.
"""

import os, glob, gzip, io
import pandas as pd
import numpy as np

BASE = "/Users/cielo69/github/spring-2026-electricity-TX"

def read_csv_file(fpath):
    if fpath.endswith(".gz"):
        with gzip.open(fpath, "rt") as f:
            return pd.read_csv(f, low_memory=False)
    return pd.read_csv(fpath, low_memory=False)

def find_file(dataset, month):
    pattern = os.path.join(BASE, "data/raw/ercot", dataset, month, "*.csv")
    files = [f for f in sorted(glob.glob(pattern)) if not f.endswith(".docids")]
    return files[0] if files else None

def parse_hour(val):
    s = str(val).strip()
    if ":" in s:
        return int(s.split(":")[0])
    return int(float(s))

def add_horizon(df, pdt_col, date_col, hour_col, is_int_hour=False):
    pdt = pd.to_datetime(df[pdt_col], errors="coerce")
    del_date = pd.to_datetime(df[date_col], errors="coerce")
    if is_int_hour:
        hour_int = df[hour_col].astype(int)
    else:
        hour_int = df[hour_col].apply(parse_hour)
    del_dt = del_date + pd.to_timedelta(hour_int - 1, unit="h")
    if pdt.dt.tz is not None:
        pdt_naive = pdt.dt.tz_localize(None)
    else:
        pdt_naive = pdt
    df = df.copy()
    df["_del_dt"] = del_dt
    df["_pdt_naive"] = pdt_naive
    df["_horizon_h"] = (del_dt - pdt_naive).dt.total_seconds() / 3600
    return df


# ─── NP6-346-CD and NP6-345-CD ───────────────────────────────────────────────
print("=" * 70)
print("NP6-346-CD / NP6-345-CD — OperDay + HourEnding, all-negative horizons")
print("=" * 70)
for ds in ["NP6-346-CD", "NP6-345-CD"]:
    fp = find_file(ds, "2024/12")
    df = read_csv_file(fp)
    df = add_horizon(df, "postDateTime", "OperDay", "HourEnding")
    print(f"\n[{ds}] columns: {list(df.columns)}")
    print(f"  postDateTime sample: {df['postDateTime'].head(3).tolist()}")
    print(f"  OperDay sample:      {df['OperDay'].head(3).tolist()}")
    print(f"  HourEnding sample:   {df['HourEnding'].head(3).tolist()}")
    print(f"  _del_dt sample:      {df['_del_dt'].head(3).tolist()}")
    print(f"  _pdt_naive sample:   {df['_pdt_naive'].head(3).tolist()}")
    print(f"  horizon_h sample:    {df['_horizon_h'].head(3).tolist()}")
    print(f"  horizon range: [{df['_horizon_h'].min():.2f}, {df['_horizon_h'].max():.2f}]")
    print(f"  => postDateTime is AFTER delivery for all rows: posting happens post-facto")

# ─── NP6-905-CD ───────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("NP6-905-CD — DeliveryDate + DeliveryHour (int 1-24), small negatives")
print("=" * 70)
fp = find_file("NP6-905-CD", "2024/12")
df = read_csv_file(fp)
df = add_horizon(df, "postDateTime", "DeliveryDate", "DeliveryHour", is_int_hour=True)
print(f"  columns: {list(df.columns)}")
print(f"  postDateTime sample: {df['postDateTime'].head(3).tolist()}")
print(f"  DeliveryDate sample: {df['DeliveryDate'].head(3).tolist()}")
print(f"  DeliveryHour sample: {df['DeliveryHour'].head(3).tolist()}")
print(f"  _del_dt sample:      {df['_del_dt'].head(3).tolist()}")
print(f"  _pdt_naive sample:   {df['_pdt_naive'].head(3).tolist()}")
print(f"  horizon_h sample:    {df['_horizon_h'].head(3).tolist()}")
print(f"  horizon range: [{df['_horizon_h'].min():.2f}, {df['_horizon_h'].max():.2f}]")
# Show unique horizon values
print(f"  unique horizon_h values: {sorted(df['_horizon_h'].unique())}")

# ─── NP4-732-CD ───────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("NP4-732-CD — DELIVERY_DATE + HOUR_ENDING, mixed sign horizons")
print("=" * 70)
fp = find_file("NP4-732-CD", "2024/12")
df = read_csv_file(fp)
df = add_horizon(df, "postDateTime", "DELIVERY_DATE", "HOUR_ENDING")
print(f"  columns: {list(df.columns)}")
print(f"  postDateTime sample: {df['postDateTime'].head(5).tolist()}")
print(f"  DELIVERY_DATE sample:{df['DELIVERY_DATE'].head(5).tolist()}")
print(f"  HOUR_ENDING sample:  {df['HOUR_ENDING'].head(5).tolist()}")
print(f"  _del_dt sample:      {df['_del_dt'].head(5).tolist()}")
print(f"  _pdt_naive sample:   {df['_pdt_naive'].head(5).tolist()}")
print(f"  horizon_h sample:    {df['_horizon_h'].head(5).tolist()}")
print(f"  horizon range: [{df['_horizon_h'].min():.2f}, {df['_horizon_h'].max():.2f}]")
print(f"  Distribution of horizon_h (binned):")
print(df['_horizon_h'].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).to_string())
# Check what postDateTime looks like for negative vs positive
neg = df[df['_horizon_h'] < 0]
pos = df[df['_horizon_h'] > 0]
print(f"\n  Negative horizon rows: {len(neg):,}")
print(f"  Positive horizon rows: {len(pos):,}")
if len(neg) > 0:
    print(f"  Neg: postDateTime={neg['postDateTime'].head(3).tolist()}, DELIVERY_DATE={neg['DELIVERY_DATE'].head(3).tolist()}, HOUR_ENDING={neg['HOUR_ENDING'].head(3).tolist()}")
if len(pos) > 0:
    print(f"  Pos: postDateTime={pos['postDateTime'].head(3).tolist()}, DELIVERY_DATE={pos['DELIVERY_DATE'].head(3).tolist()}, HOUR_ENDING={pos['HOUR_ENDING'].head(3).tolist()}")

# ─── NP3-565-CD and NP3-233-CD ────────────────────────────────────────────────
for ds, date_col, hour_col in [("NP3-565-CD", "DeliveryDate", "HourEnding"), ("NP3-233-CD", "Date", "HourEnding")]:
    print(f"\n" + "=" * 70)
    print(f"{ds} — mixed sign horizon (negative min, large max ~190h)")
    print("=" * 70)
    fp = find_file(ds, "2024/12")
    df = read_csv_file(fp)
    df = add_horizon(df, "postDateTime", date_col, hour_col)
    print(f"  postDateTime sample: {df['postDateTime'].head(5).tolist()}")
    print(f"  {date_col} sample:   {df[date_col].head(5).tolist()}")
    print(f"  {hour_col} sample:   {df[hour_col].head(5).tolist()}")
    print(f"  _del_dt sample:      {df['_del_dt'].head(5).tolist()}")
    print(f"  _pdt_naive sample:   {df['_pdt_naive'].head(5).tolist()}")
    print(f"  horizon_h sample:    {df['_horizon_h'].head(5).tolist()}")
    print(f"  horizon range: [{df['_horizon_h'].min():.2f}, {df['_horizon_h'].max():.2f}]")
    print(df['_horizon_h'].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).to_string())
    neg = df[df['_horizon_h'] < 0]
    print(f"  Negative horizon rows: {len(neg):,} / {len(df):,}")
    if len(neg) > 0:
        print(f"  Sample neg rows:")
        print(neg[['postDateTime', date_col, hour_col, '_del_dt', '_pdt_naive', '_horizon_h']].head(5).to_string(index=False))

print("\nDone.")
