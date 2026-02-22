# ERCOT Data Cleaning Runbook

Use this runbook to clean raw downloads into analysis-ready outputs.

Purpose:
- Convert raw monthly CSVs into analysis-ready tables.
- Keep output consistent (same timestamp rules, same column names).
- Avoid merge conflicts when cleaning in parallel.

## Table of Contents

1. [Prepare Environment](#1-prepare-environment)
2. [Define Input and Output Conventions](#2-define-input-and-output-conventions)
3. [Define Dataset Keys for Cleaning](#3-define-dataset-keys-for-cleaning)
4. [Apply Core Cleaning Rules](#4-apply-core-cleaning-rules)
5. [Handle NP6-905 Intervals](#5-handle-np6-905-intervals)
6. [Use Minimal Cleaning Template](#6-use-minimal-cleaning-template)
7. [Validate Outputs](#7-validate-outputs)
8. [Build EDA Merge Table (Hourly Master)](#8-build-eda-merge-table-hourly-master)
9. [Parallelize Cleaning Safely](#9-parallelize-cleaning-safely)
10. [Next Step](#10-next-step)

---

## 1. Prepare Environment

Run from project root:

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
```

Create and activate a Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install pandas pyarrow numpy
```

Package roles:
- `pandas`: read/clean CSVs.
- `pyarrow`: write parquet files for faster analysis.
- `numpy`: numeric cleanup helpers.

---

## 2. Define Input and Output Conventions

Raw input (already downloaded):
- `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/<DATASET_ID>_<YYYYMM>.csv`

Recommended cleaned output:
- `data/processed/ercot/<DATASET_ID>/year=YYYY/month=MM/part-*.parquet`

Rules:
- Do not edit raw files in place.
- Write cleaned files into `data/processed/...`.

---

## 3. Define Dataset Keys for Cleaning

Use this table to set deduplication keys in cleaned outputs.

For frequency and earliest-date reference, use `DATA_ESTIMATION.md` Section 1.

| Dataset ID | Data type | Recommended unique key for dedupe |
|---|---|---|
| `NP6-346-CD` | Actual load | `OperDay`, `HourEnding` |
| `NP3-565-CD` | Load forecast | `DeliveryDate`, `HourEnding`, `Model`, `InUseFlag` |
| `NP4-732-CD` | Wind actual/forecast | `DELIVERY_DATE`, `HOUR_ENDING` |
| `NP4-745-CD` | Solar actual/forecast | `DELIVERY_DATE`, `HOUR_ENDING` |
| `NP6-905-CD` | Settlement prices | `DeliveryDate`, `DeliveryHour`, `DeliveryInterval`, `SettlementPointName` |

Notes:
- `NP3-565-CD` can have many rows per hour due to model/scenario dimensions.
- `NP6-905-CD` is interval-based and large; process month-by-month.

---

## 4. Apply Core Cleaning Rules

1. Standardize column names:
- Keep original names in raw.
- In cleaned output, use `snake_case` and consistent naming.

2. Build a timestamp column:
- Keep one canonical timestamp column: `ts_local`.
- Preserve original date/hour columns for traceability.

3. Type cleanup:
- Parse numeric columns with `errors='coerce'`.
- Keep IDs and names as strings.

4. Deduplicate:
- Drop duplicates on dataset-specific keys (Section 3).

5. Sort:
- Sort by canonical timestamp ascending before writing final cleaned outputs.

6. Null checks:
- Report null rate for key fields and major numeric columns.

---

## 5. Handle NP6-905 Intervals

`NP6-905-CD` has one row per 15-minute settlement interval.

Columns:
- `DeliveryDate`
- `DeliveryHour` (1..24)
- `DeliveryInterval` (1..4)

Create an interval-start timestamp:

```python
import pandas as pd

df = pd.read_csv("data/raw/ercot/NP6-905-CD/2025/12/NP6-905-CD_202512.csv")

d = pd.to_datetime(df["DeliveryDate"], format="%m/%d/%Y")
he = df["DeliveryHour"].astype(int)          # 1..24
iv = df["DeliveryInterval"].astype(int)      # 1..4

df["ts_local"] = d + pd.to_timedelta(he - 1, unit="h") + pd.to_timedelta((iv - 1) * 15, unit="m")
```

Use these common aggregations:
- 15-minute to hourly price:

```python
hourly = (
    df.groupby([pd.Grouper(key="ts_local", freq="H"), "SettlementPointName"], as_index=False)
      .agg(settlement_point_price_mean=("SettlementPointPrice", "mean"))
)
```

- Revenue with fixed MW schedule (per interval):
- `revenue = SettlementPointPrice * MW * 0.25`

Storage/compute tips for NP6-905:
- Load specific columns only (`usecols=[...]`).
- Process one month at a time.
- Write parquet output and avoid giant merged CSV.
- Use categorical dtype for `SettlementPointName` and `SettlementPointType` when helpful.

---

## 6. Use Minimal Cleaning Template

```python
from pathlib import Path
import pandas as pd

DATASET = "NP6-346-CD"
raw_files = sorted(Path(f"data/raw/ercot/{DATASET}").glob("*/*/*.csv"))

parts = []
for f in raw_files:
    df = pd.read_csv(f)
    df = df.drop_duplicates(subset=["OperDay", "HourEnding"])
    df["ts_local"] = pd.to_datetime(df["OperDay"], format="%m/%d/%Y") + pd.to_timedelta(df["HourEnding"].str.slice(0, 2).astype(int) - 1, unit="h")
    parts.append(df)

out = pd.concat(parts, ignore_index=True).sort_values("ts_local")

out_dir = Path(f"data/processed/ercot/{DATASET}")
out_dir.mkdir(parents=True, exist_ok=True)
out.to_parquet(out_dir / f"{DATASET}_cleaned.parquet", index=False)
```

Adjust this template per dataset:
- Date/hour column names.
- Dedup key columns.
- Numeric columns list.

---

## 7. Validate Outputs

Run these checks before pushing cleaned outputs:

1. Row count sanity:
- No large unexplained drop from raw to cleaned.

2. Key uniqueness:
- No duplicate rows on dataset key.

3. Timestamp sanity:
- No null timestamps.
- Frequency matches expectation (hourly or 15-minute).

4. Numeric sanity:
- No impossible values from parsing errors (for example all-null numeric column).

5. Month coverage:
- Expected months exist in output.

Quick duplicate check example:

```python
dup_count = df.duplicated(subset=["DeliveryDate", "DeliveryHour", "DeliveryInterval", "SettlementPointName"]).sum()
print("duplicates:", dup_count)
```

---

## 8. Build EDA Merge Table (Hourly Master)

Use this pattern to read raw monthly files and build one hourly table.

What this example merges:
- `NP6-346-CD` (actual load)
- `NP3-565-CD` (load forecast)
- `NP4-732-CD` (wind actual)
- `NP4-745-CD` (solar actual)
- `NP6-905-CD` (15-minute settlement prices -> hourly mean)

```python
from pathlib import Path
import pandas as pd


def read_monthly_csv(dataset_id: str, usecols=None) -> pd.DataFrame:
    files = sorted(Path(f"data/raw/ercot/{dataset_id}").glob("*/*/*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found for {dataset_id}")
    return pd.concat((pd.read_csv(f, usecols=usecols) for f in files), ignore_index=True)


def parse_hour_ending(series: pd.Series) -> pd.Series:
    # Handles values like 1, 24, 01:00, 24:00.
    s = series.astype(str).str.strip().str.split(":").str[0]
    return pd.to_numeric(s, errors="coerce")


# 1) Actual load (hourly)
load = read_monthly_csv("NP6-346-CD", usecols=["OperDay", "HourEnding", "TOTAL"])
load_he = parse_hour_ending(load["HourEnding"])
load["ts_local"] = pd.to_datetime(load["OperDay"], format="%m/%d/%Y", errors="coerce") + pd.to_timedelta(load_he - 1, unit="h")
load = (
    load.rename(columns={"TOTAL": "load_mw"})
        .groupby("ts_local", as_index=False)["load_mw"].mean()
)


# 2) Load forecast (hourly, multiple models per hour -> average here)
forecast = read_monthly_csv("NP3-565-CD", usecols=["DeliveryDate", "HourEnding", "SystemTotal", "Model", "InUseFlag"])
fc_he = parse_hour_ending(forecast["HourEnding"])
forecast["ts_local"] = pd.to_datetime(forecast["DeliveryDate"], format="%m/%d/%Y", errors="coerce") + pd.to_timedelta(fc_he - 1, unit="h")
forecast = (
    forecast.rename(columns={"SystemTotal": "load_forecast_mw"})
            .groupby("ts_local", as_index=False)["load_forecast_mw"].mean()
)


# 3) Wind actual (hourly)
wind = read_monthly_csv("NP4-732-CD", usecols=["DELIVERY_DATE", "HOUR_ENDING", "ACTUAL_SYSTEM_WIDE"])
wind_he = pd.to_numeric(wind["HOUR_ENDING"], errors="coerce")
wind["ts_local"] = pd.to_datetime(wind["DELIVERY_DATE"], format="%m/%d/%Y", errors="coerce") + pd.to_timedelta(wind_he - 1, unit="h")
wind = (
    wind.rename(columns={"ACTUAL_SYSTEM_WIDE": "wind_mw"})
        .groupby("ts_local", as_index=False)["wind_mw"].mean()
)


# 4) Solar actual (hourly)
solar = read_monthly_csv("NP4-745-CD", usecols=["DELIVERY_DATE", "HOUR_ENDING", "SYSTEM_WIDE_GEN"])
solar_he = pd.to_numeric(solar["HOUR_ENDING"], errors="coerce")
solar["ts_local"] = pd.to_datetime(solar["DELIVERY_DATE"], format="%m/%d/%Y", errors="coerce") + pd.to_timedelta(solar_he - 1, unit="h")
solar = (
    solar.rename(columns={"SYSTEM_WIDE_GEN": "solar_mw"})
         .groupby("ts_local", as_index=False)["solar_mw"].mean()
)


# 5) Settlement prices (15-minute -> hourly mean)
price = read_monthly_csv(
    "NP6-905-CD",
    usecols=["DeliveryDate", "DeliveryHour", "DeliveryInterval", "SettlementPointPrice"]
)
d = pd.to_datetime(price["DeliveryDate"], format="%m/%d/%Y", errors="coerce")
dh = pd.to_numeric(price["DeliveryHour"], errors="coerce")
di = pd.to_numeric(price["DeliveryInterval"], errors="coerce")
price["ts_15m"] = d + pd.to_timedelta(dh - 1, unit="h") + pd.to_timedelta((di - 1) * 15, unit="m")
price_hourly = (
    price.groupby(pd.Grouper(key="ts_15m", freq="H"), as_index=False)["SettlementPointPrice"]
         .mean()
         .rename(columns={"ts_15m": "ts_local", "SettlementPointPrice": "rt_price_mean"})
)


# 6) Merge to one EDA table
eda = (
    load.merge(forecast, on="ts_local", how="outer")
        .merge(wind, on="ts_local", how="outer")
        .merge(solar, on="ts_local", how="outer")
        .merge(price_hourly, on="ts_local", how="outer")
        .drop_duplicates(subset=["ts_local"])
        .sort_values("ts_local")
)

# Optional derived feature
eda["net_load_mw"] = eda["load_mw"] - eda["wind_mw"] - eda["solar_mw"]

out_dir = Path("data/processed/ercot/eda_master")
out_dir.mkdir(parents=True, exist_ok=True)
eda.to_parquet(out_dir / "eda_hourly_master.parquet", index=False)

print("rows:", len(eda))
print("range:", eda["ts_local"].min(), "to", eda["ts_local"].max())
print("null_rate:\n", eda.isna().mean().sort_values(ascending=False))
```

EDA notes:
- This merge uses `outer` join to keep full coverage from each dataset.
- If you want only rows where all metrics exist, switch to `how="inner"`.
- For NP6-905, you can replace system-wide mean with a hub/zone-specific filter before aggregation.

---

## 9. Parallelize Cleaning Safely

1. Assign non-overlapping datasets.
- Example split: one owner per dataset family to avoid output path conflicts.

2. Use one branch per workstream:

```bash
git checkout main
git pull
git checkout -b clean/<dataset-or-owner>
```

3. Write only assigned dataset outputs under:
- `data/processed/ercot/<ASSIGNED_DATASET>/...`

4. Commit only assigned outputs + code/notebook changes:

```bash
git add DATA_CLEANING.md scripts/ data/processed/ercot/<ASSIGNED_DATASET>/
git commit -m "Clean <ASSIGNED_DATASET> and add validation outputs"
git push -u origin clean/<dataset-or-owner>
```

5. Merge PRs one by one.
- Avoid two PRs editing the same cleaned file path.

---

## 10. Next Step

Create one small cleaning script per dataset in `scripts/cleaning/` so processing is reproducible without notebook-only logic.
