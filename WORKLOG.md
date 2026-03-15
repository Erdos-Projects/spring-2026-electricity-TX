# Project Work Log — spring-2026-electricity-TX

_Updated: 2026-03-15_

---

## Current Status
- **Phase**: Feature engineering complete; training matrix built; ready for model training
- **Train window**: 2017-07-04 → 2024-12-31
- **Test window**: 2025
- **Hold-out**: 2026 (never touch until final evaluation)

---

## To-Do List

### High Priority
- [ ] Train baseline models on `train_features.parquet` (ridge regression, random forest, XGBoost)
- [ ] Rebuild `train_features.parquet` with weather features added (now that weather data is available)
- [ ] Add weather features to `build_features()` in eda.ipynb
- [ ] Evaluate model performance on 2025 test window

### Medium Priority
- [ ] Investigate `mcpc_ecrs` sparse coverage (pre-2021 nulls) — consider separate model for pre/post-ECRS periods
- [ ] Add `houston_resource_mw` from `np3_233_outage_houston.parquet` as Houston-specific outage feature
- [ ] Build test feature matrix (2025 delivery dates) for evaluation

### Low Priority / Nice to Have
- [ ] Add temperature × load interaction term once weather features are integrated
- [ ] Explore GARCH model for volatility (compare with ML baseline)
- [ ] Add `np6_345_coast` (Coast zone load) as additional load signal
- [ ] Neeraj: integrate any additional features from his analysis

### Blocked
- [ ] ~~Weather features~~ — UNBLOCKED: weather CSVs copied to `data/raw/weather/` on 2026-03-15

---

## Completed Work

### 2026-03-15

#### Data Pipeline
- [x] Removed Git LFS entirely — all data files now gitignored (`/data/` rule)
- [x] Removed Makefile (LFS locking targets obsolete; download scripts run directly)
- [x] Stopped tracking NP6-346-CD raw CSVs in git (regenerate via backfill script)
- [x] Fixed `weather_hourly.parquet` — was only Dec 2025 sample; rebuilt from full dataset (75,288 rows, 2017-07 → 2026-01)
- [x] Created `weather_daily.parquet` (3,137 rows, 481 wide columns — daily pivot format)
- [x] Rebuilt `np4_732_wind_system.parquet` and `np4_732_wind_houston.parquet` using **first post-delivery actual** per slot (post_datetime now reflects true first availability ~1.5–3h after delivery)

#### Feature Engineering
- [x] Established `build_features(delivery_date)` in `eda.ipynb` cell `vv567ec2vqn`
- [x] Audited all features for data leakage — no leakage found
- [x] Fixed 3 issues found in audit:
  - Outage fallback bug (used ts_utc sort instead of post_datetime sort)
  - Raw `mcpc_ecrs` not filled (now `fillna(0)` pre-2021)
  - Added `ecrs_available` flag to distinguish pre-2021 zeros from genuine zeros
- [x] Built full training matrix: `data/processed/ercot/train_features.parquet` (65,712 rows × 26 cols, 7.2 MB)

#### EDA
- [x] Volatility target analysis: `log(rtm_price_std + 1)` chosen as regression target; binary spike flag (`rtm_price_mean > $100`, 3.1% of hours) as secondary
- [x] Feature correlation with log volatility: mcpc_ecrs (r=0.37) and fc_system_total (r=0.35) are top predictors
- [x] Non-linear effects: ECRS threshold at $50 confirmed; wind error × load interaction not confirmed; DAM × outage not confirmed
- [x] Weather usefulness: Dec 2025 sample shows weak correlations (misleading — December is low-volatility month; expect stronger signal in summer)

#### Git / Repo
- [x] Resolved LFS budget issue — removed LFS, force-pushed cleaned history
- [x] Removed 3 large sample files (NP3-565, NP6-788, NP6-905 — 121–437 MB) from git history
- [x] Updated README.md and DATA_DOWNLOAD.md to remove Makefile and LFS references
- [x] Updated agent system: DataAgent split into PipelineAgent + GitAgent (6 agents total)

---

## Issues Encountered & Resolved

| Date | Issue | Resolution |
|---|---|---|
| 2026-03-15 | Git LFS budget exceeded — pull failed | Used `GIT_LFS_SKIP_SMUDGE=1 git pull`; later removed LFS entirely |
| 2026-03-15 | Stash pop conflict on weather CSVs | Accepted both hourly and daily files; kept all 8 |
| 2026-03-15 | 3 sample CSVs (121–437 MB) blocked push after LFS removal | Removed from git history with `git filter-branch`; added to .gitignore |
| 2026-03-15 | NP4-732 wind parquet used latest revision post_datetime (D+2) | Rebuilt with first post-delivery actual (~D+3h); restored `_avail()` filter |
| 2026-03-15 | `build_features()` outage fallback borrowed from wrong ts_utc | Fixed to use `sort_values('post_datetime').iloc[-1]` |
| 2026-03-15 | `mcpc_ecrs` 80% null pre-2021 caused ambiguity in engineered features | Added `ecrs_available` flag; `fillna(0)` on raw column |
| 2026-03-15 | `weather_hourly.parquet` only had Dec 2025 sample data | Rebuilt from full `data/raw/weather/` files after receiving real weather CSVs |
| 2026-03-15 | Teammate (Neeraj) couldn't pull after force push | Fresh clone recommended; data backup instructions provided |

---

## Key Files

| File | Description |
|---|---|
| `eda.ipynb` | Main EDA notebook; contains `build_features()`, volatility analysis, feature audit |
| `data_cleaning.ipynb` | Raw → processed parquet pipeline |
| `data/processed/ercot/train_features.parquet` | Full training matrix (gitignored) |
| `data/processed/ercot/weather_hourly.parquet` | Hourly weather features (gitignored) |
| `data/processed/ercot/weather_daily.parquet` | Daily-pivot weather features (gitignored) |
| `agent_notes/AGENTS.md` | Multi-agent system roles and routing guide |
| `DATA_DOWNLOAD.md` | ERCOT download runbook |
| `scripts/backfill_post_datetime.py` | Backfill pipeline for post_datetime |

---

## Agent System

| Agent | Role |
|---|---|
| Manager (this session) | Routes tasks, tracks progress, synthesizes results |
| PipelineAgent | Raw → processed pipeline, notebook edits |
| GitAgent | All git operations (commit, push, history) |
| EDAAgent | Exploratory analysis, plots, audits |
| ModelAgent | Feature matrices, model training, evaluation |
| MemoryAgent | Memory files, agent_notes, this WORKLOG |
