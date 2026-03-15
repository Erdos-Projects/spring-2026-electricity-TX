# Project To-Do List
_Last updated: 2026-03-15_

Team: Yun, Eric, Neeraj (+ others)

---

## ✅ Completed

- Downloaded all 9 ERCOT datasets (2017-07 → 2024-12) + weather CSVs
- Backfilled `postDateTime` for all datasets (100% coverage)
- Removed Git LFS — all CSVs are local only (gitignored)
- Built processed parquets: load, DAM, RTM, ancillary, wind, outage, weather
- EDA notebook (`eda.ipynb`): time series plots, target selection, feature correlations, non-linear effects
- Designed leakage-safe `build_features(delivery_date)` using 6PM D-1 cutoff
- Audited `build_features()` — no direct leakage; 3 minor issues fixed
- Built training matrix `train_features.parquet` (65,712 rows × 26 cols, 2017-07 → 2024-12)
- Confirmed target: `log(rtm_price_std + 1)` for regression; `spike_flag` (RTM > $100) for classification

---

## 🔥 High Priority — Next Steps

- [ ] **Add weather features to `build_features()`** — weather parquets are rebuilt and ready; integrate `temp_f`, `humidity_pct` into training matrix (Yun/Eric)
- [ ] **Rebuild `train_features.parquet` with weather** — re-run the loop after adding weather to `build_features()`
- [ ] **Baseline models** — train GARCH / HAR-RV / simple ML on `train_features.parquet`; evaluate on 2025 test set

---

## 🟡 Medium Priority

- [ ] **Evaluate 2025 test set** — load 2025 data and run `build_features()` over 2025 dates; compute held-out metrics
- [ ] **Hyperparameter tuning** — once baseline is established, tune and compare models
- [ ] **Feature selection** — assess which of the 21 current features are useful; consider dropping correlated ones

---

## 🔵 Low Priority / Ideas

- [ ] **Zone-level models** — extend targets to NORTH, SOUTH, WEST zones (not just Houston)
- [ ] **Intraday RTM forecasting** — shorter-horizon models using RTM data from earlier in day
- [ ] **Ensemble / stacking** — combine GARCH with ML for improved vol forecasts

---

## ⛔ Blocked / On Hold

- **2026 out-of-sample evaluation** — hold-out until final model is locked
- **NP4-745-CD (SCED prices)** — excluded from analysis (starts 2022-06, no full history)
- **NP3-911-ER (ancillary offers)** — excluded (schema changed 4×, not coherent)

---

## Collaboration Notes

- **Data lives locally** (gitignored) — each person needs their own copy. See `README.md` for setup.
- **Fresh clone instructions**: See README §Setup if you get git errors — the force-push earlier rewrote history.
- **Notebooks**: `data_cleaning.ipynb` builds all parquets; `eda.ipynb` has all analysis and `build_features()`.
- **Train/test split**: Train 2017-07-04 → 2024-12-31 | Test 2025 | Hold-out 2026
- **WORKLOG.md** is a detailed local work log (gitignored — ask Yun for current version if needed).
