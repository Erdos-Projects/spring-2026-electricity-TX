# eda.ipynb — Component Brief

## Purpose

Exploratory data analysis, feature design, and leakage-safe feature engineering for the ERCOT Houston Hub volatility forecasting project. Sections 1–5 cover EDA; Appendix A–B document the feature set and audit the leakage-prevention design. The notebook also contains `build_features(delivery_date)` — the function that produces one day of model-ready features using only information available at midnight CST.

---

## Section Outline

| Section | Cell ID | Description |
|---|---|---|
| Title / Contents | `eda-title` | Overview, outline, data range 2017-07 → 2025-12 |
| §1 Setup | `eda-s1-hdr` | Load `ercot_combined.parquet`, define constants and helpers |
| §1 Missing Data Check | `0t2rwcqcspek` | Gap report on ercot_combined + feature parquets at session start |
| §2 Target Variable | `eda-s2-hdr` | RTM price volatility distribution, log-transform justification |
| §2.4 Spike Threshold | `70w07q87z9g` | Justifies $100/MWh cutoff (p97, ~3% rate, ~2,400 training positives) |
| §3 Feature EDA | `eda-s3-hdr` | Driver analysis across all feature categories |
| §3.1 Net Load | `fido7b9e7zb` | Net load as primary volatility driver |
| §3.2 Wind Error | `eda-s3-2-hdr` | Wind forecast error as proximate spike cause |
| §3.3 Forecast Revision Std | `eda-s3-3-hdr` | Load/wind forecast revision volatility as forward-looking uncertainty signal |
| §3.4 DAM Price | `eda-s3-4-hdr` | DAM price level and spread vs RTM volatility |
| §3.5 Ancillary Prices | `eda-s3-5-hdr` | MCPC prices as reserve tightness indicators |
| §3.6 Feature Candidates | `dyezec04w9f` | Final feature selection table at midnight CST cutoff |
| Non-Linear Effects | `2k0lwjbibru`, `vcl13m4b3hn` | Wind × load interaction; threshold effects analysis |
| §3.7 Weather Correlation | `7vth74jc3xm` | Houston vs Texas-wide weather; 4-feature Houston-only design |
| §4 Regime Analysis | `eda-s4-hdr` | Winter Storm Uri (Feb 2021) as structural break |
| §4.1 Uri Event | `5e704fbf42` | Uri zoom: price spike, recovery, regime shift |
| §4.2 High-Volatility Hours | `eda-s4-2-hdr` | Seasonal/diurnal concentration of volatility |
| §4.3 Volatility Autocorrelation | `eda-s4-3-hdr` | Persistence structure — justifies lag features and GARCH |
| §5 Modeling Roadmap | `eda-s5-hdr` | Feature correlation ranking, validation strategy |
| §5.1 Feature Correlation | `kpdsx2irjfr` | Pearson r with log_rtm_std for all 30 features |
| §5.2 Modeling Plan | `eda-s5-2-plan` | Target variables, feature tiers, model ladder, CV strategy |
| §5.3 Data Design | `455mrg7pgsn` | Two-table design (EDA vs features), post_datetime filter, leakage audit, data availability table |
| **Appendix A** | `9uhfq09mjk` | Feature Dictionary — all 22 base features with descriptions and leakage status |
| **Appendix B** | `00eyihmjurdcd` | Feature Matrix Audit — leakage verification, training stats, null summary |

---

## Key Outputs

**Feature parquets** (written by `build_features()` loop in §5.3):
- `train_features.parquet` — 65,712 rows × 26 cols, 2017-07-04 → 2024-12-31
- `test_features.parquet` — 8,760 rows × 26 cols, 2025-01-01 → 2025-12-31

**Figures** (saved to `figures/eda/`):
- `spike_threshold_selection.png` — CDF + spike rate vs threshold justifying $100/MWh
- `volatility_feature_correlation.png` — Pearson r ranking of all features
- `volatility_nonlinear_effects.png` — Wind × load non-linear interaction
- `weather_houston_texas_scatter.png` — Houston vs Texas weather comparison

---

## Feature Dictionary (Appendix A)

All 22 base features documented with descriptions. Features are organized by category:

| Category | Features |
|---|---|
| Market Prices | `dam_price_houston`, `system_lambda`, `rtm_mean_lag48`, `rtm_std_lag48` |
| Ancillary MCPC | `mcpc_regup`, `mcpc_regdn`, `mcpc_rrs`, `mcpc_nspin` (mcpc_ecrs excluded — started 2023-06) |
| Load & Supply | `load_houston_lag48`, `fc_coast`, `fc_system_total`, `total_resource_mw` |
| Wind | `wgrpp_lz_south_houston`, `wind_error_houston`, `wf_stwpf_lz_south_houston` |
| Weather (Houston) | `temp_f_houston_avg`, `humidity_pct_houston_avg`, `wind_gust_mph_houston_avg`, `precip_in_houston_avg` |
| Calendar | `hour`, `month`, `dow` |

**Additional features documented in modeling.ipynb:**
- `garch_cond_vol` — GARCH(1,1)-t conditional volatility (§7.3); #2 feature by importance
- `rtm_price_std`, `rtm_price_mean` — raw targets
- `log_rtm_std` — regression target; `spike_flag` — classifier target

**Status:** All 22 base features have full descriptions and leakage status in Appendix A. Engineered features (`garch_cond_vol`) documented in modeling.ipynb §7.3.

---

## Key Design Decisions

- **Midnight CST cutoff** (00:00 D): DAM/lambda/ancillary available same-day D (post ~12:35 CST D-1). Load/RTM/wind actuals require D-2 lag (post after midnight).
- **D-2 lag columns**: `load_houston_lag48`, `rtm_mean_lag48`, `rtm_std_lag48` (D-2 actual shifted +48h to align with delivery hour)
- **Houston-first design**: all wind/load features use Houston zone (`lz_south_houston`) not system-wide
- **`build_features()` location**: cell `vv567ec2vqn`; returns 24-row DataFrame per delivery date
- **Two-table design**: `ercot_combined.parquet` for EDA only (no post_datetime); individual parquets for leakage-safe feature engineering

---

## Known Issues / Caveats

1. **Sections 2.1–2.3 absent** — only §2.4 (Spike Threshold) exists as a named subsection; earlier target EDA is inline in §2 cells.
2. **`mcpc_ecrs` excluded** — 52,104 nulls pre-2023; explicitly noted in Appendix A as excluded.
3. **`build_features()` is slow on first run** — iterates 2,920 delivery days; subsequent reads from parquet are instant.

---

## Pending

- None — all cells execute cleanly as of 2026-03-20.
