# Volatility Clustering Analysis: ERCOT Electricity Prices

Use this document for model selection, EDA planning, and benchmark design for detecting and forecasting volatility clustering in ERCOT electricity prices.

## Why Electricity Volatility Is Different

Electricity prices differ from equities/commodities in ways that affect model choice:

| Property | Implication |
|---|---|
| Extreme spikes (10x-100x normal) | Fat-tailed distributions required; Gaussian GARCH fails |
| Mean reversion | Spikes collapse within hours; long-memory vol models may overfit |
| Strong seasonality (hourly, daily, weekly, annual) | Must de-seasonalize before GARCH or seasonality masquerades as clustering |
| Negative prices | Log returns undefined near zero; use price differences instead |
| Non-storability | No arbitrage smoothing; supply shocks pass through immediately |
| Regime structure | Normal operation vs scarcity events are fundamentally different states |

## Table of Contents

1. [Dataset Selection](#1-dataset-selection)
2. [EDA Roadmap](#2-eda-roadmap)
3. [Data Preparation](#3-data-preparation)
4. [Model Hierarchy](#4-model-hierarchy)
5. [Benchmark Design](#5-benchmark-design)
6. [Evaluation Framework](#6-evaluation-framework)
7. [Implementation Plan](#7-implementation-plan)
8. [References and Tools](#8-references-and-tools)

## 1. Dataset Selection

### Primary: NP6-905-CD (RT Settlement Prices)

The core dataset for volatility analysis:
- 15-min resolution captures intraday volatility dynamics
- Analysis window: `2017-07-01` to `2024-12-31` (90 months, ~7.5 years)
- Covers hubs, load zones, and resource nodes

Start with hub-level prices to keep dimensionality tractable:

| Settlement Point | Region | Volatility Profile |
|---|---|---|
| `HB_BUSAVG` | System-wide weighted average | Best single representative series |
| `HB_WEST` | West Texas (wind-heavy, constrained) | Highest volatility; best for spike analysis |
| `HB_NORTH` | North Texas | Moderate |
| `HB_SOUTH` | South Texas | Moderate |
| `HB_HOUSTON` | Houston area | Load-driven spikes |

Recommendation: start with `HB_WEST` (most volatile, clearest clustering signal) and `HB_BUSAVG` (system benchmark).

### Supporting Datasets (Volatility Drivers)

All supporting datasets must have coverage from `2017-07` to align with the primary analysis window.

| Dataset | Available From | Feature | Role in Volatility |
|---|---|---|---|
| `NP6-346-CD` | 2017-07 | Actual system load (hourly) | High load = tighter supply margin = higher vol |
| `NP3-565-CD` | 2017-07 | 7-day load forecast (hourly) | Forecast error = demand surprise = spike trigger |
| `NP4-732-CD` | 2017-07 | Wind actual + forecast (hourly) | Wind forecast error is a primary vol driver in ERCOT |
| `NP3-233-CD` | 2017-07 | Outage capacity (hourly) | Sudden outage jumps correlate with regime switches |
| `NP4-190-CD` | 2017-07 | DA settlement prices (daily) | DA-RT spread measures market uncertainty |
| `NP4-523-CD` | 2017-07 | DA system lambda (daily) | DA marginal cost; large DA-RT gaps signal volatility |

Excluded from primary analysis (insufficient history):

| Dataset | Available From | Reason for Exclusion |
|---|---|---|
| `NP4-745-CD` | 2022-06 | Solar data starts mid-2022; only ~2.5 years of overlap with the analysis window. Use in a supplementary sub-analysis from 2022-07 onward if solar effects are of interest, but do not include in the main model comparison. |
| `NP6-788-CD` | 2024-02 | RT LMPs start 2024; too short for training. |
| `NP6-331-CD` | 2025-12 | RT ancillary prices; only ~1 month in analysis window. |

### Derived Features to Engineer

Features available for the full `2017-07` to `2024-12` window:

| Feature | Formula | Interpretation |
|---|---|---|
| Load forecast error | `NP3-565-CD.SystemTotal - NP6-346-CD.TOTAL` | Demand surprise |
| Wind forecast error | `NP4-732-CD.COP_HSL_SYSTEM_WIDE - NP4-732-CD.ACTUAL_SYSTEM_WIDE` | Supply surprise |
| Net load (wind-only) | `actual_load - wind_actual` | Thermal generation need (pre-2022) |
| Reserve margin proxy | `available_capacity - actual_load` | Scarcity indicator |
| DA-RT spread | `NP6-905-CD.SPP - NP4-190-CD.SPP` (matched by hour + point) | Market uncertainty |

Features available only from `2022-07` onward (supplementary sub-analysis):

| Feature | Formula | Interpretation |
|---|---|---|
| Solar forecast error | `NP4-745-CD.COP_HSL - NP4-745-CD.SYSTEM_WIDE_GEN` | Solar supply surprise |
| Net load (wind+solar) | `actual_load - wind_actual - solar_actual` | Full renewable-adjusted demand |

## 2. EDA Roadmap

Run these analyses before any modeling. Each step builds evidence for volatility clustering and informs model selection.

### 2.1 Price Distribution

- Histogram of 15-min and hourly prices: expect extreme right tail, possible left tail (negative prices from wind over-generation)
- QQ-plot vs normal distribution: will show massive departure in tails
- Summary statistics by settlement point: mean, median, std, skewness, kurtosis, min, max
- Compare distribution of price *differences* (p_t - p_{t-1}) vs price *levels*

### 2.2 Volatility Clustering Evidence

This is the core diagnostic for the phenomenon:

- **Squared returns time series**: plot r^2_t over time; visual clustering is usually obvious in ERCOT
- **ACF of squared returns**: compute ACF of r^2_t out to 500+ lags; slow hyperbolic decay = clustering
- **ACF of absolute returns**: |r_t| ACF is often more robust than r^2_t (less outlier-sensitive)
- **Ljung-Box test**: formal test on r^2_t; reject the iid null at very high significance
- **ARCH-LM test**: Engle's test for ARCH effects in residuals; confirms conditional heteroskedasticity

Expected result: highly significant clustering at hourly frequency, with decay over ~48-168 lags (2-7 days).

### 2.3 Seasonality in Volatility

Volatility in electricity is strongly time-structured:

| Pattern | How to Check | Expected Result |
|---|---|---|
| Hourly profile | Average |r_t| by hour-of-day | Peaks at morning ramp (6-9am) and evening peak (5-8pm) |
| Day-of-week | Average |r_t| by weekday | Weekdays > weekends |
| Monthly/seasonal | Average |r_t| by month | Summer (Jul-Aug) and winter (Jan-Feb) highest |
| Holiday effects | Compare holiday vs non-holiday vol | Lower vol on major holidays |

This matters because seasonality in volatility must be removed before fitting GARCH, otherwise the model interprets predictable daily patterns as "clustering."

### 2.4 Spike Analysis

- Define spike threshold: price > $200/MWh, or |return| > 3 rolling standard deviations
- Count spike frequency by month and year
- Duration analysis: how many consecutive intervals does a spike last?
- Conditional analysis: merge spikes with load, wind, outage data; what are the physical conditions during spikes?
- Recovery speed: how fast do prices revert to normal after a spike?

Key ERCOT events to look for in data:
- **Winter Storm Uri** (Feb 2021): prices hit $9,000/MWh cap for days
- **Summer 2023 heat**: sustained high prices
- **Wind ramp events**: sudden drops in wind generation causing price spikes

### 2.5 DA-RT Spread

- Merge NP4-190-CD (DA) with NP6-905-CD (RT) on delivery hour and settlement point
- Compute spread = RT - DA
- Plot spread distribution: should be zero-mean but fat-tailed
- Test if spread volatility predicts next-day RT volatility

## 3. Data Preparation

### 3.1 Aggregation

Analysis window: `2017-07-01` to `2024-12-31`. Work at hourly frequency for most analyses:

```
NP6-905-CD (15-min) → hourly mean price per settlement point
NP6-346-CD (hourly)  → use directly
NP3-565-CD (hourly)  → filter to InUseFlag or average across models
NP4-732-CD (hourly)  → use directly
NP3-233-CD (hourly)  → use directly
NP4-190-CD (daily)   → match to RT hours for DA-RT spread
NP4-523-CD (daily)   → match to delivery hours
```

NP4-745-CD (solar, hourly) is available only from `2022-07`. Include in a supplementary analysis; do not use in the main `2017-07` models.

### 3.2 Returns Definition

Use **arithmetic price differences**, not log returns:

```
r_t = p_t - p_{t-1}    (preferred for electricity)
```

Rationale: electricity prices can be negative or near-zero, making log(p_t) undefined or numerically unstable. Price differences are the standard in electricity econometrics literature.

### 3.3 De-Seasonalization

Remove predictable periodic patterns before fitting volatility models:

```
1. Estimate hourly profile:    μ_h = mean(p_t | hour = h)
2. Estimate day-of-week effect: μ_d = mean(p_t | dow = d) - mean(p_t)
3. Estimate monthly effect:     μ_m = mean(p_t | month = m) - mean(p_t)
4. De-seasonalized price:       p*_t = p_t - μ_h - μ_d - μ_m
5. Compute returns on p*_t:     r*_t = p*_t - p*_{t-1}
```

Alternative: use a Fourier series with daily (24h) and weekly (168h) harmonics.

### 3.4 Validation Strategy: Rolling-Window Time Series CV

All data stays within the `2017-07` to `2024-12` shared window.

A single fixed train/val/test split is fragile for electricity data — it only tests against one market regime. Rolling-window cross-validation tests across multiple conditions (summer peaks, winter storms, shoulder seasons) and produces more robust model selection.

#### Hold-out test set

Reserve the final 6 months for a single, untouched final evaluation:

```
Final test:  2024-07 to 2024-12   (6 months, never used during model selection)
```

#### Rolling-window CV on 2017-07 to 2024-06

Use the remaining data for model selection via rolling folds. Each fold trains on past data and validates on the next evaluation window.

**Expanding-window CV** (for GARCH and regime-switching models):

```
Fold 1:  Train 2017-07 → 2019-06   Val 2019-07 → 2019-12
Fold 2:  Train 2017-07 → 2019-12   Val 2020-01 → 2020-06
Fold 3:  Train 2017-07 → 2020-06   Val 2020-07 → 2020-12
Fold 4:  Train 2017-07 → 2020-12   Val 2021-01 → 2021-06  ← includes Winter Storm Uri
Fold 5:  Train 2017-07 → 2021-06   Val 2021-07 → 2021-12
Fold 6:  Train 2017-07 → 2021-12   Val 2022-01 → 2022-06
Fold 7:  Train 2017-07 → 2022-06   Val 2022-07 → 2022-12
Fold 8:  Train 2017-07 → 2022-12   Val 2023-01 → 2023-06
Fold 9:  Train 2017-07 → 2023-06   Val 2023-07 → 2023-12
Fold 10: Train 2017-07 → 2023-12   Val 2024-01 → 2024-06
```

Why expanding: GARCH and regime-switching models benefit from all available history. More data improves regime identification and tail estimation.

**Rolling-window CV** (for ML models — XGBoost, LSTM):

```
Fold 1:  Train 2017-07 → 2019-06   Val 2019-07 → 2019-12   (24-month window)
Fold 2:  Train 2017-07 → 2019-12   Val 2020-01 → 2020-06   (30-month window)
...same folds, but cap training window at 36 months (rolling) once enough data is available:
Fold 5:  Train 2019-07 → 2021-06   Val 2021-07 → 2021-12   (24-month window, rolling)
Fold 6:  Train 2020-01 → 2021-12   Val 2022-01 → 2022-06   (24-month window, rolling)
...
```

Why rolling: ML models are sensitive to concept drift from structural market changes (increasing renewable penetration, rule changes). A fixed-size lookback keeps the training distribution closer to the validation period.

#### Summary of CV strategy by model type

| Model Type | CV Strategy | Min Training Size | Val Window | Folds |
|---|---|---|---|---|
| GARCH family | Expanding | 24 months | 6 months | 10 |
| Regime-switching | Expanding | 24 months | 6 months | 10 |
| HAR-RV | Rolling (24 months) | 24 months | 6 months | 10 |
| ML (XGBoost, LSTM) | Rolling (24 months) | 24 months | 6 months | 10 |
| Naive benchmarks | Expanding | N/A | 6 months | 10 |

#### Model selection procedure

1. For each model configuration, compute the evaluation metric (QLIKE, MSE) on every fold.
2. Average across folds for the primary ranking.
3. Also report per-fold results — performance on Fold 4 (Uri winter) vs summer folds reveals regime sensitivity.
4. Select the model with the best average metric.
5. Re-train the selected model on all data through 2024-06 and evaluate once on the 2024-07 to 2024-12 test set.

#### Why 6-month validation windows

- 6 months is long enough to capture both peak and off-peak conditions within each fold.
- Shorter windows (1-3 months) may fall entirely within one season, giving misleading results.
- 10 folds provide enough variation to test across all seasons and major events (Uri, summer 2023 heat).

#### Optional supplementary split for solar-inclusive models

NP4-745-CD (solar) is available from `2022-07`. For solar sub-analysis, use a smaller rolling CV:

```
Fold 1:  Train 2022-07 → 2023-06   Val 2023-07 → 2023-12
Fold 2:  Train 2022-07 → 2023-12   Val 2024-01 → 2024-06
Test:    Train 2022-07 → 2024-06   Test 2024-07 → 2024-12
```

## 4. Model Hierarchy

### Tier 0: Naive Benchmarks

These are non-negotiable baselines. Any proposed model must beat these.

| Model | Formula | Notes |
|---|---|---|
| **Historical Vol (rolling)** | σ_t = std(r_{t-W}, ..., r_{t-1}) | W = 24 (1 day), 168 (1 week), 720 (30 days) |
| **Seasonal Naive** | σ_t = |r_{t-168}| (same hour, 1 week ago) | Captures weekly pattern |
| **Expanding Window** | σ_t = std(r_1, ..., r_{t-1}) | Cumulative; very stable but unresponsive |
| **EWMA** | σ^2_t = λ σ^2_{t-1} + (1-λ) r^2_{t-1} | λ = 0.94 (RiskMetrics); no estimation needed |

### Tier 1: GARCH Family (Core Benchmarks)

| Model | Key Feature | When to Use |
|---|---|---|
| **GARCH(1,1)** | σ^2_t = ω + α r^2_{t-1} + β σ^2_{t-1} | Default starting model; baseline for all comparisons |
| **EGARCH(1,1)** | log(σ^2_t) = ω + α g(z_{t-1}) + β log(σ^2_{t-1}) | Captures asymmetric response to positive vs negative shocks |
| **GJR-GARCH(1,1)** | σ^2_t = ω + (α + γ I_{r<0}) r^2_{t-1} + β σ^2_{t-1} | Alternative asymmetry; γ > 0 means negative shocks increase vol more |
| **GARCH-X** | GARCH(1,1) + exogenous regressors in variance equation | Add load, wind error, outage capacity as vol drivers |
| **IGARCH(1,1)** | GARCH with α + β = 1 (integrated) | If GARCH α + β is very close to 1, vol shocks are persistent |

Critical GARCH configuration for electricity:
- **Innovation distribution**: Student-t (df ~ 3-6) or skewed-t, never Gaussian
- **Mean equation**: AR(1) or AR(24) to capture hourly autocorrelation, then GARCH on residuals
- **Input series**: de-seasonalized price differences, not raw prices

### Tier 2: Regime-Switching Models

Strongly recommended for electricity. Electricity prices exhibit at least two distinct regimes:

| Regime | Price Behavior | Volatility |
|---|---|---|
| **Normal (base)** | Stable, mean-reverting around seasonal pattern | Low, predictable |
| **Stressed (spike)** | Extreme jumps, rapid reversion | Very high, clustered |

| Model | Description | Advantage |
|---|---|---|
| **MS-AR(1)** | Markov-switching AR with regime-dependent mean + variance | Simple; captures regime shifts in both level and vol |
| **MS-GARCH** | Markov-switching with GARCH dynamics within each regime | Most flexible; GARCH handles within-regime clustering |
| **MS-AR + exogenous transition** | Transition probability depends on observables (load, wind, outage) | Economically interpretable: what triggers regime switches? |
| **Threshold GARCH** | Regime determined by observable variable (e.g., reserve margin) | No latent state estimation; simpler inference |

Two-regime models consistently outperform single-regime GARCH in electricity price literature. The key insight: a single GARCH overestimates base-regime vol and underestimates spike-regime vol.

### Tier 3: Realized Volatility and HAR

| Model | Description |
|---|---|
| **Realized Volatility (RV)** | RV_t = sum(r^2_{t,i}) over 15-min intraday returns. Use 15-min NP6-905-CD data to construct daily RV. |
| **HAR-RV** | RV_t = c + β_d RV_{t-1} + β_w RV_{t-5:t-1} + β_m RV_{t-22:t-1}. Heterogeneous Autoregressive model using daily, weekly, monthly RV components. Simple, linear, strong baseline. |
| **HAR-RV-X** | HAR-RV with exogenous regressors (load forecast error, wind ramp, outage jump). |

HAR-RV is attractive because:
- 15-min data from NP6-905-CD provides a clean measure of daily realized volatility
- The model is linear and easy to estimate
- It captures the multi-scale nature of vol persistence (daily, weekly, monthly)

### Tier 4: Machine Learning

| Model | Use Case | Features |
|---|---|---|
| **XGBoost / Random Forest** | Regime classification (high-vol vs low-vol day) | Lagged returns, RV, load, wind, outage, DA-RT spread, calendar |
| **LSTM / GRU** | Sequence-to-sequence vol forecasting | Multivariate time series of returns + fundamentals |
| **Temporal Fusion Transformer** | Multi-horizon vol forecasting with attention | Best for multi-step-ahead; interpretable attention weights |

ML models are hardest to benchmark fairly. They require more data engineering but can capture nonlinear interactions (e.g., wind ramp + high load + outage = spike). Solar features (NP4-745-CD) can be added in a supplementary sub-analysis from 2022-07 onward.

## 5. Benchmark Design

### Univariate vs Multivariate Benchmark Layers

Every model should first be tested in its **univariate form** (price series only) before adding external features. If the multivariate version can't beat the univariate version, the extra data isn't helping.

This creates two benchmark layers:

**Layer 1 — Univariate (price series only)**

These models see only the RT price series (NP6-905-CD returns). No load, wind, outages, or DA prices. This is the baseline that any multivariate model must beat.

| Model | Input | Category | Parameters | Re-estimation |
|---|---|---|---|---|
| Historical Vol (168h) | Past returns | Naive | 0 | None |
| EWMA (λ=0.94) | Past returns | Naive | 0 | None |
| GARCH(1,1)-t | Past returns | Parametric | ~5 | Weekly expanding |
| EGARCH(1,1)-t | Past returns | Parametric | ~6 | Weekly expanding |
| MS-AR(1), 2 regimes | Past returns | Regime-switching | ~8 | Monthly expanding |
| HAR-RV | 15-min returns → daily RV | Realized vol | ~4 | Weekly rolling |
| XGBoost (univariate) | Lagged returns, RV, calendar | ML | Tuned | Monthly retrain |

**Layer 2 — Multivariate (price + fundamentals)**

Same model structures, but with exogenous regressors added. The improvement over Layer 1 quantifies the value of external data.

| Model | Added Features | Category | Parameters | Re-estimation |
|---|---|---|---|---|
| GARCH-X(1,1)-t | Load, wind error, outage | Parametric | ~8 | Weekly expanding |
| MS-AR + exogenous transition | Load, outage → transition prob | Regime-switching | ~12 | Monthly expanding |
| HAR-RV-X | Load error, wind ramp, outage | Realized vol | ~8 | Weekly rolling |
| XGBoost (multivariate) | + load, wind, outage, DA-RT spread | ML | Tuned | Monthly retrain |
| LSTM | Returns + all fundamentals | Deep learning | Tuned | Monthly retrain |

### Paired Univariate vs Multivariate Comparisons

Each row is a direct test of whether external data improves the same model structure:

| Univariate Baseline | Multivariate Extension | Question Answered |
|---|---|---|
| GARCH(1,1)-t | GARCH-X(1,1)-t | Do load/wind/outage improve GARCH vol forecasts? |
| MS-AR(1) | MS-AR + exogenous transition | Do fundamentals predict regime switches? |
| HAR-RV | HAR-RV-X | Do fundamentals improve realized vol forecasts? |
| XGBoost (univariate) | XGBoost (multivariate) | Does adding fundamentals help nonlinear models? |

If univariate models already capture most of the volatility dynamics (which is common in well-functioning markets), the multivariate improvement may be small. But in ERCOT — where supply shocks from wind ramps and outages directly cause spikes — fundamentals are more likely to add value.

### Within-Tier Ablation Tests

| Comparison | Question |
|---|---|
| GARCH(1,1) vs EGARCH | Does asymmetry in shock response matter? |
| GARCH(1,1) vs MS-AR(1) | Do regimes outperform a single-state model? |
| HAR-RV vs GARCH(1,1) | Does using realized vol beat conditional vol? |
| Best univariate vs best multivariate | How much does external data improve the best model? |
| Single hub (HB_WEST) vs multi-hub | Does cross-sectional info help? |

### Forecasting Targets

| Target | Definition | Horizon |
|---|---|---|
| 1-hour-ahead σ | Conditional std dev of next hourly return | 1 step |
| 1-day-ahead RV | Realized volatility of next 24 hourly returns | 24 steps |
| Spike probability | P(\|r_t\| > threshold) in next 24 hours | 24 steps |
| Regime forecast | P(spike regime) tomorrow | 1 day |

## 6. Evaluation Framework

### Loss Functions

| Metric | Formula | Properties |
|---|---|---|
| **QLIKE** | log(σ^2_f) + r^2 / σ^2_f | Robust to noise in vol proxy; preferred in literature |
| **MSE** | (r^2 - σ^2_f)^2 | Standard but outlier-sensitive |
| **MAE** | \||r| - σ_f\| | More robust than MSE |
| **R^2 (Mincer-Zarnowitz)** | Regress r^2 on σ^2_f; report R^2 | Tests forecast calibration |

Use QLIKE as the primary ranking metric. Report MSE and MAE as secondary.

### Risk Metrics

| Metric | Definition | Target |
|---|---|---|
| **VaR (1%)** | 1st percentile of return distribution forecast | Coverage should be ~1% |
| **VaR (5%)** | 5th percentile | Coverage should be ~5% |
| **ES / CVaR** | Expected loss given VaR exceedance | Average tail loss |
| **Kupiec test** | Binomial test on VaR violation rate | p-value > 0.05 = pass |
| **Christoffersen test** | Tests independence of VaR violations | Violations should not cluster |

### Statistical Tests

| Test | Purpose |
|---|---|
| **Diebold-Mariano** | Pairwise comparison of forecast accuracy (e.g., GARCH vs MS-GARCH) |
| **Model Confidence Set (MCS)** | Identify the set of models with statistically indistinguishable performance |
| **Ljung-Box on standardized residuals** | Residual diagnostics; model adequacy check |

## 7. Implementation Plan

### Phase 1: EDA and Data Pipeline

1. Build hourly price series for hub settlement points from NP6-905-CD
2. Compute and plot squared returns, ACF, ARCH-LM test
3. Characterize seasonality (hourly, weekly, monthly volatility profiles)
4. Identify and catalog spike events
5. Merge supporting datasets; compute forecast errors
6. De-seasonalize prices; produce clean return series

### Phase 2: Naive + GARCH Benchmarks

1. Implement historical vol, EWMA, seasonal naive
2. Fit GARCH(1,1)-t on de-seasonalized returns
3. Fit EGARCH, GJR-GARCH variants
4. Run expanding-window CV (10 folds); evaluate using QLIKE; select best GARCH spec
5. Add exogenous regressors (GARCH-X) and test improvement
6. Report per-fold metrics — check whether Fold 4 (Uri winter) is an outlier or informative

### Phase 3: Regime-Switching

1. Fit 2-regime MS-AR on de-seasonalized returns
2. Extract smoothed regime probabilities; plot against known events (Uri, heat waves)
3. If feasible, fit MS-GARCH for within-regime dynamics
4. Test regime-dependent features (transition probabilities driven by outage/load)

### Phase 4: Realized Volatility

1. Construct daily RV from 15-min returns
2. Fit HAR-RV baseline
3. Add exogenous regressors (HAR-RV-X)
4. Compare against GARCH daily vol forecasts

### Phase 5: ML Models (Optional)

1. Feature engineering: lagged returns, RV, calendar, load, wind forecast error, outage, DA-RT spread
2. Train XGBoost regime classifier
3. Train LSTM vol forecaster
4. Compare against parametric models on test set
5. Optional: re-run with solar features on `2022-07` to `2024-12` sub-window

### Phase 6: Final Evaluation

1. Select best model per tier based on average QLIKE across 10 CV folds
2. Re-train each selected model on all data through 2024-06
3. Evaluate once on held-out test set (2024-07 to 2024-12)
4. Compute QLIKE, MSE, MAE, VaR coverage for all models on the test set
5. Run Diebold-Mariano pairwise tests on test-set forecasts
6. Construct Model Confidence Set
7. Write findings report with both CV results (model selection) and test results (final performance)

## 8. References and Tools

### Python Packages

```bash
pip install arch statsmodels scikit-learn xgboost pandas numpy matplotlib seaborn
```

| Package | Use |
|---|---|
| `arch` | GARCH, EGARCH, GJR-GARCH, HAR-RV, ARCH-LM test, conditional volatility |
| `statsmodels` | Markov-switching models (`tsa.regime_switching`), ACF/PACF, Ljung-Box |
| `scikit-learn` | Random Forest, preprocessing, cross-validation |
| `xgboost` | Gradient boosting for regime classification |
| `pandas` | Data manipulation, time series handling |
| `matplotlib` / `seaborn` | Visualization |

### Key Literature

- Weron (2014), "Electricity price forecasting: A review of the state-of-the-art" — comprehensive survey
- Janczura & Weron (2010), "Regime-switching models for electricity spot prices" — MS models for power
- Knittel & Roberts (2005), "An empirical examination of restructured electricity prices" — regime-switching in deregulated markets
- Corsi (2009), "A simple approximate long-memory model of realized volatility" — HAR-RV
- Bollerslev (1986), "Generalized autoregressive conditional heteroskedasticity" — original GARCH
- Hansen, Huang & Shek (2012), "Realized GARCH" — combining realized measures with GARCH

### Data Paths (This Repo)

Primary datasets (full `2017-07` to `2024-12` coverage):

| Dataset | Path | Size | Available From |
|---|---|---|---|
| NP6-905-CD (RT prices) | `data/raw/ercot/NP6-905-CD/` | 14G | 2017-07 |
| NP6-346-CD (actual load) | `data/raw/ercot/NP6-346-CD/` | 7.3M | 2017-07 |
| NP3-565-CD (load forecast) | `data/raw/ercot/NP3-565-CD/` | 13G | 2017-07 |
| NP4-732-CD (wind) | `data/raw/ercot/NP4-732-CD/` | 2.0G | 2017-07 |
| NP3-233-CD (outages) | `data/raw/ercot/NP3-233-CD/` | 989M | 2017-07 |
| NP4-190-CD (DA prices) | `data/raw/ercot/NP4-190-CD/` | 5.0G | 2017-07 |
| NP4-523-CD (DA lambda) | `data/raw/ercot/NP4-523-CD/` | 4.7M | 2017-07 |

Supplementary (limited history — use only in sub-analyses):

| Dataset | Path | Size | Available From | Note |
|---|---|---|---|---|
| NP4-745-CD (solar) | `data/raw/ercot/NP4-745-CD/` | 959M | 2022-06 | Solar sub-analysis only |
