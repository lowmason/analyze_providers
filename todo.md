# TODO — `analyze_provider`

Gaps between the merged methodology document (`payroll_provider_analysis_alt_nfp.md`) and the current codebase, plus bugs and stubs found during audit.

---

## 1. New modules

### 1.1 `analysis/flows.py` — Worker-level gross job flows (methodology §4)

The methodology adds an entire tier for employee-level labor-market flows. Nothing in the codebase touches employee-level records today.

- [ ] Define expected employee-level input columns (`employee_id`, `hire_date`, `separation_date`, or derive hires/separations from month-over-month presence at a client).
- [ ] `compute_job_flows(panel, grouping_cols)` — monthly hires, separations, continuing employment, derived rates (hire rate, separation rate, churn rate, net growth rate) as share of employment.
- [ ] `compute_job_flows_by_geography(panel, geo_col, grouping_cols)` — flows stratified by `state_fips`, Census region, Census division.
- [ ] `compute_job_flows_by_industry(panel, industry_col, grouping_cols)` — flows stratified by supersector, 2-digit NAICS, goods/services domain.
- [ ] `compute_job_flows_by_size(panel, grouping_cols)` — flows stratified by `size_class`.
- [ ] Add JOLTS validation pathway: compare provider hire/separation rates to published JOLTS rates at supersector level.

### 1.2 `analysis/tenure.py` — Client tenure and churn (methodology §5)

The methodology adds a full tier for understanding sample stability. The current `panel.py` computes `entry_count` / `exit_count` but none of the richer metrics.

- [ ] `compute_client_tenure(payroll)` — per-client: `first_observed`, `last_observed`, `tenure_months`, `months_observed`, `initial_emp`, `final_emp`, `avg_emp`, `is_likely_birth`.
- [ ] `compute_client_entry_exit(payroll, grouping_cols)` — monthly counts/rates of entries, exits, churn rate, net client change.
- [ ] `client_churn_by_geography(payroll, geo_col)` / `client_churn_by_industry(...)` / `client_churn_by_size(...)` — stratified entry/exit rates.
- [ ] `compute_vintage_analysis(payroll, grouping_cols)` — employment and payroll aggregates stratified by client vintage (year of first appearance).
- [ ] `compute_vintage_shares(payroll)` — time series of employment share by vintage; flag contamination when recent-vintage share is high.
- [ ] `tenure_summary_by_group(payroll, group_col)` — mean, median, percentiles, std of tenure for any grouping variable.

### 1.3 `analysis/data_quality.py` — Data quality flags (methodology §7)

No data-quality module exists.

- [ ] `flag_data_quality_issues(payroll)` — flag at the client-month level:
  - Extreme employment changes (>50% MoM).
  - Zero-employment months (client present but `qualified_employment == 0`).
  - Multi-client employees (same `employee_id` at multiple clients in same month — requires employee-level data).
  - Filing-date anomalies (`filing_date` after first observation).
- [ ] Return a summary DataFrame with flag counts by type, plus the flagged rows for downstream exclusion/winsorization.

### 1.4 `analysis/earnings.py` — Earnings distribution (methodology §4.3)

The methodology calls for earnings analysis. No earnings code exists and the payroll input schema lacks a wages column.

- [ ] Extend payroll input schema to accept an optional `gross_pay` column (or similar).
- [ ] `compute_earnings_distribution(payroll, grouping_cols)` — mean, median, P10/P25/P75/P90, std, CV of monthly gross pay.
- [ ] `compute_earnings_growth(payroll, grouping_cols)` — YoY growth in mean and median earnings.
- [ ] Validation pathway against QCEW average weekly wages.

---

## 2. Missing methods in existing modules

### 2.1 `analysis/coverage.py`

- [ ] **Composition Shift Index** (methodology §2.3): Add `compute_composition_shift_index(payroll_agg, dimension)` — `CSI_t = Σ|s_{i,t} − s_{i,t−1}|`, bounded [0, 2]. Track separately for industry, geography, size class.

### 2.2 `analysis/growth.py`

- [ ] **Employment change decomposition** (methodology §3.3): Add `decompose_employment_change(payroll, grouping_cols)` — decompose monthly employment change into within-client (intensive margin), entry contribution (extensive margin), and exit contribution (extensive margin). Distinct from the existing shift-share which decomposes *divergence from CES*.
- [ ] **Fix `decompose_growth_divergence`**: Current implementation is a stub that hard-codes `composition_effect = 0`. Implement proper shift-share: for each cell, compute `Δg = Σ(w_p − w_o) * g_o + Σ w_p * (g_p − g_o)` where `w` = weight and `g` = cell growth rate for payroll (p) and official (o).
- [ ] **Fix `analyze_turning_points`**: Current lead/lag calculation is naïve (all-pairs Cartesian of turning-point dates). Should match each payroll turning point to its nearest official turning point (or vice versa) and report per-event lead/lag.

### 2.3 `analysis/births.py`

- [ ] **Fix `test_birth_lead`**: Only runs the concurrent model. Implement all three Granger-style regressions from the methodology:
  - Model 1 (concurrent): `BED(q) = α + β·payroll(q)`
  - Model 2 (leading): `BED(q) = α + β₁·payroll(q) + β₂·payroll(q−1)`
  - Model 3 (incremental / Granger): `BED(q) = α + β₁·BED(q−1) + β₂·payroll(q−1)`
- [ ] **Cross-correlation**: Add explicit cross-correlation at lags 0 to `max_lag` quarters (currently absent; only regression is attempted).
- [ ] **Fix `compute_survival_curves`**: Current implementation is a stub that returns cohort counts with a null `survival_4q` column. Implement Kaplan-Meier-style survival at 4, 8, 12, 16, 20 quarters after entry, stratified by cohort year. Compare to BED survival rates.
- [ ] **Fix `compare_birth_determinable_composition`**: Calls `.collect()` mid-function via `.group_by().agg()` without `.lazy()`, making it inconsistent with the lazy-first convention. Also computes misallocation index inline instead of reusing `coverage.compute_share_comparison`.

### 2.4 `analysis/reweight.py`

- [ ] **Full IPF loop**: Current `rake_to_qcew` runs a single margin adjustment. Implement iterative proportional fitting: loop over each dimension in `dimensions`, adjusting weights to match that dimension's QCEW marginals, repeating until `max_iter` or `tolerance` is met.
- [ ] Support multi-dimensional raking (e.g. supersector × state × size class simultaneously).

---

## 3. Data layer gaps

### 3.1 `data/payroll.py`

- [ ] **Employee-level support**: The current schema is client-month only. Worker-level flows (§4) require either employee-level records or pre-aggregated hire/separation counts per client-month. Add an optional `load_payroll_employees(path)` loader or extend the schema to accept `hires`, `separations`, `gross_pay` columns.
- [ ] **`gross_pay` / earnings column**: Not in `REQUIRED_COLUMNS` or the parquet spec. Needed for earnings analysis. Make it optional with a validation check.
- [ ] **`filing_date` column**: Referenced in data quality flags but not in `REQUIRED_COLUMNS`. Make it optional.

### 3.2 `data/qcew.py`

- [ ] **State-level fetching**: `fetch_qcew` only fetches national data (`industry_code='10'`). The methodology requires state-level and supersector-level QCEW for coverage cross-tabs, the usability map, and reweighting. Fetch by area for all 51 state FIPS codes using `bls.get_qcew_area()`.
- [ ] **Size-class fetching**: QCEW size data (Q1 only) is not fetched. Use `bls.get_qcew_size()` for size codes `'1'`–`'9'` to support size-class coverage and reweighting.
- [ ] **Supersector mapping**: QCEW returns `industry_code`; the pipeline needs to map these to supersectors for joins against the payroll panel. Add a mapping step post-fetch.

### 3.3 `data/ces.py`

- [ ] **Supersector name mapping**: `fetch_ces` stores the raw 2-digit supersector *code* (e.g. `'00'`, `'05'`) in the `supersector` column. The payroll panel uses supersector *names* (e.g. `'Construction'`). Add a join to `bls.get_mapping('CE', 'supersector')` so the column contains names, or add a mapping step in the CLI before joining.

### 3.4 `data/bed.py`

- [ ] **Deaths and total establishments**: `fetch_bed` only fetches births (`data_element='02'`). The methodology needs deaths and total establishments for death rates and BED birth rate denominators. Fetch `data_element` codes for deaths, total establishments, and compute `birth_rate = births / beginning-of-quarter establishments`.
- [ ] **Industry and state stratification**: Only fetches national total (`state_fips='00'`, `industry='000000'`). The methodology calls for BED comparisons by supersector and state.

---

## 4. Panel / pipeline

### 4.1 `panel.py`

- [ ] **Continuing-client panel**: The methodology (§5) emphasizes filtering to clients with 12–24+ months tenure before computing employment dynamics. Add a helper `filter_stable_panel(payroll, min_tenure_months)` that restricts to long-tenure clients.
- [ ] **Within-client employment change**: For the employment change decomposition (§3.3), the panel needs to distinguish employment change at continuing clients from entry/exit contributions. Add `continuing_employment` (employment at clients present in both current and prior month) to the panel aggregation.

### 4.2 `cli.py` — Pipeline ordering

The methodology appendix defines a recommended workflow. The current `run` command doesn't follow it:

- [ ] Run data quality assessment first (before any analysis).
- [ ] Compute client tenure and churn before coverage or growth.
- [ ] Run vintage assessment to determine contamination.
- [ ] Construct stable panel (filter by tenure) before running employment dynamics.
- [ ] Add worker-level flows step.
- [ ] Add earnings analysis step.
- [ ] Integrate new modules into `run` (flows, tenure, data_quality, earnings).
- [ ] Pass new analysis outputs to `generate_all_exhibits`.

---

## 5. Exhibits

### 5.1 New exhibits needed

- [ ] **Gross job flows chart** — hire rate, separation rate, churn rate over time (national and key supersectors).
- [ ] **Client churn chart** — entry rate, exit rate, net client change over time.
- [ ] **Vintage composition chart** — stacked area showing employment share by client vintage over time.
- [ ] **Composition Shift Index chart** — CSI over time for industry, geography, size class.
- [ ] **Employment change decomposition chart** — stacked bar: within-client vs entry vs exit contributions by month.
- [ ] **Earnings distribution chart** — median and P10/P90 earnings over time.
- [ ] **Data quality summary table** — counts of each flag type by month.
- [ ] **Client survival curves** — Kaplan-Meier curves by entry cohort.
- [ ] **Tenure distribution histogram** — distribution of client tenure months.

### 5.2 Existing exhibit fixes

- [ ] `coverage_over_time`: Crashes if `coverage_time_df` has tick labels but no text (`ax.get_xticklabels()` returns empty `Text` objects before draw). Use `ax.set_xticks` + `ax.set_xticklabels` explicitly from the quarter column.
- [ ] `size_class_distribution`: Indexes columns by position (`size_df.columns[1]`, `size_df.columns[-1]`) instead of named columns when `payroll_share` / `qcew_share` are missing. Fragile — should raise or handle gracefully.

---

## 6. Documentation

- [ ] **Update `docs/guide/data-sources.md`**: Document optional columns (`gross_pay`, `filing_date`, employee-level fields) and their role in enabling earnings, data quality, and worker-flow analyses.
- [ ] **Update `docs/guide/pipeline.md`**: Add steps for data quality, tenure/churn, vintage, worker flows, earnings, and the recommended workflow order.
- [ ] **Update `docs/onboard.md`**: Add module specs for `analysis/flows.py`, `analysis/tenure.py`, `analysis/data_quality.py`, `analysis/earnings.py`.
- [ ] **Add API doc stubs**: `docs/api/analysis_flows.md`, `docs/api/analysis_tenure.md`, `docs/api/analysis_data_quality.md`, `docs/api/analysis_earnings.md`.
- [ ] **Update `mkdocs.yml` nav**: Add new API reference pages.
- [ ] **Update `README.md` features list**: Add worker flows, client tenure/churn, vintage analysis, data quality, earnings.
- [ ] **Update package structure tree** in `README.md` and `docs/onboard.md`.

---

## 7. Tests

- [ ] `test_births.py`: Add tests for `test_birth_lead` (all three models), `compute_survival_curves`, `compare_birth_determinable_composition`.
- [ ] `test_growth.py`: Add tests for `decompose_growth_divergence` (real shift-share), `analyze_turning_points`, `decompose_employment_change`.
- [ ] `test_coverage.py`: Add test for `compute_share_comparison`, `compute_composition_shift_index`.
- [ ] `test_reweight.py`: New file — test that `rake_to_qcew` converges and that marginals match after raking.
- [ ] `test_flows.py`: New file — test `compute_job_flows` with synthetic employee-level data.
- [ ] `test_tenure.py`: New file — test `compute_client_tenure`, `compute_client_entry_exit`, `compute_vintage_analysis`.
- [ ] `test_data_quality.py`: New file — test `flag_data_quality_issues` catches extreme changes, zero-employment, filing anomalies.
- [ ] `test_earnings.py`: New file — test `compute_earnings_distribution`, `compute_earnings_growth`.
- [ ] `test_panel.py`: Add test for `filter_stable_panel`, `continuing_employment` column.

---

## 8. Bugs and code quality

- [ ] **`naics.py` — missing mapping**: NAICS `53` maps to `'Professional and business services'` but CES groups `53` (Real Estate) under `'Financial activities'`. Verify against `bls.get_mapping('CE', 'supersector')` and correct. Currently `52` → Financial activities is correct, but `53` should be Financial activities (Real Estate, Rental, Leasing), not Professional and business services.
- [ ] **`cli.py` — `exhibits` command name collision**: The local import `from analyze_provider.output import exhibits` shadows the function name `exhibits` used as the Typer command. Rename the import or the command function.
- [ ] **`cli.py` — QCEW `ref_date` alignment**: The `run` command computes `ref_date` from QCEW as `date(year, (qtr-1)*3+1, 12)`, which gives only the first month of each quarter (Jan, Apr, Jul, Oct). QCEW has `month1_emplvl`, `month2_emplvl`, `month3_emplvl` for all three months. The pipeline should unpivot these into three rows per quarter to align with all 12 monthly payroll observations, not just 4.
- [ ] **`growth.py` — `compare_growth` rolling correlation**: Uses `.rolling(12).over(...).corr(...)` which is not valid polars syntax. Polars `rolling_corr` or `rolling` with a struct-based approach is needed. Rewrite using `pl.rolling_corr` or a UDF.
- [ ] **`births.py` — `compare_birth_determinable_composition`**: Calls eager `.group_by().agg()` on LazyFrames without `.collect()` (or `.lazy()`), mixing lazy/eager. Also indexes result with `merged['abs_dev']` which fails on LazyFrame.
- [ ] **`births.py` — `test_birth_lead`**: Uses `model.params.iloc[1]` (pandas indexing) on statsmodels output. If polars is the only dataframe library, this works only because statsmodels returns pandas internally — but should use `model.params[1]` (numpy indexing) for clarity.
