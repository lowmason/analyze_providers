# Pipeline overview

The package supports a single **run** command that executes the full pipeline. Each step can be understood and re-run using cached intermediate outputs where applicable.

## Execution order

1. **Load and validate payroll data** — `data/payroll.py`: `load_payroll()`. Validates required columns, casts types, adds `naics2`, `naics3`, `supersector`, `size_class`, `quarter`. Detects optional columns (`gross_pay`, `filing_date`, `employee_id`, `hires`, `separations`) and logs which extended analyses are available. Returns a `LazyFrame`.

2. **Fetch or load official data** — `data/qcew.py`, `data/ces.py`, `data/bed.py`: `fetch_qcew()`, `fetch_ces()`, `fetch_bed()` via eco-stats `BLSClient`. Data is cached as parquet under `CACHE_DIR`. Use `--force-refresh` to bypass cache.

3. **Data quality assessment** — `analysis/data_quality.py`: `assess_quality()`. Runs **first**, before any analytical step. Computes column completeness rates, zero-employment month counts, NAICS-code validity, filing-lag distribution (when `filing_date` is present), and produces a data-quality scorecard. The scorecard flags columns and time ranges that may affect downstream results and writes `analysis/data_quality.parquet`.

4. **Client tenure and churn** — `analysis/tenure.py`: `compute_tenure()`, `compute_churn_rates()`. Measures how long clients remain in the provider's book, computes quarterly churn (exit) rates, and produces tenure-distribution summaries by supersector and size class. Results written to `analysis/tenure.parquet`.

5. **Vintage assessment** — `analysis/tenure.py`: `compute_vintage_composition()`. Groups clients by entry cohort (vintage) and tracks each vintage's share of total employment over time. Useful for understanding whether representativeness depends on a few long-tenured anchor clients or a broad base.

6. **Build aggregated panel** — `panel.py`: `build_panel()`. Builds the client-month panel with grouping levels: national, supersector, state, size_class, supersector x state, supersector x size_class. Output: one LazyFrame with `grouping_level` and aggregation columns (`payroll_employment`, `client_count`, `birth_count`, etc.).

7. **Coverage analysis** — `analysis/coverage.py`: `compute_coverage()` at national (and optionally other levels). Compares payroll employment and client count to QCEW employment and establishments; writes coverage ratios. `compute_cell_reliability()` produces the usability map (reliable / marginal / insufficient).

8. **Growth analysis** — `analysis/growth.py`: `compute_growth_rates()` on payroll and CES; `compare_growth()` merges and computes differences and rolling correlation. `decompose_growth_divergence()` gives shift-share (composition vs within-cell). Includes employment-change decomposition that attributes net employment change to continuing, entering, and exiting clients. Results written to `analysis/growth.parquet`.

9. **Worker-level flows** — `analysis/flows.py`: `compute_flows()`. When `employee_id` or `hires`/`separations` columns are present, computes accession and separation rates at monthly and quarterly frequencies. `compare_flows_to_bed()` benchmarks against BED gross-flows data. Optionally computes job-to-job transition rates when `employee_id` is available. Results written to `analysis/flows.parquet`. Skipped if no flow-relevant columns are present.

10. **Earnings analysis** — `analysis/earnings.py`: `compute_earnings()`. When `gross_pay` is present, computes average pay per employee, pay-growth rates, and pay distributions by supersector and size class. `compare_earnings_to_qcew()` benchmarks against QCEW average weekly wages. Results written to `analysis/earnings.parquet`. Skipped if `gross_pay` is absent.

11. **Reweighting** — `analysis/reweight.py`: `rake_to_qcew()` so payroll margins match QCEW on chosen dimensions (e.g. supersector). Growth is recomputed with reweighted employment for the reweighting-impact exhibit.

12. **Birth analysis** — `analysis/births.py`: `compute_payroll_birth_rates()` (births / birth-determinable by quarter). Optional: `compare_birth_rates()` vs BED, `test_birth_lead()` for Granger-style regressions. Results written to `analysis/birth_rates.parquet`.

13. **Exhibits** — `output/exhibits.py`: `generate_all_exhibits()`. Produces coverage heatmaps, industry composition, size distribution, coverage over time, growth tracking, growth decomposition, birth rate comparison, cross-correlation, regression table, usability map, reweighting impact, tenure distributions, vintage composition, worker-flow rates, and earnings comparisons (when data is available). Saves PNG and PDF under `exhibits/`.

14. **Report** — `output/report.py`: `build_executive_summary()`, `build_dashboard()`, `build_technical_appendix()`. Assembles the three-layer deliverable: one-page summary, dashboard of exhibits, and appendix with cell-level CSVs and methodology. New sections are included for data quality, tenure/churn, worker flows, and earnings when the corresponding analyses were run.

## Recommended workflow

The steps above represent the default `run` command order. For iterative work:

1. Start with **data quality assessment** (step 3) to identify potential issues before investing compute in downstream analyses.
2. Review **tenure and vintage** (steps 4-5) to understand client-base stability.
3. Run **coverage** and **growth** (steps 7-8) for the core representativeness assessment.
4. Add **worker flows** and **earnings** (steps 9-10) for deeper labour-market benchmarking, if the optional columns are available.
5. Finish with **reweighting**, **births**, **exhibits**, and **report** (steps 11-14).

## Data flow

- **Data layer** (`data/`): payroll load/validate; QCEW/CES/BED fetch and cache. No knowledge of analyses.
- **Panel** (`panel.py`): builds one panel LazyFrame with multiple grouping levels for downstream use.
- **Analysis layer** (`analysis/`): accepts LazyFrames/DataFrames and grouping parameters; no direct BLS calls.
- **Output layer** (`output/`): takes analysis outputs and writes exhibits and report files.

Lazy evaluation is used throughout; `.collect()` is called only where needed (e.g. before writing parquet or when mkdocstrings/stats require eager data).
