# Pipeline overview

The package supports a single **run** command that executes the full pipeline. Each step can be understood and re-run using cached intermediate outputs where applicable.

## Execution order

1. **Load and validate payroll data** — `data/payroll.py`: `load_payroll()`. Validates required columns, casts types, adds `naics2`, `naics3`, `supersector`, `size_class`, `quarter`. Returns a `LazyFrame`.

2. **Fetch or load official data** — `data/qcew.py`, `data/ces.py`, `data/bed.py`: `fetch_qcew()`, `fetch_ces()`, `fetch_bed()` via eco-stats `BLSClient`. Data is cached as parquet under `CACHE_DIR`. Use `--force-refresh` to bypass cache.

3. **Build aggregated panel** — `panel.py`: `build_panel()`. Builds the client–month panel with grouping levels: national, supersector, state, size_class, supersector×state, supersector×size_class. Output: one LazyFrame with `grouping_level` and aggregation columns (`payroll_employment`, `client_count`, `birth_count`, etc.).

4. **Coverage analysis** — `analysis/coverage.py`: `compute_coverage()` at national (and optionally other levels). Compares payroll employment and client count to QCEW employment and establishments; writes coverage ratios. `compute_cell_reliability()` produces the usability map (reliable / marginal / insufficient).

5. **Growth analysis** — `analysis/growth.py`: `compute_growth_rates()` on payroll and CES; `compare_growth()` merges and computes differences and rolling correlation. `decompose_growth_divergence()` gives shift-share (composition vs within-cell). Results written to `analysis/growth.parquet`.

6. **Reweighting** — `analysis/reweight.py`: `rake_to_qcew()` so payroll margins match QCEW on chosen dimensions (e.g. supersector). Growth is recomputed with reweighted employment for the reweighting-impact exhibit.

7. **Birth analysis** — `analysis/births.py`: `compute_payroll_birth_rates()` (births / birth-determinable by quarter). Optional: `compare_birth_rates()` vs BED, `test_birth_lead()` for Granger-style regressions. Results written to `analysis/birth_rates.parquet`.

8. **Exhibits** — `output/exhibits.py`: `generate_all_exhibits()`. Produces coverage heatmaps, industry composition, size distribution, coverage over time, growth tracking, growth decomposition, birth rate comparison, cross-correlation, regression table, usability map, reweighting impact. Saves PNG and PDF under `exhibits/`.

9. **Report** — `output/report.py`: `build_executive_summary()`, `build_dashboard()`, `build_technical_appendix()`. Assembles the three-layer deliverable: one-page summary, dashboard of exhibits, and appendix with cell-level CSVs and methodology.

## Data flow

- **Data layer** (`data/`): payroll load/validate; QCEW/CES/BED fetch and cache. No knowledge of analyses.
- **Panel** (`panel.py`): builds one panel LazyFrame with multiple grouping levels for downstream use.
- **Analysis layer** (`analysis/`): accepts LazyFrames/DataFrames and grouping parameters; no direct BLS calls.
- **Output layer** (`output/`): takes analysis outputs and writes exhibits and report files.

Lazy evaluation is used throughout; `.collect()` is called only where needed (e.g. before writing parquet or when mkdocstrings/stats require eager data).
