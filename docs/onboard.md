# analyze_provider: Payroll Data Representativeness Analysis Package

## Purpose

Build a Python package that evaluates how representative a private payroll provider's microdata is relative to official government employment statistics. The package produces static comparisons (levels and distributions), dynamic comparisons (growth rates and turning points), birth/death analysis, and a set of summary exhibits suitable for a non-technical audience.

---


## Data Sources

### Payroll Provider Data (Input)

A client-month panel with the following fields, in this order:

| Field | Description |
|---|---|
| `client_id` | Unique client identifier |
| `ref_date` | Monthly reference date, 2019-01 through 2026-01. Employment measured as of the 12th of each month (CES-compatible pay period) |
| `entry_month` | First month the client appears in the data |
| `exit_month` | Last month the client appears in the data (null if still active) |
| `is_birth` | Boolean or null. `True` if the client's EIN registration date is within ±3 months of their entry into the payroll provider's client base. `False` if the EIN date is available but outside that window. `null` if EIN registration date is unavailable. **This field is sparse but reliable when non-null.** |
| `naics_code` | 6-digit NAICS industry code |
| `state_fips` | State FIPS code |
| `qualified_employment` | Total employees with CES-qualified pay |

Assume this arrives as a parquet file or set of parquet files.

### Official Statistics (via `eco-stats`)

Official government data is fetched using the [`eco-stats`](https://github.com/lowmason/eco-stats) library. The primary entry point is `BLSClient`, which wraps three layers of access:

1. **JSON API** — `get_series()` for time-series data by series ID
2. **Flat files** — `get_bulk_data()` and `get_mapping()` for complete datasets and lookup tables from `download.bls.gov`
3. **QCEW CSV slices** — `get_qcew_industry()`, `get_qcew_area()`, `get_qcew_size()` using the CEW open-data API at `data.bls.gov`

All data methods return `polars.DataFrame` objects with typed columns. Time-series results include a `date` column derived from BLS year/period fields, using **day=12** for CE (CES) and EN (QCEW) programs to match the survey reference period.

| Program | eco-stats access | Notes |
|---|---|---|
| **QCEW** | `bls.get_qcew_industry()`, `bls.get_qcew_area()`, `bls.get_qcew_size()` | Size data is **Q1 only**. Uses CSV slice API (no bulk file downloads needed). |
| **CES** | `bls.get_series(series_ids=[...])` or `bls.get_employment()` or `bls.get_bulk_data('CE')` | Series IDs built via `build_series_id('CE', seasonal='S', supersector='00', industry='000000', data_type='01')` |
| **BED** | `bls.get_series(series_ids=[...])` or `bls.get_bulk_data('BD')` | Series IDs built via `build_series_id('BD', seasonal='U', state_fips='00', ...)`. Mapping tables via `bls.get_mapping('BD', 'dataelement')` etc. |

**Lookup/mapping tables** are available via `bls.get_mapping(program, name)` for any registered program. For example:
- `bls.get_mapping('CE', 'supersector')` — CES supersector codes
- `bls.get_mapping('CE', 'industry')` — CES industry codes
- `bls.get_mapping('BD', 'dataelement')` — BED data element codes (births, deaths, etc.)
- `bls.get_mapping('BD', 'sizeclass')` — BED size class codes
- `bls.get_mapping('BD', 'state')` — BED state codes

**Series ID utilities:**
- `build_series_id(program, **components)` — construct a series ID from named fields
- `parse_series_id(series_id)` — decompose an existing ID into named fields

The programs registry (`PROGRAMS`) defines field layouts for CE, BD, EN, and others. Access via `get_program('CE')` to inspect field positions and names.

Install `eco-stats` from source:

```bash
pip install git+https://github.com/lowmason/eco-stats.git
```

API keys should be set as environment variables (`BLS_API_KEY`) or passed directly to `BLSClient(api_key=...)`.

---


## Package Structure (src-style)

```
analyze_provider/
├── pyproject.toml
├── README.md
├── src/
│   └── analyze_provider/
│       ├── __init__.py
│       ├── config.py              # API keys, paths, constants
│       ├── naics.py               # NAICS mapping utilities (6-digit to supersector, 3-digit, etc.)
│       ├── size_class.py          # Employment-to-size-class assignment
│       ├── data/
│       │   ├── __init__.py
│       │   ├── payroll.py         # Load and validate payroll provider data
│       │   ├── qcew.py            # Fetch QCEW via BLSClient.get_qcew_*(), cache as parquet
│       │   ├── ces.py             # Fetch CES via BLSClient.get_series() / get_bulk_data()
│       │   └── bed.py             # Fetch BED via BLSClient.get_series() / get_bulk_data()
│       ├── panel.py               # Build the client-month panel with derived fields
│       ├── analysis/
│       │   ├── __init__.py
│       │   ├── data_quality.py    # Data quality assessment and scorecard
│       │   ├── coverage.py        # Coverage ratios and distributional comparisons
│       │   ├── growth.py          # Growth rate comparisons and decompositions
│       │   ├── flows.py           # Worker-level flows (accessions, separations, transitions)
│       │   ├── tenure.py          # Client tenure, churn rates, and vintage analysis
│       │   ├── earnings.py        # Earnings analysis and QCEW wage comparisons
│       │   ├── births.py          # Birth rate analysis and BED comparisons
│       │   └── reweight.py        # Raking / reweighting to match QCEW margins
│       ├── output/
│       │   ├── __init__.py
│       │   ├── exhibits.py        # Generate all charts and tables
│       │   └── report.py          # Assemble the layered deliverable
│       └── cli.py                 # Typer CLI entry point
└── tests/
    ├── __init__.py
    ├── test_panel.py
    ├── test_coverage.py
    ├── test_growth.py
    ├── test_births.py
    ├── test_data_quality.py
    ├── test_flows.py
    ├── test_tenure.py
    └── test_earnings.py
```

The `pyproject.toml` should define:

```toml
[project]
name = 'analyze_provider'
version = '0.1.0'
requires-python = '>=3.11'
dependencies = [
    'polars>=1.0',
    'matplotlib',
    'numpy',
    'statsmodels',
    'typer[all]',
    'eco-stats @ git+https://github.com/lowmason/eco-stats.git',
]

[project.optional-dependencies]
interactive = ['plotly']
dev = ['pytest', 'ruff']

[project.scripts]
analyze-provider = 'analyze_provider.cli:app'

[build-system]
requires = ['hatchling']
build-backend = 'hatchling.build'

[tool.hatch.build.targets.wheel]
packages = ['src/analyze_provider']
```

Note: `eco-stats` already depends on `httpx[http2]`, `polars`, `requests`, and `python-dotenv`, so these do not need to be listed separately.

---


## Coding Conventions

- **Python 3.11+**
- **polars** for all dataframe operations (not pandas)
- **Single quotes** for all strings
- **Two blank lines** after class and top-level function definitions
- Use **eco-stats** (`BLSClient`) for all government data access; do not write raw HTTP calls to BLS APIs
- Use **polars lazy frames** where possible; call `.collect()` as late as feasible
- Cache all fetched official data to local parquet files; never re-fetch if cache exists and is current
- Type hints on all function signatures
- Docstrings on all public functions (Google style)

---


## Module Specifications


### `config.py`

Store:
- BLS API key (read from environment variable `BLS_API_KEY`)
- Base paths for cached data (`CACHE_DIR`), output (`OUTPUT_DIR`)
- Reference period bounds: `START_MONTH = '2019-01'`, `END_MONTH = '2026-01'`
- QCEW size class breakpoints: `[1, 5, 10, 20, 50, 100, 250, 500]`


### `naics.py`

Provide mappings:
- `naics6_to_naics2(code: str) -> str` — first two digits
- `naics6_to_naics3(code: str) -> str` — first three digits
- `naics2_to_supersector(code: str) -> str` — BLS supersector name
- Include a constant `SUPERSECTOR_MAP` dictionary

The supersector mapping follows CES conventions. The 2-digit NAICS to supersector mapping is not always one-to-one (e.g., CES groups NAICS 31-33 into "Manufacturing"). Use the CES supersector definitions. The mapping table can be initialized from `bls.get_mapping('CE', 'supersector')` and hardcoded as a constant.


### `size_class.py`

- `assign_size_class(employment: int) -> str` — returns a label like `'1-4'`, `'5-9'`, `'10-19'`, `'20-49'`, `'50-99'`, `'100-249'`, `'250-499'`, `'500+'`
- Also provide a polars expression version: `size_class_expr(col: str) -> pl.Expr` using `pl.when().then()` chains for use inside `with_columns`


### `data/payroll.py`

- `load_payroll(path: str | Path) -> pl.LazyFrame`
- Validate required columns exist and are in the expected order: `client_id`, `ref_date`, `entry_month`, `exit_month`, `is_birth`, `naics_code`, `state_fips`, `qualified_employment`
- Cast types (ensure `ref_date` is `pl.Date`, `naics_code` is string with zero-padding, etc.)
- Add derived columns: `naics2`, `naics3`, `supersector`, `size_class`, `quarter` (e.g., `'2019Q1'`)
- Return a lazy frame


### `data/qcew.py`

Use `BLSClient` for all QCEW access. The client wraps `QCEWClient` internally and provides three slice methods:

```python
from eco_stats import BLSClient

bls = BLSClient(api_key=config.BLS_API_KEY)

# All areas for a given industry across years
df = bls.get_qcew_industry(
    industry_code='10',       # '10' = total all industries
    start_year=2019,
    end_year=2025,
    quarters=[1, 2, 3, 4],
)

# All industries for a given area across years
df = bls.get_qcew_area(
    area_code='US000',        # national; state codes like '26000' for Michigan
    start_year=2019,
    end_year=2025,
)

# By establishment-size class (Q1 only, size codes '1'-'9')
df = bls.get_qcew_size(
    size_code='1',
    start_year=2019,
    end_year=2025,
)
```

QCEW DataFrames have columns including: `area_fips`, `own_code`, `industry_code`, `agglvl_code`, `size_code`, `year`, `qtr`, `month1_emplvl`, `month2_emplvl`, `month3_emplvl`, `qtrly_estabs_count`, various wage columns, and disclosure codes. Text-like columns (`area_fips`, `own_code`, `industry_code`, `agglvl_code`, `size_code`, `qtr`, `disclosure_code`) are typed as `Utf8`; `year` is `Int32`.

Key functions for this module:
- `fetch_qcew(bls: BLSClient) -> pl.DataFrame` — fetch QCEW data across the required year range using the appropriate slice methods and cache to parquet
- `load_qcew() -> pl.LazyFrame` — load from cache
- Filter to private ownership only (`own_code == '5'`)

**Important**: QCEW monthly employment fields (`month1_emplvl`, `month2_emplvl`, `month3_emplvl`) within each quarter map directly to the payroll data's monthly reference periods. For example, QCEW 2024Q1 `month1_emplvl` corresponds to payroll data for January 2024. QCEW size data is only published for Q1, so size-class comparisons are annual (Q1 only).


### `data/ces.py`

Fetch CES data via `BLSClient`:

```python
from eco_stats import BLSClient
from eco_stats.api.bls import build_series_id

bls = BLSClient(api_key=config.BLS_API_KEY)

# Build series IDs programmatically
total_nonfarm_sa = build_series_id(
    'CE', seasonal='S', supersector='00',
    industry='000000', data_type='01',
)  # => 'CES0000000001'

total_nonfarm_nsa = build_series_id(
    'CE', seasonal='U', supersector='00',
    industry='000000', data_type='01',
)  # => 'CEU0000000001'

# Fetch via JSON API
df = bls.get_series(
    series_ids=[total_nonfarm_sa, total_nonfarm_nsa],
    start_year='2019',
    end_year='2026',
)
# Returns DataFrame with: series_id, date, year, period, period_name, value
# date uses day=12 for CE program (matching CES reference period)

# Or use the convenience method for total nonfarm SA
df = bls.get_employment(start_year='2019', end_year='2026')

# Or use flat file bulk download for all CES series
df = bls.get_bulk_data('CE', file_suffix='0.AllCESSeries')
```

To get CES supersector codes for building series IDs:
```python
supersectors = bls.get_mapping('CE', 'supersector')
# Returns DataFrame with supersector_code, supersector_name
```

Key functions for this module:
- `fetch_ces(bls: BLSClient) -> pl.DataFrame` — build series IDs for all supersectors (SA and NSA), fetch via `get_series()`, cache to parquet
- `load_ces() -> pl.LazyFrame` — load from cache
- Output columns: `ref_date`, `supersector`, `employment`, `seasonal_adjustment`

Note: BLS API v2 allows up to 50 series per request and 500 requests per day with a registered key. `BLSClient.get_series()` handles chunking automatically with 0.5s delays between batches.


### `data/bed.py`

Fetch BED data via `BLSClient`:

```python
from eco_stats import BLSClient
from eco_stats.api.bls import build_series_id

bls = BLSClient(api_key=config.BLS_API_KEY)

# BED series ID format (prefix 'BD'):
# seasonal(1) + state_fips(2) + msa(5) + industry(6) +
# data_element(2) + sizeclass(1) + data_class(1) + ratelevel(1) + periodicity(1)

# Example: quarterly establishment births, all sizes, national
births_series = build_series_id(
    'BD',
    seasonal='U',
    state_fips='00',
    msa='00000',
    industry='000000',
    data_element='02',    # check via bls.get_mapping('BD', 'dataelement')
    sizeclass='0',
    data_class='0',
    ratelevel='R',        # R=rate, L=level
    periodicity='Q',
)

# Get mapping tables to understand codes
data_elements = bls.get_mapping('BD', 'dataelement')
size_classes = bls.get_mapping('BD', 'sizeclass')
states = bls.get_mapping('BD', 'state')
industries = bls.get_mapping('BD', 'industry')

# Fetch time series
df = bls.get_series(series_ids=[births_series], start_year='2019', end_year='2025')

# Or bulk download all BED data
df = bls.get_bulk_data('BD')
```

Key functions for this module:
- `fetch_bed(bls: BLSClient) -> pl.DataFrame` — programmatically build relevant BED series IDs (births, deaths, total establishments by state, industry, size class), fetch and cache
- `load_bed() -> pl.LazyFrame` — load from cache
- Output columns: `year`, `quarter`, `naics_code`, `size_class`, `state_fips`, `births`, `deaths`, `total_establishments`, `birth_rate`, `death_rate`

BED birth rate = births / beginning-of-quarter establishments.


### `panel.py`

Build the master client-month panel from the payroll data:
- `build_panel(payroll: pl.LazyFrame) -> pl.LazyFrame`
- For each `ref_date`, compute:
  - Total qualified employment (sum across clients)
  - Client count (distinct clients with `qualified_employment > 0`)
  - Employment by supersector, state, size class, and cross-tabulations
  - Birth counts (where `is_birth == True`)
  - Total birth-determinable clients (where `is_birth` is not null)
  - Entry/exit counts

The panel module should produce aggregated summary frames at multiple levels of granularity:
- National total
- By supersector
- By state
- By size class
- By supersector × state
- By supersector × size class

Each aggregation level should be stored as a separate lazy frame (or a single frame with a `grouping_level` column) for easy joining to official data.


### `analysis/data_quality.py`

Data quality assessment and scorecard. Runs as the **first** analysis step, before any other analytical module.

**Quality assessment:**
- `assess_quality(payroll: pl.LazyFrame) -> pl.DataFrame`
- Computes column completeness rates (fraction of non-null values for every column)
- Counts zero-employment months per client
- Validates NAICS codes against the known 6-digit code list
- When `filing_date` is present, computes filing-lag distribution (days between `ref_date` and `filing_date`) and data-freshness scores
- Flags columns and time ranges that may affect downstream results

**Scorecard output:**
- Produces a data-quality scorecard summarising completeness, validity, and timeliness metrics
- Writes results to `analysis/data_quality.parquet`
- The scorecard is included in the report's technical appendix when available

**Design notes:**
- Accepts a `pl.LazyFrame` from the loader; calls `.collect()` only for the final scorecard output
- Does not depend on official data — runs on payroll data alone
- Emits warnings (via `logging`) for any metric below configurable thresholds


### `analysis/tenure.py`

Client tenure, churn rates, and vintage analysis.

**Tenure computation:**
- `compute_tenure(payroll: pl.LazyFrame) -> pl.LazyFrame`
- For each client, computes tenure in months as the span from `entry_month` to `exit_month` (or the latest `ref_date` if still active)
- Produces tenure-distribution summaries by supersector and size class

**Churn rates:**
- `compute_churn_rates(payroll: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Computes quarterly churn (exit) rates: number of clients whose `exit_month` falls in the quarter divided by the beginning-of-quarter client count
- Returns churn rates by quarter and grouping level

**Vintage composition:**
- `compute_vintage_composition(payroll: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Groups clients by entry cohort (vintage, defined by `entry_month` quarter)
- Tracks each vintage's share of total employment over time
- Useful for understanding whether representativeness depends on a few long-tenured anchor clients or a broad, regularly refreshed base

**Output:**
- Results written to `analysis/tenure.parquet`
- Tenure distributions and vintage composition charts are added to the exhibits when this module runs


### `analysis/flows.py`

Worker-level flows: accessions, separations, and job-to-job transitions.

**Flow computation:**
- `compute_flows(payroll: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- When `employee_id` is present, computes monthly and quarterly accession and separation rates by tracking individual worker appearances across consecutive months
- When `employee_id` is absent but `hires` and `separations` columns are present, uses these pre-aggregated counts as a fallback
- Skipped entirely if no flow-relevant columns are present (a log message is emitted)

**BED benchmarking:**
- `compare_flows_to_bed(flows: pl.LazyFrame, bed: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Benchmarks computed accession and separation rates against BED gross-flows data
- Compares levels and trends; computes correlation over time

**Job-to-job transitions (optional):**
- When `employee_id` is available, the module can additionally compute job-to-job transition rates (workers moving between clients within the same month or consecutive months)
- Produces worker-level tenure distributions (time at current client)

**Output:**
- Results written to `analysis/flows.parquet`
- Worker-flow rate charts and BED comparison exhibits are added when this module runs


### `analysis/earnings.py`

Earnings analysis and QCEW wage comparisons. Requires the optional `gross_pay` column.

**Earnings computation:**
- `compute_earnings(payroll: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Computes average pay per employee (`gross_pay / qualified_employment`) by month and grouping level
- Computes pay-growth rates (month-over-month and year-over-year)
- Produces pay distributions by supersector and size class

**QCEW wage benchmarking:**
- `compare_earnings_to_qcew(earnings: pl.LazyFrame, qcew: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Benchmarks average pay against QCEW average weekly wages
- Compares pay levels and pay-growth trajectories
- Computes correlation of pay-growth rates over time

**Output:**
- Results written to `analysis/earnings.parquet`
- Pay-level comparison and pay-growth tracking exhibits are added when `gross_pay` is present
- Skipped if `gross_pay` is absent (a log message is emitted; no error raised)


### `analysis/coverage.py`

Static representativeness analysis against QCEW.

**Coverage ratios:**
- `compute_coverage(payroll_agg: pl.LazyFrame, qcew: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Returns: all grouping columns + `payroll_employment`, `qcew_employment`, `coverage_ratio_employment`, `payroll_clients`, `qcew_establishments`, `coverage_ratio_estab`
- Compute at each aggregation level (national, supersector, state, size class, cross-tabs)
- Compute per quarter (using the monthly payroll data matched to QCEW `month1_emplvl` / `month2_emplvl` / `month3_emplvl`)

**Distributional comparisons:**
- `compute_share_comparison(payroll_agg: pl.LazyFrame, qcew: pl.LazyFrame, dimension: str) -> pl.LazyFrame`
- For a given dimension (e.g., `'supersector'`), compute payroll share and QCEW share in each category
- Compute absolute deviation: `abs_dev = abs(payroll_share - qcew_share)`
- Compute total misallocation index: `sum(abs_dev) / 2` — this gives the fraction of employment that would need to be "moved" between categories to match QCEW's distribution

**Temporal stability:**
- `compute_coverage_over_time(payroll_agg: pl.LazyFrame, qcew: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Same as coverage ratios, but with quarter as a column. Used for time series plots of coverage evolution.

**Cell reliability:**
- `compute_cell_reliability(coverage: pl.LazyFrame, min_clients: int = 30, min_coverage: float = 0.005) -> pl.LazyFrame`
- Flag each cell as `'reliable'`, `'marginal'`, or `'insufficient'` based on thresholds
- This produces the "usability map"


### `analysis/growth.py`

Dynamic comparisons of employment growth.

**Growth rate computation:**
- `compute_growth_rates(df: pl.LazyFrame, employment_col: str, grouping_cols: list[str]) -> pl.LazyFrame`
- Compute month-over-month and year-over-year growth rates
- Apply to both payroll aggregates and CES data

**Growth comparison:**
- `compare_growth(payroll_growth: pl.LazyFrame, ces_growth: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Merge on `ref_date` + grouping columns
- Compute: difference, absolute difference, signed difference (bias)
- Compute rolling correlations (12-month window)

**Growth decomposition:**
- `decompose_growth_divergence(payroll: pl.LazyFrame, ces: pl.LazyFrame) -> pl.DataFrame`
- Shift-share decomposition separating:
  - **Composition effect**: how much of the growth difference is due to different industry/state weights
  - **Within-cell effect**: how much is due to different growth rates within the same cells
- Return a dataframe with `quarter`, `total_divergence`, `composition_effect`, `within_cell_effect`

**Turning point analysis:**
- `analyze_turning_points(payroll_growth: pl.LazyFrame, official_growth: pl.LazyFrame) -> pl.DataFrame`
- Identify months where growth changes sign in either series
- For each turning point, compute the lead/lag in months between payroll and official
- Summarize: median lead/lag, mean lead/lag, by supersector

**Reweighted estimates (see also `reweight.py`):**
- After reweighting, recompute growth rates and compare to raw payroll growth and official growth
- If reweighting improves tracking (lower MAE, higher correlation), that confirms divergence is compositional


### `analysis/births.py`

Birth rate analysis against BED.

**Birth rate computation:**
- `compute_payroll_birth_rates(panel: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- For each quarter × grouping level:
  - `births = count where is_birth == True`
  - `determinable = count where is_birth is not null`
  - `birth_rate = births / determinable`
- **Critical**: only use clients where `is_birth` is non-null. The denominator is the determinable subset, not all clients.

**BED comparison:**
- `compare_birth_rates(payroll_births: pl.LazyFrame, bed: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- Merge on quarter + grouping columns
- Compute: level difference, ratio, correlation over time

**Compositional check:**
- `compare_birth_determinable_composition(determinable_subset: pl.LazyFrame, full_client_base: pl.LazyFrame) -> pl.DataFrame`
- Compare industry, state, and size distributions of the birth-determinable subset to the full client base
- Use the same misallocation index from `coverage.py`
- If the compositions are similar, the birth rate levels generalize; if they differ, note the caveat

**Predictive lead analysis:**
- `test_birth_lead(payroll_births: pl.LazyFrame, bed: pl.LazyFrame, grouping_cols: list[str], max_lag: int = 4) -> pl.DataFrame`
- Cross-correlation at lags 0 to `max_lag` quarters
- Granger-style regressions:
  - Model 1 (concurrent): `BED_rate(q) = α + β * payroll_rate(q) + ε`
  - Model 2 (leading): `BED_rate(q) = α + β₁ * payroll_rate(q) + β₂ * payroll_rate(q-1) + ε`
  - Model 3 (incremental): `BED_rate(q) = α + β₁ * BED_rate(q-1) + β₂ * payroll_rate(q-1) + ε`
- Report R², coefficient estimates, and significance for each model
- Run at national level and by supersector (where cell sizes permit)
- Use `statsmodels` for the regressions

**Survival analysis:**
- `compute_survival_curves(panel: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame`
- For clients flagged as births, compute the fraction still active at 4, 8, 12, 16, 20 quarters after entry
- Compare to BED survival rates where available


### `analysis/reweight.py`

Raking / iterative proportional fitting to adjust payroll data to match QCEW margins.

- `rake_to_qcew(payroll: pl.LazyFrame, qcew: pl.LazyFrame, dimensions: list[str], max_iter: int = 100, tolerance: float = 1e-6) -> pl.LazyFrame`
- Dimensions to rake on: supersector, state, size class (iteratively fit marginal totals)
- Return the payroll data with an additional `rake_weight` column
- Downstream analyses in `growth.py` should accept an optional weight column


### `output/exhibits.py`

Generate all charts and tables. Use **matplotlib** for static charts (suitable for PDF/print) and optionally **plotly** for interactive HTML versions.

Required exhibits:

1. **Coverage heatmap** — state (rows) × supersector (columns), cell color = coverage ratio. One for employment, one for establishment counts.
2. **Industry composition comparison** — grouped bar chart, payroll share vs. QCEW share by supersector
3. **Size class distribution** — side-by-side bar chart
4. **Coverage over time** — line chart, national coverage ratio by quarter
5. **Growth tracking** — dual-axis or overlaid line chart, payroll vs. CES employment growth (YoY), national + 3-4 key supersectors as small multiples
6. **Growth decomposition waterfall** — stacked bar or waterfall showing composition vs. within-cell effects by quarter
7. **Birth rate comparison** — line chart, payroll birth rate vs. BED birth rate by quarter
8. **Birth rate cross-correlation** — bar chart of cross-correlations at different lags
9. **Birth lead regression results** — table with R², coefficients, p-values for the three Granger models
10. **Usability map** — state × supersector matrix, cells colored green/yellow/red by reliability classification
11. **Reweighting impact** — line chart showing raw payroll growth, reweighted payroll growth, and CES growth

Each exhibit function should:
- Accept the relevant analysis output dataframe
- Return a `matplotlib.figure.Figure` object
- Save to `OUTPUT_DIR / 'exhibits' / '{exhibit_name}.png'` (and `.pdf`)


### `output/report.py`

Assemble the three-layer deliverable:

- **Layer 1 (executive summary)**: A single-page PDF or markdown file with total coverage, the usability heatmap, one growth tracking chart, and 3 bullet points (biggest strength, biggest weakness, key recommendation). Use a template.
- **Layer 2 (dashboard)**: A 5-10 page PDF assembling all exhibits with brief captions.
- **Layer 3 (technical appendix)**: Full cell-level tables as CSV exports + methodology description.


### `cli.py`

Typer CLI application:

```python
import typer

app = typer.Typer(help='Analyze payroll provider representativeness.')


@app.command()
def run(
    payroll_path: str = typer.Option(..., help='Path to payroll parquet file(s)'),
    output_dir: str = typer.Option('./output', help='Output directory'),
    force_refresh: bool = typer.Option(False, help='Re-fetch official data even if cached'),
):
    '''Run the full analysis pipeline.'''
    ...


@app.command()
def fetch_official(
    start_year: int = typer.Option(2019),
    end_year: int = typer.Option(2025),
    force_refresh: bool = typer.Option(False),
):
    '''Fetch and cache official data (QCEW, CES, BED) via eco-stats.'''
    ...


@app.command()
def exhibits(
    analysis_dir: str = typer.Option('./output/analysis', help='Directory with analysis outputs'),
    output_dir: str = typer.Option('./output/exhibits'),
):
    '''Generate all exhibits from completed analysis.'''
    ...


if __name__ == '__main__':
    app()
```

Usage:

```bash
analyze-provider run --payroll-path /path/to/data.parquet --output-dir ./output
analyze-provider fetch-official --start-year 2019 --end-year 2025
analyze-provider exhibits --analysis-dir ./output/analysis
```

---


## Key Design Principles

1. **Separation of data access, analysis, and output.** The analysis modules should accept polars dataframes, not know how to fetch data. The data modules should not know about the analyses.

2. **eco-stats as the data access layer.** All government data fetching goes through `BLSClient`. Do not write raw HTTP calls to BLS APIs. `BLSClient` already handles request chunking, rate limiting (0.5s between batches), caching (via `QCEWClient` and `BLSFlatFileClient`), Akamai blocking workarounds for flat files, and polars DataFrame construction with correct types.

3. **Lazy evaluation.** Keep everything as `pl.LazyFrame` until the point of output. This lets polars optimize query plans across complex joins and aggregations. Note: `BLSClient` methods return eager `pl.DataFrame` — convert to lazy immediately after receipt with `.lazy()`.

4. **Idempotent caching.** eco-stats has its own file-level cache (24h TTL by default). On top of that, cache the *processed* official data as parquet files for faster reload. The `--force-refresh` flag should bypass both layers.

5. **Cell-level thinking.** Every analysis should be parameterized by `grouping_cols: list[str]` so the same function works at national, state, supersector, or cross-tabulated levels. Never hard-code a specific aggregation level.

6. **Temporal alignment discipline.** Always join payroll and official data on the *reference period*, not the publication date. The payroll `ref_date` uses day=12; CES data from eco-stats also uses day=12 for the `date` column. QCEW data uses quarter + month position (`month1_emplvl` = first month of quarter). Document the mapping explicitly.

7. **Communicate fitness for purpose.** The output should never just say "coverage is X%." It should say "for analyses of [specific cell], coverage is X%, which is [sufficient/marginal/insufficient]." The usability map is the most important single exhibit.

---


## Dependencies

```
polars >= 1.0
eco-stats @ git+https://github.com/lowmason/eco-stats.git
matplotlib
numpy
statsmodels
typer[all]
```

Optional:
```
plotly
```

Note: eco-stats brings in `httpx[http2]`, `polars`, `requests`, `python-dotenv`, and `bs4` as transitive dependencies.

---


## Execution Order

The package should support a single `run` command that executes the full pipeline:

1. Load and validate payroll data (`data/payroll.py`)
2. Fetch/load official data via eco-stats (`data/qcew.py`, `data/ces.py`, `data/bed.py`)
3. Data quality assessment (`analysis/data_quality.py`)
4. Client tenure and churn (`analysis/tenure.py`)
5. Vintage assessment (`analysis/tenure.py`)
6. Build aggregated panel (`panel.py`)
7. Run coverage analysis (`analysis/coverage.py`)
8. Run growth analysis, including employment-change decomposition (`analysis/growth.py`)
9. Worker-level flows (`analysis/flows.py`)
10. Earnings analysis (`analysis/earnings.py`)
11. Run reweighting (`analysis/reweight.py`), then re-run growth analysis with weights
12. Run birth analysis (`analysis/births.py`)
13. Generate exhibits (`output/exhibits.py`)
14. Assemble report (`output/report.py`)

Each step should log progress and be independently re-runnable using cached intermediate outputs.