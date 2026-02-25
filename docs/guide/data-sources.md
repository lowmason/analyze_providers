# Data sources

## Payroll provider data (input)

A client–month panel with the following fields, in this order:

| Field | Description |
|-------|-------------|
| `client_id` | Unique client identifier |
| `ref_date` | Monthly reference date, 2019-01 through 2026-01. Employment measured as of the 12th of each month (CES-compatible pay period) |
| `entry_month` | First month the client appears in the data |
| `exit_month` | Last month the client appears (null if still active) |
| `is_birth` | Boolean or null. `True` if the client's EIN registration date is within ±3 months of their entry into the payroll provider's client base. `False` if the EIN date is available but outside that window. `null` if EIN registration date is unavailable. **Sparse but reliable when non-null.** |
| `naics_code` | 6-digit NAICS industry code |
| `state_fips` | State FIPS code |
| `qualified_employment` | Total employees with CES-qualified pay |

Input is one or more **parquet** files. The loader accepts a single file path or a directory (scans `*.parquet`). The package validates that these columns exist and adds derived columns: `naics2`, `naics3`, `supersector`, `size_class`, `quarter`.

### Optional columns

The following columns are not required but, when present, unlock additional analysis modules:

| Field | Description | Unlocks |
|-------|-------------|---------|
| `gross_pay` | Total gross pay for the client in the reference month | **Earnings analysis** — average pay, pay-growth tracking, pay-distribution comparisons |
| `filing_date` | Date the client first filed payroll with the provider | **Data quality assessment** — filing-lag metrics, data-freshness scoring |
| `employee_id` | Anonymised worker identifier (unique within client) | **Worker-level flows** — hires, separations, accessions/separations rates, job-to-job transitions |
| `hires` | Count of new hires during the reference month | **Worker-level flows** — when `employee_id` is unavailable, pre-aggregated hire counts are used as a fallback |
| `separations` | Count of separations during the reference month | **Worker-level flows** — when `employee_id` is unavailable, pre-aggregated separation counts are used as a fallback |

The loader (`data/payroll.py`) detects these columns automatically. If a column is absent the corresponding analysis module is skipped and a log message is emitted. No error is raised for missing optional columns.

**Earnings analysis** (`analysis/earnings.py`) uses `gross_pay` together with `qualified_employment` to compute average pay per employee and to track pay-growth rates against official (QCEW) average weekly wages. When `gross_pay` is present, the pipeline adds earnings exhibits (pay-level comparison, pay-growth tracking) to the report.

**Data quality assessment** (`analysis/data_quality.py`) uses `filing_date` to measure reporting lags and data freshness. It also performs completeness checks on all columns (missing-value rates, zero-employment months, NAICS coverage) and produces a data-quality scorecard that runs as the first analysis step in the pipeline.

**Worker-level flows** (`analysis/flows.py`) uses either `employee_id` (preferred) or the `hires`/`separations` aggregates to compute accession and separation rates. These rates are compared to BED gross-flows data and JOLTS where available. When `employee_id` is present the module can additionally compute job-to-job transition rates and worker-level tenure distributions.

---

## Official statistics (eco-stats)

Official data is fetched using the [eco-stats](https://github.com/lowmason/eco-stats) library. The main entry point is **BLSClient**, which provides:

1. **JSON API** — `get_series()` for time-series by series ID  
2. **Flat files** — `get_bulk_data()`, `get_mapping()` from download.bls.gov  
3. **QCEW CSV slices** — `get_qcew_industry()`, `get_qcew_area()`, `get_qcew_size()` from the CEW open-data API  

All methods return **polars** DataFrames. Time-series use a `date` column with **day=12** for CE (CES) and EN (QCEW) to match the survey reference period.

| Program | eco-stats access | Notes |
|--------|------------------|--------|
| **QCEW** | `get_qcew_industry()`, `get_qcew_area()`, `get_qcew_size()` | Size data is Q1 only. CSV slice API. |
| **CES** | `get_series()`, `get_employment()`, `get_bulk_data('CE')` | Series IDs via `build_series_id('CE', seasonal='S', supersector='00', ...)` |
| **BED** | `get_series()`, `get_bulk_data('BD')` | Series IDs via `build_series_id('BD', ...)`. Mappings: `get_mapping('BD', 'dataelement')`, etc. |

**Lookup tables** — e.g. `get_mapping('CE', 'supersector')`, `get_mapping('BD', 'dataelement')`, `get_mapping('BD', 'sizeclass')`, `get_mapping('BD', 'state')`.

**Series ID helpers** — `build_series_id(program, **components)`, `parse_series_id(series_id)`.

Install eco-stats:

```bash
pip install git+https://github.com/lowmason/eco-stats.git
```

Set `BLS_API_KEY` in the environment or pass `api_key` to `BLSClient()`.

---

## Temporal alignment

- **Payroll** `ref_date`: month with day=12.  
- **CES**: eco-stats returns `date` with day=12 for CE.  
- **QCEW**: use quarter and month-in-quarter; e.g. `month1_emplvl` = first month of quarter (Jan for Q1). The package aligns payroll to QCEW by quarter and month position when computing coverage.

This alignment is documented in the methodology section of the technical appendix generated by the pipeline.
