# Quick start

## Install

From the project root:

```bash
pip install -e .
```

For interactive Plotly exhibits:

```bash
pip install -e ".[interactive]"
```

## BLS API key

To fetch official data (QCEW, CES, BED), set your BLS API key:

```bash
export BLS_API_KEY=your_key_here
```

You can [register for a key](https://www.bls.gov/developers/home.htm) at the BLS website. The eco-stats library uses it for the BLS API v2 (and handles chunking and rate limits).

## Payroll data

Provide a parquet file (or directory of parquet files) with the required columns in this order:

| Column | Description |
|--------|-------------|
| `client_id` | Unique client identifier |
| `ref_date` | Monthly reference date (employment as of 12th of month) |
| `entry_month` | First month the client appears |
| `exit_month` | Last month (null if still active) |
| `is_birth` | True if EIN registration within ±3 months of entry; False or null otherwise |
| `naics_code` | 6-digit NAICS industry code |
| `state_fips` | State FIPS code |
| `qualified_employment` | Total employees with CES-qualified pay |

See [Data sources](data-sources.md) for full details.

## Run the pipeline

```bash
analyze-provider run --payroll-path /path/to/payroll.parquet --output-dir ./output
```

This will:

1. Load and validate payroll data
2. Fetch or load official data (QCEW, CES, BED) and cache under `./output/cache`
3. Build the panel and run coverage, growth, birth, and reweighting analyses
4. Write exhibits to `./output/exhibits` and the report layers to `./output`

## Pre-fetch official data (optional)

To populate the cache without running the full pipeline:

```bash
analyze-provider fetch-official --start-year 2019 --end-year 2025
```

By default the cache directory is `./cache`; when you run `run`, it uses `--output-dir` and stores cache under that directory.

## Regenerate exhibits

If you already have analysis outputs and only want to regenerate charts:

```bash
analyze-provider exhibits --analysis-dir ./output/analysis --output-dir ./output/exhibits
```

## Output layout

After `run`, you should see:

- **output/executive_summary.md** — One-page summary
- **output/dashboard.md** — Markdown that references all exhibits
- **output/exhibits/** — PNG and PDF figures (coverage heatmaps, growth tracking, usability map, etc.)
- **output/analysis/** — Parquet files (panel, coverage, growth, birth_rates)
- **output/appendix/** — CSVs and methodology

See [Pipeline overview](pipeline.md) for the exact execution order and module roles.
