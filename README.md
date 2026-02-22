# analyze_provider

Evaluate how representative a private payroll provider's microdata is relative to official government employment statistics (**QCEW**, **CES**, **BED**). The package produces static comparisons (coverage and distributions), dynamic comparisons (growth rates and turning points), birth/death analysis, and a three-layer deliverable (executive summary, dashboard, technical appendix).

**Full documentation** is in the `docs/` folder (MkDocs + Material + mkdocstrings). Build and serve with `pip install -e ".[docs]"` and `mkdocs serve`. See [CLI Reference](docs/cli.md) for full command and option docs.

---

## Features

- **Coverage analysis** — Ratios of payroll employment and client count to QCEW employment and establishments; by national, supersector, state, size class, and cross-tabs. Cell reliability (usability map: reliable / marginal / insufficient).
- **Growth analysis** — Month-over-month and year-over-year growth; comparison to CES; shift-share decomposition; turning point lead/lag.
- **Birth analysis** — Payroll birth rates (births / birth-determinable) vs BED; cross-correlation and Granger-style regressions.
- **Reweighting** — Raking to QCEW margins (e.g. supersector) and reweighted growth comparison.
- **Output** — Matplotlib exhibits (heatmaps, growth tracking, usability map, etc.), executive summary, dashboard, and technical appendix (CSV + methodology).

All official data is fetched via [eco-stats](https://github.com/lowmason/eco-stats) (`BLSClient`). Data handling uses **polars**; the CLI is built with **Typer**.

---

## Install

```bash
pip install -e .
```

Optional extras:

```bash
# Interactive exhibits (Plotly)
pip install -e ".[interactive]"

# Documentation (MkDocs + Material + mkdocstrings)
pip install -e ".[docs]"

# Development (pytest, ruff)
pip install -e ".[dev]"
```

Set `BLS_API_KEY` in the environment for BLS API access when fetching official data.

---

## CLI

The entry point is **analyze-provider** with three commands:

| Command | Description |
|---------|-------------|
| **run** | Full pipeline: load payroll → fetch/load official data → panel → coverage, growth, births, reweight → exhibits → report |
| **fetch-official** | Fetch and cache QCEW, CES, and BED (no payroll required) |
| **exhibits** | Regenerate all exhibits from existing analysis outputs |

### Examples

```bash
# Full pipeline (required: --payroll-path)
analyze-provider run --payroll-path /path/to/payroll.parquet --output-dir ./output

# Optional: force refresh of cached official data
analyze-provider run --payroll-path /path/to/payroll.parquet --force-refresh

# Pre-fetch official data only
analyze-provider fetch-official --start-year 2019 --end-year 2025

# Regenerate exhibits from existing analysis
analyze-provider exhibits --analysis-dir ./output/analysis --output-dir ./output/exhibits
```

**Full CLI reference:** [docs/cli.md](docs/cli.md) — all options, behavior, and examples.

---

## Payroll input

Provide a **parquet** file (or directory of parquet files) with these columns in order:

`client_id` · `ref_date` · `entry_month` · `exit_month` · `is_birth` · `naics_code` · `state_fips` · `qualified_employment`

See [Data sources](docs/guide/data-sources.md) for field definitions and temporal alignment with QCEW/CES/BED.

---

## Output layout (after `run`)

| Path | Contents |
|------|----------|
| `output/cache/` | Cached QCEW, CES, BED parquet files |
| `output/analysis/` | Panel, coverage, growth, birth_rates parquet files |
| `output/exhibits/` | PNG/PDF figures (coverage heatmaps, growth tracking, usability map, etc.) |
| `output/executive_summary.md` | One-page summary |
| `output/dashboard.md` | Dashboard that references all exhibits |
| `output/appendix/` | Cell-level CSVs and methodology |

---

## Package structure

```
src/analyze_provider/
├── __init__.py
├── config.py          # API keys, paths, constants
├── naics.py           # NAICS → supersector mapping
├── size_class.py      # Employment → size class
├── panel.py           # Build client-month panel with aggregation levels
├── cli.py             # Typer CLI (run, fetch-official, exhibits)
├── data/
│   ├── payroll.py     # Load/validate payroll parquet
│   ├── qcew.py        # Fetch/load QCEW
│   ├── ces.py         # Fetch/load CES
│   └── bed.py         # Fetch/load BED
├── analysis/
│   ├── coverage.py    # Coverage ratios, share comparison, cell reliability
│   ├── growth.py      # Growth rates, comparison, decomposition
│   ├── births.py      # Birth rates, BED comparison, lead tests
│   └── reweight.py    # Rake to QCEW margins
└── output/
    ├── exhibits.py    # Generate all charts/tables
    └── report.py      # Executive summary, dashboard, appendix
```

---

## Documentation

- **Quick start** — [docs/guide/quickstart.md](docs/guide/quickstart.md)
- **CLI reference** — [docs/cli.md](docs/cli.md) (all commands and options)
- **Data sources** — [docs/guide/data-sources.md](docs/guide/data-sources.md)
- **Pipeline overview** — [docs/guide/pipeline.md](docs/guide/pipeline.md)
- **Onboarding & specification** — [docs/onboard.md](docs/onboard.md) (purpose, module specs, design principles)
- **API reference** — Generated from docstrings in `docs/api/` (MkDocs + mkdocstrings)

Build and serve the full site:

```bash
pip install -e ".[docs]"
mkdocs serve
```

---

## Develop / test

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Dependencies

- **polars** (≥1.0), **typer**, **matplotlib**, **numpy**, **statsmodels**
- **eco-stats** (from GitHub) for QCEW/CES/BED access

See `pyproject.toml` for exact versions and optional dependencies.
