# analyze_provider

**Evaluate how representative a private payroll provider's microdata is relative to official government employment statistics.**

analyze_provider is a Python package that compares a client–month payroll panel to **QCEW** (Quarterly Census of Employment and Wages), **CES** (Current Employment Statistics), and **BED** (Business Employment Dynamics). It produces:

- **Static comparisons** — coverage ratios and distributional comparisons (industry, state, size class)
- **Dynamic comparisons** — growth rates, turning points, and shift-share decompositions
- **Birth/death analysis** — birth rates vs BED, lead/lag and Granger-style regressions
- **Summary deliverables** — executive summary, dashboard of exhibits, and technical appendix

All official data is accessed via the [eco-stats](https://github.com/lowmason/eco-stats) library (BLSClient). The package uses **polars** for data and **Typer** for the CLI.

---

## Quick links

| Topic | Description |
|-------|-------------|
| [Quick start](guide/quickstart.md) | Install, run the pipeline, and view output |
| [CLI Reference](cli.md) | Full documentation of `analyze-provider` commands and options |
| [Data sources](guide/data-sources.md) | Payroll input schema and official data (QCEW, CES, BED) |
| [Pipeline overview](guide/pipeline.md) | Execution order and module roles |
| [Onboarding & specification](onboard.md) | Purpose, package structure, module specs, and design principles |
| [API Reference](api/analyze_provider.md) | Module and function documentation (auto-generated from docstrings) |

---

## Install

```bash
pip install -e .
```

Optional: interactive exhibits with Plotly:

```bash
pip install -e ".[interactive]"
```

Documentation (MkDocs + Material + mkdocstrings):

```bash
pip install -e ".[docs]"
mkdocs serve
```

Set `BLS_API_KEY` in the environment for BLS API access when fetching official data.

---

## Usage at a glance

Run the full analysis pipeline (payroll parquet → exhibits and report):

```bash
analyze-provider run --payroll-path /path/to/data.parquet --output-dir ./output
```

Fetch and cache official data only:

```bash
analyze-provider fetch-official --start-year 2019 --end-year 2025
```

Regenerate exhibits from existing analysis outputs:

```bash
analyze-provider exhibits --analysis-dir ./output/analysis --output-dir ./output/exhibits
```

See the [CLI Reference](cli.md) for all commands and options.

---

## Output structure

After `run`, the output directory contains:

- **cache/** — Cached QCEW, CES, and BED parquet files
- **analysis/** — Panel, coverage, growth, and birth-rate parquet outputs
- **exhibits/** — PNG/PDF charts (coverage heatmaps, growth tracking, usability map, etc.)
- **executive_summary.md** — One-page summary with coverage and key bullets
- **dashboard.md** — Dashboard assembling all exhibits
- **appendix/** — Cell-level CSVs and methodology description

---

## Develop / test

```bash
pip install -e ".[dev]"
pytest tests/ -v
```
