# CLI Reference

The **analyze-provider** CLI is built with [Typer](https://typer.tyserver.com/). Entry point:

```bash
analyze-provider [COMMAND] [OPTIONS]
```

Get help:

```bash
analyze-provider --help
analyze-provider run --help
analyze-provider fetch-official --help
analyze-provider exhibits --help
```

---

## Commands overview

| Command | Description |
|--------|-------------|
| [`run`](#run) | Run the full analysis pipeline (payroll → panel → coverage, growth, births, reweight → exhibits → report) |
| [`fetch-official`](#fetch-official) | Fetch and cache official data (QCEW, CES, BED) via eco-stats |
| [`exhibits`](#exhibits) | Generate all exhibits from completed analysis outputs |

---

## run

Run the full analysis pipeline: load payroll data, fetch or load official data, build the panel, run coverage/growth/birth analyses and reweighting, generate exhibits, and assemble the three-layer deliverable (executive summary, dashboard, technical appendix).

**Usage:**

```bash
analyze-provider run [OPTIONS]
```

### Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--payroll-path` | **Yes** | — | Path to the payroll parquet file or directory of parquet files. Must contain the required columns (see [Data sources](guide/data-sources.md)). |
| `--output-dir` | No | `./output` | Base output directory. Creates `output_dir/cache`, `output_dir/analysis`, `output_dir/exhibits`, and `output_dir/appendix` as needed. |
| `--force-refresh` | No | `False` | If set, re-fetch official data (QCEW, CES, BED) even if cached parquet exists. |

### Behavior

1. Sets `OUTPUT_DIR` and `CACHE_DIR` from `--output-dir` (cache lives under `output_dir/cache`).
2. Loads and validates payroll data via `analyze_provider.data.payroll.load_payroll`.
3. Uses **eco-stats** `BLSClient` to fetch or load QCEW, CES, and BED; on fetch failure, attempts to use existing cache.
4. Builds the client–month panel with `build_panel`, writes `analysis/panel.parquet`.
5. Runs coverage analysis (national level), growth comparison vs CES, reweighting (rake to QCEW on supersector), and birth-rate computation; writes parquet files under `analysis/`.
6. Generates all exhibits (coverage heatmaps, growth tracking, usability map, etc.) into `exhibits/`.
7. Writes `executive_summary.md`, `dashboard.md`, and the technical appendix under `appendix/`.

If **eco-stats** is not installed, the command exits with an error and instructs you to install it (e.g. `pip install git+https://github.com/lowmason/eco-stats.git`).

### Examples

```bash
# Default output directory
analyze-provider run --payroll-path ./data/payroll.parquet

# Custom output directory and force refresh of official data
analyze-provider run --payroll-path ./data/payroll.parquet --output-dir ./out --force-refresh

# Directory of parquet files
analyze-provider run --payroll-path ./data/parquet_dir/
```

---

## fetch-official

Fetch official data (QCEW, CES, BED) via eco-stats and cache as parquet under `CACHE_DIR`. Use this to pre-populate the cache or refresh data without running the full pipeline.

**Usage:**

```bash
analyze-provider fetch-official [OPTIONS]
```

### Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--start-year` | No | `2019` | First calendar year to fetch. |
| `--end-year` | No | `2025` | Last calendar year to fetch. CES is fetched through `end_year + 1` (e.g. 2026 when end_year is 2025). |
| `--force-refresh` | No | `False` | If set, overwrite existing cache files for the given year range. |

### Behavior

- Ensures `CACHE_DIR` exists (uses default from `config` unless overridden by a prior `run` in the same process).
- Requires **eco-stats** and a valid `BLS_API_KEY` (environment variable or equivalent).
- Calls `qcew.fetch_qcew`, `ces.fetch_ces`, and `bed.fetch_bed` with the given year range and `force_refresh`.
- Writes parquet files such as `qcew_2019_2025.parquet`, `ces_2019_2026.parquet`, `bed_2019_2025.parquet` (names depend on start/end year).

### Examples

```bash
# Default range (2019–2025)
analyze-provider fetch-official

# Custom range and force overwrite
analyze-provider fetch-official --start-year 2020 --end-year 2024 --force-refresh
```

---

## exhibits

Generate all exhibits (charts and tables) from **existing** analysis outputs, without re-running the pipeline. Useful when you change exhibit code or want to regenerate figures from the same analysis directory.

**Usage:**

```bash
analyze-provider exhibits [OPTIONS]
```

### Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--analysis-dir` | No | `./output/analysis` | Directory containing analysis parquet files (e.g. `coverage_national.parquet`, `growth.parquet`, `birth_rates.parquet`). |
| `--output-dir` | No | `./output/exhibits` | Directory where exhibit PNG/PDF files will be written. |

### Behavior

- Reads from `analysis_dir`: coverage, growth, birth rates, etc. Missing files are treated as empty DataFrames.
- Computes cell reliability (usability map) from coverage when available.
- Calls `exhibits.generate_all_exhibits` and writes all exhibit figures to `output_dir`.

### Examples

```bash
# Default paths
analyze-provider exhibits

# Custom analysis and output directories
analyze-provider exhibits --analysis-dir ./out/analysis --output-dir ./out/figs
```

---

## Environment

| Variable | Description |
|----------|-------------|
| `BLS_API_KEY` | BLS API key used by eco-stats for QCEW/CES/BED requests. Set for live fetches; optional if using only cached data. |

---

## Exit codes

- **0** — Success.
- **1** — Error (e.g. eco-stats not installed when required, or other failure). Check stderr for the message.
