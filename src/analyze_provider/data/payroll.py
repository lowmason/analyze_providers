"""Load and validate payroll provider data."""

from pathlib import Path

import polars as pl

from analyze_provider.naics import SUPERSECTOR_MAP
from analyze_provider.size_class import size_class_expr

REQUIRED_COLUMNS: list[str] = [
    'client_id',
    'ref_date',
    'entry_month',
    'exit_month',
    'is_birth',
    'naics_code',
    'state_fips',
    'qualified_employment',
]

OPTIONAL_COLUMNS: list[str] = [
    'employee_id',
    'gross_pay',
    'filing_date',
    'hires',
    'separations',
]


def load_payroll(path: str | Path) -> pl.LazyFrame:
    """Load payroll provider data from parquet, validate, and add derived columns.

    Validates required columns exist. Casts types and adds
    naics2, naics3, supersector, size_class, quarter. Preserves optional columns
    (employee_id, gross_pay, filing_date, hires, separations) if present.

    Args:
        path: Path to a single parquet file or directory of parquet files.

    Returns:
        LazyFrame with required and derived columns.

    Raises:
        ValueError: If required columns are missing.
    """
    p = Path(path)
    if p.is_dir():
        lf = pl.scan_parquet(p / '*.parquet')
    else:
        lf = pl.scan_parquet(p)

    schema_cols = lf.collect_schema().names()
    missing = [c for c in REQUIRED_COLUMNS if c not in schema_cols]
    if missing:
        raise ValueError(f'Payroll data missing required columns: {missing}; got {schema_cols}')

    # Keep required columns first, then optional, then any extras
    extra_cols = [c for c in schema_cols if c not in REQUIRED_COLUMNS]
    lf = lf.select(REQUIRED_COLUMNS + extra_cols)

    lf = (
        lf
        .with_columns(
            pl.col('ref_date').cast(pl.Date),
            pl.col('entry_month').cast(pl.Date),
            pl.when(pl.col('exit_month').is_not_null())
            .then(pl.col('exit_month').cast(pl.Date))
            .otherwise(pl.lit(None).cast(pl.Date)),
            pl.col('naics_code').cast(pl.Utf8).str.zfill(6).alias('naics_code'),
            pl.col('state_fips').cast(pl.Utf8),
            pl.col('qualified_employment').cast(pl.Int64),
        )
        .with_columns(
            pl.col('naics_code').str.slice(0, 2).alias('naics2'),
            pl.col('naics_code').str.slice(0, 3).alias('naics3'),
        )
        .with_columns(
            pl.col('naics2').replace(SUPERSECTOR_MAP).alias('supersector'),
        )
        .with_columns(
            pl.when(pl.col('supersector').is_null()).then(pl.lit('Other services')).otherwise(pl.col('supersector')).alias('supersector'),
        )
        .with_columns(size_class_expr('qualified_employment').alias('size_class'))
        .with_columns(
            (pl.col('ref_date').dt.year().cast(pl.Utf8) + pl.lit('Q') + pl.col('ref_date').dt.quarter().cast(pl.Utf8)).alias('quarter'),
        )
    )

    # Cast optional columns if present
    if 'gross_pay' in schema_cols:
        lf = lf.with_columns(pl.col('gross_pay').cast(pl.Float64))
    if 'filing_date' in schema_cols:
        lf = lf.with_columns(pl.col('filing_date').cast(pl.Date))

    return lf


def load_payroll_employees(path: str | Path) -> pl.LazyFrame:
    """Load employee-level payroll data from parquet.

    Expects columns: employee_id, client_id, ref_date, plus optional hire_date, separation_date, gross_pay.
    This is an alternative to the client-month loader for worker-level flow analysis.

    Args:
        path: Path to employee-level parquet file(s).

    Returns:
        LazyFrame with employee-level data.

    Raises:
        ValueError: If required employee columns are missing.
    """
    p = Path(path)
    if p.is_dir():
        lf = pl.scan_parquet(p / '*.parquet')
    else:
        lf = pl.scan_parquet(p)

    schema_cols = lf.collect_schema().names()
    required = ['employee_id', 'client_id', 'ref_date']
    missing = [c for c in required if c not in schema_cols]
    if missing:
        raise ValueError(f'Employee data missing required columns: {missing}; got {schema_cols}')

    lf = lf.with_columns(pl.col('ref_date').cast(pl.Date))
    if 'hire_date' in schema_cols:
        lf = lf.with_columns(pl.col('hire_date').cast(pl.Date))
    if 'separation_date' in schema_cols:
        lf = lf.with_columns(pl.col('separation_date').cast(pl.Date))
    if 'gross_pay' in schema_cols:
        lf = lf.with_columns(pl.col('gross_pay').cast(pl.Float64))

    return lf
