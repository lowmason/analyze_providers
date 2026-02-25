"""Fetch QCEW via BLSClient and cache as parquet.

QCEW monthly employment: month1_emplvl, month2_emplvl, month3_emplvl map to
the first, second, third month of each quarter (e.g. 2024Q1 -> Jan, Feb, Mar).
Payroll ref_date uses day=12; align by quarter and month position.
"""

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from analyze_provider import config
from analyze_provider.naics import SUPERSECTOR_MAP

if TYPE_CHECKING:
    from eco_stats import BLSClient

# State FIPS codes for all 50 states + DC
STATE_FIPS: list[str] = [
    '01', '02', '04', '05', '06', '08', '09', '10', '11', '12',
    '13', '15', '16', '17', '18', '19', '20', '21', '22', '23',
    '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
    '34', '35', '36', '37', '38', '39', '40', '41', '42', '44',
    '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56',
]


def _cache_path(start_year: int, end_year: int) -> Path:
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return config.CACHE_DIR / f'qcew_{start_year}_{end_year}.parquet'


def _add_supersector(df: pl.DataFrame) -> pl.DataFrame:
    """Map industry_code to supersector name for joins against payroll panel."""
    if 'industry_code' not in df.columns:
        return df
    # QCEW industry_code is variable length; take first 2 digits for NAICS mapping
    return df.with_columns(
        pl.col('industry_code').cast(pl.Utf8).str.slice(0, 2).replace(SUPERSECTOR_MAP).alias('supersector'),
    )


def fetch_qcew(bls: 'BLSClient', start_year: int = 2019, end_year: int = 2025, force_refresh: bool = False) -> pl.DataFrame:
    """Fetch QCEW data for the given year range and cache to parquet.

    Fetches national industry data, state-level data via area slices, and Q1 size-class data.
    Filters to private ownership (own_code == '5'). Unpivots monthly employment columns into
    three rows per quarter for alignment with all 12 monthly payroll observations.

    Args:
        bls: BLSClient instance (e.g. from eco_stats).
        start_year: First year.
        end_year: Last year.
        force_refresh: If True, overwrite existing cache.

    Returns:
        Eager DataFrame with QCEW data (private only), with supersector mapping applied.
    """
    cache = _cache_path(start_year, end_year)
    if cache.exists() and not force_refresh:
        return pl.read_parquet(cache)

    frames = []

    # National all industries
    try:
        df_nat = bls.get_qcew_industry(
            industry_code='10',
            start_year=start_year,
            end_year=end_year,
            quarters=[1, 2, 3, 4],
        )
        if not isinstance(df_nat, pl.DataFrame):
            df_nat = pl.DataFrame(df_nat) if hasattr(df_nat, '__iter__') else pl.from_pandas(df_nat)
        if 'own_code' in df_nat.columns:
            df_nat = df_nat.filter(pl.col('own_code') == '5')
        frames.append(df_nat)
    except Exception:
        pass

    # State-level fetching via area slices
    for fips in STATE_FIPS:
        try:
            area_code = f'{fips}000'  # State-level FIPS area code
            df_state = bls.get_qcew_area(
                area=area_code,
                start_year=start_year,
                end_year=end_year,
                quarters=[1, 2, 3, 4],
            )
            if not isinstance(df_state, pl.DataFrame):
                df_state = pl.DataFrame(df_state) if hasattr(df_state, '__iter__') else pl.from_pandas(df_state)
            if 'own_code' in df_state.columns:
                df_state = df_state.filter(pl.col('own_code') == '5')
            frames.append(df_state)
        except Exception:
            continue

    # Size-class data (Q1 only)
    for size_code in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
        try:
            df_size = bls.get_qcew_size(
                size_code=size_code,
                start_year=start_year,
                end_year=end_year,
                quarters=[1],
            )
            if not isinstance(df_size, pl.DataFrame):
                df_size = pl.DataFrame(df_size) if hasattr(df_size, '__iter__') else pl.from_pandas(df_size)
            if 'own_code' in df_size.columns:
                df_size = df_size.filter(pl.col('own_code') == '5')
            frames.append(df_size)
        except Exception:
            continue

    if not frames:
        out = pl.DataFrame()
        out.write_parquet(cache)
        return out

    out = pl.concat(frames, how='vertical_relaxed')

    # Add supersector mapping
    out = _add_supersector(out)

    out.write_parquet(cache)
    return out


def unpivot_qcew_months(qcew_df: pl.DataFrame) -> pl.DataFrame:
    """Unpivot month1/2/3_emplvl into three rows per quarter for monthly alignment.

    Converts quarterly QCEW data into monthly records with proper ref_date (day=12),
    so all 12 monthly payroll observations can be matched.

    Args:
        qcew_df: QCEW DataFrame with year, qtr, month1_emplvl, month2_emplvl, month3_emplvl.

    Returns:
        DataFrame with ref_date (monthly, day=12), qcew_employment, and other columns preserved.
    """
    if qcew_df.is_empty():
        return qcew_df

    month_cols = [c for c in ['month1_emplvl', 'month2_emplvl', 'month3_emplvl'] if c in qcew_df.columns]
    if not month_cols or 'year' not in qcew_df.columns or 'qtr' not in qcew_df.columns:
        return qcew_df

    # Keep non-month columns for the join
    id_cols = [c for c in qcew_df.columns if c not in month_cols]

    rows = []
    for month_pos, col_name in enumerate(month_cols, 1):
        sub = qcew_df.select(id_cols + [col_name]).rename({col_name: 'qcew_employment'})
        sub = sub.with_columns(pl.lit(month_pos).alias('month_in_quarter'))
        rows.append(sub)

    result = pl.concat(rows, how='vertical_relaxed')

    # Compute actual month: (qtr - 1) * 3 + month_in_quarter
    result = result.with_columns(
        ((pl.col('qtr').cast(pl.Int32) - 1) * 3 + pl.col('month_in_quarter')).alias('_month'),
    ).with_columns(
        pl.date(pl.col('year'), pl.col('_month'), 12).alias('ref_date'),
    ).drop(['_month', 'month_in_quarter'])

    # Add quarter string
    result = result.with_columns(
        (pl.col('year').cast(pl.Utf8) + pl.lit('Q') + pl.col('qtr').cast(pl.Utf8)).alias('quarter'),
    )

    # Add qcew_establishments from qtrly_estabs_count if present
    if 'qtrly_estabs_count' in result.columns:
        result = result.rename({'qtrly_estabs_count': 'qcew_establishments'})

    return result


def load_qcew(start_year: int = 2019, end_year: int = 2025) -> pl.LazyFrame:
    """Load QCEW data from cache.

    Args:
        start_year: First year (used for cache filename).
        end_year: Last year (used for cache filename).

    Returns:
        LazyFrame of cached QCEW (private only).

    Raises:
        FileNotFoundError: If cache file does not exist.
    """
    cache = _cache_path(start_year, end_year)
    if not cache.exists():
        raise FileNotFoundError(f'QCEW cache not found: {cache}. Run fetch_qcew first.')
    return pl.scan_parquet(cache)
