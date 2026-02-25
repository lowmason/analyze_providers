"""Fetch CES via BLSClient and cache as parquet."""

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from analyze_provider import config

if TYPE_CHECKING:
    from eco_stats import BLSClient

# Supersector code-to-name mapping (CES supersector codes to display names)
CES_SUPERSECTOR_NAMES: dict[str, str] = {
    '00': 'Total nonfarm',
    '05': 'Total private',
    '06': 'Goods-producing',
    '07': 'Service-providing',
    '08': 'Private service-providing',
    '10': 'Mining and logging',
    '15': 'Construction',
    '20': 'Manufacturing',
    '30': 'Trade, transportation, and utilities',
    '31': 'Wholesale trade',
    '32': 'Retail trade',
    '40': 'Transportation and warehousing',
    '41': 'Utilities',
    '42': 'Information',
    '43': 'Financial activities',
    '44': 'Professional and business services',
    '45': 'Education and health services',
    '50': 'Leisure and hospitality',
    '55': 'Other services',
    '60': 'Government',
    '65': 'Federal government',
    '90': 'State government',
    '91': 'Local government',
}


def _cache_path(start_year: int, end_year: int) -> Path:
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return config.CACHE_DIR / f'ces_{start_year}_{end_year}.parquet'


def fetch_ces(bls: 'BLSClient', start_year: int = 2019, end_year: int = 2026, force_refresh: bool = False) -> pl.DataFrame:
    """Build CES series IDs for supersectors (SA and NSA), fetch via get_series, cache.

    Maps supersector codes to names for joining with the payroll panel.

    Output columns: ref_date (date with day=12), supersector (name), employment, seasonal_adjustment.

    Args:
        bls: BLSClient instance.
        start_year: First year.
        end_year: Last year.
        force_refresh: If True, overwrite cache.

    Returns:
        Eager DataFrame with ref_date, supersector (name), employment, seasonal_adjustment.
    """
    cache = _cache_path(start_year, end_year)
    if cache.exists() and not force_refresh:
        return pl.read_parquet(cache)

    try:
        from eco_stats.api.bls import build_series_id
    except ImportError:
        from eco_stats.api import bls as _bls
        build_series_id = getattr(_bls, 'build_series_id', lambda *a, **k: 'CES0000000001')

    # Supersector codes from CES: 00 total, then sector codes. Build series for total + key supersectors.
    supersector_codes = ['00', '05', '06', '08', '10', '15', '20', '30', '31', '40', '45', '50', '55', '60', '65']
    series_ids = []
    for ss in supersector_codes:
        series_ids.append(build_series_id('CE', seasonal='S', supersector=ss, industry='000000', data_type='01'))
        series_ids.append(build_series_id('CE', seasonal='U', supersector=ss, industry='000000', data_type='01'))

    # Chunk to 50 per request
    chunks = [series_ids[i : i + 50] for i in range(0, len(series_ids), 50)]
    dfs = []
    for chunk in chunks:
        df = bls.get_series(series_ids=chunk, start_year=str(start_year), end_year=str(end_year))
        if not isinstance(df, pl.DataFrame):
            df = pl.DataFrame(df) if hasattr(df, '__iter__') else pl.from_pandas(df)
        dfs.append(df)

    raw = pl.concat(dfs) if len(dfs) > 1 else dfs[0]

    # Map series_id to supersector and seasonal; then reshape
    if 'date' in raw.columns:
        ref_date = pl.col('date')
    else:
        ref_date = pl.struct(['year', 'period']).alias('ref_date')

    # Build supersector code and seasonal from series_id
    raw = raw.with_columns(
        pl.col('series_id').str.slice(3, 2).alias('supersector_code'),
        pl.when(pl.col('series_id').str.starts_with('CES')).then(pl.lit('SA')).otherwise(pl.lit('NSA')).alias('seasonal_adjustment'),
    )

    # Map supersector code to name
    raw = raw.with_columns(
        pl.col('supersector_code').replace(CES_SUPERSECTOR_NAMES).alias('supersector'),
    )

    # Keep one row per ref_date x supersector; prefer SA for employment
    out = (
        raw.rename({'value': 'employment', 'date': 'ref_date'})
        .select(['ref_date', 'supersector_code', 'supersector', 'employment', 'seasonal_adjustment'])
    )
    # Drop duplicate supersector_code if we have both SA and NSA; keep SA for display
    out = out.filter(pl.col('seasonal_adjustment') == 'SA').drop('supersector_code')
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out.write_parquet(cache)
    return out


def load_ces(start_year: int = 2019, end_year: int = 2026) -> pl.LazyFrame:
    """Load CES data from cache."""
    cache = _cache_path(start_year, end_year)
    if not cache.exists():
        raise FileNotFoundError(f'CES cache not found: {cache}. Run fetch_ces first.')
    return pl.scan_parquet(cache)
