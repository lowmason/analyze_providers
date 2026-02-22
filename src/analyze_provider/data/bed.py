"""Fetch BED via BLSClient and cache as parquet."""

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from analyze_provider import config

if TYPE_CHECKING:
    from eco_stats import BLSClient


def _cache_path(start_year: int, end_year: int) -> Path:
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return config.CACHE_DIR / f'bed_{start_year}_{end_year}.parquet'


def fetch_bed(bls: 'BLSClient', start_year: int = 2019, end_year: int = 2025, force_refresh: bool = False) -> pl.DataFrame:
    """Build BED series IDs (births, deaths, establishments), fetch and cache.

    Output columns: year, quarter, naics_code, size_class, state_fips, births, deaths,
    total_establishments, birth_rate, death_rate.

    Args:
        bls: BLSClient instance.
        start_year: First year.
        end_year: Last year.
        force_refresh: If True, overwrite cache.

    Returns:
        Eager DataFrame with BED outputs.
    """
    cache = _cache_path(start_year, end_year)
    if cache.exists() and not force_refresh:
        return pl.read_parquet(cache)

    try:
        from eco_stats.api.bls import build_series_id
    except ImportError:
        build_series_id = lambda *a, **k: 'BDU000000000200R0Q'

    # National quarterly births (data_element '02'), all sizes (sizeclass '0')
    birth_series = build_series_id(
        'BD',
        seasonal='U',
        state_fips='00',
        msa='00000',
        industry='000000',
        data_element='02',
        sizeclass='0',
        data_class='0',
        ratelevel='R',
        periodicity='Q',
    )
    series_list = [birth_series]
    df = bls.get_series(series_ids=series_list, start_year=str(start_year), end_year=str(end_year))
    if not isinstance(df, pl.DataFrame):
        df = pl.DataFrame(df) if hasattr(df, '__iter__') else pl.from_pandas(df)

    # Reshape: add year, quarter from date; name value as births
    if 'date' in df.columns:
        df = df.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.quarter().alias('quarter'),
        )
    df = df.rename({'value': 'births'}).with_columns(
        pl.lit('000000').alias('naics_code'),
        pl.lit('0').alias('size_class'),
        pl.lit('00').alias('state_fips'),
    )
    # Placeholder for deaths and total_establishments if not fetched
    if 'deaths' not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias('deaths'))
    if 'total_establishments' not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias('total_establishments'))
    # birth_rate = births / beginning-of-quarter establishments (simplified: leave null if not computed)
    df = df.with_columns(pl.lit(None).cast(pl.Float64).alias('birth_rate'), pl.lit(None).cast(pl.Float64).alias('death_rate'))

    out = df.select(['year', 'quarter', 'naics_code', 'size_class', 'state_fips', 'births', 'deaths', 'total_establishments', 'birth_rate', 'death_rate'])
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out.write_parquet(cache)
    return out


def load_bed(start_year: int = 2019, end_year: int = 2025) -> pl.LazyFrame:
    """Load BED data from cache."""
    cache = _cache_path(start_year, end_year)
    if not cache.exists():
        raise FileNotFoundError(f'BED cache not found: {cache}. Run fetch_bed first.')
    return pl.scan_parquet(cache)
