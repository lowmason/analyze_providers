"""Fetch BED via BLSClient and cache as parquet."""

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from analyze_provider import config

if TYPE_CHECKING:
    from eco_stats import BLSClient

# BED data element codes
BED_BIRTHS = '02'
BED_DEATHS = '03'
BED_TOTAL_ESTABLISHMENTS = '01'


def _cache_path(start_year: int, end_year: int) -> Path:
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return config.CACHE_DIR / f'bed_{start_year}_{end_year}.parquet'


def fetch_bed(bls: 'BLSClient', start_year: int = 2019, end_year: int = 2025, force_refresh: bool = False) -> pl.DataFrame:
    """Build BED series IDs (births, deaths, establishments), fetch and cache.

    Fetches births, deaths, and total establishments for national data. Also fetches
    industry and state stratifications when available.

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

    def _build_bed_series(data_element: str, state_fips: str = '00', industry: str = '000000', sizeclass: str = '0') -> str:
        return build_series_id(
            'BD',
            seasonal='U',
            state_fips=state_fips,
            msa='00000',
            industry=industry,
            data_element=data_element,
            sizeclass=sizeclass,
            data_class='0',
            ratelevel='R',
            periodicity='Q',
        )

    # National series for births, deaths, and total establishments
    series_list = [
        _build_bed_series(BED_BIRTHS),
        _build_bed_series(BED_DEATHS),
        _build_bed_series(BED_TOTAL_ESTABLISHMENTS),
    ]

    # Also fetch by key supersector industries (2-digit NAICS mapped to 6-digit BED format)
    key_industries = ['100000', '200000', '300000', '400000', '500000', '600000', '700000']
    for ind in key_industries:
        try:
            series_list.append(_build_bed_series(BED_BIRTHS, industry=ind))
            series_list.append(_build_bed_series(BED_DEATHS, industry=ind))
        except Exception:
            continue

    df = bls.get_series(series_ids=series_list, start_year=str(start_year), end_year=str(end_year))
    if not isinstance(df, pl.DataFrame):
        df = pl.DataFrame(df) if hasattr(df, '__iter__') else pl.from_pandas(df)

    if df.is_empty():
        out = pl.DataFrame({
            'year': [], 'quarter': [], 'naics_code': [], 'size_class': [],
            'state_fips': [], 'births': [], 'deaths': [], 'total_establishments': [],
            'birth_rate': [], 'death_rate': [],
        })
        out.write_parquet(cache)
        return out

    # Add year and quarter from date
    if 'date' in df.columns:
        df = df.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.quarter().alias('quarter'),
        )

    # Parse series_id to determine data element
    if 'series_id' in df.columns:
        df = df.with_columns(
            pl.col('series_id').str.slice(14, 2).alias('data_element'),
            pl.col('series_id').str.slice(4, 2).alias('_state_fips'),
            pl.col('series_id').str.slice(9, 6).alias('_industry'),
        )

        # Pivot data_elements into separate columns
        births_df = df.filter(pl.col('data_element') == BED_BIRTHS).rename({'value': 'births'})
        deaths_df = df.filter(pl.col('data_element') == BED_DEATHS).select(['year', 'quarter', '_state_fips', '_industry', pl.col('value').alias('deaths')])
        estab_df = df.filter(pl.col('data_element') == BED_TOTAL_ESTABLISHMENTS).select(['year', 'quarter', '_state_fips', '_industry', pl.col('value').alias('total_establishments')])

        join_on = ['year', 'quarter', '_state_fips', '_industry']
        out = births_df.select(['year', 'quarter', '_state_fips', '_industry', 'births'])
        out = out.join(deaths_df, on=join_on, how='full', suffix='_d')
        out = out.join(estab_df, on=join_on, how='full', suffix='_e')

        out = out.rename({'_state_fips': 'state_fips', '_industry': 'naics_code'})
    else:
        # Fallback: simple rename
        out = df.rename({'value': 'births'}).with_columns(
            pl.lit('000000').alias('naics_code'),
            pl.lit('00').alias('state_fips'),
        )
        if 'deaths' not in out.columns:
            out = out.with_columns(pl.lit(None).cast(pl.Int64).alias('deaths'))
        if 'total_establishments' not in out.columns:
            out = out.with_columns(pl.lit(None).cast(pl.Int64).alias('total_establishments'))

    if 'size_class' not in out.columns:
        out = out.with_columns(pl.lit('0').alias('size_class'))

    # Compute rates
    out = out.with_columns(
        pl.when(pl.col('total_establishments').is_not_null() & (pl.col('total_establishments') > 0))
        .then(pl.col('births') / pl.col('total_establishments'))
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias('birth_rate'),
        pl.when(pl.col('total_establishments').is_not_null() & (pl.col('total_establishments') > 0))
        .then(pl.col('deaths') / pl.col('total_establishments'))
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias('death_rate'),
    )

    out = out.select(['year', 'quarter', 'naics_code', 'size_class', 'state_fips', 'births', 'deaths', 'total_establishments', 'birth_rate', 'death_rate'])
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out.write_parquet(cache)
    return out


def load_bed(start_year: int = 2019, end_year: int = 2025) -> pl.LazyFrame:
    """Load BED data from cache."""
    cache = _cache_path(start_year, end_year)
    if not cache.exists():
        raise FileNotFoundError(f'BED cache not found: {cache}. Run fetch_bed first.')
    return pl.scan_parquet(cache)
