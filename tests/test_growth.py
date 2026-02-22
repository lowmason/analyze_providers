"""Tests for growth analysis."""

import polars as pl

from analyze_provider.analysis.growth import compute_growth_rates


def test_compute_growth_rates() -> None:
    df = pl.LazyFrame({
        'ref_date': pl.date_range(pl.date(2019, 1, 1), pl.date(2020, 12, 1), '1mo'),
        'employment': [100 + i for i in range(24)],
    })
    out = compute_growth_rates(df, 'employment', []).collect()
    assert 'yoy_growth' in out.columns
    assert 'mom_growth' in out.columns
