"""Tests for birth rate analysis."""

import polars as pl

from analyze_provider.analysis.births import compute_payroll_birth_rates


def test_compute_payroll_birth_rates() -> None:
    panel = pl.LazyFrame({
        'ref_date': [pl.date(2019, 1, 12), pl.date(2019, 2, 12)],
        'quarter': ['2019Q1', '2019Q1'],
        'birth_count': [2, 1],
        'birth_determinable_count': [10, 10],
    })
    out = compute_payroll_birth_rates(panel, []).collect()
    assert 'birth_rate' in out.columns
    assert out['births'].sum() == 3
    assert out['determinable'].sum() == 20
