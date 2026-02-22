"""Tests for coverage analysis."""

import polars as pl

from analyze_provider.analysis.coverage import compute_cell_reliability, compute_coverage


def test_compute_cell_reliability() -> None:
    cov = pl.LazyFrame({
        'payroll_clients': [10, 50, 100],
        'coverage_ratio_employment': [0.001, 0.01, 0.05],
    })
    out = compute_cell_reliability(cov, min_clients=30, min_coverage=0.005).collect()
    assert 'reliability' in out.columns
    assert out['reliability'].to_list() == ['insufficient', 'marginal', 'reliable']
