"""Tests for earnings distribution and growth analysis."""

from datetime import date

import polars as pl

from analyze_provider.analysis.earnings import (
    compute_earnings_distribution,
    compute_earnings_growth,
)


def _make_payroll_with_earnings() -> pl.LazyFrame:
    """Synthetic payroll with gross_pay spanning 24 months."""
    dates = pl.date_range(date(2019, 1, 12), date(2020, 12, 12), '1mo', eager=True)
    n = len(dates)
    return pl.LazyFrame({
        'client_id': ['c1'] * n,
        'ref_date': dates,
        'qualified_employment': [10] * n,
        'gross_pay': [float(3000 + i * 50) for i in range(n)],
    })


def test_earnings_distribution_columns() -> None:
    payroll = _make_payroll_with_earnings()
    out = compute_earnings_distribution(payroll, []).collect()
    expected_cols = [
        'ref_date', 'mean_earnings', 'median_earnings',
        'p10_earnings', 'p25_earnings', 'p75_earnings',
        'p90_earnings', 'std_earnings', 'cv_earnings',
    ]
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_earnings_distribution_values() -> None:
    """With a single client per month, mean == median == gross_pay."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'ref_date': [date(2019, 1, 12)] * 3,
        'qualified_employment': [10, 20, 30],
        'gross_pay': [1000.0, 2000.0, 3000.0],
    })
    out = compute_earnings_distribution(payroll, []).collect()
    assert out.height == 1
    assert out['mean_earnings'].to_list()[0] == 2000.0
    assert out['median_earnings'].to_list()[0] == 2000.0
    assert out['p10_earnings'].to_list()[0] <= 1000.0
    assert out['p90_earnings'].to_list()[0] >= 3000.0


def test_earnings_distribution_grouped() -> None:
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3', 'c4'],
        'ref_date': [date(2019, 1, 12)] * 4,
        'qualified_employment': [10, 20, 30, 40],
        'gross_pay': [1000.0, 2000.0, 3000.0, 4000.0],
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade'],
    })
    out = compute_earnings_distribution(payroll, ['supersector']).collect()
    assert 'supersector' in out.columns
    sectors = out['supersector'].unique().to_list()
    assert 'Construction' in sectors
    assert 'Retail trade' in sectors


def test_earnings_distribution_no_gross_pay() -> None:
    """When payroll lacks gross_pay column, return empty frame with expected schema."""
    payroll = pl.LazyFrame({
        'client_id': ['c1'],
        'ref_date': [date(2019, 1, 12)],
        'qualified_employment': [10],
    })
    out = compute_earnings_distribution(payroll, []).collect()
    assert 'mean_earnings' in out.columns
    assert out.height == 0


def test_earnings_distribution_filters_zero_pay() -> None:
    """Zero or null gross_pay rows should be excluded."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'ref_date': [date(2019, 1, 12)] * 3,
        'qualified_employment': [10, 20, 30],
        'gross_pay': [0.0, None, 5000.0],
    })
    out = compute_earnings_distribution(payroll, []).collect()
    assert out.height == 1
    # Only c3 has valid gross_pay
    assert out['mean_earnings'].to_list()[0] == 5000.0


def test_earnings_growth_columns() -> None:
    payroll = _make_payroll_with_earnings()
    out = compute_earnings_growth(payroll, []).collect()
    expected_cols = ['mean_earnings_yoy', 'median_earnings_yoy']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_earnings_growth_yoy_null_for_first_year() -> None:
    """YoY growth should be null for the first 12 months."""
    payroll = _make_payroll_with_earnings()
    out = compute_earnings_growth(payroll, []).collect().sort('ref_date')
    # First 12 rows should have null YoY
    first_12 = out.head(12)
    assert first_12['mean_earnings_yoy'].null_count() == 12


def test_earnings_growth_yoy_positive() -> None:
    """With steadily increasing pay, YoY growth should be positive after 12 months."""
    payroll = _make_payroll_with_earnings()
    out = compute_earnings_growth(payroll, []).collect().sort('ref_date')
    # Rows after 12 months should have non-null, positive YoY
    after_12 = out.tail(out.height - 12).filter(pl.col('mean_earnings_yoy').is_not_null())
    assert after_12.height > 0
    for val in after_12['mean_earnings_yoy'].to_list():
        assert val > 0


def test_earnings_growth_grouped() -> None:
    """Grouped earnings growth should contain grouping columns."""
    dates = pl.date_range(date(2019, 1, 12), date(2020, 12, 12), '1mo', eager=True)
    n = len(dates)
    payroll = pl.LazyFrame({
        'client_id': ['c1'] * n + ['c2'] * n,
        'ref_date': dates.to_list() * 2,
        'qualified_employment': [10] * (2 * n),
        'gross_pay': [float(3000 + i * 50) for i in range(n)] + [float(4000 + i * 60) for i in range(n)],
        'supersector': ['Construction'] * n + ['Retail trade'] * n,
    })
    out = compute_earnings_growth(payroll, ['supersector']).collect()
    assert 'supersector' in out.columns
    sectors = out['supersector'].unique().to_list()
    assert 'Construction' in sectors
    assert 'Retail trade' in sectors
