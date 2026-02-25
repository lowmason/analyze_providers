"""Tests for raking/iterative proportional fitting to QCEW margins."""

from datetime import date

import polars as pl

from analyze_provider.analysis.reweight import rake_to_qcew


def _make_payroll_and_qcew():
    """Synthetic payroll and QCEW data for raking tests."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3', 'c4'],
        'ref_date': [date(2019, 1, 12)] * 4,
        'quarter': ['2019Q1'] * 4,
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade'],
        'state_fips': ['26', '36', '26', '36'],
        'qualified_employment': [100, 200, 150, 50],
    })
    qcew = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'qcew_employment': [600, 400],
    })
    return payroll, qcew


def test_rake_to_qcew_adds_weight_column() -> None:
    payroll, qcew = _make_payroll_and_qcew()
    out = rake_to_qcew(payroll, qcew, ['supersector']).collect()
    assert 'rake_weight' in out.columns


def test_rake_to_qcew_converges_single_dimension() -> None:
    """After raking on supersector, weighted payroll should match QCEW supersector totals."""
    payroll, qcew = _make_payroll_and_qcew()
    out = rake_to_qcew(payroll, qcew, ['supersector']).collect()

    # Weighted payroll by supersector
    weighted = out.group_by('supersector').agg(
        (pl.col('qualified_employment') * pl.col('rake_weight')).sum().alias('weighted_emp'),
    ).sort('supersector')
    qcew_df = qcew.collect().sort('supersector')

    construction = weighted.filter(pl.col('supersector') == 'Construction')['weighted_emp'].to_list()[0]
    retail = weighted.filter(pl.col('supersector') == 'Retail trade')['weighted_emp'].to_list()[0]

    assert abs(construction - 600) < 1.0
    assert abs(retail - 400) < 1.0


def test_rake_to_qcew_preserves_total() -> None:
    """Total weighted employment should equal total QCEW employment when all cells are present."""
    payroll, qcew = _make_payroll_and_qcew()
    out = rake_to_qcew(payroll, qcew, ['supersector']).collect()

    total_weighted = (out['qualified_employment'].cast(pl.Float64) * out['rake_weight']).sum()
    total_qcew = 600 + 400
    assert abs(total_weighted - total_qcew) < 1.0


def test_rake_to_qcew_no_dimensions() -> None:
    """With empty dimensions list, weights should remain 1.0."""
    payroll, qcew = _make_payroll_and_qcew()
    out = rake_to_qcew(payroll, qcew, []).collect()
    assert 'rake_weight' in out.columns
    for w in out['rake_weight'].to_list():
        assert abs(w - 1.0) < 1e-9


def test_rake_to_qcew_empty_qcew() -> None:
    """With empty QCEW, weights should remain 1.0."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2'],
        'ref_date': [date(2019, 1, 12)] * 2,
        'qualified_employment': [100, 200],
        'supersector': ['Construction', 'Retail trade'],
    })
    qcew = pl.LazyFrame({
        'supersector': [],
        'qcew_employment': [],
    }).cast({'supersector': pl.Utf8, 'qcew_employment': pl.Int64})
    out = rake_to_qcew(payroll, qcew, ['supersector']).collect()
    assert 'rake_weight' in out.columns
    for w in out['rake_weight'].to_list():
        assert abs(w - 1.0) < 1e-9


def test_rake_to_qcew_with_month1_emplvl() -> None:
    """QCEW using month1_emplvl column name should still work."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2'],
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'qualified_employment': [100, 200],
    })
    qcew = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'month1_emplvl': [500, 300],
    })
    out = rake_to_qcew(payroll, qcew, ['supersector']).collect()
    weighted = out.group_by('supersector').agg(
        (pl.col('qualified_employment') * pl.col('rake_weight')).sum().alias('weighted_emp'),
    ).sort('supersector')
    construction = weighted.filter(pl.col('supersector') == 'Construction')['weighted_emp'].to_list()[0]
    assert abs(construction - 500) < 1.0


def test_rake_to_qcew_multi_dimension_convergence() -> None:
    """Multi-dimensional raking (supersector x state_fips) should converge."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3', 'c4'],
        'ref_date': [date(2019, 1, 12)] * 4,
        'quarter': ['2019Q1'] * 4,
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade'],
        'state_fips': ['26', '36', '26', '36'],
        'qualified_employment': [100, 200, 150, 50],
    })
    qcew = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 4,
        'quarter': ['2019Q1'] * 4,
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade'],
        'state_fips': ['26', '36', '26', '36'],
        'qcew_employment': [250, 350, 200, 200],
    })
    out = rake_to_qcew(payroll, qcew, ['supersector', 'state_fips'], max_iter=200, tolerance=1e-8).collect()
    assert 'rake_weight' in out.columns
    # After raking, all weights should be positive
    for w in out['rake_weight'].to_list():
        assert w > 0


def test_rake_to_qcew_tolerance() -> None:
    """With tight tolerance, raking should converge (no error raised)."""
    payroll, qcew = _make_payroll_and_qcew()
    out = rake_to_qcew(payroll, qcew, ['supersector'], max_iter=500, tolerance=1e-10).collect()
    assert 'rake_weight' in out.columns
    weighted = out.group_by('supersector').agg(
        (pl.col('qualified_employment') * pl.col('rake_weight')).sum().alias('weighted_emp'),
    ).sort('supersector')
    construction = weighted.filter(pl.col('supersector') == 'Construction')['weighted_emp'].to_list()[0]
    assert abs(construction - 600) < 0.01
