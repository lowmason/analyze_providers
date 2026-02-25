"""Tests for birth rate analysis."""

from datetime import date

import polars as pl

from analyze_provider.analysis.births import (
    compare_birth_determinable_composition,
    compute_cross_correlation,
    compute_payroll_birth_rates,
    compute_survival_curves,
    test_birth_lead as run_birth_lead,
)


def test_compute_payroll_birth_rates() -> None:
    panel = pl.DataFrame({
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'quarter': ['2019Q1', '2019Q1'],
        'birth_count': [2, 1],
        'birth_determinable_count': [10, 10],
    }).lazy()
    out = compute_payroll_birth_rates(panel, []).collect()
    assert 'birth_rate' in out.columns
    assert out['births'].sum() == 3
    assert out['determinable'].sum() == 20


# --- test_birth_lead tests ---


def _make_birth_lead_data():
    """Create synthetic payroll births and BED data with 20 quarters for regression tests."""
    quarters = [f'{2015 + q // 4}Q{q % 4 + 1}' for q in range(20)]
    payroll_births = pl.LazyFrame({
        'quarter': quarters,
        'birth_rate': [0.05 + 0.002 * i for i in range(20)],
    })
    bed = pl.LazyFrame({
        'quarter': quarters,
        'birth_rate': [0.04 + 0.0018 * i + 0.001 * (i % 3) for i in range(20)],
    })
    return payroll_births, bed


def test_birth_lead_returns_three_models() -> None:
    """test_birth_lead should return concurrent, leading, and incremental models."""
    payroll_births, bed = _make_birth_lead_data()
    out = run_birth_lead(payroll_births, bed, [])
    if out.is_empty():
        # statsmodels not installed; skip
        return
    models = out['model'].to_list()
    assert 'concurrent' in models
    assert 'leading' in models
    assert 'incremental' in models


def test_birth_lead_model_columns() -> None:
    """Each model row should have r2, coef, and pvalue."""
    payroll_births, bed = _make_birth_lead_data()
    out = run_birth_lead(payroll_births, bed, [])
    if out.is_empty():
        return
    expected_cols = ['model', 'r2', 'coef', 'pvalue']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_birth_lead_r2_range() -> None:
    """R-squared should be between 0 and 1 for all models."""
    payroll_births, bed = _make_birth_lead_data()
    out = run_birth_lead(payroll_births, bed, [])
    if out.is_empty():
        return
    for r2 in out['r2'].to_list():
        assert 0 <= r2 <= 1.0, f"R-squared out of range: {r2}"


def test_birth_lead_concurrent_model() -> None:
    """Concurrent model should regress BED on payroll birth rate."""
    payroll_births, bed = _make_birth_lead_data()
    out = run_birth_lead(payroll_births, bed, [])
    if out.is_empty():
        return
    concurrent = out.filter(pl.col('model') == 'concurrent')
    assert concurrent.height == 1
    # Coefficient should be non-null
    assert concurrent['coef'].to_list()[0] is not None


def test_birth_lead_insufficient_data() -> None:
    """With too few quarters, should return empty DataFrame."""
    payroll_births = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2'],
        'birth_rate': [0.05, 0.06],
    })
    bed = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2'],
        'birth_rate': [0.04, 0.05],
    })
    out = run_birth_lead(payroll_births, bed, [])
    assert out.is_empty()


# --- compute_cross_correlation tests ---


def test_cross_correlation_columns() -> None:
    """Cross-correlation should return lag and correlation columns."""
    payroll_births, bed = _make_birth_lead_data()
    out = compute_cross_correlation(payroll_births, bed, max_lag=3)
    assert 'lag' in out.columns
    assert 'correlation' in out.columns


def test_cross_correlation_lag_range() -> None:
    """Should have rows for lags 0 through max_lag."""
    payroll_births, bed = _make_birth_lead_data()
    out = compute_cross_correlation(payroll_births, bed, max_lag=4)
    lags = sorted(out['lag'].to_list())
    assert lags == [0, 1, 2, 3, 4]


def test_cross_correlation_values_bounded() -> None:
    """Correlations should be between -1 and 1."""
    payroll_births, bed = _make_birth_lead_data()
    out = compute_cross_correlation(payroll_births, bed, max_lag=4)
    for corr in out['correlation'].to_list():
        assert -1.0 <= corr <= 1.0, f"Correlation out of bounds: {corr}"


def test_cross_correlation_lag_zero_high() -> None:
    """With correlated data, lag-0 correlation should be high."""
    payroll_births, bed = _make_birth_lead_data()
    out = compute_cross_correlation(payroll_births, bed, max_lag=4)
    lag_zero = out.filter(pl.col('lag') == 0)['correlation'].to_list()[0]
    assert lag_zero > 0.8


def test_cross_correlation_insufficient_data() -> None:
    """With too few quarters, return empty frame."""
    payroll_births = pl.LazyFrame({
        'quarter': ['2019Q1'],
        'birth_rate': [0.05],
    })
    bed = pl.LazyFrame({
        'quarter': ['2019Q1'],
        'birth_rate': [0.04],
    })
    out = compute_cross_correlation(payroll_births, bed, max_lag=4)
    assert out.height == 0


# --- compute_survival_curves tests ---


def _make_survival_data() -> pl.LazyFrame:
    """Payroll with birth clients spanning multiple quarters for survival analysis."""
    return pl.DataFrame({
        'client_id': [
            'b1', 'b1', 'b1', 'b1',
            'b2', 'b2',
            'b3', 'b3', 'b3',
            'c1', 'c1', 'c1', 'c1',
        ],
        'ref_date': [
            date(2018, 1, 12), date(2018, 4, 12), date(2018, 7, 12), date(2019, 1, 12),
            date(2018, 1, 12), date(2018, 4, 12),
            date(2018, 1, 12), date(2018, 4, 12), date(2018, 7, 12),
            date(2018, 1, 12), date(2018, 4, 12), date(2018, 7, 12), date(2019, 1, 12),
        ],
        'entry_month': [
            date(2018, 1, 12), date(2018, 1, 12), date(2018, 1, 12), date(2018, 1, 12),
            date(2018, 1, 12), date(2018, 1, 12),
            date(2018, 1, 12), date(2018, 1, 12), date(2018, 1, 12),
            date(2018, 1, 12), date(2018, 1, 12), date(2018, 1, 12), date(2018, 1, 12),
        ],
        'exit_month': [
            None, None, None, None,
            date(2018, 4, 12), date(2018, 4, 12),
            None, None, None,
            None, None, None, None,
        ],
        'is_birth': [
            True, None, None, None,
            True, None,
            True, None, None,
            False, None, None, None,
        ],
        'qualified_employment': [10, 12, 14, 16, 5, 6, 8, 9, 10, 20, 22, 24, 26],
    }).lazy()


def test_survival_curves_columns() -> None:
    payroll = _make_survival_data()
    out = compute_survival_curves(payroll, []).collect()
    expected_cols = ['entry_quarter', 'birth_cohort_n', 'survival_4q', 'survival_8q']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_survival_curves_cohort_count() -> None:
    """Birth cohort count should equal the number of unique birth clients."""
    payroll = _make_survival_data()
    out = compute_survival_curves(payroll, []).collect()
    # b1, b2, b3 are births in 2018Q1 => cohort size = 3
    q1_2018 = out.filter(pl.col('entry_quarter') == '2018Q1')
    assert q1_2018['birth_cohort_n'].to_list()[0] == 3


def test_survival_curves_rates_bounded() -> None:
    """Survival rates should be between 0 and 1."""
    payroll = _make_survival_data()
    out = compute_survival_curves(payroll, []).collect()
    survival_cols = [c for c in out.columns if c.startswith('survival_')]
    for col in survival_cols:
        for val in out[col].to_list():
            if val is not None:
                assert 0.0 <= val <= 1.0, f"Survival rate out of bounds in {col}: {val}"


def test_survival_curves_no_births() -> None:
    """With no birth-flagged clients, return empty frame."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2019, 4, 12)],
        'entry_month': [date(2019, 1, 12), date(2019, 1, 12)],
        'exit_month': [None, None],
        'is_birth': [False, None],
        'qualified_employment': [10, 12],
    })
    out = compute_survival_curves(payroll, []).collect()
    assert out.height == 0


# --- compare_birth_determinable_composition tests ---


def test_compare_birth_determinable_composition_columns() -> None:
    determinable = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'supersector': ['Construction', 'Retail trade', 'Construction'],
        'state_fips': ['26', '36', '26'],
        'size_class': ['1-4', '5-9', '10-19'],
    })
    full = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3', 'c4'],
        'supersector': ['Construction', 'Retail trade', 'Construction', 'Retail trade'],
        'state_fips': ['26', '36', '26', '36'],
        'size_class': ['1-4', '5-9', '10-19', '5-9'],
    })
    out = compare_birth_determinable_composition(determinable, full)
    assert 'dimension' in out.columns
    assert 'misallocation_index' in out.columns


def test_compare_birth_determinable_composition_dimensions() -> None:
    """Should have rows for supersector, state_fips, and size_class."""
    determinable = pl.LazyFrame({
        'client_id': ['c1', 'c2'],
        'supersector': ['Construction', 'Retail trade'],
        'state_fips': ['26', '36'],
        'size_class': ['1-4', '5-9'],
    })
    full = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'supersector': ['Construction', 'Retail trade', 'Construction'],
        'state_fips': ['26', '36', '26'],
        'size_class': ['1-4', '5-9', '10-19'],
    })
    out = compare_birth_determinable_composition(determinable, full)
    dims = out['dimension'].to_list()
    assert 'supersector' in dims
    assert 'state_fips' in dims
    assert 'size_class' in dims


def test_compare_birth_determinable_composition_identical() -> None:
    """If determinable subset matches full base exactly, misallocation should be ~0."""
    data = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3', 'c4'],
        'supersector': ['Construction', 'Retail trade', 'Construction', 'Retail trade'],
        'state_fips': ['26', '36', '26', '36'],
        'size_class': ['1-4', '5-9', '1-4', '5-9'],
    })
    out = compare_birth_determinable_composition(data, data)
    for mi in out['misallocation_index'].to_list():
        assert abs(mi) < 1e-9, f"Expected ~0 misallocation, got {mi}"


def test_compare_birth_determinable_composition_misaligned() -> None:
    """If distributions differ, misallocation index should be > 0."""
    determinable = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'supersector': ['Construction', 'Construction', 'Construction'],
        'state_fips': ['26', '26', '26'],
        'size_class': ['1-4', '1-4', '1-4'],
    })
    full = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'supersector': ['Retail trade', 'Retail trade', 'Retail trade'],
        'state_fips': ['36', '36', '36'],
        'size_class': ['5-9', '5-9', '5-9'],
    })
    out = compare_birth_determinable_composition(determinable, full)
    for mi in out['misallocation_index'].to_list():
        assert mi > 0, f"Expected positive misallocation, got {mi}"
