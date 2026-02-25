"""Tests for coverage analysis."""

from datetime import date

import polars as pl

from analyze_provider.analysis.coverage import (
    compute_cell_reliability,
    compute_composition_shift_index,
    compute_coverage,
    compute_share_comparison,
)


def test_compute_cell_reliability() -> None:
    cov = pl.LazyFrame({
        'payroll_clients': [10, 50, 100],
        'coverage_ratio_employment': [0.001, 0.003, 0.05],
    })
    out = compute_cell_reliability(cov, min_clients=30, min_coverage=0.005).collect()
    assert 'reliability' in out.columns
    assert out['reliability'].to_list() == ['insufficient', 'marginal', 'reliable']


# --- compute_share_comparison tests ---


def _make_payroll_and_qcew_for_share():
    """Synthetic payroll_agg and qcew for share comparison."""
    payroll_agg = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 3,
        'quarter': ['2019Q1'] * 3,
        'supersector': ['Construction', 'Retail trade', 'Manufacturing'],
        'payroll_employment': [300, 200, 500],
    })
    qcew = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 3,
        'quarter': ['2019Q1'] * 3,
        'supersector': ['Construction', 'Retail trade', 'Manufacturing'],
        'qcew_employment': [1000, 2000, 2000],
    })
    return payroll_agg, qcew


def test_share_comparison_columns() -> None:
    payroll_agg, qcew = _make_payroll_and_qcew_for_share()
    out = compute_share_comparison(payroll_agg, qcew, 'supersector').collect()
    expected_cols = ['payroll_share', 'qcew_share', 'abs_dev', 'misallocation_index']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_share_comparison_shares_sum_to_one() -> None:
    """Shares within each period should sum to approximately 1."""
    payroll_agg, qcew = _make_payroll_and_qcew_for_share()
    out = compute_share_comparison(payroll_agg, qcew, 'supersector').collect()
    payroll_total = out['payroll_share'].sum()
    qcew_total = out['qcew_share'].sum()
    assert abs(payroll_total - 1.0) < 1e-9, f"Payroll shares sum to {payroll_total}"
    assert abs(qcew_total - 1.0) < 1e-9, f"QCEW shares sum to {qcew_total}"


def test_share_comparison_abs_dev_nonnegative() -> None:
    """abs_dev should always be non-negative."""
    payroll_agg, qcew = _make_payroll_and_qcew_for_share()
    out = compute_share_comparison(payroll_agg, qcew, 'supersector').collect()
    for val in out['abs_dev'].to_list():
        assert val >= 0, f"abs_dev should be non-negative, got {val}"


def test_share_comparison_misallocation_index_range() -> None:
    """Misallocation index should be between 0 and 1."""
    payroll_agg, qcew = _make_payroll_and_qcew_for_share()
    out = compute_share_comparison(payroll_agg, qcew, 'supersector').collect()
    for mi in out['misallocation_index'].to_list():
        assert 0 <= mi <= 1.0, f"Misallocation index out of range: {mi}"


def test_share_comparison_identical_distributions() -> None:
    """If payroll and QCEW have the same distribution, abs_dev and misallocation should be ~0."""
    payroll_agg = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'payroll_employment': [500, 500],
    })
    qcew = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'qcew_employment': [1000, 1000],
    })
    out = compute_share_comparison(payroll_agg, qcew, 'supersector').collect()
    for val in out['abs_dev'].to_list():
        assert abs(val) < 1e-9
    for mi in out['misallocation_index'].to_list():
        assert abs(mi) < 1e-9


def test_share_comparison_with_month1_emplvl() -> None:
    """QCEW using month1_emplvl column should be handled transparently."""
    payroll_agg = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'payroll_employment': [300, 700],
    })
    qcew = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12)] * 2,
        'quarter': ['2019Q1'] * 2,
        'supersector': ['Construction', 'Retail trade'],
        'month1_emplvl': [1000, 4000],
    })
    out = compute_share_comparison(payroll_agg, qcew, 'supersector').collect()
    assert 'qcew_share' in out.columns
    assert out.height == 2


# --- compute_composition_shift_index tests ---


def _make_payroll_agg_for_csi() -> pl.LazyFrame:
    """Payroll aggregate with shifting composition over 3 months."""
    return pl.LazyFrame({
        'ref_date': [
            date(2019, 1, 12), date(2019, 1, 12),
            date(2019, 2, 12), date(2019, 2, 12),
            date(2019, 3, 12), date(2019, 3, 12),
        ],
        'supersector': [
            'Construction', 'Retail trade',
            'Construction', 'Retail trade',
            'Construction', 'Retail trade',
        ],
        'payroll_employment': [
            500, 500,
            600, 400,
            700, 300,
        ],
    })


def test_composition_shift_index_columns() -> None:
    payroll_agg = _make_payroll_agg_for_csi()
    out = compute_composition_shift_index(payroll_agg, 'supersector').collect()
    assert 'ref_date' in out.columns
    assert 'csi' in out.columns


def test_composition_shift_index_bounded() -> None:
    """CSI should be bounded between 0 and 2."""
    payroll_agg = _make_payroll_agg_for_csi()
    out = compute_composition_shift_index(payroll_agg, 'supersector').collect()
    for val in out['csi'].to_list():
        if val is not None:
            assert 0 <= val <= 2.0, f"CSI out of bounds: {val}"


def test_composition_shift_index_no_shift() -> None:
    """With constant composition, CSI should be 0 (except first period)."""
    payroll_agg = pl.LazyFrame({
        'ref_date': [
            date(2019, 1, 12), date(2019, 1, 12),
            date(2019, 2, 12), date(2019, 2, 12),
            date(2019, 3, 12), date(2019, 3, 12),
        ],
        'supersector': [
            'Construction', 'Retail trade',
            'Construction', 'Retail trade',
            'Construction', 'Retail trade',
        ],
        'payroll_employment': [
            500, 500,
            500, 500,
            500, 500,
        ],
    })
    out = compute_composition_shift_index(payroll_agg, 'supersector').collect().sort('ref_date')
    # Feb and Mar should have CSI = 0 (shares unchanged)
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert abs(feb['csi'].to_list()[0]) < 1e-9
    mar = out.filter(pl.col('ref_date') == date(2019, 3, 12))
    assert abs(mar['csi'].to_list()[0]) < 1e-9


def test_composition_shift_index_increasing_shift() -> None:
    """With increasingly skewed composition, CSI should be positive for later periods."""
    payroll_agg = _make_payroll_agg_for_csi()
    out = compute_composition_shift_index(payroll_agg, 'supersector').collect().sort('ref_date')
    # Feb: shares shifted from 50/50 to 60/40 => CSI > 0
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert feb['csi'].to_list()[0] > 0
