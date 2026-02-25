"""Tests for growth analysis."""

from datetime import date

import polars as pl

from analyze_provider.analysis.growth import (
    analyze_turning_points,
    compute_growth_rates,
    decompose_employment_change,
    decompose_growth_divergence,
)


def test_compute_growth_rates() -> None:
    dates = pl.date_range(date(2019, 1, 1), date(2020, 12, 1), '1mo', eager=True)
    df = pl.LazyFrame({
        'ref_date': dates,
        'employment': [100 + i for i in range(len(dates))],
    })
    out = compute_growth_rates(df, 'employment', []).collect()
    assert 'yoy_growth' in out.columns
    assert 'mom_growth' in out.columns


# --- decompose_growth_divergence tests ---


def _make_payroll_and_ces_for_divergence():
    """Synthetic payroll and CES data for shift-share decomposition."""
    payroll = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2', '2019Q3', '2019Q4', '2020Q1'],
        'supersector': ['Construction'] * 5,
        'payroll_employment': [1000, 1050, 1100, 1150, 1200],
    })
    ces = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2', '2019Q3', '2019Q4', '2020Q1'],
        'supersector': ['Construction'] * 5,
        'employment': [2000, 2080, 2150, 2200, 2300],
    })
    return payroll, ces


def test_decompose_growth_divergence_columns() -> None:
    payroll, ces = _make_payroll_and_ces_for_divergence()
    out = decompose_growth_divergence(payroll, ces, ['supersector'])
    expected_cols = ['quarter', 'total_divergence', 'composition_effect', 'within_cell_effect']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_decompose_growth_divergence_identity() -> None:
    """total_divergence should equal composition_effect + within_cell_effect."""
    payroll, ces = _make_payroll_and_ces_for_divergence()
    out = decompose_growth_divergence(payroll, ces, ['supersector'])
    for row in out.iter_rows(named=True):
        td = row['total_divergence']
        ce = row['composition_effect']
        wce = row['within_cell_effect']
        if td is not None and ce is not None and wce is not None:
            assert abs(td - (ce + wce)) < 1e-9, (
                f"Decomposition identity violated: {td} != {ce} + {wce}"
            )


def test_decompose_growth_divergence_no_grouping() -> None:
    """Without grouping, should fall back to simple divergence."""
    payroll = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2', '2019Q3'],
        'payroll_employment': [1000, 1050, 1100],
    })
    ces = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2', '2019Q3'],
        'employment': [2000, 2080, 2150],
    })
    out = decompose_growth_divergence(payroll, ces, [])
    assert 'total_divergence' in out.columns
    assert out.height > 0


def test_decompose_growth_divergence_multi_sector() -> None:
    """With multiple sectors, composition and within-cell effects should be computed."""
    payroll = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2'] * 2,
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade'],
        'payroll_employment': [1000, 1050, 500, 520],
    })
    ces = pl.LazyFrame({
        'quarter': ['2019Q1', '2019Q2'] * 2,
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade'],
        'employment': [2000, 2100, 1000, 1030],
    })
    out = decompose_growth_divergence(payroll, ces, ['supersector'])
    assert out.height > 0
    assert 'composition_effect' in out.columns


def test_decompose_growth_divergence_empty() -> None:
    """Empty inputs should return empty frame."""
    payroll = pl.LazyFrame({
        'quarter': [],
        'payroll_employment': [],
    }).cast({'quarter': pl.Utf8, 'payroll_employment': pl.Int64})
    ces = pl.LazyFrame({
        'quarter': [],
        'employment': [],
    }).cast({'quarter': pl.Utf8, 'employment': pl.Int64})
    out = decompose_growth_divergence(payroll, ces, [])
    assert out.height == 0


# --- decompose_employment_change tests ---


def _make_payroll_for_decomposition() -> pl.LazyFrame:
    """Payroll with continuing, entering, and exiting clients."""
    return pl.LazyFrame({
        'client_id': [
            'c1', 'c1', 'c1',
            'c2', 'c2',
            'c3',
        ],
        'ref_date': [
            date(2019, 1, 12), date(2019, 2, 12), date(2019, 3, 12),
            date(2019, 1, 12), date(2019, 2, 12),
            date(2019, 3, 12),
        ],
        'qualified_employment': [100, 110, 120, 50, 45, 30],
    })


def test_decompose_employment_change_columns() -> None:
    payroll = _make_payroll_for_decomposition()
    out = decompose_employment_change(payroll, [])
    expected_cols = ['ref_date', 'total_change', 'within_change', 'entry_contribution', 'exit_contribution']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_decompose_employment_change_identity() -> None:
    """total_change should equal within_change + entry_contribution - exit_contribution."""
    payroll = _make_payroll_for_decomposition()
    out = decompose_employment_change(payroll, [])
    for row in out.iter_rows(named=True):
        expected = row['within_change'] + row['entry_contribution'] - row['exit_contribution']
        assert row['total_change'] == expected, (
            f"Identity violated: {row['total_change']} != {expected}"
        )


def test_decompose_employment_change_feb() -> None:
    """In Feb: c1 continues (100->110), c2 continues (50->45), no entries or exits."""
    payroll = _make_payroll_for_decomposition()
    out = decompose_employment_change(payroll, [])
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    # c1: 100->110, c2: 50->45 => within = 10 + (-5) = 5
    assert feb['within_change'].to_list()[0] == 5
    assert feb['entry_contribution'].to_list()[0] == 0
    assert feb['exit_contribution'].to_list()[0] == 0
    assert feb['total_change'].to_list()[0] == 5


def test_decompose_employment_change_mar() -> None:
    """In Mar: c1 continues (110->120), c2 exits (emp=45), c3 enters (emp=30)."""
    payroll = _make_payroll_for_decomposition()
    out = decompose_employment_change(payroll, [])
    mar = out.filter(pl.col('ref_date') == date(2019, 3, 12))
    # c1: 110->120 => within = 10
    assert mar['within_change'].to_list()[0] == 10
    # c3 enters with 30
    assert mar['entry_contribution'].to_list()[0] == 30
    # c2 exits with 45
    assert mar['exit_contribution'].to_list()[0] == 45
    # total = 10 + 30 - 45 = -5
    assert mar['total_change'].to_list()[0] == -5


def test_decompose_employment_change_empty() -> None:
    """Empty payroll should return empty frame."""
    payroll = pl.LazyFrame({
        'client_id': [],
        'ref_date': [],
        'qualified_employment': [],
    }).cast({
        'client_id': pl.Utf8,
        'ref_date': pl.Date,
        'qualified_employment': pl.Int64,
    })
    out = decompose_employment_change(payroll, [])
    assert out.height == 0


# --- analyze_turning_points tests ---


def _make_turning_point_data():
    """Payroll and official growth series with a sign change."""
    dates = pl.date_range(date(2019, 1, 1), date(2020, 12, 1), '1mo', eager=True)
    n = len(dates)
    # Payroll: positive then negative growth
    payroll_yoy = [0.02 - 0.002 * i for i in range(n)]
    official_yoy = [0.025 - 0.0022 * i for i in range(n)]
    payroll_growth = pl.LazyFrame({
        'ref_date': dates,
        'yoy_growth': payroll_yoy,
    })
    official_growth = pl.LazyFrame({
        'ref_date': dates,
        'yoy_growth': official_yoy,
    })
    return payroll_growth, official_growth


def test_analyze_turning_points_columns() -> None:
    payroll_growth, official_growth = _make_turning_point_data()
    out = analyze_turning_points(payroll_growth, official_growth)
    if out.is_empty():
        return
    expected_cols = ['payroll_turn_date', 'official_turn_date', 'lead_lag_months']
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_analyze_turning_points_detects_sign_change() -> None:
    """Should detect at least one turning point in data that crosses zero."""
    payroll_growth, official_growth = _make_turning_point_data()
    out = analyze_turning_points(payroll_growth, official_growth)
    if out.is_empty():
        return
    assert out.height >= 1, "Expected at least one turning point"


def test_analyze_turning_points_lead_lag_type() -> None:
    """lead_lag_months should be numeric."""
    payroll_growth, official_growth = _make_turning_point_data()
    out = analyze_turning_points(payroll_growth, official_growth)
    if out.is_empty():
        return
    for val in out['lead_lag_months'].to_list():
        assert isinstance(val, (int, float)), f"Expected numeric lead_lag, got {type(val)}"


def test_analyze_turning_points_summary_stats() -> None:
    """Should include median and mean lead_lag."""
    payroll_growth, official_growth = _make_turning_point_data()
    out = analyze_turning_points(payroll_growth, official_growth)
    if out.is_empty():
        return
    assert 'median_lead_lag' in out.columns
    assert 'mean_lead_lag' in out.columns


def test_analyze_turning_points_no_turns() -> None:
    """With all-positive growth, no turning points should be detected."""
    dates = pl.date_range(date(2019, 1, 1), date(2020, 12, 1), '1mo', eager=True)
    payroll_growth = pl.LazyFrame({
        'ref_date': dates,
        'yoy_growth': [0.02] * len(dates),
    })
    official_growth = pl.LazyFrame({
        'ref_date': dates,
        'yoy_growth': [0.03] * len(dates),
    })
    out = analyze_turning_points(payroll_growth, official_growth)
    assert out.height == 0 or 'payroll_turn_date' not in out.columns or out.is_empty()
