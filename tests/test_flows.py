"""Tests for job flows analysis."""

from datetime import date

import polars as pl

from analyze_provider.analysis.flows import (
    compute_job_flows,
    compute_job_flows_by_geography,
    compute_job_flows_by_industry,
    compute_job_flows_by_size,
)


def _make_payroll_with_entry_exit() -> pl.LazyFrame:
    """Payroll with entry_month/exit_month for deriving hires/separations."""
    return pl.LazyFrame({
        'client_id': ['c1', 'c1', 'c2', 'c2', 'c3'],
        'ref_date': [
            date(2019, 1, 12),
            date(2019, 2, 12),
            date(2019, 1, 12),
            date(2019, 2, 12),
            date(2019, 2, 12),
        ],
        'entry_month': [
            date(2019, 1, 12),
            date(2019, 1, 12),
            date(2019, 1, 12),
            date(2019, 1, 12),
            date(2019, 2, 12),
        ],
        'exit_month': [
            None,
            None,
            date(2019, 2, 12),
            date(2019, 2, 12),
            None,
        ],
        'qualified_employment': [10, 12, 5, 6, 8],
        'supersector': ['Construction', 'Construction', 'Retail trade', 'Retail trade', 'Construction'],
        'state_fips': ['26', '26', '36', '36', '26'],
        'size_class': ['5-9', '10-19', '1-4', '5-9', '5-9'],
    })


def test_compute_job_flows_columns() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows(payroll, []).collect()
    expected_cols = [
        'hires', 'separations', 'employment',
        'continuing_employment', 'hire_rate', 'separation_rate',
        'churn_rate', 'net_growth_rate',
    ]
    for col in expected_cols:
        assert col in out.columns, f"Missing column: {col}"


def test_compute_job_flows_national_jan() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows(payroll, []).collect()
    jan = out.filter(pl.col('ref_date') == date(2019, 1, 12))
    # In Jan, both c1 and c2 have entry_month == ref_date => hires = 10 + 5 = 15
    assert jan['hires'].to_list()[0] == 15
    # No exits in Jan
    assert jan['separations'].to_list()[0] == 0
    assert jan['employment'].to_list()[0] == 15


def test_compute_job_flows_national_feb() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows(payroll, []).collect()
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    # In Feb, c3 is a new entry => hires = 8
    assert feb['hires'].to_list()[0] == 8
    # c2 exits in Feb => separations = 6
    assert feb['separations'].to_list()[0] == 6
    assert feb['employment'].to_list()[0] == 26  # 12 + 6 + 8


def test_compute_job_flows_rates() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows(payroll, []).collect()
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    emp = feb['employment'].to_list()[0]
    hires = feb['hires'].to_list()[0]
    seps = feb['separations'].to_list()[0]
    assert abs(feb['hire_rate'].to_list()[0] - hires / emp) < 1e-9
    assert abs(feb['separation_rate'].to_list()[0] - seps / emp) < 1e-9
    assert abs(feb['churn_rate'].to_list()[0] - (hires + seps) / emp) < 1e-9
    assert abs(feb['net_growth_rate'].to_list()[0] - (hires - seps) / emp) < 1e-9


def test_compute_job_flows_with_hires_separations_cols() -> None:
    """Test the branch that uses pre-aggregated hires/separations columns."""
    payroll = pl.LazyFrame({
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'client_id': ['c1', 'c1'],
        'qualified_employment': [100, 110],
        'hires': [20, 15],
        'separations': [10, 5],
    })
    out = compute_job_flows(payroll, []).collect()
    assert out['hires'].to_list() == [20, 15]
    assert out['separations'].to_list() == [10, 5]
    assert out['continuing_employment'].to_list() == [80, 95]


def test_compute_job_flows_with_employee_id() -> None:
    """Test the employee-level branch."""
    payroll = pl.LazyFrame({
        'ref_date': [
            date(2019, 1, 12), date(2019, 1, 12),
            date(2019, 2, 12), date(2019, 2, 12), date(2019, 2, 12),
        ],
        'client_id': ['c1', 'c1', 'c1', 'c1', 'c1'],
        'employee_id': ['e1', 'e2', 'e2', 'e3', 'e4'],
        'qualified_employment': [1, 1, 1, 1, 1],
    })
    out = compute_job_flows(payroll, []).collect()
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    # e1 left, e3 and e4 are new hires, e2 continues
    assert feb['hires'].to_list()[0] == 2
    assert feb['separations'].to_list()[0] == 1
    assert feb['continuing_employment'].to_list()[0] == 1


def test_compute_job_flows_by_geography() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows_by_geography(payroll, 'state_fips', []).collect()
    assert 'state_fips' in out.columns
    # Should have rows for both state_fips values
    states = out['state_fips'].unique().to_list()
    assert '26' in states
    assert '36' in states


def test_compute_job_flows_by_industry() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows_by_industry(payroll, 'supersector', []).collect()
    assert 'supersector' in out.columns


def test_compute_job_flows_by_size() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows_by_size(payroll, []).collect()
    assert 'size_class' in out.columns


def test_compute_job_flows_grouped_by_supersector() -> None:
    payroll = _make_payroll_with_entry_exit()
    out = compute_job_flows(payroll, ['supersector']).collect()
    jan_construction = out.filter(
        (pl.col('ref_date') == date(2019, 1, 12)) & (pl.col('supersector') == 'Construction')
    )
    # Only c1 in construction in Jan, entry_month == ref_date => hires = 10
    assert jan_construction['hires'].to_list()[0] == 10
    assert jan_construction['employment'].to_list()[0] == 10
