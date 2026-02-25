"""Tests for client tenure and churn analysis."""

from datetime import date

import polars as pl

from analyze_provider.analysis.tenure import (
    compute_client_entry_exit,
    compute_client_tenure,
    compute_vintage_analysis,
    compute_vintage_shares,
    tenure_summary_by_group,
)


def _make_payroll() -> pl.LazyFrame:
    """Synthetic payroll spanning 3 months with 3 clients."""
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
        'entry_month': [
            date(2019, 1, 12), date(2019, 1, 12), date(2019, 1, 12),
            date(2019, 1, 12), date(2019, 1, 12),
            date(2019, 3, 12),
        ],
        'exit_month': [
            None, None, None,
            date(2019, 2, 12), date(2019, 2, 12),
            None,
        ],
        'is_birth': [
            True, None, None,
            False, None,
            True,
        ],
        'qualified_employment': [10, 12, 14, 5, 6, 8],
        'supersector': [
            'Construction', 'Construction', 'Construction',
            'Retail trade', 'Retail trade',
            'Construction',
        ],
    })


def test_compute_client_tenure_columns() -> None:
    payroll = _make_payroll()
    out = compute_client_tenure(payroll).collect()
    expected = [
        'client_id', 'first_observed', 'last_observed', 'tenure_months',
        'months_observed', 'initial_emp', 'final_emp', 'avg_emp', 'is_likely_birth',
    ]
    for col in expected:
        assert col in out.columns, f"Missing column: {col}"


def test_compute_client_tenure_values() -> None:
    payroll = _make_payroll()
    out = compute_client_tenure(payroll).collect().sort('client_id')
    # c1: 3 months observed (Jan-Mar), tenure_months ~ 2
    c1 = out.filter(pl.col('client_id') == 'c1')
    assert c1['months_observed'].to_list()[0] == 3
    assert c1['tenure_months'].to_list()[0] == 2
    assert c1['initial_emp'].to_list()[0] == 10
    assert c1['is_likely_birth'].to_list()[0] == True

    # c2: 2 months observed (Jan-Feb), tenure_months ~ 1
    c2 = out.filter(pl.col('client_id') == 'c2')
    assert c2['months_observed'].to_list()[0] == 2
    assert c2['tenure_months'].to_list()[0] == 1

    # c3: 1 month observed (Mar), tenure_months = 0
    c3 = out.filter(pl.col('client_id') == 'c3')
    assert c3['months_observed'].to_list()[0] == 1
    assert c3['tenure_months'].to_list()[0] == 0


def test_compute_client_entry_exit_columns() -> None:
    payroll = _make_payroll()
    out = compute_client_entry_exit(payroll, []).collect()
    expected = [
        'ref_date', 'active_clients', 'entries', 'exits',
        'entry_rate', 'exit_rate', 'churn_rate', 'net_client_change',
    ]
    for col in expected:
        assert col in out.columns, f"Missing column: {col}"


def test_compute_client_entry_exit_values() -> None:
    payroll = _make_payroll()
    out = compute_client_entry_exit(payroll, []).collect().sort('ref_date')
    # Jan: c1 and c2 enter
    jan = out.filter(pl.col('ref_date') == date(2019, 1, 12))
    assert jan['entries'].to_list()[0] == 2
    assert jan['exits'].to_list()[0] == 0

    # Feb: c2 exits (exit_month == ref_date)
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert feb['exits'].to_list()[0] == 1  # c2 exits
    assert feb['entries'].to_list()[0] == 0

    # Mar: c3 enters
    mar = out.filter(pl.col('ref_date') == date(2019, 3, 12))
    assert mar['entries'].to_list()[0] == 1
    assert mar['exits'].to_list()[0] == 0


def test_compute_client_entry_exit_net_change() -> None:
    payroll = _make_payroll()
    out = compute_client_entry_exit(payroll, []).collect().sort('ref_date')
    for row in out.iter_rows(named=True):
        assert row['net_client_change'] == row['entries'] - row['exits']


def test_compute_client_entry_exit_grouped() -> None:
    payroll = _make_payroll()
    out = compute_client_entry_exit(payroll, ['supersector']).collect()
    assert 'supersector' in out.columns
    sectors = out['supersector'].unique().to_list()
    assert 'Construction' in sectors
    assert 'Retail trade' in sectors


def test_compute_vintage_analysis_columns() -> None:
    payroll = _make_payroll()
    out = compute_vintage_analysis(payroll, []).collect()
    expected = ['ref_date', 'vintage_year', 'employment', 'client_count']
    for col in expected:
        assert col in out.columns, f"Missing column: {col}"


def test_compute_vintage_analysis_values() -> None:
    payroll = _make_payroll()
    out = compute_vintage_analysis(payroll, []).collect()
    # All clients have vintage_year 2019 because first observation is in 2019
    assert out['vintage_year'].unique().to_list() == [2019]
    # In Jan: total employment = 10 + 5 = 15
    jan = out.filter(pl.col('ref_date') == date(2019, 1, 12))
    assert jan['employment'].to_list()[0] == 15


def test_compute_vintage_shares() -> None:
    payroll = _make_payroll()
    out = compute_vintage_shares(payroll).collect()
    assert 'employment_share' in out.columns
    assert 'is_contaminated' in out.columns
    # With a single vintage year, share should be 1.0 for all rows
    for share in out['employment_share'].to_list():
        assert abs(share - 1.0) < 1e-9


def test_compute_vintage_shares_contamination() -> None:
    """When latest vintage accounts for >30% of employment, flag contamination."""
    # Create payroll with two vintages, latest vintage dominates
    payroll = pl.LazyFrame({
        'client_id': ['old', 'old', 'new', 'new'],
        'ref_date': [
            date(2018, 6, 12), date(2019, 6, 12),
            date(2019, 6, 12), date(2019, 6, 12),
        ],
        'entry_month': [
            date(2018, 6, 12), date(2018, 6, 12),
            date(2019, 6, 12), date(2019, 6, 12),
        ],
        'exit_month': [None, None, None, None],
        'is_birth': [False, None, True, None],
        'qualified_employment': [10, 10, 100, 100],
    })
    out = compute_vintage_shares(payroll).collect()
    # In 2019-06, new vintage has 200/(200+10) > 0.3 => contaminated
    june_2019 = out.filter(
        (pl.col('ref_date') == date(2019, 6, 12))
        & (pl.col('vintage_year') == 2019)
    )
    assert june_2019['is_contaminated'].to_list()[0] == True


def test_tenure_summary_by_group() -> None:
    payroll = _make_payroll()
    out = tenure_summary_by_group(payroll, 'supersector').collect()
    assert 'mean_tenure' in out.columns
    assert 'median_tenure' in out.columns
    assert 'p25_tenure' in out.columns
    assert 'p75_tenure' in out.columns
    assert 'std_tenure' in out.columns
    assert 'client_count' in out.columns
    sectors = out['supersector'].to_list()
    assert 'Construction' in sectors
