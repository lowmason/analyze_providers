"""Tests for panel aggregation."""

import tempfile
from pathlib import Path

from datetime import date

import polars as pl

from analyze_provider.data.payroll import load_payroll
from analyze_provider.panel import build_panel, filter_stable_panel


def _make_payroll_parquet(tmpdir: str) -> Path:
    df = pl.DataFrame({
        'client_id': ['c1', 'c1', 'c2', 'c2'],
        'ref_date': ['2019-01-12', '2019-02-12', '2019-01-12', '2019-02-12'],
        'entry_month': ['2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01'],
        'exit_month': [None, None, None, None],
        'is_birth': [True, None, False, None],
        'naics_code': ['236220', '236220', '445110', '445110'],
        'state_fips': ['26', '26', '36', '36'],
        'qualified_employment': [10, 12, 5, 6],
    })
    path = Path(tmpdir) / 'payroll.parquet'
    df.write_parquet(path)
    return path


def test_build_panel_columns() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = _make_payroll_parquet(d)
        payroll = load_payroll(path)
        panel = build_panel(payroll)
        out = panel.collect()
    assert 'grouping_level' in out.columns
    assert 'ref_date' in out.columns
    assert 'quarter' in out.columns
    assert 'payroll_employment' in out.columns
    assert 'client_count' in out.columns
    assert 'birth_count' in out.columns
    assert 'birth_determinable_count' in out.columns
    assert 'continuing_employment' in out.columns


def test_build_panel_national_total() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = _make_payroll_parquet(d)
        payroll = load_payroll(path)
        panel = build_panel(payroll)
        out = panel.collect()
    national = out.filter(pl.col('grouping_level') == 'national')
    jan = national.filter(pl.col('ref_date').dt.month() == 1)
    assert jan['payroll_employment'].to_list()[0] == 15


# --- continuing_employment tests ---


def test_continuing_employment_national_jan() -> None:
    """In Jan, entry_month (2019-01-01) != ref_date (2019-01-12), so clients are continuing."""
    with tempfile.TemporaryDirectory() as d:
        path = _make_payroll_parquet(d)
        payroll = load_payroll(path)
        panel = build_panel(payroll)
        out = panel.collect()
    national = out.filter(pl.col('grouping_level') == 'national')
    jan = national.filter(pl.col('ref_date').dt.month() == 1)
    # entry_month='2019-01-01' != ref_date='2019-01-12' => both are continuing
    assert jan['continuing_employment'].to_list()[0] == 15


def test_continuing_employment_national_feb() -> None:
    """In Feb, both clients continue (no exit, entry was Jan) => continuing = total employment."""
    with tempfile.TemporaryDirectory() as d:
        path = _make_payroll_parquet(d)
        payroll = load_payroll(path)
        panel = build_panel(payroll)
        out = panel.collect()
    national = out.filter(pl.col('grouping_level') == 'national')
    feb = national.filter(pl.col('ref_date').dt.month() == 2)
    # c1: 12, c2: 6; both continue (entry_month != ref_date, exit_month is null)
    assert feb['continuing_employment'].to_list()[0] == 18


def test_continuing_employment_with_exits() -> None:
    """Client exiting in a month should not count as continuing."""
    df = pl.DataFrame({
        'client_id': ['c1', 'c1', 'c2', 'c2'],
        'ref_date': ['2019-01-12', '2019-02-12', '2019-01-12', '2019-02-12'],
        'entry_month': ['2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01'],
        'exit_month': [None, None, None, '2019-02-12'],
        'is_birth': [True, None, False, None],
        'naics_code': ['236220', '236220', '445110', '445110'],
        'state_fips': ['26', '26', '36', '36'],
        'qualified_employment': [10, 12, 5, 6],
    })
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / 'payroll.parquet'
        df.write_parquet(path)
        payroll = load_payroll(path)
        panel = build_panel(payroll)
        out = panel.collect()
    national = out.filter(pl.col('grouping_level') == 'national')
    feb = national.filter(pl.col('ref_date').dt.month() == 2)
    # c1 continues (12), c2 exits in Feb (exit_month == ref_date) => not continuing
    assert feb['continuing_employment'].to_list()[0] == 12


def test_continuing_employment_excludes_new_entries() -> None:
    """Clients entering in a month (entry_month == ref_date) should not count as continuing."""
    df = pl.DataFrame({
        'client_id': ['c1', 'c1', 'c2'],
        'ref_date': ['2019-01-12', '2019-02-12', '2019-02-12'],
        'entry_month': ['2019-01-12', '2019-01-12', '2019-02-12'],
        'exit_month': [None, None, None],
        'is_birth': [True, None, True],
        'naics_code': ['236220', '236220', '445110'],
        'state_fips': ['26', '26', '36'],
        'qualified_employment': [10, 12, 8],
    })
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / 'payroll.parquet'
        df.write_parquet(path)
        payroll = load_payroll(path)
        panel = build_panel(payroll)
        out = panel.collect()
    national = out.filter(pl.col('grouping_level') == 'national')
    # Jan: c1 enters (entry_month == ref_date) => continuing = 0
    jan = national.filter(pl.col('ref_date').dt.month() == 1)
    assert jan['continuing_employment'].to_list()[0] == 0
    # Feb: c1 continues (12), c2 enters (8) => continuing = 12
    feb = national.filter(pl.col('ref_date').dt.month() == 2)
    assert feb['continuing_employment'].to_list()[0] == 12


# --- filter_stable_panel tests ---


def test_filter_stable_panel_keeps_long_tenure() -> None:
    """Clients with tenure >= min_tenure_months should be kept."""
    df = pl.LazyFrame({
        'client_id': ['c1', 'c1', 'c2'],
        'ref_date': [date(2019, 1, 12), date(2020, 6, 12), date(2019, 1, 12)],
        'qualified_employment': [10, 12, 5],
    })
    filtered = filter_stable_panel(df, min_tenure_months=12).collect()
    # c1 has ~17 months tenure, c2 has 0 months
    assert 'c1' in filtered['client_id'].to_list()
    assert 'c2' not in filtered['client_id'].to_list()


def test_filter_stable_panel_excludes_short_tenure() -> None:
    """Clients with tenure < min_tenure_months should be excluded."""
    df = pl.LazyFrame({
        'client_id': [
            'c1', 'c1', 'c1', 'c1', 'c1', 'c1',
            'c1', 'c1', 'c1', 'c1', 'c1', 'c1',
            'c1', 'c1', 'c1', 'c1', 'c1', 'c1',
            'c1', 'c1', 'c1', 'c1', 'c1', 'c1',
            'c2', 'c2', 'c2', 'c2', 'c2', 'c2',
            'c3',
        ],
        'ref_date': (
            [date(2018, m, 12) for m in range(1, 13)]
            + [date(2019, m, 12) for m in range(1, 13)]
            + [date(2019, m, 12) for m in range(1, 7)]
            + [date(2019, 1, 12)]
        ),
        'qualified_employment': [10] * 31,
    })
    out = filter_stable_panel(df, min_tenure_months=12).collect()
    clients = out['client_id'].unique().to_list()
    assert 'c1' in clients
    assert 'c2' not in clients
    assert 'c3' not in clients


def test_filter_stable_panel_preserves_all_rows() -> None:
    """All rows of qualifying clients should be preserved."""
    df = pl.LazyFrame({
        'client_id': ['c1', 'c1', 'c2'],
        'ref_date': [date(2019, 1, 12), date(2020, 6, 12), date(2019, 1, 12)],
        'qualified_employment': [10, 12, 5],
    })
    out = filter_stable_panel(df, min_tenure_months=12).collect()
    # c1 should keep both rows
    c1_rows = out.filter(pl.col('client_id') == 'c1')
    assert c1_rows.height == 2


def test_filter_stable_panel_zero_threshold() -> None:
    """With min_tenure_months=0, all clients should be included."""
    df = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c3'],
        'ref_date': [date(2019, 1, 12), date(2019, 1, 12), date(2019, 1, 12)],
        'qualified_employment': [10, 20, 30],
    })
    out = filter_stable_panel(df, min_tenure_months=0).collect()
    assert out.height == 3


def test_filter_stable_panel_returns_lazyframe() -> None:
    """Output should be a LazyFrame."""
    df = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2020, 6, 12)],
        'qualified_employment': [10, 12],
    })
    out = filter_stable_panel(df, min_tenure_months=12)
    assert isinstance(out, pl.LazyFrame)
