"""Tests for panel aggregation."""

import tempfile
from pathlib import Path

import polars as pl

from analyze_provider.data.payroll import load_payroll
from analyze_provider.panel import build_panel


def _make_payroll_lf() -> pl.LazyFrame:
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
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / 'payroll.parquet'
        df.write_parquet(path)
        return load_payroll(path)


def test_build_panel_columns() -> None:
    payroll = _make_payroll_lf()
    panel = build_panel(payroll)
    out = panel.collect()
    assert 'grouping_level' in out.columns
    assert 'ref_date' in out.columns
    assert 'quarter' in out.columns
    assert 'payroll_employment' in out.columns
    assert 'client_count' in out.columns
    assert 'birth_count' in out.columns
    assert 'birth_determinable_count' in out.columns


def test_build_panel_national_total() -> None:
    payroll = _make_payroll_lf()
    panel = build_panel(payroll)
    out = panel.collect()
    national = out.filter(pl.col('grouping_level') == 'national')
    jan = national.filter(pl.col('ref_date').dt.month() == 1)
    assert jan['payroll_employment'].to_list()[0] == 15
