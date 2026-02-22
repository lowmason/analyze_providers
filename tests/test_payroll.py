"""Tests for payroll data loading."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from analyze_provider.data.payroll import REQUIRED_COLUMNS, load_payroll


def test_load_payroll_valid() -> None:
    df = pl.DataFrame({
        'client_id': ['c1', 'c1', 'c2'],
        'ref_date': ['2019-01-12', '2019-02-12', '2019-01-12'],
        'entry_month': ['2019-01-01', '2019-01-01', '2019-01-01'],
        'exit_month': [None, None, None],
        'is_birth': [True, None, False],
        'naics_code': ['236220', '236220', '445110'],
        'state_fips': ['26', '26', '36'],
        'qualified_employment': [10, 12, 5],
    })
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / 'payroll.parquet'
        df.write_parquet(path)
        lf = load_payroll(path)
        out = lf.collect()
    assert 'naics2' in out.columns
    assert 'naics3' in out.columns
    assert 'supersector' in out.columns
    assert 'size_class' in out.columns
    assert 'quarter' in out.columns
    assert out['naics2'].to_list() == ['23', '23', '44']
    assert out['supersector'].to_list() == ['Construction', 'Construction', 'Retail trade']
    assert out['quarter'].to_list() == ['2019Q1', '2019Q1', '2019Q1']


def test_load_payroll_missing_column_raises() -> None:
    df = pl.DataFrame({
        'client_id': ['c1'],
        'ref_date': ['2019-01-12'],
        'entry_month': ['2019-01-01'],
        'exit_month': [None],
        'is_birth': [None],
        'naics_code': ['236220'],
        'state_fips': ['26'],
    })
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / 'payroll.parquet'
        df.write_parquet(path)
        with pytest.raises(ValueError, match='missing required columns'):
            load_payroll(path).collect()
