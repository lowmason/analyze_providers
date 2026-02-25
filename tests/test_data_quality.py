"""Tests for data quality flagging."""

from datetime import date

import polars as pl

from analyze_provider.analysis.data_quality import flag_data_quality_issues


def test_flag_extreme_change() -> None:
    """Employment jumps >50% MoM should be flagged."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1', 'c1'],
        'ref_date': [
            date(2019, 1, 12),
            date(2019, 2, 12),
            date(2019, 3, 12),
        ],
        'qualified_employment': [100, 200, 210],  # 100->200 is +100% change
    })
    flagged, summary = flag_data_quality_issues(payroll)
    out = flagged.collect()
    assert 'flag_extreme_change' in out.columns
    # Row for Feb should be flagged (100 -> 200 = +100%)
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert feb['flag_extreme_change'].to_list()[0] == True
    # Row for Jan has no previous -> not flagged
    jan = out.filter(pl.col('ref_date') == date(2019, 1, 12))
    assert jan['flag_extreme_change'].to_list()[0] == False
    # Row for Mar: 200 -> 210 = +5% -> not flagged
    mar = out.filter(pl.col('ref_date') == date(2019, 3, 12))
    assert mar['flag_extreme_change'].to_list()[0] == False


def test_no_extreme_change_for_small_change() -> None:
    """Employment change <=50% should not be flagged."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'qualified_employment': [100, 140],  # 40% change
    })
    flagged, _ = flag_data_quality_issues(payroll)
    out = flagged.collect()
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert feb['flag_extreme_change'].to_list()[0] == False


def test_flag_zero_employment() -> None:
    """Zero employment months should be flagged."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1', 'c2'],
        'ref_date': [
            date(2019, 1, 12),
            date(2019, 2, 12),
            date(2019, 1, 12),
        ],
        'qualified_employment': [0, 10, 5],
    })
    flagged, summary = flag_data_quality_issues(payroll)
    out = flagged.collect()
    assert 'flag_zero_employment' in out.columns
    zero_rows = out.filter(pl.col('flag_zero_employment') == True)
    assert zero_rows.height == 1
    assert zero_rows['client_id'].to_list()[0] == 'c1'
    assert zero_rows['qualified_employment'].to_list()[0] == 0


def test_flag_filing_anomaly() -> None:
    """Filing date after first observation should be flagged."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'qualified_employment': [10, 12],
        'filing_date': [date(2019, 3, 1), date(2019, 3, 1)],
    })
    flagged, summary = flag_data_quality_issues(payroll)
    out = flagged.collect()
    assert 'flag_filing_anomaly' in out.columns
    # filing_date (2019-03-01) > first_observation (2019-01-12) => flagged
    assert out['flag_filing_anomaly'].to_list() == [True, True]


def test_no_filing_anomaly_when_normal() -> None:
    """Filing date before first observation should not be flagged."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'qualified_employment': [10, 12],
        'filing_date': [date(2018, 12, 1), date(2018, 12, 1)],
    })
    flagged, _ = flag_data_quality_issues(payroll)
    out = flagged.collect()
    assert all(v == False for v in out['flag_filing_anomaly'].to_list())


def test_flag_multi_client_employee() -> None:
    """Employee at multiple clients in same month should be flagged."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c2', 'c1'],
        'ref_date': [
            date(2019, 1, 12),
            date(2019, 1, 12),
            date(2019, 2, 12),
        ],
        'employee_id': ['e1', 'e1', 'e1'],
        'qualified_employment': [10, 5, 12],
    })
    flagged, summary = flag_data_quality_issues(payroll)
    out = flagged.collect()
    assert 'flag_multi_client' in out.columns
    # In Jan, e1 is at c1 and c2 => both Jan rows should be flagged
    jan = out.filter(pl.col('ref_date') == date(2019, 1, 12))
    assert all(v == True for v in jan['flag_multi_client'].to_list())
    # In Feb, e1 is only at c1 => not flagged
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert feb['flag_multi_client'].to_list()[0] == False


def test_has_any_flag_composite() -> None:
    """has_any_flag should be True if any individual flag is True."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'qualified_employment': [0, 10],
    })
    flagged, _ = flag_data_quality_issues(payroll)
    out = flagged.collect()
    jan = out.filter(pl.col('ref_date') == date(2019, 1, 12))
    assert jan['has_any_flag'].to_list()[0] == True  # zero employment
    feb = out.filter(pl.col('ref_date') == date(2019, 2, 12))
    assert feb['has_any_flag'].to_list()[0] == False


def test_summary_dataframe() -> None:
    """Summary should be a DataFrame with flag counts by ref_date."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1', 'c2'],
        'ref_date': [
            date(2019, 1, 12),
            date(2019, 2, 12),
            date(2019, 1, 12),
        ],
        'qualified_employment': [0, 100, 5],
    })
    _, summary = flag_data_quality_issues(payroll)
    assert isinstance(summary, pl.DataFrame)
    assert 'ref_date' in summary.columns
    assert 'extreme_change_count' in summary.columns
    assert 'zero_employment_count' in summary.columns
    assert 'total_flagged' in summary.columns
    assert 'total_records' in summary.columns
    # Jan has 1 zero-employment flag (c1)
    jan = summary.filter(pl.col('ref_date') == date(2019, 1, 12))
    assert jan['zero_employment_count'].to_list()[0] == 1
    assert jan['total_records'].to_list()[0] == 2


def test_prev_employment_not_in_output() -> None:
    """The helper column prev_employment should be dropped from output."""
    payroll = pl.LazyFrame({
        'client_id': ['c1', 'c1'],
        'ref_date': [date(2019, 1, 12), date(2019, 2, 12)],
        'qualified_employment': [100, 200],
    })
    flagged, _ = flag_data_quality_issues(payroll)
    out = flagged.collect()
    assert 'prev_employment' not in out.columns
