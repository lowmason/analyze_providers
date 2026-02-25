"""Earnings distribution analysis (methodology section 4.3).

Computes earnings distribution metrics, growth rates, and validation against QCEW wages.
"""

import polars as pl


def compute_earnings_distribution(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute earnings distribution: mean, median, P10/P25/P75/P90, std, CV of monthly gross pay.

    Args:
        payroll: LazyFrame with ref_date and gross_pay column.
        grouping_cols: Columns to group by (e.g. [] for national, ['supersector']).

    Returns:
        LazyFrame with ref_date, grouping_cols, and earnings statistics.
    """
    schema = payroll.collect_schema().names()
    if 'gross_pay' not in schema:
        return pl.DataFrame({
            'ref_date': [], 'mean_earnings': [], 'median_earnings': [],
            'p10_earnings': [], 'p25_earnings': [], 'p75_earnings': [],
            'p90_earnings': [], 'std_earnings': [], 'cv_earnings': [],
        }).lazy()

    by = ['ref_date'] + grouping_cols
    return payroll.filter(pl.col('gross_pay').is_not_null() & (pl.col('gross_pay') > 0)).group_by(by).agg(
        pl.col('gross_pay').mean().alias('mean_earnings'),
        pl.col('gross_pay').median().alias('median_earnings'),
        pl.col('gross_pay').quantile(0.10).alias('p10_earnings'),
        pl.col('gross_pay').quantile(0.25).alias('p25_earnings'),
        pl.col('gross_pay').quantile(0.75).alias('p75_earnings'),
        pl.col('gross_pay').quantile(0.90).alias('p90_earnings'),
        pl.col('gross_pay').std().alias('std_earnings'),
        (pl.col('gross_pay').std() / pl.col('gross_pay').mean()).alias('cv_earnings'),
    ).sort(by)


def compute_earnings_growth(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute YoY growth in mean and median earnings.

    Args:
        payroll: LazyFrame with ref_date and gross_pay column.
        grouping_cols: Columns to group by.

    Returns:
        LazyFrame with ref_date, grouping_cols, mean_earnings, median_earnings,
        mean_earnings_yoy, median_earnings_yoy.
    """
    dist = compute_earnings_distribution(payroll, grouping_cols)
    by = grouping_cols if grouping_cols else []
    if by:
        return dist.sort(['ref_date'] + by).with_columns(
            pl.col('mean_earnings').pct_change(12).over(by).alias('mean_earnings_yoy'),
            pl.col('median_earnings').pct_change(12).over(by).alias('median_earnings_yoy'),
        )
    return dist.sort(['ref_date']).with_columns(
        pl.col('mean_earnings').pct_change(12).alias('mean_earnings_yoy'),
        pl.col('median_earnings').pct_change(12).alias('median_earnings_yoy'),
    )
