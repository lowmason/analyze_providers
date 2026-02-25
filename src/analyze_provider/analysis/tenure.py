"""Client tenure and churn analysis (methodology section 5).

Computes tenure metrics, entry/exit rates, vintage analysis, and contamination flagging.
"""

import polars as pl


def compute_client_tenure(payroll: pl.LazyFrame) -> pl.LazyFrame:
    """Compute per-client tenure metrics.

    Returns: client_id, first_observed, last_observed, tenure_months, months_observed,
    initial_emp, final_emp, avg_emp, is_likely_birth.
    """
    df = payroll.group_by('client_id').agg(
        pl.col('ref_date').min().alias('first_observed'),
        pl.col('ref_date').max().alias('last_observed'),
        pl.col('ref_date').n_unique().alias('months_observed'),
        pl.col('qualified_employment').first().alias('initial_emp'),
        pl.col('qualified_employment').last().alias('final_emp'),
        pl.col('qualified_employment').mean().alias('avg_emp'),
        pl.col('is_birth').first().alias('is_likely_birth'),
    )
    df = df.with_columns(
        ((pl.col('last_observed') - pl.col('first_observed')).dt.total_days() / 30.0).round(0).cast(pl.Int64).alias('tenure_months'),
    )
    return df


def compute_client_entry_exit(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute monthly counts/rates of client entries, exits, churn rate, net client change.

    Args:
        payroll: LazyFrame with ref_date, client_id, entry_month, exit_month.
        grouping_cols: Columns to group by.

    Returns:
        LazyFrame with ref_date, grouping_cols, entries, exits, churn_rate, net_client_change.
    """
    by = ['ref_date'] + grouping_cols
    agg = payroll.group_by(by).agg(
        pl.col('client_id').n_unique().alias('active_clients'),
        pl.when(pl.col('entry_month') == pl.col('ref_date')).then(1).otherwise(0).sum().alias('entries'),
        pl.when(pl.col('exit_month') == pl.col('ref_date')).then(1).otherwise(0).sum().alias('exits'),
    )
    agg = agg.with_columns(
        (pl.col('entries') / pl.col('active_clients').fill_null(1)).alias('entry_rate'),
        (pl.col('exits') / pl.col('active_clients').fill_null(1)).alias('exit_rate'),
        ((pl.col('entries') + pl.col('exits')) / pl.col('active_clients').fill_null(1)).alias('churn_rate'),
        (pl.col('entries') - pl.col('exits')).alias('net_client_change'),
    )
    return agg.sort(by)


def client_churn_by_geography(
    payroll: pl.LazyFrame,
    geo_col: str,
) -> pl.LazyFrame:
    """Stratified entry/exit rates by geography."""
    return compute_client_entry_exit(payroll, [geo_col])


def client_churn_by_industry(
    payroll: pl.LazyFrame,
    industry_col: str,
) -> pl.LazyFrame:
    """Stratified entry/exit rates by industry."""
    return compute_client_entry_exit(payroll, [industry_col])


def client_churn_by_size(
    payroll: pl.LazyFrame,
) -> pl.LazyFrame:
    """Stratified entry/exit rates by size class."""
    return compute_client_entry_exit(payroll, ['size_class'])


def compute_vintage_analysis(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Employment and payroll aggregates stratified by client vintage (year of first appearance).

    Args:
        payroll: LazyFrame with client_id, ref_date, qualified_employment, entry_month.
        grouping_cols: Additional grouping columns.

    Returns:
        LazyFrame with ref_date, vintage_year, grouping_cols, employment, client_count.
    """
    # Determine vintage = year of first observation
    client_vintage = payroll.group_by('client_id').agg(
        pl.col('ref_date').min().dt.year().alias('vintage_year'),
    )
    with_vintage = payroll.join(client_vintage, on='client_id')

    by = ['ref_date', 'vintage_year'] + grouping_cols
    agg = with_vintage.group_by(by).agg(
        pl.col('qualified_employment').sum().alias('employment'),
        pl.col('client_id').n_unique().alias('client_count'),
    )
    return agg.sort(by)


def compute_vintage_shares(payroll: pl.LazyFrame) -> pl.LazyFrame:
    """Time series of employment share by vintage; flag contamination when recent-vintage share is high.

    Returns: ref_date, vintage_year, employment, employment_share, is_contaminated.
    """
    vintage = compute_vintage_analysis(payroll, [])
    totals = vintage.group_by('ref_date').agg(pl.col('employment').sum().alias('total_emp'))
    with_share = vintage.join(totals, on='ref_date').with_columns(
        (pl.col('employment') / pl.col('total_emp')).alias('employment_share'),
    )
    # Flag contamination: if most recent vintage year accounts for >30% of employment
    with_share = with_share.with_columns(
        pl.col('vintage_year').max().over('ref_date').alias('latest_vintage'),
    ).with_columns(
        (
            (pl.col('vintage_year') == pl.col('latest_vintage'))
            & (pl.col('employment_share') > 0.3)
        ).alias('is_contaminated'),
    ).drop('latest_vintage')
    return with_share


def tenure_summary_by_group(
    payroll: pl.LazyFrame,
    group_col: str,
) -> pl.LazyFrame:
    """Mean, median, percentiles, std of tenure for any grouping variable.

    Args:
        payroll: LazyFrame with client_id, ref_date, and group_col.
        group_col: Column to group by.

    Returns:
        LazyFrame with group_col, mean_tenure, median_tenure, p25_tenure, p75_tenure, std_tenure.
    """
    tenure = compute_client_tenure(payroll)
    # Join back the grouping column (lost during client-level aggregation)
    client_group = payroll.group_by('client_id').agg(
        pl.col(group_col).first().alias(group_col),
    )
    tenure = tenure.join(client_group, on='client_id')
    return tenure.group_by(group_col).agg(
        pl.col('tenure_months').mean().alias('mean_tenure'),
        pl.col('tenure_months').median().alias('median_tenure'),
        pl.col('tenure_months').quantile(0.25).alias('p25_tenure'),
        pl.col('tenure_months').quantile(0.75).alias('p75_tenure'),
        pl.col('tenure_months').std().alias('std_tenure'),
        pl.col('client_id').n_unique().alias('client_count'),
    )
