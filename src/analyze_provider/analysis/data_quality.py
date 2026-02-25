"""Data quality flags (methodology section 7).

Flags data quality issues at the client-month level:
- Extreme employment changes (>50% MoM)
- Zero-employment months
- Multi-client employees (requires employee-level data)
- Filing-date anomalies
"""

import polars as pl


def flag_data_quality_issues(payroll: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.DataFrame]:
    """Flag data quality issues at the client-month level.

    Args:
        payroll: LazyFrame with client_id, ref_date, qualified_employment, and optionally
                 employee_id, filing_date columns.

    Returns:
        Tuple of (flagged_rows LazyFrame, summary DataFrame).
        flagged_rows has original columns plus flag columns (flag_extreme_change,
        flag_zero_employment, flag_multi_client, flag_filing_anomaly, has_any_flag).
        summary has flag counts by type and ref_date.
    """
    schema = payroll.collect_schema().names()

    df = payroll.sort(['client_id', 'ref_date'])

    # Flag 1: Extreme employment changes (>50% MoM)
    df = df.with_columns(
        pl.col('qualified_employment').shift(1).over('client_id').alias('prev_employment'),
    )
    df = df.with_columns(
        pl.when(
            (pl.col('prev_employment').is_not_null())
            & (pl.col('prev_employment') > 0)
            & (
                ((pl.col('qualified_employment') - pl.col('prev_employment')).abs() / pl.col('prev_employment'))
                > 0.5
            )
        ).then(True).otherwise(False).alias('flag_extreme_change'),
    )

    # Flag 2: Zero-employment months
    df = df.with_columns(
        (pl.col('qualified_employment') == 0).alias('flag_zero_employment'),
    )

    # Flag 3: Multi-client employees (same employee_id at multiple clients in same month)
    if 'employee_id' in schema:
        multi_client = (
            df.group_by(['ref_date', 'employee_id'])
            .agg(pl.col('client_id').n_unique().alias('client_count_per_emp'))
            .filter(pl.col('client_count_per_emp') > 1)
            .select(['ref_date', 'employee_id'])
            .with_columns(pl.lit(True).alias('flag_multi_client'))
        )
        df = df.join(multi_client, on=['ref_date', 'employee_id'], how='left')
        df = df.with_columns(pl.col('flag_multi_client').fill_null(False))
    else:
        df = df.with_columns(pl.lit(False).alias('flag_multi_client'))

    # Flag 4: Filing-date anomalies (filing_date after first observation)
    if 'filing_date' in schema:
        first_obs = df.group_by('client_id').agg(pl.col('ref_date').min().alias('first_observation'))
        df = df.join(first_obs, on='client_id', how='left')
        df = df.with_columns(
            pl.when(
                pl.col('filing_date').is_not_null()
                & (pl.col('filing_date').cast(pl.Date) > pl.col('first_observation'))
            ).then(True).otherwise(False).alias('flag_filing_anomaly'),
        ).drop('first_observation')
    else:
        df = df.with_columns(pl.lit(False).alias('flag_filing_anomaly'))

    # Composite flag
    df = df.with_columns(
        (
            pl.col('flag_extreme_change')
            | pl.col('flag_zero_employment')
            | pl.col('flag_multi_client')
            | pl.col('flag_filing_anomaly')
        ).alias('has_any_flag'),
    )

    # Drop helper columns
    if 'prev_employment' in df.collect_schema().names():
        df = df.drop('prev_employment')

    # Summary: flag counts by type and ref_date
    summary = df.group_by('ref_date').agg(
        pl.col('flag_extreme_change').sum().alias('extreme_change_count'),
        pl.col('flag_zero_employment').sum().alias('zero_employment_count'),
        pl.col('flag_multi_client').sum().alias('multi_client_count'),
        pl.col('flag_filing_anomaly').sum().alias('filing_anomaly_count'),
        pl.col('has_any_flag').sum().alias('total_flagged'),
        pl.len().alias('total_records'),
    ).sort('ref_date').collect()

    return df, summary
