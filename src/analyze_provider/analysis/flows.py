"""Worker-level gross job flows (methodology section 4).

Computes monthly hires, separations, continuing employment, and derived rates
(hire rate, separation rate, churn rate, net growth rate) as share of employment.
"""

import polars as pl


def compute_job_flows(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute monthly hires, separations, continuing employment, and derived flow rates.

    If employee-level data (employee_id) is present, derives hires/separations from
    month-over-month presence at a client. Otherwise, uses pre-aggregated hires/separations
    columns if available.

    Args:
        payroll: LazyFrame with ref_date, client_id, qualified_employment, and optionally
                 employee_id, hires, separations columns.
        grouping_cols: Columns to group by (e.g. [] for national, ['supersector']).

    Returns:
        LazyFrame with ref_date, grouping_cols, hires, separations, continuing_employment,
        hire_rate, separation_rate, churn_rate, net_growth_rate.
    """
    schema = payroll.collect_schema().names()
    by = ['ref_date'] + grouping_cols

    if 'employee_id' in schema:
        return _compute_flows_from_employees(payroll, grouping_cols)

    if 'hires' in schema and 'separations' in schema:
        agg = payroll.group_by(by).agg(
            pl.col('hires').sum().alias('hires'),
            pl.col('separations').sum().alias('separations'),
            pl.col('qualified_employment').sum().alias('employment'),
        )
    else:
        # Derive from client-level entry/exit if available
        agg_exprs = [
            pl.col('qualified_employment').sum().alias('employment'),
        ]
        if 'entry_month' in schema:
            agg_exprs.append(
                pl.when(pl.col('entry_month') == pl.col('ref_date'))
                .then(pl.col('qualified_employment'))
                .otherwise(0)
                .sum()
                .alias('hires'),
            )
        else:
            agg_exprs.append(pl.lit(0).alias('hires'))
        if 'exit_month' in schema:
            agg_exprs.append(
                pl.when(pl.col('exit_month') == pl.col('ref_date'))
                .then(pl.col('qualified_employment'))
                .otherwise(0)
                .sum()
                .alias('separations'),
            )
        else:
            agg_exprs.append(pl.lit(0).alias('separations'))
        agg = payroll.group_by(by).agg(agg_exprs)

    agg = agg.sort(by)

    # Continuing employment = employment - hires (approximation)
    agg = agg.with_columns(
        (pl.col('employment') - pl.col('hires')).alias('continuing_employment'),
    )

    # Rates as share of employment
    agg = agg.with_columns(
        (pl.col('hires') / pl.col('employment').fill_null(1)).alias('hire_rate'),
        (pl.col('separations') / pl.col('employment').fill_null(1)).alias('separation_rate'),
        ((pl.col('hires') + pl.col('separations')) / pl.col('employment').fill_null(1)).alias('churn_rate'),
        ((pl.col('hires') - pl.col('separations')) / pl.col('employment').fill_null(1)).alias('net_growth_rate'),
    )

    return agg


def _compute_flows_from_employees(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute flows from employee-level records using month-over-month presence."""
    df = payroll.collect()
    dates = sorted(df['ref_date'].unique().to_list())
    by = ['ref_date'] + grouping_cols

    rows = []
    for i, date in enumerate(dates):
        curr = df.filter(pl.col('ref_date') == date)
        curr_employees = set(curr['employee_id'].to_list())

        if i > 0:
            prev = df.filter(pl.col('ref_date') == dates[i - 1])
            prev_employees = set(prev['employee_id'].to_list())
            new_hires = curr_employees - prev_employees
            separations = prev_employees - curr_employees
            continuing = curr_employees & prev_employees
        else:
            new_hires = curr_employees
            separations = set()
            continuing = set()

        employment = len(curr_employees)
        rows.append({
            'ref_date': date,
            'hires': len(new_hires),
            'separations': len(separations),
            'continuing_employment': len(continuing),
            'employment': employment,
        })

    result = pl.DataFrame(rows)
    result = result.with_columns(
        (pl.col('hires') / pl.col('employment').fill_null(1)).alias('hire_rate'),
        (pl.col('separations') / pl.col('employment').fill_null(1)).alias('separation_rate'),
        ((pl.col('hires') + pl.col('separations')) / pl.col('employment').fill_null(1)).alias('churn_rate'),
        ((pl.col('hires') - pl.col('separations')) / pl.col('employment').fill_null(1)).alias('net_growth_rate'),
    )
    return result.lazy()


def compute_job_flows_by_geography(
    payroll: pl.LazyFrame,
    geo_col: str,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute job flows stratified by geography (state_fips, Census region, etc.)."""
    return compute_job_flows(payroll, [geo_col] + grouping_cols)


def compute_job_flows_by_industry(
    payroll: pl.LazyFrame,
    industry_col: str,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute job flows stratified by industry (supersector, naics2, etc.)."""
    return compute_job_flows(payroll, [industry_col] + grouping_cols)


def compute_job_flows_by_size(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute job flows stratified by size class."""
    return compute_job_flows(payroll, ['size_class'] + grouping_cols)
