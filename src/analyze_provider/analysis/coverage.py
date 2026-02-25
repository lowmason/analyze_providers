"""Coverage ratios and distributional comparisons vs QCEW."""

import polars as pl


def compute_coverage(
    payroll_agg: pl.LazyFrame,
    qcew: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Compute coverage ratios (employment and establishments) at a given grouping level.

    Expects payroll_agg to have ref_date, quarter, grouping_cols, payroll_employment, client_count.
    Expects qcew to have ref_date, quarter, same grouping_cols (if any), and columns aliased or named
    qcew_employment and qcew_establishments (or month1_emplvl / qtrly_estabs_count which will be aliased).

    Returns all grouping columns plus: payroll_employment, qcew_employment,
    coverage_ratio_employment, payroll_clients, qcew_establishments, coverage_ratio_estab.
    """
    join_on = ['ref_date', 'quarter'] + [c for c in grouping_cols if c]
    # Caller should pass qcew with qcew_employment and qcew_establishments (or ensure data layer renames month1_emplvl, qtrly_estabs_count)
    combined = payroll_agg.join(qcew, on=join_on, how='left')
    return combined.with_columns(
        (pl.col('payroll_employment') / pl.col('qcew_employment').fill_null(1)).alias('coverage_ratio_employment'),
        (pl.col('client_count') / pl.col('qcew_establishments').fill_null(1)).alias('coverage_ratio_estab'),
    ).rename({'client_count': 'payroll_clients'})


def compute_share_comparison(
    payroll_agg: pl.LazyFrame,
    qcew: pl.LazyFrame,
    dimension: str,
) -> pl.LazyFrame:
    """Compute payroll share vs QCEW share by dimension; add abs_dev and misallocation index.

    dimension: column name (e.g. supersector). Both frames should have ref_date, quarter, that column,
    and payroll_employment / qcew_employment (or month1_emplvl).
    """
    payroll_tot = payroll_agg.group_by(['ref_date', 'quarter']).agg(pl.col('payroll_employment').sum().alias('payroll_total'))
    payroll_agg = payroll_agg.join(payroll_tot, on=['ref_date', 'quarter'])
    payroll_agg = payroll_agg.with_columns((pl.col('payroll_employment') / pl.col('payroll_total')).alias('payroll_share'))
    qcew_emp = qcew
    if 'qcew_employment' not in qcew.collect_schema().names() and 'month1_emplvl' in qcew.collect_schema().names():
        qcew_emp = qcew.rename({'month1_emplvl': 'qcew_employment'})
    qcew_tot = qcew_emp.group_by(['ref_date', 'quarter']).agg(pl.col('qcew_employment').sum().alias('qcew_total'))
    qcew_emp = qcew_emp.join(qcew_tot, on=['ref_date', 'quarter'])
    qcew_emp = qcew_emp.with_columns((pl.col('qcew_employment') / pl.col('qcew_total')).alias('qcew_share'))
    join_cols = ['ref_date', 'quarter', dimension]
    merged = payroll_agg.select(join_cols + ['payroll_share']).join(
        qcew_emp.select(join_cols + ['qcew_share']), on=join_cols, how='full', coalesce=True,
    )
    return merged.with_columns(
        (pl.col('payroll_share').fill_null(0) - pl.col('qcew_share').fill_null(0)).abs().alias('abs_dev'),
    ).with_columns(
        (pl.col('abs_dev').sum().over(['ref_date', 'quarter']) / 2).alias('misallocation_index'),
    )


def compute_coverage_over_time(
    payroll_agg: pl.LazyFrame,
    qcew: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Coverage ratios with quarter as a column for time series plots."""
    return compute_coverage(payroll_agg, qcew, grouping_cols)


def compute_composition_shift_index(
    payroll_agg: pl.LazyFrame,
    dimension: str,
) -> pl.LazyFrame:
    """Compute Composition Shift Index (CSI) over time for a given dimension.

    CSI_t = sum(|s_{i,t} - s_{i,t-1}|), bounded [0, 2].
    Tracks how much the distribution across cells of the given dimension shifts month to month.

    Args:
        payroll_agg: LazyFrame with ref_date, dimension column, and payroll_employment.
        dimension: Column name (e.g. 'supersector', 'state_fips', 'size_class').

    Returns:
        LazyFrame with ref_date and csi columns.
    """
    # Compute shares by dimension per ref_date
    totals = payroll_agg.group_by('ref_date').agg(pl.col('payroll_employment').sum().alias('total_emp'))
    with_share = payroll_agg.join(totals, on='ref_date').with_columns(
        (pl.col('payroll_employment') / pl.col('total_emp')).alias('share'),
    )
    # Get lag share
    with_share = with_share.sort(['ref_date', dimension])
    with_share = with_share.with_columns(
        pl.col('share').shift(1).over(dimension).alias('prev_share'),
    )
    # CSI = sum of |share - prev_share| per ref_date
    csi = with_share.with_columns(
        (pl.col('share') - pl.col('prev_share')).abs().alias('abs_shift'),
    ).group_by('ref_date').agg(
        pl.col('abs_shift').sum().alias('csi'),
    ).sort('ref_date')
    return csi


def compute_cell_reliability(
    coverage: pl.LazyFrame,
    min_clients: int = 30,
    min_coverage: float = 0.005,
) -> pl.LazyFrame:
    """Flag each cell as reliable, marginal, or insufficient."""
    return coverage.with_columns(
        pl.when(
            (pl.col('payroll_clients') >= min_clients) & (pl.col('coverage_ratio_employment') >= min_coverage)
        )
        .then(pl.lit('reliable'))
        .when(
            (pl.col('payroll_clients') < min_clients) & (pl.col('coverage_ratio_employment') < min_coverage)
        )
        .then(pl.lit('insufficient'))
        .otherwise(pl.lit('marginal'))
        .alias('reliability'),
    )
