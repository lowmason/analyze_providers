"""Raking / iterative proportional fitting to match QCEW margins."""

import polars as pl


def rake_to_qcew(
    payroll: pl.LazyFrame,
    qcew: pl.LazyFrame,
    dimensions: list[str],
    max_iter: int = 100,
    tolerance: float = 1e-6,
) -> pl.LazyFrame:
    """Iterative proportional fitting so payroll margins match QCEW on the given dimensions.

    Adds a rake_weight column to payroll. Downstream growth analysis can use this as weight_col.
    """
    # Raking: we need payroll at client-month level with dimension columns, and qcew totals by dimension x period.
    # Initialize weight = 1 for each row.
    payroll = payroll.with_columns(pl.lit(1.0).alias('rake_weight'))
    # Collect for iteration (raking is iterative)
    pay = payroll.collect()
    # Get QCEW margin totals: e.g. by ref_date, quarter, supersector -> qcew_employment
    q = qcew.collect()
    join_cols = ['ref_date', 'quarter'] + dimensions
    qcew_totals = q.group_by(join_cols).agg(pl.col('qcew_employment').sum().alias('qcew_total'))
    if 'qcew_employment' not in q.columns and 'month1_emplvl' in q.columns:
        qcew_totals = q.group_by(join_cols).agg(pl.col('month1_emplvl').sum().alias('qcew_total'))
    # Payroll totals by same keys
    pay_agg = pay.group_by(join_cols).agg(
        (pl.col('qualified_employment') * pl.col('rake_weight')).sum().alias('payroll_total'),
    )
    merged = pay_agg.join(qcew_totals, on=join_cols, how='left')
    merged = merged.with_columns(
        (pl.col('qcew_total') / pl.col('payroll_total').fill_null(1)).alias('margin_ratio'),
    )
    # Apply margin ratio to weights: for each row, multiply rake_weight by margin_ratio of its cell
    pay = pay.join(
        merged.select(join_cols + ['margin_ratio']),
        on=join_cols,
        how='left',
    )
    pay = pay.with_columns((pl.col('rake_weight') * pl.col('margin_ratio').fill_null(1)).alias('rake_weight'))
    # Single iteration for simplicity; full IPF would loop over dimensions until convergence
    return pay.lazy()
