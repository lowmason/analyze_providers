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

    Loops over each dimension, adjusting weights to match that dimension's QCEW marginals,
    repeating until max_iter or tolerance is met. Supports multi-dimensional raking
    (e.g. supersector x state x size class simultaneously).

    Adds a rake_weight column to payroll. Downstream growth analysis can use this as weight_col.
    """
    payroll = payroll.with_columns(pl.lit(1.0).alias('rake_weight'))
    pay = payroll.collect()
    q = qcew.collect()

    # Determine QCEW employment column
    qcew_emp_col = 'qcew_employment'
    if qcew_emp_col not in q.columns and 'month1_emplvl' in q.columns:
        qcew_emp_col = 'month1_emplvl'

    if q.is_empty() or qcew_emp_col not in q.columns:
        return pay.lazy()

    for iteration in range(max_iter):
        max_change = 0.0

        for dim in dimensions:
            if dim not in pay.columns or dim not in q.columns:
                continue

            join_cols = [c for c in ['ref_date', 'quarter', dim] if c in pay.columns and c in q.columns]
            if dim not in join_cols:
                join_cols = [dim]

            # QCEW marginal totals for this dimension
            qcew_totals = q.group_by(join_cols).agg(
                pl.col(qcew_emp_col).sum().alias('qcew_total'),
            )

            # Payroll weighted totals for this dimension
            pay_totals = pay.group_by(join_cols).agg(
                (pl.col('qualified_employment') * pl.col('rake_weight')).sum().alias('payroll_total'),
            )

            # Compute margin ratio
            merged = pay_totals.join(qcew_totals, on=join_cols, how='left')
            merged = merged.with_columns(
                (pl.col('qcew_total') / pl.col('payroll_total').fill_null(1)).fill_null(1).alias('margin_ratio'),
            )

            # Apply margin ratio to weights
            old_weights = pay['rake_weight'].clone()
            pay = pay.join(
                merged.select(join_cols + ['margin_ratio']),
                on=join_cols,
                how='left',
            )
            pay = pay.with_columns(
                (pl.col('rake_weight') * pl.col('margin_ratio').fill_null(1)).alias('rake_weight'),
            ).drop('margin_ratio')

            # Track convergence
            weight_change = (pay['rake_weight'] - old_weights).abs().max()
            if weight_change is not None:
                max_change = max(max_change, weight_change)

        if max_change < tolerance:
            break

    return pay.lazy()
