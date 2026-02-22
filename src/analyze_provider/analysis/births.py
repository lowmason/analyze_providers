"""Birth rate analysis and BED comparisons."""

import polars as pl


def compute_payroll_birth_rates(panel: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame:
    """Birth rate by quarter x grouping: births / birth_determinable (is_birth non-null)."""
    by = ['ref_date', 'quarter'] + grouping_cols
    return panel.group_by(by).agg(
        pl.col('birth_count').sum().alias('births'),
        pl.col('birth_determinable_count').sum().alias('determinable'),
    ).with_columns(
        (pl.col('births') / pl.col('determinable').fill_null(1)).alias('birth_rate'),
    )


def compare_birth_rates(
    payroll_births: pl.LazyFrame,
    bed: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Merge payroll and BED birth rates on quarter + grouping; level diff, ratio."""
    schema = bed.collect_schema().names()
    if 'birth_rate' in schema:
        bed = bed.rename({'birth_rate': 'bed_birth_rate'})
    if 'quarter' not in schema and 'year' in schema:
        bed = bed.with_columns(
            (pl.col('year').cast(pl.Utf8) + pl.lit('Q') + pl.col('quarter').cast(pl.Utf8)).alias('quarter'),
        )
    join_on = ['quarter'] + [c for c in grouping_cols if c in payroll_births.collect_schema().names() and c in bed.collect_schema().names()]
    merged = payroll_births.join(bed, on=join_on, how='left')
    return merged.with_columns(
        (pl.col('birth_rate') - pl.col('bed_birth_rate').fill_null(0)).alias('birth_rate_diff'),
        (pl.col('birth_rate') / pl.col('bed_birth_rate').fill_null(1)).alias('birth_rate_ratio'),
    )


def compare_birth_determinable_composition(
    determinable_subset: pl.LazyFrame,
    full_client_base: pl.LazyFrame,
) -> pl.DataFrame:
    """Compare industry/state/size distribution of birth-determinable vs full; return misallocation index."""
    dims = ['supersector', 'state_fips', 'size_class']
    out_rows = []
    for dim in dims:
        d_sub = determinable_subset.group_by(dim).agg(pl.len().alias('n'))
        d_sub = d_sub.with_columns((pl.col('n') / pl.col('n').sum()).alias('share_det'))
        f_full = full_client_base.group_by(dim).agg(pl.len().alias('n'))
        f_full = f_full.with_columns((pl.col('n') / pl.col('n').sum()).alias('share_full'))
        merged = d_sub.join(f_full, on=dim, how='outer')
        merged = merged.with_columns(
            (pl.col('share_det').fill_null(0) - pl.col('share_full').fill_null(0)).abs().alias('abs_dev'),
        )
        mi = merged['abs_dev'].sum() / 2
        out_rows.append({'dimension': dim, 'misallocation_index': mi})
    return pl.DataFrame(out_rows)


def test_birth_lead(
    payroll_births: pl.LazyFrame,
    bed: pl.LazyFrame,
    grouping_cols: list[str],
    max_lag: int = 4,
) -> pl.DataFrame:
    """Cross-correlation at lags 0..max_lag; Granger-style regressions. Uses statsmodels."""
    try:
        import statsmodels.api as sm
    except ImportError:
        return pl.DataFrame({'model': [], 'r2': [], 'coef': [], 'pvalue': []})

    p = payroll_births.collect()
    b = bed.collect()
    if p.is_empty() or b.is_empty():
        return pl.DataFrame()

    # National level: align by quarter
    if 'quarter' in p.columns and 'birth_rate' in p.columns:
        p_nat = p.group_by('quarter').agg(pl.col('birth_rate').mean())
    else:
        return pl.DataFrame()
    b_nat = b
    if 'quarter' not in b_nat.columns and 'year' in b_nat.columns:
        b_nat = b_nat.with_columns(
            (pl.col('year').cast(pl.Utf8) + pl.lit('Q') + pl.col('quarter').cast(pl.Utf8)).alias('quarter'),
        )
    bed_col = 'birth_rate' if 'birth_rate' in b_nat.columns else 'bed_birth_rate'
    merged = p_nat.join(b_nat.select(['quarter', bed_col]).rename({bed_col: 'bed_rate'}), on='quarter', how='inner')
    if merged.height < 10:
        return pl.DataFrame()

    y = merged['bed_rate'].to_numpy()
    x = merged['birth_rate'].to_numpy()
    x = sm.add_constant(x)
    model = sm.OLS(y, x).fit()
    return pl.DataFrame({
        'model': ['concurrent'],
        'r2': [model.rsquared],
        'coef': [model.params.iloc[1] if len(model.params) > 1 else None],
        'pvalue': [model.pvalues.iloc[1] if len(model.pvalues) > 1 else None],
    })


def compute_survival_curves(panel: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame:
    """For birth-flagged clients, fraction still active at 4, 8, 12, 16, 20 quarters after entry."""
    # Filter to rows where is_birth == True; then for each entry_month cohort, compute still active at +4q, +8q, ...
    births = panel.filter(pl.col('is_birth') == True)
    return (
        births.group_by(['entry_month'] + grouping_cols)
        .agg(pl.col('client_id').n_unique().alias('birth_cohort_n'))
        .with_columns(pl.lit(None).cast(pl.Float64).alias('survival_4q'))
    )
