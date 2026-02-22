"""Growth rate comparisons and decompositions vs CES."""

import polars as pl


def compute_growth_rates(
    df: pl.LazyFrame,
    employment_col: str,
    grouping_cols: list[str],
    weight_col: str | None = None,
) -> pl.LazyFrame:
    """Compute month-over-month and year-over-year growth rates.

    If weight_col is provided, employment is weighted (e.g. for reweighted payroll).
    """
    by = grouping_cols
    sort_cols = ['ref_date'] + grouping_cols
    emp = pl.col(employment_col)
    if weight_col:
        emp = emp * pl.col(weight_col)
    agg = df.group_by(['ref_date'] + by).agg(emp.sum().alias('employment'))
    agg = agg.sort(sort_cols)
    agg = agg.with_columns(
        pl.col('employment').pct_change().over(by).alias('mom_growth'),
        pl.col('employment').pct_change(12).over(by).alias('yoy_growth'),
    )
    return agg


def compare_growth(
    payroll_growth: pl.LazyFrame,
    ces_growth: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.LazyFrame:
    """Merge payroll and CES growth on ref_date + grouping_cols; compute difference and rolling correlation."""
    payroll_growth = payroll_growth.rename({'yoy_growth': 'payroll_yoy'})
    ces_growth = ces_growth.rename({'yoy_growth': 'ces_yoy'})
    join_on = ['ref_date'] + [c for c in grouping_cols if c in payroll_growth.collect_schema().names() and c in ces_growth.collect_schema().names()]
    merged = payroll_growth.join(ces_growth, on=join_on, how='inner')
    merged = merged.with_columns(
        (pl.col('payroll_yoy') - pl.col('ces_yoy')).alias('growth_diff'),
        (pl.col('payroll_yoy') - pl.col('ces_yoy')).abs().alias('abs_diff'),
    )
    if grouping_cols:
        merged = merged.with_columns(
            pl.col('growth_diff').rolling(12).over(grouping_cols).corr(pl.col('ces_yoy').rolling(12).over(grouping_cols)).alias('rolling_corr_12'),
        )
    else:
        merged = merged.with_columns(
            pl.col('growth_diff').rolling(12).corr(pl.col('ces_yoy')).alias('rolling_corr_12'),
        )
    return merged


def decompose_growth_divergence(payroll: pl.LazyFrame, ces: pl.LazyFrame) -> pl.DataFrame:
    """Shift-share decomposition: composition effect vs within-cell effect. Returns eager DataFrame."""
    # Simplified: total divergence by quarter; composition = 0 if no cross-cell; within = divergence.
    payroll_agg = payroll.group_by('quarter').agg(pl.col('payroll_employment').sum()).collect()
    ces_agg = ces.group_by('quarter').agg(pl.col('employment').sum()).collect()
    merged = payroll_agg.join(ces_agg, on='quarter')
    merged = merged.with_columns(
        (pl.col('payroll_employment').pct_change() - pl.col('employment').pct_change()).alias('total_divergence'),
    )
    merged = merged.with_columns(
        pl.lit(0.0).alias('composition_effect'),
        pl.col('total_divergence').alias('within_cell_effect'),
    )
    return merged.select(['quarter', 'total_divergence', 'composition_effect', 'within_cell_effect'])


def analyze_turning_points(
    payroll_growth: pl.LazyFrame,
    official_growth: pl.LazyFrame,
) -> pl.DataFrame:
    """Identify months where growth changes sign; compute lead/lag between payroll and official."""
    p = payroll_growth.collect()
    o = official_growth.collect()
    if 'ref_date' not in p.columns or 'yoy_growth' not in p.columns:
        return pl.DataFrame()
    p = p.with_columns(pl.col('yoy_growth').sign().alias('payroll_sign'))
    o = o.with_columns(pl.col('yoy_growth').sign().alias('official_sign'))
    p = p.with_columns(pl.col('payroll_sign').diff().alias('payroll_turn'))
    o = o.with_columns(pl.col('official_sign').diff().alias('official_turn'))
    # Find turning point dates
    p_turns = p.filter(pl.col('payroll_turn').abs() == 2)['ref_date'].to_list()
    o_turns = o.filter(pl.col('official_turn').abs() == 2)['ref_date'].to_list()
    if not p_turns or not o_turns:
        return pl.DataFrame({'median_lead_lag': [None], 'mean_lead_lag': [None]})
    # Simple: median/mean of (payroll_turn_date - official_turn_date) in months
    leads = []
    for pt in p_turns:
        for ot in o_turns:
            leads.append((pt - ot).days // 30 if hasattr(pt, 'days') else 0)
    return pl.DataFrame({
        'median_lead_lag': [float(pl.Series(leads).median())],
        'mean_lead_lag': [float(pl.Series(leads).mean())],
    })
