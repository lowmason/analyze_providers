"""Growth rate comparisons and decompositions vs CES."""

import polars as pl
import numpy as np


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
    if by:
        agg = agg.with_columns(
            pl.col('employment').pct_change().over(by).alias('mom_growth'),
            pl.col('employment').pct_change(12).over(by).alias('yoy_growth'),
        )
    else:
        agg = agg.with_columns(
            pl.col('employment').pct_change().alias('mom_growth'),
            pl.col('employment').pct_change(12).alias('yoy_growth'),
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
    # Rolling correlation using pl.rolling_corr (valid polars syntax)
    merged = merged.sort('ref_date')
    merged = merged.with_columns(
        pl.rolling_corr('payroll_yoy', 'ces_yoy', window_size=12).alias('rolling_corr_12'),
    )
    return merged


def decompose_growth_divergence(
    payroll: pl.LazyFrame,
    ces: pl.LazyFrame,
    grouping_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Shift-share decomposition: composition effect vs within-cell effect.

    For each cell (defined by grouping_cols), compute:
      composition_effect = sum((w_p - w_o) * g_o)  (weight difference * official growth)
      within_cell_effect = sum(w_p * (g_p - g_o))  (payroll weight * growth difference)
      total_divergence = composition_effect + within_cell_effect

    Returns eager DataFrame with quarter, total_divergence, composition_effect, within_cell_effect.
    """
    if grouping_cols is None:
        grouping_cols = []

    payroll_df = payroll.collect()
    ces_df = ces.collect()

    # If no grouping cols or data is flat, fall back to simple divergence
    if not grouping_cols or 'supersector' not in payroll_df.columns:
        emp_col = 'payroll_employment' if 'payroll_employment' in payroll_df.columns else 'employment'
        o_emp_col = 'employment' if 'employment' in ces_df.columns else emp_col
        payroll_agg = payroll_df.group_by('quarter').agg(pl.col(emp_col).sum()) if emp_col in payroll_df.columns else payroll_df
        ces_agg = ces_df.group_by('quarter').agg(pl.col(o_emp_col).sum().alias('employment')) if o_emp_col in ces_df.columns else ces_df
        if payroll_agg.is_empty() or ces_agg.is_empty():
            return pl.DataFrame({'quarter': [], 'total_divergence': [], 'composition_effect': [], 'within_cell_effect': []})
        merged = payroll_agg.join(ces_agg, on='quarter', how='inner')
        if emp_col in merged.columns and 'employment' in merged.columns:
            merged = merged.sort('quarter').with_columns(
                (pl.col(emp_col).pct_change() - pl.col('employment').pct_change()).alias('total_divergence'),
            ).with_columns(
                pl.lit(0.0).alias('composition_effect'),
                pl.col('total_divergence').alias('within_cell_effect'),
            )
            return merged.select(['quarter', 'total_divergence', 'composition_effect', 'within_cell_effect'])
        return pl.DataFrame({'quarter': [], 'total_divergence': [], 'composition_effect': [], 'within_cell_effect': []})

    # Full shift-share with cells
    dim = grouping_cols[0]
    emp_col = 'payroll_employment' if 'payroll_employment' in payroll_df.columns else 'employment'

    # Payroll cell-level: quarter x dim
    p_cells = payroll_df.group_by(['quarter', dim]).agg(pl.col(emp_col).sum().alias('p_emp'))
    p_total = p_cells.group_by('quarter').agg(pl.col('p_emp').sum().alias('p_total'))
    p_cells = p_cells.join(p_total, on='quarter')
    p_cells = p_cells.with_columns((pl.col('p_emp') / pl.col('p_total')).alias('w_p'))
    p_cells = p_cells.sort(['quarter', dim]).with_columns(
        pl.col('p_emp').pct_change().over(dim).alias('g_p'),
    )

    # Official cell-level
    o_emp_col = 'employment' if 'employment' in ces_df.columns else emp_col
    if dim not in ces_df.columns:
        payroll_agg = payroll_df.group_by('quarter').agg(pl.col(emp_col).sum())
        ces_agg = ces_df.group_by('quarter').agg(pl.col(o_emp_col).sum().alias('employment'))
        merged = payroll_agg.join(ces_agg, on='quarter', how='inner').sort('quarter')
        merged = merged.with_columns(
            (pl.col(emp_col).pct_change() - pl.col('employment').pct_change()).alias('total_divergence'),
        ).with_columns(
            pl.lit(0.0).alias('composition_effect'),
            pl.col('total_divergence').alias('within_cell_effect'),
        )
        return merged.select(['quarter', 'total_divergence', 'composition_effect', 'within_cell_effect'])

    o_cells = ces_df.group_by(['quarter', dim]).agg(pl.col(o_emp_col).sum().alias('o_emp'))
    o_total = o_cells.group_by('quarter').agg(pl.col('o_emp').sum().alias('o_total'))
    o_cells = o_cells.join(o_total, on='quarter')
    o_cells = o_cells.with_columns((pl.col('o_emp') / pl.col('o_total')).alias('w_o'))
    o_cells = o_cells.sort(['quarter', dim]).with_columns(
        pl.col('o_emp').pct_change().over(dim).alias('g_o'),
    )

    # Merge payroll and official cells
    merged = p_cells.join(o_cells, on=['quarter', dim], how='inner')

    # Shift-share components per cell
    merged = merged.with_columns(
        ((pl.col('w_p') - pl.col('w_o')) * pl.col('g_o')).alias('comp_cell'),
        (pl.col('w_p') * (pl.col('g_p') - pl.col('g_o'))).alias('within_cell'),
    )

    # Aggregate to quarter level
    result = merged.group_by('quarter').agg(
        pl.col('comp_cell').sum().alias('composition_effect'),
        pl.col('within_cell').sum().alias('within_cell_effect'),
    ).with_columns(
        (pl.col('composition_effect') + pl.col('within_cell_effect')).alias('total_divergence'),
    ).sort('quarter')

    return result.select(['quarter', 'total_divergence', 'composition_effect', 'within_cell_effect'])


def decompose_employment_change(
    payroll: pl.LazyFrame,
    grouping_cols: list[str],
) -> pl.DataFrame:
    """Decompose monthly employment change into intensive (within-client) and extensive (entry/exit) margins.

    For each period:
      - within_change: employment change at continuing clients (present in both t and t-1)
      - entry_contribution: employment added by entering clients
      - exit_contribution: employment lost by exiting clients
      - total_change = within_change + entry_contribution - exit_contribution

    Returns eager DataFrame.
    """
    df = payroll.collect()
    if df.is_empty() or 'ref_date' not in df.columns:
        return pl.DataFrame({
            'ref_date': [], 'total_change': [], 'within_change': [],
            'entry_contribution': [], 'exit_contribution': [],
        })

    dates = sorted(df['ref_date'].unique().to_list())
    rows = []

    for i in range(1, len(dates)):
        prev_date = dates[i - 1]
        curr_date = dates[i]

        prev = df.filter(pl.col('ref_date') == prev_date)
        curr = df.filter(pl.col('ref_date') == curr_date)

        prev_clients = set(prev['client_id'].to_list())
        curr_clients = set(curr['client_id'].to_list())

        continuing = prev_clients & curr_clients
        entering = curr_clients - prev_clients
        exiting = prev_clients - curr_clients

        # Within-client change (continuing clients)
        if continuing:
            cont_list = list(continuing)
            prev_cont = prev.filter(pl.col('client_id').is_in(cont_list))['qualified_employment'].sum()
            curr_cont = curr.filter(pl.col('client_id').is_in(cont_list))['qualified_employment'].sum()
            within_change = curr_cont - prev_cont
        else:
            within_change = 0

        # Entry contribution
        if entering:
            entry_list = list(entering)
            entry_contribution = curr.filter(pl.col('client_id').is_in(entry_list))['qualified_employment'].sum()
        else:
            entry_contribution = 0

        # Exit contribution
        if exiting:
            exit_list = list(exiting)
            exit_contribution = prev.filter(pl.col('client_id').is_in(exit_list))['qualified_employment'].sum()
        else:
            exit_contribution = 0

        total_change = within_change + entry_contribution - exit_contribution
        rows.append({
            'ref_date': curr_date,
            'total_change': total_change,
            'within_change': within_change,
            'entry_contribution': entry_contribution,
            'exit_contribution': exit_contribution,
        })

    return pl.DataFrame(rows)


def analyze_turning_points(
    payroll_growth: pl.LazyFrame,
    official_growth: pl.LazyFrame,
) -> pl.DataFrame:
    """Identify months where growth changes sign; compute lead/lag between payroll and official.

    Matches each payroll turning point to its nearest official turning point and reports
    per-event lead/lag in months.
    """
    p = payroll_growth.collect().sort('ref_date')
    o = official_growth.collect().sort('ref_date')
    if 'ref_date' not in p.columns or 'yoy_growth' not in p.columns:
        return pl.DataFrame()
    if 'ref_date' not in o.columns or 'yoy_growth' not in o.columns:
        return pl.DataFrame()

    p = p.with_columns(pl.col('yoy_growth').sign().alias('payroll_sign'))
    o = o.with_columns(pl.col('yoy_growth').sign().alias('official_sign'))
    p = p.with_columns(pl.col('payroll_sign').diff().alias('payroll_turn'))
    o = o.with_columns(pl.col('official_sign').diff().alias('official_turn'))

    # Find turning point dates (sign changes of magnitude 2)
    p_turns = p.filter(pl.col('payroll_turn').abs() == 2)['ref_date'].to_list()
    o_turns = o.filter(pl.col('official_turn').abs() == 2)['ref_date'].to_list()

    if not p_turns or not o_turns:
        return pl.DataFrame({'payroll_turn_date': [], 'official_turn_date': [], 'lead_lag_months': [], 'median_lead_lag': [], 'mean_lead_lag': []})

    # Match each payroll turning point to its nearest official turning point
    rows = []
    for pt in p_turns:
        best_ot = min(o_turns, key=lambda ot: abs((pt - ot).days))
        lead_lag_months = (pt - best_ot).days / 30.0
        rows.append({
            'payroll_turn_date': pt,
            'official_turn_date': best_ot,
            'lead_lag_months': round(lead_lag_months, 1),
        })

    result = pl.DataFrame(rows)
    leads = result['lead_lag_months']
    result = result.with_columns(
        pl.lit(float(leads.median())).alias('median_lead_lag'),
        pl.lit(float(leads.mean())).alias('mean_lead_lag'),
    )
    return result
