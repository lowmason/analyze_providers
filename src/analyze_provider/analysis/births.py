"""Birth rate analysis and BED comparisons."""

import polars as pl
import numpy as np


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
    """Compare industry/state/size distribution of birth-determinable vs full; return misallocation index.

    Uses lazy evaluation consistently throughout.
    """
    dims = ['supersector', 'state_fips', 'size_class']
    out_rows = []
    for dim in dims:
        d_sub = determinable_subset.group_by(dim).agg(pl.len().alias('n')).with_columns(
            (pl.col('n') / pl.col('n').sum()).alias('share_det'),
        )
        f_full = full_client_base.group_by(dim).agg(pl.len().alias('n')).with_columns(
            (pl.col('n') / pl.col('n').sum()).alias('share_full'),
        )
        merged = d_sub.join(f_full, on=dim, how='full', suffix='_full').with_columns(
            (pl.col('share_det').fill_null(0) - pl.col('share_full').fill_null(0)).abs().alias('abs_dev'),
        ).collect()
        mi = merged['abs_dev'].sum() / 2
        out_rows.append({'dimension': dim, 'misallocation_index': mi})
    return pl.DataFrame(out_rows)


def test_birth_lead(
    payroll_births: pl.LazyFrame,
    bed: pl.LazyFrame,
    grouping_cols: list[str],
    max_lag: int = 4,
) -> pl.DataFrame:
    """Cross-correlation at lags 0..max_lag; three Granger-style regressions.

    Model 1 (concurrent): BED(q) = a + b*payroll(q)
    Model 2 (leading): BED(q) = a + b1*payroll(q) + b2*payroll(q-1)
    Model 3 (incremental/Granger): BED(q) = a + b1*BED(q-1) + b2*payroll(q-1)
    """
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
    merged = p_nat.join(b_nat.select(['quarter', bed_col]).rename({bed_col: 'bed_rate'}), on='quarter', how='inner').sort('quarter')
    if merged.height < 10:
        return pl.DataFrame()

    y = merged['bed_rate'].to_numpy()
    x_payroll = merged['birth_rate'].to_numpy()

    results = []

    # Model 1: concurrent — BED(q) = a + b*payroll(q)
    x1 = sm.add_constant(x_payroll)
    m1 = sm.OLS(y, x1).fit()
    results.append({
        'model': 'concurrent',
        'r2': m1.rsquared,
        'coef': float(m1.params[1]) if len(m1.params) > 1 else None,
        'pvalue': float(m1.pvalues[1]) if len(m1.pvalues) > 1 else None,
    })

    # Model 2: leading — BED(q) = a + b1*payroll(q) + b2*payroll(q-1)
    if len(y) > 2:
        y2 = y[1:]
        x2_curr = x_payroll[1:]
        x2_lag = x_payroll[:-1]
        x2 = sm.add_constant(np.column_stack([x2_curr, x2_lag]))
        m2 = sm.OLS(y2, x2).fit()
        results.append({
            'model': 'leading',
            'r2': m2.rsquared,
            'coef': float(m2.params[2]) if len(m2.params) > 2 else None,
            'pvalue': float(m2.pvalues[2]) if len(m2.pvalues) > 2 else None,
        })

    # Model 3: incremental/Granger — BED(q) = a + b1*BED(q-1) + b2*payroll(q-1)
    if len(y) > 2:
        y3 = y[1:]
        x3_bed_lag = y[:-1]
        x3_payroll_lag = x_payroll[:-1]
        x3 = sm.add_constant(np.column_stack([x3_bed_lag, x3_payroll_lag]))
        m3 = sm.OLS(y3, x3).fit()
        results.append({
            'model': 'incremental',
            'r2': m3.rsquared,
            'coef': float(m3.params[2]) if len(m3.params) > 2 else None,
            'pvalue': float(m3.pvalues[2]) if len(m3.pvalues) > 2 else None,
        })

    return pl.DataFrame(results)


def compute_cross_correlation(
    payroll_births: pl.LazyFrame,
    bed: pl.LazyFrame,
    max_lag: int = 4,
) -> pl.DataFrame:
    """Compute cross-correlation between payroll and BED birth rates at lags 0..max_lag quarters."""
    p = payroll_births.collect()
    b = bed.collect()
    if p.is_empty() or b.is_empty():
        return pl.DataFrame({'lag': [], 'correlation': []})

    if 'quarter' in p.columns and 'birth_rate' in p.columns:
        p_nat = p.group_by('quarter').agg(pl.col('birth_rate').mean()).sort('quarter')
    else:
        return pl.DataFrame({'lag': [], 'correlation': []})

    b_nat = b
    if 'quarter' not in b_nat.columns and 'year' in b_nat.columns:
        b_nat = b_nat.with_columns(
            (pl.col('year').cast(pl.Utf8) + pl.lit('Q') + pl.col('quarter').cast(pl.Utf8)).alias('quarter'),
        )
    bed_col = 'birth_rate' if 'birth_rate' in b_nat.columns else 'bed_birth_rate'
    merged = p_nat.join(b_nat.select(['quarter', bed_col]).rename({bed_col: 'bed_rate'}), on='quarter', how='inner').sort('quarter')

    if merged.height < 4:
        return pl.DataFrame({'lag': [], 'correlation': []})

    x = merged['birth_rate'].to_numpy().astype(float)
    y = merged['bed_rate'].to_numpy().astype(float)

    rows = []
    for lag in range(max_lag + 1):
        if lag >= len(x):
            break
        if lag == 0:
            corr = np.corrcoef(x, y)[0, 1]
        else:
            corr = np.corrcoef(x[:-lag], y[lag:])[0, 1]
        rows.append({'lag': lag, 'correlation': float(corr) if not np.isnan(corr) else 0.0})
    return pl.DataFrame(rows)


def compute_survival_curves(panel: pl.LazyFrame, grouping_cols: list[str]) -> pl.LazyFrame:
    """For birth-flagged clients, fraction still active at 4, 8, 12, 16, 20 quarters after entry.

    Implements Kaplan-Meier-style survival by entry cohort.
    """
    df = panel.collect()

    # Filter to births
    if 'is_birth' in df.columns:
        births = df.filter(pl.col('is_birth') == True)
    elif 'birth_count' in df.columns:
        births = df.filter(pl.col('birth_count') > 0)
    else:
        return pl.DataFrame({
            'entry_quarter': [], 'birth_cohort_n': [],
            'survival_4q': [], 'survival_8q': [], 'survival_12q': [],
            'survival_16q': [], 'survival_20q': [],
        }).lazy()

    if births.is_empty() or 'entry_month' not in df.columns:
        return pl.DataFrame({
            'entry_quarter': [], 'birth_cohort_n': [],
            'survival_4q': [], 'survival_8q': [], 'survival_12q': [],
            'survival_16q': [], 'survival_20q': [],
        }).lazy()

    # Get unique birth clients with entry dates
    birth_clients = births.select(['client_id', 'entry_month']).unique()
    birth_clients = birth_clients.with_columns(
        (pl.col('entry_month').dt.year().cast(pl.Utf8) + pl.lit('Q') + pl.col('entry_month').dt.quarter().cast(pl.Utf8)).alias('entry_quarter'),
    )

    # For each client, find last observed date
    last_seen = df.group_by('client_id').agg(pl.col('ref_date').max().alias('last_seen'))
    birth_clients = birth_clients.join(last_seen, on='client_id', how='left')

    # Compute tenure in months
    birth_clients = birth_clients.with_columns(
        ((pl.col('last_seen') - pl.col('entry_month')).dt.total_days() / 30.0).alias('tenure_months'),
    )

    # Survival horizons in months (quarters * 3)
    horizons = {'survival_4q': 12, 'survival_8q': 24, 'survival_12q': 36, 'survival_16q': 48, 'survival_20q': 60}

    result = birth_clients.group_by('entry_quarter').agg(
        pl.col('client_id').n_unique().alias('birth_cohort_n'),
        *[
            (pl.col('tenure_months') >= months).mean().alias(name)
            for name, months in horizons.items()
        ],
    ).sort('entry_quarter')

    return result.lazy()
