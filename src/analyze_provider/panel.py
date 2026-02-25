"""Build the client-month panel with derived fields and multiple aggregation levels."""

import polars as pl

# Aggregation levels: grouping_level value and the list of grouping columns (excluding ref_date/quarter).
AGGREGATION_LEVELS: list[tuple[str, list[str]]] = [
    ('national', []),
    ('supersector', ['supersector']),
    ('state', ['state_fips']),
    ('size_class', ['size_class']),
    ('supersector_state', ['supersector', 'state_fips']),
    ('supersector_size_class', ['supersector', 'size_class']),
]


def build_panel(payroll: pl.LazyFrame) -> pl.LazyFrame:
    """Build the master client-month panel with grouping_level and all aggregation levels.

    For each ref_date we compute total qualified employment, client count, employment by
    supersector/state/size and cross-tabs, birth counts, birth-determinable count, entry/exit counts,
    and continuing_employment (employment at clients present in both current and prior month).
    Returns a single LazyFrame with a grouping_level column and the appropriate grouping keys
    so downstream can filter by level and join to official data.

    Args:
        payroll: LazyFrame from load_payroll (with ref_date, supersector, state_fips, size_class,
                 qualified_employment, is_birth, client_id, entry_month, exit_month, quarter).

    Returns:
        LazyFrame with columns: grouping_level, ref_date, quarter, and for each level the grouping
        keys (supersector, state_fips, size_class as applicable), then payroll_employment, client_count,
        birth_count, birth_determinable_count, entry_count, exit_count, continuing_employment.
    """
    frames: list[pl.LazyFrame] = []

    all_key_cols = ['supersector', 'state_fips', 'size_class']

    for level_name, group_cols in AGGREGATION_LEVELS:
        agg_exprs = [
            pl.col('qualified_employment').sum().alias('payroll_employment'),
            pl.col('client_id').filter(pl.col('qualified_employment') > 0).n_unique().alias('client_count'),
            pl.col('is_birth').filter(pl.col('is_birth').eq(True)).count().alias('birth_count'),
            pl.col('is_birth').filter(pl.col('is_birth').is_not_null()).count().alias('birth_determinable_count'),
            pl.when(pl.col('entry_month') == pl.col('ref_date')).then(1).otherwise(0).sum().alias('entry_count'),
            pl.when(pl.col('exit_month') == pl.col('ref_date')).then(1).otherwise(0).sum().alias('exit_count'),
            # Continuing employment: employment at clients NOT entering this month and NOT exiting this month
            pl.col('qualified_employment').filter(
                (pl.col('entry_month') != pl.col('ref_date')) & (pl.col('exit_month').is_null() | (pl.col('exit_month') != pl.col('ref_date')))
            ).sum().alias('continuing_employment'),
        ]
        by = ['ref_date', 'quarter'] + group_cols
        g = payroll.group_by(by).agg(agg_exprs)
        g = g.with_columns(pl.lit(level_name).alias('grouping_level'))
        for k in all_key_cols:
            if k not in group_cols:
                g = g.with_columns(pl.lit(None).cast(pl.Utf8).alias(k))
        select_cols = ['grouping_level', 'ref_date', 'quarter'] + all_key_cols + [
            'payroll_employment', 'client_count', 'birth_count', 'birth_determinable_count',
            'entry_count', 'exit_count', 'continuing_employment',
        ]
        g = g.select(select_cols)
        frames.append(g)

    return pl.concat(frames, how='vertical_relaxed')


def filter_stable_panel(
    payroll: pl.LazyFrame,
    min_tenure_months: int = 12,
) -> pl.LazyFrame:
    """Filter payroll to clients with at least min_tenure_months of tenure.

    Restricts to long-tenure clients for computing employment dynamics without
    entry/exit contamination.

    Args:
        payroll: LazyFrame with client_id, ref_date.
        min_tenure_months: Minimum number of months a client must be observed.

    Returns:
        Filtered LazyFrame containing only clients meeting the tenure threshold.
    """
    # Compute tenure per client
    client_tenure = payroll.group_by('client_id').agg(
        pl.col('ref_date').min().alias('first_seen'),
        pl.col('ref_date').max().alias('last_seen'),
    ).with_columns(
        ((pl.col('last_seen') - pl.col('first_seen')).dt.total_days() / 30.0).alias('tenure_months'),
    ).filter(
        pl.col('tenure_months') >= min_tenure_months,
    ).select('client_id')

    return payroll.join(client_tenure, on='client_id', how='inner')
