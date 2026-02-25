"""Generate all charts and tables (matplotlib)."""

from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from analyze_provider import config


def _save(fig: plt.Figure, name: str, output_dir: Path | None = None) -> None:
    out = output_dir or (config.OUTPUT_DIR / 'exhibits')
    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / f'{name}.png', dpi=150, bbox_inches='tight')
    fig.savefig(out / f'{name}.pdf', bbox_inches='tight')
    plt.close(fig)


def coverage_heatmap_employment(coverage_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """State (rows) x supersector (columns), cell color = coverage ratio (employment)."""
    fig, ax = plt.subplots(figsize=(12, 8))
    if coverage_df.is_empty() or 'state_fips' not in coverage_df.columns or 'supersector' not in coverage_df.columns:
        ax.set_title('Coverage ratio (employment)')
        _save(fig, 'coverage_heatmap_employment', output_dir)
        return fig
    pivot = coverage_df.pivot(index='state_fips', on='supersector', values='coverage_ratio_employment', aggregate_function='first')
    row_labels = pivot['state_fips'].to_list()
    col_labels = [c for c in pivot.columns if c != 'state_fips']
    arr = pivot.select(col_labels).to_numpy()
    im = ax.imshow(arr, aspect='auto', cmap='viridis')
    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, rotation=45, ha='right')
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels)
    plt.colorbar(im, ax=ax, label='Coverage ratio')
    ax.set_title('Coverage ratio (employment)')
    _save(fig, 'coverage_heatmap_employment', output_dir)
    return fig


def coverage_heatmap_establishments(coverage_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """State x supersector, cell color = coverage ratio (establishments)."""
    fig, ax = plt.subplots(figsize=(12, 8))
    if coverage_df.is_empty() or 'coverage_ratio_estab' not in coverage_df.columns:
        ax.set_title('Coverage ratio (establishments)')
        _save(fig, 'coverage_heatmap_establishments', output_dir)
        return fig
    pivot = coverage_df.pivot(index='state_fips', on='supersector', values='coverage_ratio_estab', aggregate_function='first')
    row_labels = pivot['state_fips'].to_list()
    col_labels = [c for c in pivot.columns if c != 'state_fips']
    arr = pivot.select(col_labels).to_numpy()
    im = ax.imshow(arr, aspect='auto', cmap='viridis')
    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, rotation=45, ha='right')
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels)
    plt.colorbar(im, ax=ax, label='Coverage ratio')
    ax.set_title('Coverage ratio (establishments)')
    _save(fig, 'coverage_heatmap_establishments', output_dir)
    return fig


def industry_composition_comparison(share_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Grouped bar: payroll share vs QCEW share by supersector."""
    fig, ax = plt.subplots(figsize=(10, 6))
    if share_df.is_empty() or 'supersector' not in share_df.columns:
        ax.set_title('Industry composition: payroll vs QCEW')
        _save(fig, 'industry_composition_comparison', output_dir)
        return fig
    agg = share_df.group_by('supersector').agg(
        pl.col('payroll_share').mean().alias('payroll_share'),
        pl.col('qcew_share').mean().alias('qcew_share'),
    )
    x = np.arange(len(agg))
    w = 0.35
    ax.bar(x - w/2, agg['payroll_share'].to_list(), w, label='Payroll')
    ax.bar(x + w/2, agg['qcew_share'].to_list(), w, label='QCEW')
    ax.set_xticks(x)
    ax.set_xticklabels(agg['supersector'].to_list(), rotation=45, ha='right')
    ax.legend()
    ax.set_title('Industry composition: payroll vs QCEW')
    _save(fig, 'industry_composition_comparison', output_dir)
    return fig


def size_class_distribution(size_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Side-by-side bar: payroll vs QCEW by size class."""
    fig, ax = plt.subplots(figsize=(10, 6))
    if size_df.is_empty():
        ax.set_title('Size class distribution')
        _save(fig, 'size_class_distribution', output_dir)
        return fig
    x = np.arange(size_df.height)
    w = 0.35
    if 'payroll_share' not in size_df.columns:
        ax.set_title('Size class distribution (data unavailable)')
        _save(fig, 'size_class_distribution', output_dir)
        return fig
    if 'qcew_share' not in size_df.columns:
        ax.set_title('Size class distribution (QCEW data unavailable)')
        _save(fig, 'size_class_distribution', output_dir)
        return fig
    ax.bar(x - w/2, size_df['payroll_share'].to_list(), w, label='Payroll')
    ax.bar(x + w/2, size_df['qcew_share'].to_list(), w, label='QCEW')
    ax.legend()
    ax.set_title('Size class distribution')
    _save(fig, 'size_class_distribution', output_dir)
    return fig


def coverage_over_time(coverage_time_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: national coverage ratio by quarter."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if coverage_time_df.is_empty() or 'quarter' not in coverage_time_df.columns:
        ax.set_title('Coverage over time')
        _save(fig, 'coverage_over_time', output_dir)
        return fig
    quarters = coverage_time_df['quarter'].to_list()
    values = coverage_time_df['coverage_ratio_employment'].to_list()
    ax.plot(range(len(quarters)), values)
    ax.set_xticks(range(len(quarters)))
    ax.set_xticklabels(quarters, rotation=45, ha='right')
    ax.set_title('National coverage ratio over time')
    ax.set_ylabel('Coverage ratio')
    _save(fig, 'coverage_over_time', output_dir)
    return fig


def growth_tracking(growth_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: payroll vs CES YoY growth (national + key supersectors)."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if growth_df.is_empty():
        ax.set_title('Growth tracking: payroll vs CES')
        _save(fig, 'growth_tracking', output_dir)
        return fig
    if 'ref_date' in growth_df.columns:
        ax.plot(growth_df['ref_date'].to_list(), growth_df['payroll_yoy'].to_list() if 'payroll_yoy' in growth_df.columns else growth_df.to_series(1).to_list(), label='Payroll')
        ax.plot(growth_df['ref_date'].to_list(), growth_df['ces_yoy'].to_list() if 'ces_yoy' in growth_df.columns else growth_df.to_series(2).to_list(), label='CES')
    ax.legend()
    ax.set_title('Growth tracking: payroll vs CES (YoY)')
    ax.set_ylabel('YoY growth')
    _save(fig, 'growth_tracking', output_dir)
    return fig


def growth_decomposition_waterfall(decomp_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Stacked bar or waterfall: composition vs within-cell effects by quarter."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if decomp_df.is_empty() or 'quarter' not in decomp_df.columns:
        ax.set_title('Growth decomposition')
        _save(fig, 'growth_decomposition_waterfall', output_dir)
        return fig
    x = range(len(decomp_df))
    comp = decomp_df['composition_effect'].to_list()
    within = decomp_df['within_cell_effect'].to_list()
    ax.bar(x, comp, label='Composition')
    ax.bar(x, within, bottom=np.array(comp), label='Within-cell')
    ax.legend()
    ax.set_title('Growth decomposition by quarter')
    ax.set_xticks(list(x))
    ax.set_xticklabels(decomp_df['quarter'].to_list(), rotation=45, ha='right')
    _save(fig, 'growth_decomposition_waterfall', output_dir)
    return fig


def employment_change_decomposition(emp_decomp_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Stacked bar: within-client vs entry vs exit contributions by month."""
    fig, ax = plt.subplots(figsize=(12, 5))
    if emp_decomp_df.is_empty() or 'ref_date' not in emp_decomp_df.columns:
        ax.set_title('Employment change decomposition')
        _save(fig, 'employment_change_decomposition', output_dir)
        return fig
    x = range(len(emp_decomp_df))
    within = emp_decomp_df['within_change'].to_list()
    entry = emp_decomp_df['entry_contribution'].to_list()
    exit_vals = [-v for v in emp_decomp_df['exit_contribution'].to_list()]
    ax.bar(x, within, label='Within-client', color='steelblue')
    ax.bar(x, entry, bottom=within, label='Entry', color='green')
    ax.bar(x, exit_vals, label='Exit', color='red')
    ax.legend()
    ax.set_title('Employment change decomposition (intensive vs extensive)')
    ax.set_ylabel('Employment change')
    _save(fig, 'employment_change_decomposition', output_dir)
    return fig


def birth_rate_comparison(birth_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: payroll birth rate vs BED birth rate by quarter."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if birth_df.is_empty():
        ax.set_title('Birth rate: payroll vs BED')
        _save(fig, 'birth_rate_comparison', output_dir)
        return fig
    q = birth_df['quarter'].to_list() if 'quarter' in birth_df.columns else list(range(len(birth_df)))
    ax.plot(q, birth_df['birth_rate'].to_list(), label='Payroll')
    if 'bed_birth_rate' in birth_df.columns:
        ax.plot(q, birth_df['bed_birth_rate'].to_list(), label='BED')
    ax.legend()
    ax.set_title('Birth rate: payroll vs BED')
    ax.set_ylabel('Birth rate')
    _save(fig, 'birth_rate_comparison', output_dir)
    return fig


def birth_rate_cross_correlation(corr_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Bar: cross-correlations at different lags."""
    fig, ax = plt.subplots(figsize=(8, 5))
    if corr_df.is_empty():
        ax.set_title('Birth rate cross-correlation')
        _save(fig, 'birth_rate_cross_correlation', output_dir)
        return fig
    lag_col = corr_df.columns[0]
    corr_col = corr_df.columns[-1]
    ax.bar(corr_df[lag_col].to_list(), corr_df[corr_col].to_list())
    ax.set_title('Birth rate cross-correlation by lag')
    ax.set_xlabel('Lag (quarters)')
    _save(fig, 'birth_rate_cross_correlation', output_dir)
    return fig


def birth_lead_regression_table(reg_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Table: R-squared, coefficients, p-values for Granger models."""
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('off')
    if reg_df.is_empty():
        ax.set_title('Birth lead regression results')
        _save(fig, 'birth_lead_regression_table', output_dir)
        return fig
    table = ax.table(cellText=reg_df.to_numpy().tolist(), colLabels=reg_df.columns, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    ax.set_title('Birth lead regression results')
    _save(fig, 'birth_lead_regression_table', output_dir)
    return fig


def usability_map(reliability_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """State x supersector: cells green/yellow/red by reliability."""
    fig, ax = plt.subplots(figsize=(12, 8))
    if reliability_df.is_empty() or 'reliability' not in reliability_df.columns:
        ax.set_title('Usability map')
        _save(fig, 'usability_map', output_dir)
        return fig
    pivot = reliability_df.pivot(index='state_fips', on='supersector', values='reliability', aggregate_function='first')
    row_labels = pivot['state_fips'].to_list()
    col_labels = [c for c in pivot.columns if c != 'state_fips']
    num = pivot.select(col_labels).replace({'reliable': 1, 'marginal': 0.5, 'insufficient': 0})
    im = ax.imshow(num.to_numpy(), aspect='auto', cmap='RdYlGn', vmin=0, vmax=1)
    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, rotation=45, ha='right')
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels)
    cbar = plt.colorbar(im, ax=ax, ticks=[0, 0.5, 1])
    cbar.ax.set_yticklabels(['Insufficient', 'Marginal', 'Reliable'])
    ax.set_title('Usability map')
    _save(fig, 'usability_map', output_dir)
    return fig


def reweighting_impact(growth_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: raw payroll growth, reweighted payroll growth, CES growth."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if growth_df.is_empty():
        ax.set_title('Reweighting impact on growth')
        _save(fig, 'reweighting_impact', output_dir)
        return fig
    if 'ref_date' in growth_df.columns:
        dates = growth_df['ref_date'].to_list()
        ax.plot(dates, growth_df['payroll_yoy'].to_list() if 'payroll_yoy' in growth_df.columns else growth_df.to_series(1).to_list(), label='Raw payroll')
        ax.plot(dates, growth_df['reweighted_yoy'].to_list() if 'reweighted_yoy' in growth_df.columns else growth_df['payroll_yoy'].to_list(), label='Reweighted')
        ax.plot(dates, growth_df['ces_yoy'].to_list() if 'ces_yoy' in growth_df.columns else growth_df.to_series(2).to_list(), label='CES')
    ax.legend()
    ax.set_title('Reweighting impact on growth')
    ax.set_ylabel('YoY growth')
    _save(fig, 'reweighting_impact', output_dir)
    return fig


# ---- New exhibits (§5.1) ----


def gross_job_flows(flows_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: hire rate, separation rate, churn rate over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if flows_df.is_empty() or 'ref_date' not in flows_df.columns:
        ax.set_title('Gross job flows')
        _save(fig, 'gross_job_flows', output_dir)
        return fig
    dates = flows_df['ref_date'].to_list()
    if 'hire_rate' in flows_df.columns:
        ax.plot(dates, flows_df['hire_rate'].to_list(), label='Hire rate')
    if 'separation_rate' in flows_df.columns:
        ax.plot(dates, flows_df['separation_rate'].to_list(), label='Separation rate')
    if 'churn_rate' in flows_df.columns:
        ax.plot(dates, flows_df['churn_rate'].to_list(), label='Churn rate')
    ax.legend()
    ax.set_title('Gross job flows')
    ax.set_ylabel('Rate')
    _save(fig, 'gross_job_flows', output_dir)
    return fig


def client_churn(entry_exit_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: entry rate, exit rate, net client change over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if entry_exit_df.is_empty() or 'ref_date' not in entry_exit_df.columns:
        ax.set_title('Client churn')
        _save(fig, 'client_churn', output_dir)
        return fig
    dates = entry_exit_df['ref_date'].to_list()
    if 'entry_rate' in entry_exit_df.columns:
        ax.plot(dates, entry_exit_df['entry_rate'].to_list(), label='Entry rate')
    if 'exit_rate' in entry_exit_df.columns:
        ax.plot(dates, entry_exit_df['exit_rate'].to_list(), label='Exit rate')
    ax.legend()
    ax2 = ax.twinx()
    if 'net_client_change' in entry_exit_df.columns:
        ax2.bar(dates, entry_exit_df['net_client_change'].to_list(), alpha=0.3, label='Net change', color='gray')
        ax2.set_ylabel('Net client change')
    ax.set_title('Client churn: entry/exit rates and net change')
    ax.set_ylabel('Rate')
    _save(fig, 'client_churn', output_dir)
    return fig


def vintage_composition(vintage_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Stacked area: employment share by client vintage over time."""
    fig, ax = plt.subplots(figsize=(12, 6))
    if vintage_df.is_empty() or 'ref_date' not in vintage_df.columns or 'vintage_year' not in vintage_df.columns:
        ax.set_title('Vintage composition')
        _save(fig, 'vintage_composition', output_dir)
        return fig
    if 'employment_share' not in vintage_df.columns:
        ax.set_title('Vintage composition (share data unavailable)')
        _save(fig, 'vintage_composition', output_dir)
        return fig
    pivot = vintage_df.pivot(index='ref_date', on='vintage_year', values='employment_share', aggregate_function='first').sort('ref_date')
    dates = pivot['ref_date'].to_list()
    vintages = [c for c in pivot.columns if c != 'ref_date']
    values = [pivot[v].fill_null(0).to_list() for v in vintages]
    ax.stackplot(range(len(dates)), *values, labels=[str(v) for v in vintages])
    ax.legend(loc='upper left', fontsize='small')
    ax.set_title('Employment share by client vintage')
    ax.set_ylabel('Share')
    _save(fig, 'vintage_composition', output_dir)
    return fig


def csi_chart(csi_results: dict[str, pl.DataFrame], output_dir: Path | None = None) -> plt.Figure:
    """Line: CSI over time for industry, geography, size class."""
    fig, ax = plt.subplots(figsize=(10, 5))
    has_data = False
    for dim_name, csi_df in csi_results.items():
        if isinstance(csi_df, pl.DataFrame) and not csi_df.is_empty() and 'csi' in csi_df.columns:
            ax.plot(csi_df['ref_date'].to_list(), csi_df['csi'].to_list(), label=dim_name)
            has_data = True
    if not has_data:
        ax.set_title('Composition Shift Index')
        _save(fig, 'csi_chart', output_dir)
        return fig
    ax.legend()
    ax.set_title('Composition Shift Index over time')
    ax.set_ylabel('CSI (bounded [0, 2])')
    _save(fig, 'csi_chart', output_dir)
    return fig


def earnings_distribution(earnings_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Line: median and P10/P90 earnings over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if earnings_df.is_empty() or 'ref_date' not in earnings_df.columns:
        ax.set_title('Earnings distribution')
        _save(fig, 'earnings_distribution', output_dir)
        return fig
    dates = earnings_df['ref_date'].to_list()
    if 'median_earnings' in earnings_df.columns:
        ax.plot(dates, earnings_df['median_earnings'].to_list(), label='Median', color='steelblue')
    if 'p10_earnings' in earnings_df.columns and 'p90_earnings' in earnings_df.columns:
        ax.fill_between(dates, earnings_df['p10_earnings'].to_list(), earnings_df['p90_earnings'].to_list(), alpha=0.2, label='P10-P90', color='steelblue')
    ax.legend()
    ax.set_title('Earnings distribution over time')
    ax.set_ylabel('Monthly gross pay')
    _save(fig, 'earnings_distribution', output_dir)
    return fig


def data_quality_summary_table(dq_summary_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Table: counts of each flag type by month."""
    fig, ax = plt.subplots(figsize=(12, max(4, dq_summary_df.height * 0.4 + 1) if not dq_summary_df.is_empty() else 4))
    ax.axis('off')
    if dq_summary_df.is_empty():
        ax.set_title('Data quality summary')
        _save(fig, 'data_quality_summary', output_dir)
        return fig
    display_cols = [c for c in dq_summary_df.columns if c != 'ref_date']
    cell_text = dq_summary_df.select(display_cols).to_numpy().tolist()
    row_labels = [str(d) for d in dq_summary_df['ref_date'].to_list()] if 'ref_date' in dq_summary_df.columns else None
    table = ax.table(cellText=cell_text, colLabels=display_cols, rowLabels=row_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    ax.set_title('Data quality flags by month')
    _save(fig, 'data_quality_summary', output_dir)
    return fig


def survival_curves(survival_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Kaplan-Meier curves by entry cohort."""
    fig, ax = plt.subplots(figsize=(10, 6))
    if survival_df.is_empty() or 'entry_quarter' not in survival_df.columns:
        ax.set_title('Client survival curves')
        _save(fig, 'survival_curves', output_dir)
        return fig
    horizons = [4, 8, 12, 16, 20]
    surv_cols = [f'survival_{h}q' for h in horizons]
    available_cols = [c for c in surv_cols if c in survival_df.columns]
    if not available_cols:
        ax.set_title('Client survival curves (data unavailable)')
        _save(fig, 'survival_curves', output_dir)
        return fig
    for row in survival_df.iter_rows(named=True):
        cohort = row.get('entry_quarter', '')
        values = [row.get(c, None) for c in available_cols]
        valid_x = []
        valid_y = []
        for j, v in enumerate(values):
            if v is not None:
                valid_x.append(horizons[j])
                valid_y.append(v)
        if valid_x:
            ax.plot(valid_x, valid_y, marker='o', label=str(cohort))
    ax.set_xlabel('Quarters after entry')
    ax.set_ylabel('Survival rate')
    ax.set_title('Client survival curves by entry cohort')
    if survival_df.height <= 10:
        ax.legend(fontsize='small')
    _save(fig, 'survival_curves', output_dir)
    return fig


def tenure_histogram(tenure_df: pl.DataFrame, output_dir: Path | None = None) -> plt.Figure:
    """Distribution of client tenure months."""
    fig, ax = plt.subplots(figsize=(10, 5))
    if tenure_df.is_empty() or 'tenure_months' not in tenure_df.columns:
        ax.set_title('Client tenure distribution')
        _save(fig, 'tenure_histogram', output_dir)
        return fig
    values = tenure_df['tenure_months'].drop_nulls().to_list()
    if values:
        ax.hist(values, bins=min(50, max(10, len(values) // 10)), edgecolor='white')
    ax.set_xlabel('Tenure (months)')
    ax.set_ylabel('Number of clients')
    ax.set_title('Distribution of client tenure')
    _save(fig, 'tenure_histogram', output_dir)
    return fig


def generate_all_exhibits(analysis_outputs: dict[str, pl.DataFrame | dict], output_dir: Path | None = None) -> list[plt.Figure]:
    """Generate all exhibits from a dict of analysis dataframes."""
    figs = []
    cov = analysis_outputs.get('coverage', pl.DataFrame())
    coverage_time = analysis_outputs.get('coverage_over_time', cov)
    share = analysis_outputs.get('share_comparison', pl.DataFrame())
    reliability = analysis_outputs.get('reliability', pl.DataFrame())
    grth = analysis_outputs.get('growth', pl.DataFrame())
    decomp = analysis_outputs.get('growth_decomposition', pl.DataFrame())
    emp_decomp = analysis_outputs.get('employment_decomposition', pl.DataFrame())
    birth = analysis_outputs.get('birth_rates', pl.DataFrame())
    corr = analysis_outputs.get('birth_cross_corr', pl.DataFrame())
    reg = analysis_outputs.get('birth_lead_regression', pl.DataFrame())
    size = analysis_outputs.get('size_class', pl.DataFrame())
    reweight_data = analysis_outputs.get('reweight_growth', grth)
    flows_data = analysis_outputs.get('job_flows', pl.DataFrame())
    entry_exit = analysis_outputs.get('client_entry_exit', pl.DataFrame())
    vintage = analysis_outputs.get('vintage_shares', pl.DataFrame())
    csi_data = analysis_outputs.get('csi', {})
    earn = analysis_outputs.get('earnings', pl.DataFrame())
    dq = analysis_outputs.get('data_quality_summary', pl.DataFrame())
    surv = analysis_outputs.get('survival_curves', pl.DataFrame())
    client_ten = analysis_outputs.get('client_tenure', pl.DataFrame())

    # Original exhibits
    figs.append(coverage_heatmap_employment(cov, output_dir))
    figs.append(coverage_heatmap_establishments(cov, output_dir))
    figs.append(industry_composition_comparison(share, output_dir))
    figs.append(size_class_distribution(size, output_dir))
    figs.append(coverage_over_time(coverage_time, output_dir))
    figs.append(growth_tracking(grth, output_dir))
    figs.append(growth_decomposition_waterfall(decomp, output_dir))
    figs.append(birth_rate_comparison(birth, output_dir))
    figs.append(birth_rate_cross_correlation(corr, output_dir))
    figs.append(birth_lead_regression_table(reg, output_dir))
    figs.append(usability_map(reliability, output_dir))
    figs.append(reweighting_impact(reweight_data, output_dir))

    # New exhibits (§5.1)
    figs.append(gross_job_flows(flows_data, output_dir))
    figs.append(client_churn(entry_exit, output_dir))
    figs.append(vintage_composition(vintage, output_dir))
    figs.append(csi_chart(csi_data if isinstance(csi_data, dict) else {}, output_dir))
    figs.append(employment_change_decomposition(emp_decomp, output_dir))
    figs.append(earnings_distribution(earn, output_dir))
    figs.append(data_quality_summary_table(dq, output_dir))
    figs.append(survival_curves(surv, output_dir))
    figs.append(tenure_histogram(client_ten, output_dir))

    return figs
