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
    pcol = size_df['payroll_share'] if 'payroll_share' in size_df.columns else size_df.columns[1]
    qcol = size_df['qcew_share'] if 'qcew_share' in size_df.columns else size_df.columns[-1]
    ax.bar(x - w/2, size_df[pcol].to_list(), w, label='Payroll')
    ax.bar(x + w/2, size_df[qcol].to_list(), w, label='QCEW')
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
    ax.plot(coverage_time_df['quarter'].to_list(), coverage_time_df['coverage_ratio_employment'].to_list())
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
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
    ax.set_xticks(x)
    ax.set_xticklabels(decomp_df['quarter'].to_list(), rotation=45, ha='right')
    _save(fig, 'growth_decomposition_waterfall', output_dir)
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
    """Table: R², coefficients, p-values for Granger models."""
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


def generate_all_exhibits(analysis_outputs: dict[str, pl.DataFrame], output_dir: Path | None = None) -> list[plt.Figure]:
    """Generate all 11 exhibits from a dict of analysis dataframes. Keys are exhibit inputs."""
    figs = []
    coverage = analysis_outputs.get('coverage', pl.DataFrame())
    coverage_time = analysis_outputs.get('coverage_over_time', coverage)
    share = analysis_outputs.get('share_comparison', pl.DataFrame())
    reliability = analysis_outputs.get('reliability', pl.DataFrame())
    growth = analysis_outputs.get('growth', pl.DataFrame())
    decomp = analysis_outputs.get('growth_decomposition', pl.DataFrame())
    birth = analysis_outputs.get('birth_rates', pl.DataFrame())
    corr = analysis_outputs.get('birth_cross_corr', pl.DataFrame())
    reg = analysis_outputs.get('birth_lead_regression', pl.DataFrame())
    size = analysis_outputs.get('size_class', pl.DataFrame())
    reweight = analysis_outputs.get('reweight_growth', growth)

    figs.append(coverage_heatmap_employment(coverage, output_dir))
    figs.append(coverage_heatmap_establishments(coverage, output_dir))
    figs.append(industry_composition_comparison(share, output_dir))
    figs.append(size_class_distribution(size, output_dir))
    figs.append(coverage_over_time(coverage_time, output_dir))
    figs.append(growth_tracking(growth, output_dir))
    figs.append(growth_decomposition_waterfall(decomp, output_dir))
    figs.append(birth_rate_comparison(birth, output_dir))
    figs.append(birth_rate_cross_correlation(corr, output_dir))
    figs.append(birth_lead_regression_table(reg, output_dir))
    figs.append(usability_map(reliability, output_dir))
    figs.append(reweighting_impact(reweight, output_dir))
    return figs
