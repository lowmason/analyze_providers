"""Assemble the three-layer deliverable: executive summary, dashboard, technical appendix."""

from pathlib import Path

import polars as pl

from analyze_provider import config


def build_executive_summary(
    coverage_pct: float,
    usability_map_path: Path | None,
    growth_chart_path: Path | None,
    bullets: list[str],
    output_path: Path | None = None,
) -> Path:
    """Layer 1: Single-page executive summary (markdown).

    Includes total coverage, usability heatmap reference, one growth chart reference, and 3 bullet points.
    """
    out = output_path or (config.OUTPUT_DIR / 'executive_summary.md')
    out.parent.mkdir(parents=True, exist_ok=True)
    usem = str(usability_map_path) if usability_map_path else 'N/A'
    growth = str(growth_chart_path) if growth_chart_path else 'N/A'
    content = f'''# Payroll Provider Representativeness: Executive Summary

## Total coverage
{coverage_pct:.1%} of QCEW employment is covered by the payroll provider sample.

## Key exhibits
- Usability map: {usem}
- Growth tracking: {growth}

## Summary
'''
    for b in bullets[:3]:
        content += f'- {b}\n'
    out.write_text(content)
    return out


def build_dashboard(exhibits_dir: Path | None = None, output_path: Path | None = None) -> Path:
    """Layer 2: Dashboard PDF or markdown assembling all exhibits with captions."""
    exhibits = exhibits_dir or (config.OUTPUT_DIR / 'exhibits')
    out = output_path or (config.OUTPUT_DIR / 'dashboard.md')
    out.parent.mkdir(parents=True, exist_ok=True)
    lines = ['# Representativeness Dashboard\n']
    for name in [
        'coverage_heatmap_employment', 'coverage_heatmap_establishments',
        'industry_composition_comparison', 'size_class_distribution',
        'coverage_over_time', 'growth_tracking', 'growth_decomposition_waterfall',
        'employment_change_decomposition',
        'birth_rate_comparison', 'birth_rate_cross_correlation',
        'birth_lead_regression_table', 'usability_map', 'reweighting_impact',
        'gross_job_flows', 'client_churn', 'vintage_composition',
        'csi_chart', 'earnings_distribution', 'data_quality_summary',
        'survival_curves', 'tenure_histogram',
    ]:
        png = exhibits / f'{name}.png'
        if png.exists():
            lines.append(f'## {name}\n\n![]({png})\n')
    out.write_text('\n'.join(lines))
    return out


def build_technical_appendix(
    coverage_df: pl.DataFrame | None = None,
    growth_df: pl.DataFrame | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Layer 3: Full cell-level CSVs and methodology description."""
    out_dir = output_dir or (config.OUTPUT_DIR / 'appendix')
    out_dir.mkdir(parents=True, exist_ok=True)
    if coverage_df is not None and not coverage_df.is_empty():
        (out_dir / 'coverage_cells.csv').write_text(coverage_df.write_csv())
    if growth_df is not None and not growth_df.is_empty():
        (out_dir / 'growth_cells.csv').write_text(growth_df.write_csv())
    method_path = out_dir / 'methodology.md'
    method_path.write_text('''# Methodology

## Coverage
Coverage ratio = payroll employment / QCEW employment at the same quarter and grouping (national, supersector, state, size class, or cross-tabs). QCEW uses month1_emplvl for the first month of each quarter to align with payroll ref_date (day 12).

## Growth
Year-over-year growth is computed for payroll and CES employment. Comparison uses inner join on ref_date and grouping columns. Rolling 12-month correlation is reported.

## Birth rates
Payroll birth rate = (clients with is_birth == True) / (clients with is_birth non-null) per quarter and grouping. BED birth rate = births / beginning-of-quarter establishments.

## Cell reliability
Cells are classified as reliable (min clients and min coverage threshold met), marginal (one threshold met), or insufficient (neither met). Default thresholds: 30 clients, 0.5% coverage.
''')
    return out_dir
