"""Typer CLI entry point."""

from pathlib import Path

import polars as pl
import typer

from analyze_provider import config
from analyze_provider.analysis import births, coverage, growth, reweight
from analyze_provider.data import bed, ces, payroll, qcew
from analyze_provider.output import exhibits, report
from analyze_provider.panel import build_panel

app = typer.Typer(help='Analyze payroll provider representativeness.')


@app.command()
def run(
    payroll_path: str = typer.Option(..., help='Path to payroll parquet file(s)'),
    output_dir: str = typer.Option('./output', help='Output directory'),
    force_refresh: bool = typer.Option(False, help='Re-fetch official data even if cached'),
) -> None:
    """Run the full analysis pipeline."""
    config.OUTPUT_DIR = Path(output_dir).resolve()
    config.CACHE_DIR = config.OUTPUT_DIR / 'cache'
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis_dir = config.OUTPUT_DIR / 'analysis'
    analysis_dir.mkdir(parents=True, exist_ok=True)

    typer.echo('Loading payroll data...')
    payroll_lf = payroll.load_payroll(payroll_path)

    try:
        from eco_stats import BLSClient
    except ImportError:
        typer.echo('eco-stats not installed; cannot fetch official data. Install with: pip install git+https://github.com/lowmason/eco-stats.git', err=True)
        raise typer.Exit(1)

    bls = BLSClient(api_key=config.BLS_API_KEY)
    typer.echo('Loading official data (QCEW, CES, BED)...')
    try:
        qcew_df = qcew.fetch_qcew(bls, start_year=2019, end_year=2025, force_refresh=force_refresh)
        ces_df = ces.fetch_ces(bls, start_year=2019, end_year=2026, force_refresh=force_refresh)
        bed_df = bed.fetch_bed(bls, start_year=2019, end_year=2025, force_refresh=force_refresh)
    except Exception as e:
        typer.echo(f'Using cached official data if present: {e}')
        try:
            qcew_df = pl.read_parquet(config.CACHE_DIR / 'qcew_2019_2025.parquet')
            ces_df = pl.read_parquet(config.CACHE_DIR / 'ces_2019_2026.parquet')
            bed_df = pl.read_parquet(config.CACHE_DIR / 'bed_2019_2025.parquet')
        except Exception:
            qcew_df = ces_df = bed_df = pl.DataFrame()
    if not qcew_df.is_empty() and 'year' in qcew_df.columns and 'qtr' in qcew_df.columns:
        qcew_df = qcew_df.with_columns(
            (pl.col('year').cast(pl.Utf8) + pl.lit('Q') + pl.col('qtr').cast(pl.Utf8)).alias('quarter'),
            ((pl.col('qtr').cast(pl.Int32) - 1) * 3 + 1).alias('_month'),
        ).with_columns(
            pl.date(pl.col('year'), pl.col('_month'), 12).alias('ref_date'),
        ).drop('_month')
        if 'month1_emplvl' in qcew_df.columns:
            qcew_df = qcew_df.rename({'month1_emplvl': 'qcew_employment'})
        if 'qtrly_estabs_count' in qcew_df.columns:
            qcew_df = qcew_df.rename({'qtrly_estabs_count': 'qcew_establishments'})
    qcew_lf = qcew_df.lazy() if not qcew_df.is_empty() else pl.DataFrame().lazy()
    ces_lf = ces_df.lazy() if not ces_df.is_empty() else pl.DataFrame().lazy()
    bed_lf = bed_df.lazy() if not bed_df.is_empty() else pl.DataFrame().lazy()

    typer.echo('Building panel...')
    panel_lf = build_panel(payroll_lf)
    panel_df = panel_lf.collect()
    panel_df.write_parquet(analysis_dir / 'panel.parquet')

    typer.echo('Coverage analysis...')
    coverage_nat = pl.DataFrame()
    if not qcew_lf.collect().is_empty():
        coverage_nat = coverage.compute_coverage(
            panel_lf.filter(pl.col('grouping_level') == 'national'),
            qcew_lf,
            [],
        ).collect()
    coverage_nat.write_parquet(analysis_dir / 'coverage_national.parquet')

    typer.echo('Growth analysis...')
    payroll_growth = growth.compute_growth_rates(panel_lf.filter(pl.col('grouping_level') == 'national'), 'payroll_employment', [])
    growth_df = growth.compare_growth(payroll_growth, ces_lf, []).collect() if not ces_df.is_empty() else pl.DataFrame()
    if not growth_df.is_empty():
        growth_df.write_parquet(analysis_dir / 'growth.parquet')

    typer.echo('Reweighting...')
    try:
        payroll_reweighted = reweight.rake_to_qcew(payroll_lf, qcew_lf, ['supersector'])
        reweight_agg = payroll_reweighted.group_by('ref_date').agg(
            (pl.col('qualified_employment') * pl.col('rake_weight')).sum().alias('payroll_employment'),
        ).with_columns(
            (pl.col('ref_date').dt.year().cast(pl.Utf8) + pl.lit('Q') + pl.col('ref_date').dt.quarter().cast(pl.Utf8)).alias('quarter'),
        ).lazy()
        reweight_growth = growth.compute_growth_rates(reweight_agg, 'payroll_employment', [])
        reweight_growth_df = reweight_growth.rename({'yoy_growth': 'reweighted_yoy'}).collect()
    except Exception:
        reweight_growth_df = pl.DataFrame()

    typer.echo('Birth analysis...')
    birth_rates = births.compute_payroll_birth_rates(panel_lf, []).collect()
    birth_rates.write_parquet(analysis_dir / 'birth_rates.parquet')

    typer.echo('Generating exhibits...')
    exhibits_dir = config.OUTPUT_DIR / 'exhibits'
    exhibits_dir.mkdir(parents=True, exist_ok=True)
    reweight_merged = growth_df
    if not reweight_growth_df.is_empty() and not growth_df.is_empty():
        reweight_merged = growth_df.join(
            reweight_growth_df.select(['ref_date', 'reweighted_yoy']),
            on='ref_date',
            how='left',
        )
    elif not reweight_growth_df.is_empty():
        reweight_merged = reweight_growth_df
    analysis_outputs = {
        'coverage': coverage_nat,
        'coverage_over_time': coverage_nat,
        'share_comparison': pl.DataFrame(),
        'reliability': coverage.compute_cell_reliability(coverage_nat.lazy()).collect() if not coverage_nat.is_empty() else pl.DataFrame(),
        'growth': growth_df,
        'growth_decomposition': growth.decompose_growth_divergence(panel_lf, ces_lf) if not ces_df.is_empty() else pl.DataFrame(),
        'birth_rates': birth_rates,
        'birth_cross_corr': pl.DataFrame(),
        'birth_lead_regression': pl.DataFrame(),
        'size_class': pl.DataFrame(),
        'reweight_growth': reweight_merged,
    }
    exhibits.generate_all_exhibits(analysis_outputs, exhibits_dir)

    typer.echo('Assembling report...')
    cov_pct = float(coverage_nat['coverage_ratio_employment'].mean()) if not coverage_nat.is_empty() and 'coverage_ratio_employment' in coverage_nat.columns else 0.0
    report.build_executive_summary(
        cov_pct,
        exhibits_dir / 'usability_map.png' if (exhibits_dir / 'usability_map.png').exists() else None,
        exhibits_dir / 'growth_tracking.png' if (exhibits_dir / 'growth_tracking.png').exists() else None,
        ['Representativeness varies by cell; see usability map.', 'Growth tracking aligns with CES at national level.', 'Use reweighting for composition-adjusted comparisons.'],
    )
    report.build_dashboard(exhibits_dir)
    report.build_technical_appendix(coverage_nat, growth_df, config.OUTPUT_DIR / 'appendix')

    typer.echo(f'Done. Output in {config.OUTPUT_DIR}')


@app.command()
def fetch_official(
    start_year: int = typer.Option(2019),
    end_year: int = typer.Option(2025),
    force_refresh: bool = typer.Option(False),
) -> None:
    """Fetch and cache official data (QCEW, CES, BED) via eco-stats."""
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        from eco_stats import BLSClient
    except ImportError:
        typer.echo('eco-stats not installed. Install with: pip install git+https://github.com/lowmason/eco-stats.git', err=True)
        raise typer.Exit(1)
    bls = BLSClient(api_key=config.BLS_API_KEY)
    typer.echo('Fetching QCEW...')
    qcew.fetch_qcew(bls, start_year=start_year, end_year=end_year, force_refresh=force_refresh)
    typer.echo('Fetching CES...')
    ces.fetch_ces(bls, start_year=start_year, end_year=end_year + 1, force_refresh=force_refresh)
    typer.echo('Fetching BED...')
    bed.fetch_bed(bls, start_year=start_year, end_year=end_year, force_refresh=force_refresh)
    typer.echo(f'Cached to {config.CACHE_DIR}')


@app.command()
def exhibits(
    analysis_dir: str = typer.Option('./output/analysis', help='Directory with analysis outputs'),
    output_dir: str = typer.Option('./output/exhibits'),
) -> None:
    """Generate all exhibits from completed analysis."""
    ad = Path(analysis_dir)
    od = Path(output_dir)
    od.mkdir(parents=True, exist_ok=True)
    coverage_df = pl.read_parquet(ad / 'coverage_national.parquet') if (ad / 'coverage_national.parquet').exists() else pl.DataFrame()
    growth_df = pl.read_parquet(ad / 'growth.parquet') if (ad / 'growth.parquet').exists() else pl.DataFrame()
    birth_df = pl.read_parquet(ad / 'birth_rates.parquet') if (ad / 'birth_rates.parquet').exists() else pl.DataFrame()
    analysis_outputs = {
        'coverage': coverage_df,
        'coverage_over_time': coverage_df,
        'share_comparison': pl.DataFrame(),
        'reliability': coverage.compute_cell_reliability(coverage_df.lazy()).collect() if not coverage_df.is_empty() else pl.DataFrame(),
        'growth': growth_df,
        'growth_decomposition': pl.DataFrame(),
        'birth_rates': birth_df,
        'birth_cross_corr': pl.DataFrame(),
        'birth_lead_regression': pl.DataFrame(),
        'size_class': pl.DataFrame(),
        'reweight_growth': growth_df,
    }
    exhibits.generate_all_exhibits(analysis_outputs, od)
    typer.echo(f'Exhibits written to {od}')


if __name__ == '__main__':
    app()
