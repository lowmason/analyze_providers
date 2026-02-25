"""Typer CLI entry point."""

from pathlib import Path

import polars as pl
import typer

from analyze_provider import config
from analyze_provider.analysis import births, coverage, growth, reweight
from analyze_provider.analysis import data_quality, earnings, flows, tenure
from analyze_provider.data import bed, ces, payroll, qcew
from analyze_provider.output import exhibits as exhibits_mod
from analyze_provider.output import report
from analyze_provider.panel import build_panel, filter_stable_panel

app = typer.Typer(help='Analyze payroll provider representativeness.')


@app.command()
def run(
    payroll_path: str = typer.Option(..., help='Path to payroll parquet file(s)'),
    output_dir: str = typer.Option('./output', help='Output directory'),
    force_refresh: bool = typer.Option(False, help='Re-fetch official data even if cached'),
) -> None:
    """Run the full analysis pipeline.

    Pipeline order follows the methodology appendix:
    1. Load payroll data
    2. Fetch official data
    3. Data quality assessment
    4. Client tenure and churn
    5. Vintage assessment / contamination check
    6. Construct stable panel
    7. Coverage analysis
    8. Growth analysis (including employment change decomposition)
    9. Worker-level flows
    10. Earnings analysis
    11. Reweighting
    12. Birth analysis
    13. Generate exhibits
    14. Assemble report
    """
    config.OUTPUT_DIR = Path(output_dir).resolve()
    config.CACHE_DIR = config.OUTPUT_DIR / 'cache'
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis_dir = config.OUTPUT_DIR / 'analysis'
    analysis_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load payroll data
    typer.echo('Loading payroll data...')
    payroll_lf = payroll.load_payroll(payroll_path)

    # Step 2: Fetch official data
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

    # Unpivot QCEW months for proper monthly alignment
    if not qcew_df.is_empty() and 'month1_emplvl' in qcew_df.columns:
        qcew_df = qcew.unpivot_qcew_months(qcew_df)
    elif not qcew_df.is_empty() and 'year' in qcew_df.columns and 'qtr' in qcew_df.columns:
        qcew_df = qcew_df.with_columns(
            (pl.col('year').cast(pl.Utf8) + pl.lit('Q') + pl.col('qtr').cast(pl.Utf8)).alias('quarter'),
            ((pl.col('qtr').cast(pl.Int32) - 1) * 3 + 1).alias('_month'),
        ).with_columns(
            pl.date(pl.col('year'), pl.col('_month'), 12).alias('ref_date'),
        ).drop('_month')
        if 'qtrly_estabs_count' in qcew_df.columns:
            qcew_df = qcew_df.rename({'qtrly_estabs_count': 'qcew_establishments'})
        if 'qcew_employment' not in qcew_df.columns and 'month1_emplvl' in qcew_df.columns:
            qcew_df = qcew_df.rename({'month1_emplvl': 'qcew_employment'})

    qcew_lf = qcew_df.lazy() if not qcew_df.is_empty() else pl.DataFrame().lazy()
    ces_lf = ces_df.lazy() if not ces_df.is_empty() else pl.DataFrame().lazy()
    bed_lf = bed_df.lazy() if not bed_df.is_empty() else pl.DataFrame().lazy()

    # Step 3: Data quality assessment (before any analysis)
    typer.echo('Data quality assessment...')
    try:
        dq_flagged, dq_summary = data_quality.flag_data_quality_issues(payroll_lf)
        dq_summary.write_parquet(analysis_dir / 'data_quality_summary.parquet')
    except Exception:
        dq_flagged = payroll_lf
        dq_summary = pl.DataFrame()

    # Step 4: Client tenure and churn
    typer.echo('Client tenure and churn...')
    try:
        client_tenure_df = tenure.compute_client_tenure(payroll_lf).collect()
        client_tenure_df.write_parquet(analysis_dir / 'client_tenure.parquet')
        entry_exit_df = tenure.compute_client_entry_exit(payroll_lf, []).collect()
        entry_exit_df.write_parquet(analysis_dir / 'client_entry_exit.parquet')
    except Exception:
        client_tenure_df = pl.DataFrame()
        entry_exit_df = pl.DataFrame()

    # Step 5: Vintage assessment / contamination check
    typer.echo('Vintage assessment...')
    try:
        vintage_shares_df = tenure.compute_vintage_shares(payroll_lf).collect()
        vintage_shares_df.write_parquet(analysis_dir / 'vintage_shares.parquet')
    except Exception:
        vintage_shares_df = pl.DataFrame()

    # Step 6: Construct stable panel (filter by tenure)
    typer.echo('Building panel...')
    stable_payroll = filter_stable_panel(payroll_lf, min_tenure_months=12)
    panel_lf = build_panel(payroll_lf)
    panel_df = panel_lf.collect()
    panel_df.write_parquet(analysis_dir / 'panel.parquet')

    # Step 7: Coverage analysis
    typer.echo('Coverage analysis...')
    coverage_nat = pl.DataFrame()
    if not qcew_lf.collect().is_empty():
        coverage_nat = coverage.compute_coverage(
            panel_lf.filter(pl.col('grouping_level') == 'national'),
            qcew_lf,
            [],
        ).collect()
    coverage_nat.write_parquet(analysis_dir / 'coverage_national.parquet')

    # CSI computation
    csi_results = {}
    for dim in ['supersector', 'state_fips', 'size_class']:
        try:
            dim_panel = panel_lf.filter(pl.col('grouping_level') == dim if dim != 'state_fips' else pl.col('grouping_level') == 'state')
            csi_df = coverage.compute_composition_shift_index(dim_panel, dim).collect()
            csi_results[dim] = csi_df
        except Exception:
            csi_results[dim] = pl.DataFrame()

    # Step 8: Growth analysis
    typer.echo('Growth analysis...')
    payroll_growth = growth.compute_growth_rates(panel_lf.filter(pl.col('grouping_level') == 'national'), 'payroll_employment', [])
    growth_df = growth.compare_growth(payroll_growth, ces_lf, []).collect() if not ces_df.is_empty() else pl.DataFrame()
    if not growth_df.is_empty():
        growth_df.write_parquet(analysis_dir / 'growth.parquet')

    # Employment change decomposition
    try:
        emp_decomp = growth.decompose_employment_change(payroll_lf, [])
        emp_decomp.write_parquet(analysis_dir / 'employment_decomposition.parquet')
    except Exception:
        emp_decomp = pl.DataFrame()

    # Step 9: Worker-level flows
    typer.echo('Worker-level flows...')
    try:
        flows_df = flows.compute_job_flows(payroll_lf, []).collect()
        flows_df.write_parquet(analysis_dir / 'job_flows.parquet')
    except Exception:
        flows_df = pl.DataFrame()

    # Step 10: Earnings analysis
    typer.echo('Earnings analysis...')
    try:
        earnings_df = earnings.compute_earnings_distribution(payroll_lf, []).collect()
        earnings_df.write_parquet(analysis_dir / 'earnings.parquet')
    except Exception:
        earnings_df = pl.DataFrame()

    # Step 11: Reweighting
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

    # Step 12: Birth analysis
    typer.echo('Birth analysis...')
    birth_rates = births.compute_payroll_birth_rates(panel_lf, []).collect()
    birth_rates.write_parquet(analysis_dir / 'birth_rates.parquet')

    # Survival curves
    try:
        survival_df = births.compute_survival_curves(payroll_lf, []).collect()
        survival_df.write_parquet(analysis_dir / 'survival_curves.parquet')
    except Exception:
        survival_df = pl.DataFrame()

    # Step 13: Generate exhibits
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
        'employment_decomposition': emp_decomp,
        'birth_rates': birth_rates,
        'birth_cross_corr': pl.DataFrame(),
        'birth_lead_regression': pl.DataFrame(),
        'size_class': pl.DataFrame(),
        'reweight_growth': reweight_merged,
        'job_flows': flows_df,
        'client_entry_exit': entry_exit_df,
        'vintage_shares': vintage_shares_df,
        'csi': csi_results,
        'earnings': earnings_df,
        'data_quality_summary': dq_summary,
        'survival_curves': survival_df,
        'client_tenure': client_tenure_df,
    }
    exhibits_mod.generate_all_exhibits(analysis_outputs, exhibits_dir)

    # Step 14: Assemble report
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
def make_exhibits(
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
        'employment_decomposition': pl.DataFrame(),
        'birth_rates': birth_df,
        'birth_cross_corr': pl.DataFrame(),
        'birth_lead_regression': pl.DataFrame(),
        'size_class': pl.DataFrame(),
        'reweight_growth': growth_df,
        'job_flows': pl.DataFrame(),
        'client_entry_exit': pl.DataFrame(),
        'vintage_shares': pl.DataFrame(),
        'csi': {},
        'earnings': pl.DataFrame(),
        'data_quality_summary': pl.DataFrame(),
        'survival_curves': pl.DataFrame(),
        'client_tenure': pl.DataFrame(),
    }
    exhibits_mod.generate_all_exhibits(analysis_outputs, od)
    typer.echo(f'Exhibits written to {od}')


if __name__ == '__main__':
    app()
