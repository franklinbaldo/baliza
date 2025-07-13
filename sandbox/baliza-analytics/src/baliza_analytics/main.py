"""
Main CLI for Baliza Analytics - fraud detection and analysis.
"""
import typer
from rich import print
from rich.table import Table
from rich.console import Console
from pathlib import Path
from typing import Optional

from .data_connector import BalizaDataConnector
from .fraud_detection import ContractFraudDetector

app = typer.Typer(help="Baliza Analytics - Advanced contract analysis and fraud detection")
console = Console()


@app.command()
def stats(
    baliza_db: Optional[str] = typer.Option(None, help="Path to Baliza database")
):
    """Show basic statistics about the Baliza dataset."""
    try:
        connector = BalizaDataConnector(baliza_db)
        stats = connector.get_summary_stats()
        
        table = Table(title="📊 Baliza Dataset Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Contracts", f"{stats['total_contracts']:,}")
        table.add_row("Total Agencies", f"{stats['total_agencies']:,}")
        table.add_row("Total Suppliers", f"{stats['total_suppliers']:,}")
        table.add_row("Total States", f"{stats['total_states']:,}")
        table.add_row("Date Range", f"{stats['earliest_contract']} to {stats['latest_contract']}")
        table.add_row("Total Value", f"R$ {stats['total_value_millions_brl']:,.2f} million")
        table.add_row("Average Value", f"R$ {stats['avg_value_millions_brl']:.4f} million")
        
        console.print(table)
        
    except Exception as e:
        print(f"[red]Error: {e}[/red]")


@app.command()
def analyze(
    start_date: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD)"),
    uf: Optional[str] = typer.Option(None, help="State filter (e.g., RO,SP)"),
    min_value: Optional[float] = typer.Option(None, help="Minimum contract value"),
    min_score: int = typer.Option(50, help="Minimum suspicious score (0-100)"),
    baliza_db: Optional[str] = typer.Option(None, help="Path to Baliza database")
):
    """Analyze contracts for suspicious patterns."""
    try:
        connector = BalizaDataConnector(baliza_db)
        detector = ContractFraudDetector()
        
        print("🔍 Loading contract data...")
        
        uf_filter = uf.split(',') if uf else None
        df = connector.get_contracts(
            start_date=start_date,
            end_date=end_date, 
            uf_filter=uf_filter,
            min_value=min_value
        )
        
        if df.empty:
            print("[yellow]No contracts found with the specified filters.[/yellow]")
            return
        
        print(f"📈 Analyzing {len(df):,} contracts...")
        
        # Generate fraud report
        report = detector.generate_fraud_report(df)
        
        # Summary table
        summary_table = Table(title="🚨 Fraud Detection Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        summary = report['summary']
        summary_table.add_row("Total Contracts", f"{summary['total_contracts']:,}")
        summary_table.add_row("Suspicious Contracts", f"{summary['suspicious_contracts']:,}")
        summary_table.add_row("Suspicion Rate", f"{summary['suspicion_rate']:.2f}%")
        summary_table.add_row("ML Anomalies", f"{summary['ml_anomalies']:,}")
        summary_table.add_row("Anomaly Rate", f"{summary['anomaly_rate']:.2f}%")
        summary_table.add_row("High Risk Suppliers", f"{summary['high_risk_suppliers']:,}")
        
        console.print(summary_table)
        
        # Top suspicious contracts
        if report['top_suspicious']:
            print("\n🔥 Top Suspicious Contracts:")
            suspicious_table = Table()
            suspicious_table.add_column("Score", style="red")
            suspicious_table.add_column("Contract ID", style="cyan")
            suspicious_table.add_column("Agency", style="blue")
            suspicious_table.add_column("Supplier", style="magenta")
            suspicious_table.add_column("Value (R$ M)", style="green")
            
            for contract in report['top_suspicious']:
                suspicious_table.add_row(
                    f"{contract['score_suspeita']}",
                    contract['contrato_id'][:20] + "..." if len(contract['contrato_id']) > 20 else contract['contrato_id'],
                    contract['orgao_razao_social'][:30] + "..." if len(contract['orgao_razao_social']) > 30 else contract['orgao_razao_social'],
                    contract['fornecedor_nome'][:25] + "..." if len(contract['fornecedor_nome']) > 25 else contract['fornecedor_nome'],
                    f"{contract['valor_global_brl']/1000000:.2f}"
                )
            
            console.print(suspicious_table)
        
        # Risk distribution
        print("\n📊 Risk Distribution:")
        risk_table = Table()
        risk_table.add_column("Risk Level", style="cyan")
        risk_table.add_column("Count", style="green")
        risk_table.add_column("Percentage", style="yellow")
        
        total = summary['total_contracts']
        for level, count in report['risk_distribution'].items():
            percentage = (count / total * 100) if total > 0 else 0
            risk_table.add_row(
                level.replace('_', ' ').title(),
                f"{count:,}",
                f"{percentage:.1f}%"
            )
        
        console.print(risk_table)
        
    except Exception as e:
        print(f"[red]Error: {e}[/red]")


@app.command()
def report(
    output: str = typer.Option("fraud_analysis", help="Report name to run"),
    port: int = typer.Option(2718, help="Port for marimo server")
):
    """Generate interactive marimo report."""
    import subprocess
    from pathlib import Path
    
    reports_dir = Path(__file__).parent.parent.parent / "reports"
    report_file = reports_dir / f"{output}.py"
    
    if not report_file.exists():
        print(f"[red]Report not found: {report_file}[/red]")
        return
    
    print(f"🚀 Starting marimo report: {output}")
    print(f"📊 Open http://localhost:{port} in your browser")
    
    try:
        subprocess.run([
            "marimo", "run", str(report_file), "--port", str(port)
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"[red]Error running report: {e}[/red]")
    except FileNotFoundError:
        print("[red]marimo not found. Install with: pip install marimo[/red]")


@app.command()
def suppliers(
    limit: int = typer.Option(20, help="Number of top suppliers to show"),
    baliza_db: Optional[str] = typer.Option(None, help="Path to Baliza database")
):
    """Show top suppliers and their risk analysis."""
    try:
        connector = BalizaDataConnector(baliza_db)
        detector = ContractFraudDetector()
        
        print("🏢 Loading supplier data...")
        
        # Get all contracts for supplier analysis
        df = connector.get_contracts()
        
        if df.empty:
            print("[yellow]No contract data found.[/yellow]")
            return
        
        # Analyze supplier risks
        supplier_risks = detector.analyze_supplier_risk(df)
        top_suppliers = supplier_risks.head(limit)
        
        # Display results
        table = Table(title=f"🏆 Top {limit} Suppliers by Risk Score")
        table.add_column("Supplier", style="cyan")
        table.add_column("Contracts", style="green")
        table.add_column("Total Value (R$ M)", style="yellow")
        table.add_column("Agencies", style="blue")
        table.add_column("States", style="magenta")
        table.add_column("Risk Score", style="red")
        
        for _, supplier in top_suppliers.iterrows():
            table.add_row(
                supplier['fornecedor_nome'][:40] + "..." if len(supplier['fornecedor_nome']) > 40 else supplier['fornecedor_nome'],
                f"{supplier['total_contratos']:,}",
                f"{supplier['valor_total']/1000000:.2f}",
                f"{supplier['orgaos_diferentes']}",
                f"{supplier['ufs_diferentes']}",
                f"{supplier['risk_score']}"
            )
        
        console.print(table)
        
    except Exception as e:
        print(f"[red]Error: {e}[/red]")


if __name__ == "__main__":
    app()