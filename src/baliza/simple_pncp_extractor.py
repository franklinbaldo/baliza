#!/usr/bin/env python3
"""
Simple PNCP Data Extractor - Major Refactor Version
Simplified script that iterates through all available PNCP endpoints extracting all data
and stores it in a new PSA (Persistent Staging Area) with raw responses.
"""

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import time

import duckdb
import httpx
import typer
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
from rich.panel import Panel

console = Console()

# Configuration
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta"
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1.0  # seconds between requests
USER_AGENT = "BALIZA/2.0 (Backup Aberto de Licitacoes)"

# Data directory
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"

# All authentication-free endpoints from OpenAPI analysis
PNCP_ENDPOINTS = [
    {
        "name": "contratos_publicacao",
        "path": "/v1/contratos",
        "description": "Contratos por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["cnpjOrgao", "codigoUnidadeAdministrativa", "usuarioId"],
        "max_page_size": 500,
        "default_page_size": 100,
        "supports_date_range": True
    },
    {
        "name": "contratos_atualizacao", 
        "path": "/v1/contratos/atualizacao",
        "description": "Contratos por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["cnpjOrgao", "codigoUnidadeAdministrativa", "usuarioId"],
        "max_page_size": 500,
        "default_page_size": 100,
        "supports_date_range": True
    },
    {
        "name": "contratacoes_publicacao",
        "path": "/v1/contratacoes/publicacao",
        "description": "Contratações por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina", "codigoModalidadeContratacao"],
        "optional_params": ["codigoModoDisputa", "uf", "codigoMunicipioIbge", "cnpj", "codigoUnidadeAdministrativa", "idUsuario"],
        "max_page_size": 50,
        "default_page_size": 50,
        "supports_date_range": True,
        "modalidades": [1, 3, 4, 6, 8, 9, 10, 11, 12, 13, 14]  # From enhanced_endpoint_test.py
    },
    {
        "name": "contratacoes_atualizacao",
        "path": "/v1/contratacoes/atualizacao", 
        "description": "Contratações por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina", "codigoModalidadeContratacao"],
        "optional_params": ["codigoModoDisputa", "uf", "codigoMunicipioIbge", "cnpj", "codigoUnidadeAdministrativa", "idUsuario"],
        "max_page_size": 50,
        "default_page_size": 50,
        "supports_date_range": True,
        "modalidades": [1, 3, 4, 6, 8, 9, 10, 11, 12, 13, 14]
    },
    {
        "name": "contratacoes_proposta",
        "path": "/v1/contratacoes/proposta",
        "description": "Contratações com Recebimento de Propostas Aberto",
        "date_params": ["dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["codigoModalidadeContratacao", "uf", "codigoMunicipioIbge", "cnpj", "codigoUnidadeAdministrativa", "idUsuario"],
        "max_page_size": 50,
        "default_page_size": 50,
        "supports_date_range": False
    },
    {
        "name": "atas_periodo",
        "path": "/v1/atas",
        "description": "Atas de Registro de Preço por Período de Vigência",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["idUsuario", "cnpj", "codigoUnidadeAdministrativa"],
        "max_page_size": 500,
        "default_page_size": 100,
        "supports_date_range": True
    },
    {
        "name": "atas_atualizacao",
        "path": "/v1/atas/atualizacao",
        "description": "Atas por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["idUsuario", "cnpj", "codigoUnidadeAdministrativa"],
        "max_page_size": 500,
        "default_page_size": 100,
        "supports_date_range": True
    },
    {
        "name": "instrumentos_cobranca",
        "path": "/v1/instrumentoscobranca/inclusao",
        "description": "Instrumentos de Cobrança por Data de Inclusão",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["tipoInstrumentoCobranca", "cnpjOrgao"],
        "max_page_size": 100,
        "default_page_size": 100,
        "supports_date_range": True
    },
    {
        "name": "pca_atualizacao",
        "path": "/v1/pca/atualizacao",
        "description": "PCA por Data de Atualização Global",
        "date_params": ["dataInicio", "dataFim"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["cnpj", "codigoUnidade"],
        "max_page_size": 500,
        "default_page_size": 100,
        "supports_date_range": True
    }
]


class SimplePNCPExtractor:
    """Simplified PNCP data extractor that stores all responses as raw data."""
    
    def __init__(self, base_url: str = PNCP_BASE_URL):
        self.base_url = base_url
        self.client = httpx.Client(
            base_url=base_url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT}
        )
        self.run_id = str(uuid.uuid4())
        self._init_database()
        
    def _init_database(self):
        """Initialize database with simplified PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        self.conn = duckdb.connect(str(BALIZA_DB_PATH))
        
        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")
        
        # Create simplified raw responses table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
                -- Response metadata
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Request information
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                http_method VARCHAR DEFAULT 'GET',
                request_parameters JSON,
                
                -- Response information
                response_code INTEGER NOT NULL,
                response_content TEXT,
                response_headers JSON,
                
                -- Processing metadata
                data_date DATE,
                run_id VARCHAR,
                endpoint_type VARCHAR,
                modalidade INTEGER,
                
                -- Additional metadata
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER
            )
        """)
        
        # Create indexes for performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_endpoint_url ON psa.pncp_raw_responses(endpoint_url)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_data_date ON psa.pncp_raw_responses(data_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_response_code ON psa.pncp_raw_responses(response_code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_extracted_at ON psa.pncp_raw_responses(extracted_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_endpoint_name ON psa.pncp_raw_responses(endpoint_name)")
        
    def _format_date(self, date_obj: datetime) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")
        
    def _make_request(self, endpoint: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Make HTTP request to PNCP API endpoint."""
        try:
            time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
            
            response = self.client.get(endpoint["path"], params=params)
            
            # Parse response content if possible
            response_content = response.text
            total_records = 0
            total_pages = 0
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    total_records = json_data.get("totalRegistros", 0)
                    total_pages = json_data.get("totalPaginas", 0)
                except Exception:
                    pass
            
            return {
                "status_code": response.status_code,
                "content": response_content,
                "headers": dict(response.headers),
                "total_records": total_records,
                "total_pages": total_pages
            }
            
        except Exception as e:
            return {
                "status_code": 0,
                "content": f"Error: {str(e)}",
                "headers": {},
                "total_records": 0,
                "total_pages": 0
            }
    
    def _store_response(self, endpoint: Dict[str, Any], params: Dict[str, Any], 
                       response_data: Dict[str, Any], data_date: datetime, modalidade: Optional[int] = None):
        """Store raw response in PSA."""
        endpoint_url = f"{self.base_url}{endpoint['path']}"
        
        self.conn.execute("""
            INSERT INTO psa.pncp_raw_responses (
                endpoint_url, endpoint_name, http_method, request_parameters,
                response_code, response_content, response_headers,
                data_date, run_id, endpoint_type, modalidade,
                total_records, total_pages, current_page, page_size
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            endpoint_url,
            endpoint["name"],
            "GET",
            json.dumps(params),
            response_data["status_code"],
            response_data["content"],
            json.dumps(response_data["headers"]),
            data_date.date(),
            self.run_id,
            endpoint["name"].split("_")[0],  # Extract type: contratos, contratacoes, atas, etc.
            modalidade,
            response_data["total_records"],
            response_data["total_pages"],
            params.get("pagina", 1),
            params.get("tamanhoPagina", endpoint["default_page_size"])
        ])
        
    def extract_endpoint_data(self, endpoint: Dict[str, Any], data_date: datetime) -> Dict[str, Any]:
        """Extract all data from a single endpoint for a specific date."""
        results = {
            "endpoint": endpoint["name"],
            "date": data_date.date(),
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "modalidades_processed": []
        }
        
        # Handle endpoints that require modalidade iteration
        if "modalidades" in endpoint:
            for modalidade in endpoint["modalidades"]:
                modalidade_results = self._extract_endpoint_modalidade(endpoint, data_date, modalidade)
                results["total_requests"] += modalidade_results["total_requests"]
                results["total_records"] += modalidade_results["total_records"]
                results["success_requests"] += modalidade_results["success_requests"]
                results["error_requests"] += modalidade_results["error_requests"]
                results["modalidades_processed"].append({
                    "modalidade": modalidade,
                    "records": modalidade_results["total_records"]
                })
        else:
            # Regular endpoint without modalidade
            modalidade_results = self._extract_endpoint_modalidade(endpoint, data_date, None)
            results["total_requests"] += modalidade_results["total_requests"]
            results["total_records"] += modalidade_results["total_records"]
            results["success_requests"] += modalidade_results["success_requests"]
            results["error_requests"] += modalidade_results["error_requests"]
            
        return results
    
    def _extract_endpoint_modalidade(self, endpoint: Dict[str, Any], data_date: datetime, modalidade: Optional[int]) -> Dict[str, Any]:
        """Extract data from endpoint for a specific modalidade (or no modalidade)."""
        results = {
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0
        }
        
        # Build base parameters
        params = {
            "pagina": 1,
            "tamanhoPagina": endpoint["default_page_size"]
        }
        
        # Add date parameters
        if endpoint["supports_date_range"]:
            if "dataInicio" in endpoint["date_params"]:
                params["dataInicio"] = self._format_date(data_date)
                params["dataFim"] = self._format_date(data_date)
            else:
                params["dataInicial"] = self._format_date(data_date)
                params["dataFinal"] = self._format_date(data_date)
        else:
            # For endpoints like proposta that only use dataFinal
            params["dataFinal"] = self._format_date(data_date)
        
        # Add modalidade if required
        if modalidade is not None:
            params["codigoModalidadeContratacao"] = modalidade
        
        # Make first request to get total pages
        response_data = self._make_request(endpoint, params)
        results["total_requests"] += 1
        
        if response_data["status_code"] == 200:
            results["success_requests"] += 1
            total_pages = response_data["total_pages"]
            results["total_records"] += response_data["total_records"]
        else:
            results["error_requests"] += 1
            total_pages = 1  # Still store the error response
            
        # Store first response
        self._store_response(endpoint, params, response_data, data_date, modalidade)
        
        # Get remaining pages if there are more
        if total_pages > 1:
            for page in range(2, total_pages + 1):
                params["pagina"] = page
                response_data = self._make_request(endpoint, params)
                results["total_requests"] += 1
                
                if response_data["status_code"] == 200:
                    results["success_requests"] += 1
                else:
                    results["error_requests"] += 1
                    
                # Store response
                self._store_response(endpoint, params, response_data, data_date, modalidade)
                
        return results
    
    def extract_all_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Extract data from all endpoints for a date range."""
        console.print(Panel(f"🔄 Starting PNCP Data Extraction", style="bold blue"))
        console.print(f"📅 Date Range: {start_date.date()} to {end_date.date()}")
        console.print(f"🆔 Run ID: {self.run_id}")
        console.print(f"📊 Endpoints: {len(PNCP_ENDPOINTS)}")
        
        total_results = {
            "run_id": self.run_id,
            "start_date": start_date.date(),
            "end_date": end_date.date(),
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "endpoints_processed": [],
            "dates_processed": []
        }
        
        # Process each date
        current_date = start_date
        while current_date <= end_date:
            console.print(f"\n📅 Processing Date: {current_date.date()}")
            
            date_results = {
                "date": current_date.date(),
                "total_requests": 0,
                "total_records": 0,
                "success_requests": 0,
                "error_requests": 0,
                "endpoints": []
            }
            
            # Process each endpoint
            with Progress() as progress:
                task = progress.add_task(f"[green]Processing {current_date.date()}", total=len(PNCP_ENDPOINTS))
                
                for endpoint in PNCP_ENDPOINTS:
                    progress.update(task, description=f"[green]{endpoint['name']}")
                    
                    endpoint_results = self.extract_endpoint_data(endpoint, current_date)
                    
                    date_results["total_requests"] += endpoint_results["total_requests"]
                    date_results["total_records"] += endpoint_results["total_records"]
                    date_results["success_requests"] += endpoint_results["success_requests"]
                    date_results["error_requests"] += endpoint_results["error_requests"]
                    date_results["endpoints"].append(endpoint_results)
                    
                    progress.advance(task)
            
            total_results["total_requests"] += date_results["total_requests"]
            total_results["total_records"] += date_results["total_records"]
            total_results["success_requests"] += date_results["success_requests"]
            total_results["error_requests"] += date_results["error_requests"]
            total_results["dates_processed"].append(date_results)
            
            current_date += timedelta(days=1)
        
        self._print_summary(total_results)
        return total_results
    
    def _print_summary(self, results: Dict[str, Any]):
        """Print extraction summary."""
        table = Table(title="🔍 PNCP Data Extraction Summary")
        
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        
        table.add_row("Run ID", results["run_id"])
        table.add_row("Date Range", f"{results['start_date']} to {results['end_date']}")
        table.add_row("Total Requests", f"{results['total_requests']:,}")
        table.add_row("Total Records", f"{results['total_records']:,}")
        table.add_row("Success Requests", f"{results['success_requests']:,}")
        table.add_row("Error Requests", f"{results['error_requests']:,}")
        
        success_rate = (results['success_requests'] / results['total_requests'] * 100) if results['total_requests'] > 0 else 0
        table.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(table)
        
        # Database summary
        record_count = self.conn.execute("SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE run_id = ?", [results["run_id"]]).fetchone()[0]
        console.print(Panel(
            f"💾 Database: {record_count:,} responses stored\\n"
            f"📊 Database Path: {BALIZA_DB_PATH}",
            title="Storage Summary",
            style="bold green"
        ))
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get statistics about stored data."""
        stats = {}
        
        # Overall stats
        stats["total_responses"] = self.conn.execute("SELECT COUNT(*) FROM psa.pncp_raw_responses").fetchone()[0]
        stats["unique_endpoints"] = self.conn.execute("SELECT COUNT(DISTINCT endpoint_name) FROM psa.pncp_raw_responses").fetchone()[0]
        stats["date_range"] = self.conn.execute("SELECT MIN(data_date), MAX(data_date) FROM psa.pncp_raw_responses").fetchone()
        
        # Success rate
        success_responses = self.conn.execute("SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200").fetchone()[0]
        stats["success_rate"] = (success_responses / stats["total_responses"] * 100) if stats["total_responses"] > 0 else 0
        
        # Records by endpoint
        endpoint_stats = self.conn.execute("""
            SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
            FROM psa.pncp_raw_responses 
            WHERE response_code = 200
            GROUP BY endpoint_name
            ORDER BY total_records DESC
        """).fetchall()
        
        stats["endpoints"] = [
            {"name": row[0], "responses": row[1], "total_records": row[2]}
            for row in endpoint_stats
        ]
        
        return stats
    
    def __del__(self):
        """Cleanup."""
        if hasattr(self, 'client'):
            self.client.close()
        if hasattr(self, 'conn'):
            self.conn.close()


# CLI interface
app = typer.Typer()

@app.command()
def extract(
    start_date: str = typer.Option(
        (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD)"
    )
):
    """Extract data from all PNCP endpoints for a date range."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        console.print("❌ Invalid date format. Use YYYY-MM-DD", style="bold red")
        raise typer.Exit(1)
    
    if start_dt > end_dt:
        console.print("❌ Start date must be before end date", style="bold red")
        raise typer.Exit(1)
    
    extractor = SimplePNCPExtractor()
    results = extractor.extract_all_data(start_dt, end_dt)
    
    # Save results to file
    results_file = DATA_DIR / f"extraction_results_{results['run_id']}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    console.print(f"📄 Results saved to: {results_file}")

@app.command()
def stats():
    """Show extraction statistics."""
    extractor = SimplePNCPExtractor()
    stats = extractor.get_extraction_stats()
    
    console.print(Panel("📊 PNCP Extraction Statistics", style="bold blue"))
    
    table = Table()
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    
    table.add_row("Total Responses", f"{stats['total_responses']:,}")
    table.add_row("Unique Endpoints", f"{stats['unique_endpoints']:,}")
    table.add_row("Success Rate", f"{stats['success_rate']:.1f}%")
    
    if stats['date_range'][0]:
        table.add_row("Date Range", f"{stats['date_range'][0]} to {stats['date_range'][1]}")
    
    console.print(table)
    
    # Endpoint breakdown
    if stats['endpoints']:
        endpoint_table = Table(title="Records by Endpoint")
        endpoint_table.add_column("Endpoint", style="cyan")
        endpoint_table.add_column("Responses", style="yellow", justify="right")
        endpoint_table.add_column("Total Records", style="green", justify="right")
        
        for endpoint in stats['endpoints']:
            endpoint_table.add_row(
                endpoint['name'],
                f"{endpoint['responses']:,}",
                f"{endpoint['total_records']:,}"
            )
        
        console.print(endpoint_table)

if __name__ == "__main__":
    app()