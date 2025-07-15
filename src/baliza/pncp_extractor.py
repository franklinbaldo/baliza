#!/usr/bin/env python3
"""
Simple PNCP Data Extractor - Major Refactor Version
Simplified script that iterates through all available PNCP endpoints extracting all data
and stores it in a new PSA (Persistent Staging Area) with raw responses.
"""

import asyncio
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
import time

import duckdb
import httpx
import typer
from rich.console import Console
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
# Only keep endpoints that work reliably without 400 errors
PNCP_ENDPOINTS = [
    {
        "name": "contratos_publicacao",
        "path": "/v1/contratos",
        "description": "Contratos por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["cnpjOrgao", "codigoUnidadeAdministrativa", "usuarioId"],
        "max_page_size": 500,
        "default_page_size": 500,
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
        "default_page_size": 500,
        "supports_date_range": True
    },
    {
        "name": "atas_periodo",
        "path": "/v1/atas",
        "description": "Atas de Registro de Preço por Período de Vigência",
        "date_params": ["dataInicial", "dataFinal"],
        "required_params": ["pagina", "tamanhoPagina"],
        "optional_params": ["idUsuario", "cnpj", "codigoUnidadeAdministrativa"],
        "max_page_size": 500,
        "default_page_size": 500,
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
        "default_page_size": 500,
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
        # Add async client for concurrent requests
        self.async_client = None
        self.run_id = str(uuid.uuid4())
        
        # Request tracking for progress and metrics
        self.total_requests_made = 0
        self.total_pages_processed = 0
        self.total_pages_expected = 0
        self.extraction_start_time = None
        
        self._init_database()
        
    def _start_extraction_timer(self):
        """Start the extraction timer for RPS calculations."""
        self.extraction_start_time = time.time()
        self.total_requests_made = 0
        self.total_pages_processed = 0
        self.total_pages_expected = 0
        console.print("📊 [dim]Starting page and RPS tracking...[/dim]")
        
    def _get_rps(self) -> float:
        """Calculate current requests per second."""
        if self.extraction_start_time is None or self.total_requests_made == 0:
            return 0.0
        elapsed = time.time() - self.extraction_start_time
        return self.total_requests_made / elapsed if elapsed > 0 else 0.0
        
    def _get_progress_info(self) -> Dict[str, Any]:
        """Get current progress information."""
        return {
            "pages_processed": self.total_pages_processed,
            "pages_expected": self.total_pages_expected,
            "requests_made": self.total_requests_made,
            "rps": self._get_rps(),
            "progress_percent": (self.total_pages_processed / self.total_pages_expected * 100) if self.total_pages_expected > 0 else 0
        }
        
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
    
    def _check_exact_url_exists(self, endpoint: Dict[str, Any], params: Dict[str, Any]) -> bool:
        """Check if an exact URL with these parameters already exists in PSA."""
        # Generate the exact URL that would be called
        endpoint_url = f"{self.base_url}{endpoint['path']}"
        
        # Check if we have this exact URL and parameters combination
        query = """
            SELECT COUNT(*) 
            FROM psa.pncp_raw_responses 
            WHERE endpoint_url = ? 
            AND json_extract(request_parameters, '$.pagina') = ?
            AND json_extract(request_parameters, '$.tamanhoPagina') = ?
        """
        
        query_params = [endpoint_url, str(params.get('pagina', 1)), str(params.get('tamanhoPagina', 500))]
        
        # Add date parameter checks
        for date_param in endpoint.get("date_params", []):
            if date_param in params:
                query += f" AND json_extract(request_parameters, '$.{date_param}') = ?"
                query_params.append(str(params[date_param]))
        
        # Add modalidade check if applicable
        if 'codigoModalidadeContratacao' in params:
            query += " AND json_extract(request_parameters, '$.codigoModalidadeContratacao') = ?"
            query_params.append(str(params['codigoModalidadeContratacao']))
        
        # Only consider successful responses
        query += " AND response_code = 200"
        
        result = self.conn.execute(query, query_params).fetchone()
        return (result[0] if result else 0) > 0

    def _check_existing_extraction_range(self, endpoint: Dict[str, Any], start_date: datetime, end_date: datetime, modalidade: Optional[int] = None) -> Dict[str, Any]:
        """Check if data for this endpoint/date-range/modalidade combination already exists."""
        # For date ranges, we need to check if we have any overlapping extractions
        # For simplicity, we'll only consider it "complete" if we have the exact same date range
        query = """
            SELECT 
                COUNT(*) as total_responses,
                COUNT(CASE WHEN response_code = 200 THEN 1 END) as success_responses,
                SUM(CASE WHEN response_code = 200 THEN total_records ELSE 0 END) as total_records,
                MAX(CASE WHEN response_code = 200 THEN total_pages ELSE 0 END) as max_total_pages,
                COUNT(DISTINCT CASE WHEN response_code = 200 THEN current_page END) as unique_pages_fetched
            FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? 
            AND request_parameters LIKE ?
        """
        
        # Create a pattern to match the date range in request_parameters JSON
        # This is a simplified approach - in production, you'd want more robust JSON querying
        date_pattern = f'%"dataInicial":"{self._format_date(start_date)}"%"dataFinal":"{self._format_date(end_date)}"%'
        if "dataInicio" in endpoint.get("date_params", []):
            date_pattern = f'%"dataInicio":"{self._format_date(start_date)}"%"dataFim":"{self._format_date(end_date)}"%'
        
        params = [endpoint["name"], date_pattern]
        
        # Add modalidade filter if applicable
        if modalidade is not None:
            query += " AND modalidade = ?"
            params.append(modalidade)
        else:
            query += " AND modalidade IS NULL"
        
        result = self.conn.execute(query, params).fetchone()
        
        return {
            "total_responses": result[0] or 0,
            "success_responses": result[1] or 0,
            "total_records": result[2] or 0,
            "max_total_pages": result[3] or 0,
            "unique_pages_fetched": result[4] or 0,
            "is_complete": False  # Will be determined by caller
        }

    def _check_existing_extraction(self, endpoint: Dict[str, Any], data_date: datetime, modalidade: Optional[int] = None) -> Dict[str, Any]:
        """Check if data for this endpoint/date/modalidade combination already exists."""
        # Build the query to check existing data
        query = """
            SELECT 
                COUNT(*) as total_responses,
                COUNT(CASE WHEN response_code = 200 THEN 1 END) as success_responses,
                SUM(CASE WHEN response_code = 200 THEN total_records ELSE 0 END) as total_records,
                MAX(CASE WHEN response_code = 200 THEN total_pages ELSE 0 END) as max_total_pages,
                COUNT(DISTINCT CASE WHEN response_code = 200 THEN current_page END) as unique_pages_fetched
            FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? 
            AND data_date = ?
        """
        params = [endpoint["name"], data_date.date()]
        
        # Add modalidade filter if applicable
        if modalidade is not None:
            query += " AND modalidade = ?"
            params.append(modalidade)
        else:
            query += " AND modalidade IS NULL"
        
        result = self.conn.execute(query, params).fetchone()
        
        return {
            "total_responses": result[0] or 0,
            "success_responses": result[1] or 0,
            "total_records": result[2] or 0,
            "max_total_pages": result[3] or 0,
            "unique_pages_fetched": result[4] or 0,
            "is_complete": False  # Will be determined by caller
        }
        
    async def _init_async_client(self):
        """Initialize async HTTP client."""
        if self.async_client is None:
            self.async_client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
            )

    async def _make_request_async(self, endpoint: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Make async HTTP request to PNCP API endpoint."""
        try:
            # Rate limiting with async sleep
            await asyncio.sleep(RATE_LIMIT_DELAY)
            
            if self.async_client is None:
                await self._init_async_client()
            
            # Track request
            self.total_requests_made += 1
            
            response = await self.async_client.get(endpoint["path"], params=params)
            
            # Track page processed if successful
            if response.status_code == 200:
                self.total_pages_processed += 1
            
            # Log error responses with full request details for debugging
            if response.status_code >= 400:
                full_url = f"{self.base_url}{endpoint['path']}"
                param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                console.print(f"    ❌ [red]HTTP {response.status_code} - {endpoint['name']}[/red]")
                console.print(f"    🌐 [dim red]Full URL: {full_url}?{param_str}[/dim red]")
                console.print(f"    📋 [dim red]Parameters: {params}[/dim red]")
                if response.text and len(response.text) < 500:
                    console.print(f"    📄 [dim red]Response: {response.text[:200]}[/dim red]")
            
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
            full_url = f"{self.base_url}{endpoint['path']}"
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            console.print(f"    💥 [red]Request Exception - {endpoint['name']}[/red]")
            console.print(f"    🌐 [dim red]Full URL: {full_url}?{param_str}[/dim red]")
            console.print(f"    📋 [dim red]Parameters: {params}[/dim red]")
            console.print(f"    ⚠️ [dim red]Error: {str(e)}[/dim red]")
            
            return {
                "status_code": 0,
                "content": f"Error: {str(e)}",
                "headers": {},
                "total_records": 0,
                "total_pages": 0
            }

    def _make_request(self, endpoint: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Make HTTP request to PNCP API endpoint."""
        try:
            time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
            
            # Track request
            self.total_requests_made += 1
            
            response = self.client.get(endpoint["path"], params=params)
            
            # Track page processed if successful
            if response.status_code == 200:
                self.total_pages_processed += 1
            
            # Log error responses with full request details for debugging
            if response.status_code >= 400:
                full_url = f"{self.base_url}{endpoint['path']}"
                param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                console.print(f"    ❌ [red]HTTP {response.status_code} - {endpoint['name']}[/red]")
                console.print(f"    🌐 [dim red]Full URL: {full_url}?{param_str}[/dim red]")
                console.print(f"    📋 [dim red]Parameters: {params}[/dim red]")
                if response.text and len(response.text) < 500:
                    console.print(f"    📄 [dim red]Response: {response.text[:200]}[/dim red]")
            
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
            full_url = f"{self.base_url}{endpoint['path']}"
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            console.print(f"    💥 [red]Request Exception - {endpoint['name']}[/red]")
            console.print(f"    🌐 [dim red]Full URL: {full_url}?{param_str}[/dim red]")
            console.print(f"    📋 [dim red]Parameters: {params}[/dim red]")
            console.print(f"    ⚠️ [dim red]Error: {str(e)}[/dim red]")
            
            return {
                "status_code": 0,
                "content": f"Error: {str(e)}",
                "headers": {},
                "total_records": 0,
                "total_pages": 0
            }
    
    def _store_response_range(self, endpoint: Dict[str, Any], params: Dict[str, Any], 
                            response_data: Dict[str, Any], start_date: datetime, end_date: datetime, modalidade: Optional[int] = None):  # noqa: ARG002
        """Store raw response in PSA with date range metadata."""
        endpoint_url = f"{self.base_url}{endpoint['path']}"
        
        # For date ranges, we'll use the start_date as the primary data_date 
        # but store the full range in request_parameters
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
            start_date.date(),  # Use start_date as primary date
            self.run_id,
            endpoint["name"].split("_")[0],  # Extract type: contratos, contratacoes, atas, etc.
            modalidade,
            response_data["total_records"],
            response_data["total_pages"],
            params.get("pagina", 1),
            params.get("tamanhoPagina", endpoint["default_page_size"])
        ])

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
        
    async def extract_endpoint_date_range(self, endpoint: Dict[str, Any], start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Extract all data from a single endpoint for a date range with concurrent modalidade processing."""
        results = {
            "endpoint": endpoint["name"],
            "start_date": start_date.date(),
            "end_date": end_date.date(),
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "modalidades_processed": [],
            "skipped_count": 0,
            "resumed_count": 0
        }
        
        # Handle endpoints based on modalidade strategy
        if "modalidades" in endpoint:
            modalidade_strategy = endpoint.get("modalidade_strategy", "required_individual")
            
            if modalidade_strategy == "optional_unrestricted":
                # Try unrestricted extraction first (no modalidade parameter)
                console.print(f"    🔍 [blue]Trying unrestricted extraction for {endpoint['name']}[/blue]")
                unrestricted_results = await self._extract_endpoint_modalidade_range_async(endpoint, start_date, end_date, None)
                
                if unrestricted_results["success_requests"] > 0 and unrestricted_results["total_records"] > 0:
                    # Unrestricted extraction succeeded and has data
                    console.print(f"    ✅ [green]Unrestricted extraction successful: {unrestricted_results['total_records']:,} records[/green]")
                    results["total_requests"] += unrestricted_results["total_requests"]
                    results["total_records"] += unrestricted_results["total_records"]
                    results["success_requests"] += unrestricted_results["success_requests"]
                    results["error_requests"] += unrestricted_results["error_requests"]
                    
                    if unrestricted_results.get("skipped"):
                        results["skipped_count"] += 1
                    if unrestricted_results.get("resumed"):
                        results["resumed_count"] += 1
                        
                    results["modalidades_processed"].append({
                        "modalidade": "unrestricted",
                        "records": unrestricted_results["total_records"],
                        "skipped": unrestricted_results.get("skipped", False),
                        "resumed": unrestricted_results.get("resumed", False)
                    })
                else:
                    # Unrestricted failed, fallback to individual modalidades
                    console.print(f"    ⚠️ [yellow]Unrestricted extraction failed (success: {unrestricted_results['success_requests']}, records: {unrestricted_results['total_records']}, errors: {unrestricted_results['error_requests']}), falling back to individual modalidades[/yellow]")
                    await self._process_individual_modalidades(endpoint, start_date, end_date, results)
            else:
                # Default: process individual modalidades
                await self._process_individual_modalidades(endpoint, start_date, end_date, results)
        else:
            # Regular endpoint without modalidade
            modalidade_results = await self._extract_endpoint_modalidade_range_async(endpoint, start_date, end_date, None)
            results["total_requests"] += modalidade_results["total_requests"]
            results["total_records"] += modalidade_results["total_records"]
            results["success_requests"] += modalidade_results["success_requests"]
            results["error_requests"] += modalidade_results["error_requests"]
            
            if modalidade_results.get("skipped"):
                results["skipped_count"] += 1
            if modalidade_results.get("resumed"):
                results["resumed_count"] += 1
            
        return results

    async def _process_individual_modalidades(self, endpoint: Dict[str, Any], start_date: datetime, end_date: datetime, results: Dict[str, Any]):
        """Process modalidades individually with concurrent execution."""
        # Process modalidades concurrently
        modalidade_tasks = []
        for modalidade in endpoint["modalidades"]:
            task = self._extract_endpoint_modalidade_range_async(endpoint, start_date, end_date, modalidade)
            modalidade_tasks.append(task)
        
        # Wait for all modalidades to complete
        modalidade_results_list = await asyncio.gather(*modalidade_tasks, return_exceptions=True)
        
        # Process results
        for i, modalidade_results in enumerate(modalidade_results_list):
            modalidade = endpoint["modalidades"][i]
            
            # Handle exceptions
            if isinstance(modalidade_results, Exception):
                console.print(f"❌ Error processing modalidade {modalidade}: {modalidade_results}")
                results["error_requests"] += 1
                continue
            
            results["total_requests"] += modalidade_results["total_requests"]
            results["total_records"] += modalidade_results["total_records"]
            results["success_requests"] += modalidade_results["success_requests"]
            results["error_requests"] += modalidade_results["error_requests"]
            
            if modalidade_results.get("skipped"):
                results["skipped_count"] += 1
            if modalidade_results.get("resumed"):
                results["resumed_count"] += 1
                
            results["modalidades_processed"].append({
                "modalidade": modalidade,
                "records": modalidade_results["total_records"],
                "skipped": modalidade_results.get("skipped", False),
                "resumed": modalidade_results.get("resumed", False)
            })

    def extract_endpoint_data(self, endpoint: Dict[str, Any], data_date: datetime) -> Dict[str, Any]:
        """Extract all data from a single endpoint for a specific date."""
        results = {
            "endpoint": endpoint["name"],
            "date": data_date.date(),
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "modalidades_processed": [],
            "skipped_count": 0,
            "resumed_count": 0
        }
        
        # Handle endpoints that require modalidade iteration
        if "modalidades" in endpoint:
            for modalidade in endpoint["modalidades"]:
                modalidade_results = self._extract_endpoint_modalidade(endpoint, data_date, modalidade)
                results["total_requests"] += modalidade_results["total_requests"]
                results["total_records"] += modalidade_results["total_records"]
                results["success_requests"] += modalidade_results["success_requests"]
                results["error_requests"] += modalidade_results["error_requests"]
                
                if modalidade_results.get("skipped"):
                    results["skipped_count"] += 1
                if modalidade_results.get("resumed"):
                    results["resumed_count"] += 1
                    
                results["modalidades_processed"].append({
                    "modalidade": modalidade,
                    "records": modalidade_results["total_records"],
                    "skipped": modalidade_results.get("skipped", False),
                    "resumed": modalidade_results.get("resumed", False)
                })
        else:
            # Regular endpoint without modalidade
            modalidade_results = self._extract_endpoint_modalidade(endpoint, data_date, None)
            results["total_requests"] += modalidade_results["total_requests"]
            results["total_records"] += modalidade_results["total_records"]
            results["success_requests"] += modalidade_results["success_requests"]
            results["error_requests"] += modalidade_results["error_requests"]
            
            if modalidade_results.get("skipped"):
                results["skipped_count"] += 1
            if modalidade_results.get("resumed"):
                results["resumed_count"] += 1
            
        return results

    def _extract_endpoint_modalidade_range(self, endpoint: Dict[str, Any], start_date: datetime, end_date: datetime, modalidade: Optional[int], force: bool = False) -> Dict[str, Any]:
        """Extract data from endpoint for a specific modalidade (or no modalidade) using date range."""
        results = {
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "skipped": False,
            "resumed": False
        }
        
        # Check if we already have complete data for this combination
        existing = self._check_existing_extraction_range(endpoint, start_date, end_date, modalidade)
        
        # Build base parameters for date range
        params = {
            "pagina": 1,
            "tamanhoPagina": endpoint["default_page_size"]
        }
        
        # Add date parameters for the full range
        if endpoint["supports_date_range"]:
            if "dataInicio" in endpoint["date_params"]:
                params["dataInicio"] = self._format_date(start_date)
                params["dataFim"] = self._format_date(end_date)
            else:
                params["dataInicial"] = self._format_date(start_date)
                params["dataFinal"] = self._format_date(end_date)
        else:
            # For endpoints like proposta that only use dataFinal, use end_date
            params["dataFinal"] = self._format_date(end_date)
        
        # Add modalidade if required
        if modalidade is not None:
            params["codigoModalidadeContratacao"] = modalidade
        
        # Check if this exact date range extraction is complete (unless force=True)
        if not force and existing["success_responses"] > 0:
            max_pages = existing["max_total_pages"]
            pages_fetched = existing["unique_pages_fetched"]
            
            if max_pages > 0 and pages_fetched >= max_pages:
                console.print(f"    ✅ [cyan]Skipping {endpoint['name']} {start_date.date()}-{end_date.date()}" + 
                            (f" modalidade={modalidade}" if modalidade else "") + 
                            f" - already complete ({pages_fetched}/{max_pages} pages)[/cyan]")
                results["skipped"] = True
                results["total_records"] = existing["total_records"]
                results["success_requests"] = existing["success_responses"]
                return results
        
        # If we have no existing data or incomplete data, extract fresh
        # Make first request to get total pages
        response_data = self._make_request(endpoint, params)
        results["total_requests"] += 1
        
        if response_data["status_code"] == 200:
            results["success_requests"] += 1
            total_pages = response_data["total_pages"]
            results["total_records"] = response_data["total_records"]
            
            # Track expected pages for progress display
            self.total_pages_expected += total_pages
            
            # Show progress for date range with URL pattern
            endpoint_url = f"{PNCP_BASE_URL}{endpoint['path']}"
            sample_params = {k: v for k, v in params.items() if k != 'pagina'}
            sample_params['pagina'] = '[1-N]'
            url_pattern = f"{endpoint_url}?{'&'.join([f'{k}={v}' for k, v in sample_params.items()])}"
            
            console.print(f"    🔄 [green]Extracting {endpoint['name']} {start_date.date()}-{end_date.date()}" + 
                        (f" modalidade={modalidade}" if modalidade else "") + 
                        f" - {total_pages} pages, {results['total_records']:,} records[/green]")
            console.print(f"    🌐 [dim cyan]URL: {url_pattern}[/dim cyan]")
        else:
            results["error_requests"] += 1
            total_pages = 1  # Still store the error response
            
        # Store first response with date range metadata
        self._store_response_range(endpoint, params, response_data, start_date, end_date, modalidade)
        
        # Get remaining pages if there are more - process concurrently
        if total_pages > 1:
            # Process pages concurrently for better performance
            remaining_pages = list(range(2, total_pages + 1))
            concurrent_results = asyncio.run(self._fetch_pages_concurrently(
                endpoint, params, remaining_pages, start_date, end_date, modalidade
            ))
            
            # Aggregate results from concurrent processing
            results["total_requests"] += concurrent_results["total_requests"]
            results["success_requests"] += concurrent_results["success_requests"]
            results["error_requests"] += concurrent_results["error_requests"]
                
        return results

    async def _extract_endpoint_modalidade_range_async(self, endpoint: Dict[str, Any], start_date: datetime, end_date: datetime, modalidade: Optional[int], force: bool = False) -> Dict[str, Any]:
        """Async version of _extract_endpoint_modalidade_range for concurrent modalidade processing."""
        results = {
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "skipped": False,
            "resumed": False
        }
        
        # Check if we already have complete data for this combination
        existing = self._check_existing_extraction_range(endpoint, start_date, end_date, modalidade)
        
        # Build base parameters for date range
        params = {
            "pagina": 1,
            "tamanhoPagina": endpoint["default_page_size"]
        }
        
        # Add date parameters for the full range
        if endpoint["supports_date_range"]:
            if "dataInicio" in endpoint["date_params"]:
                params["dataInicio"] = self._format_date(start_date)
                params["dataFim"] = self._format_date(end_date)
            else:
                params["dataInicial"] = self._format_date(start_date)
                params["dataFinal"] = self._format_date(end_date)
        else:
            # For endpoints like proposta that only use dataFinal, use end_date
            params["dataFinal"] = self._format_date(end_date)
        
        # Add modalidade if required
        if modalidade is not None:
            params["codigoModalidadeContratacao"] = modalidade
        
        # Check if this exact date range extraction is complete (unless force=True)
        if not force and existing["success_responses"] > 0:
            max_pages = existing["max_total_pages"]
            pages_fetched = existing["unique_pages_fetched"]
            
            if max_pages > 0 and pages_fetched >= max_pages:
                console.print(f"    ✅ [cyan]Skipping {endpoint['name']} {start_date.date()}-{end_date.date()}" + 
                            (f" modalidade={modalidade}" if modalidade else "") + 
                            f" - already complete ({pages_fetched}/{max_pages} pages)[/cyan]")
                results["skipped"] = True
                results["total_records"] = existing["total_records"]
                results["success_requests"] = existing["success_responses"]
                return results
        
        # If force=True, show that we're re-extracting
        if force and existing["success_responses"] > 0:
            console.print(f"    🔄 [yellow]Force re-extracting {endpoint['name']} {start_date.date()}-{end_date.date()}" + 
                        (f" modalidade={modalidade}" if modalidade else "") + 
                        " - overriding existing data[/yellow]")
        
        # If we have no existing data or incomplete data, extract fresh
        # Make first request to get total pages (async version)
        response_data = await self._make_request_async(endpoint, params)
        results["total_requests"] += 1
        
        if response_data["status_code"] == 200:
            results["success_requests"] += 1
            total_pages = response_data["total_pages"]
            results["total_records"] = response_data["total_records"]
            
            # Track expected pages for progress display
            self.total_pages_expected += total_pages
            
            # Show progress for date range with URL pattern
            endpoint_url = f"{PNCP_BASE_URL}{endpoint['path']}"
            sample_params = {k: v for k, v in params.items() if k != 'pagina'}
            sample_params['pagina'] = '[1-N]'
            url_pattern = f"{endpoint_url}?{'&'.join([f'{k}={v}' for k, v in sample_params.items()])}"
            
            console.print(f"    🔄 [green]Extracting {endpoint['name']} {start_date.date()}-{end_date.date()}" + 
                        (f" modalidade={modalidade}" if modalidade else "") + 
                        f" - {total_pages} pages, {results['total_records']:,} records[/green]")
            console.print(f"    🌐 [dim cyan]URL: {url_pattern}[/dim cyan]")
        else:
            results["error_requests"] += 1
            total_pages = 1  # Still store the error response
            
        # Store first response with date range metadata
        self._store_response_range(endpoint, params, response_data, start_date, end_date, modalidade)
        
        # Get remaining pages if there are more - process concurrently
        if total_pages > 1:
            # Process pages concurrently for better performance
            remaining_pages = list(range(2, total_pages + 1))
            concurrent_results = await self._fetch_pages_concurrently(
                endpoint, params, remaining_pages, start_date, end_date, modalidade
            )
            
            # Aggregate results from concurrent processing
            results["total_requests"] += concurrent_results["total_requests"]
            results["success_requests"] += concurrent_results["success_requests"]
            results["error_requests"] += concurrent_results["error_requests"]
                
        return results

    async def _fetch_pages_concurrently(self, endpoint: Dict[str, Any], base_params: Dict[str, Any], 
                                       page_numbers: List[int], start_date: datetime, end_date: datetime, 
                                       modalidade: Optional[int] = None, max_concurrent: int = 5) -> Dict[str, Any]:
        """Fetch multiple pages concurrently with controlled concurrency."""
        results = {
            "total_requests": 0,
            "success_requests": 0,
            "error_requests": 0
        }
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_single_page(page_num: int) -> Dict[str, Any]:
            async with semaphore:
                # Create new params dict for this page
                params = base_params.copy()
                params["pagina"] = page_num
                
                # Make async request
                response_data = await self._make_request_async(endpoint, params)
                
                # Store response (note: this is still synchronous database write)
                self._store_response_range(endpoint, params, response_data, start_date, end_date, modalidade)
                
                return {
                    "status_code": response_data["status_code"],
                    "page": page_num
                }
        
        # Execute all page requests concurrently
        tasks = [fetch_single_page(page) for page in page_numbers]
        page_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results
        for result in page_results:
            results["total_requests"] += 1
            if isinstance(result, dict) and result.get("status_code") == 200:
                results["success_requests"] += 1
            else:
                results["error_requests"] += 1
        
        return results
    
    def _extract_endpoint_modalidade(self, endpoint: Dict[str, Any], data_date: datetime, modalidade: Optional[int], force: bool = False) -> Dict[str, Any]:
        """Extract data from endpoint for a specific modalidade (or no modalidade)."""
        results = {
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "skipped": False,
            "resumed": False
        }
        
        # Check if we already have complete data for this combination
        existing = self._check_existing_extraction(endpoint, data_date, modalidade)
        
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
        
        # If force=True, show that we're re-extracting
        if force and existing["success_responses"] > 0:
            console.print(f"    🔄 [yellow]Force re-extracting {endpoint['name']} {data_date.date()}" + 
                        (f" modalidade={modalidade}" if modalidade else "") + 
                        " - overriding existing data[/yellow]")
        
        # If we have no existing data, or only error responses, start fresh (or if force=True)
        if existing["success_responses"] == 0 or force:
            # Make first request to get total pages
            response_data = self._make_request(endpoint, params)
            results["total_requests"] += 1
            
            if response_data["status_code"] == 200:
                results["success_requests"] += 1
                total_pages = response_data["total_pages"]
                results["total_records"] = response_data["total_records"]
            else:
                results["error_requests"] += 1
                total_pages = 1  # Still store the error response
                
            # Store first response
            self._store_response(endpoint, params, response_data, data_date, modalidade)
            
            start_page = 2
        else:
            # We have some existing data - check if it's complete
            max_pages = existing["max_total_pages"]
            pages_fetched = existing["unique_pages_fetched"]
            
            # If we have all pages, skip this extraction entirely
            if max_pages > 0 and pages_fetched >= max_pages:
                console.print(f"    ✅ [cyan]Skipping {endpoint['name']} {data_date.date()}" + 
                            (f" modalidade={modalidade}" if modalidade else "") + 
                            f" - already complete ({pages_fetched}/{max_pages} pages)[/cyan]")
                results["skipped"] = True
                results["total_records"] = existing["total_records"]
                results["success_requests"] = existing["success_responses"]
                return results
            
            # Incomplete extraction - resume from where we left off
            total_pages = max_pages
            results["total_records"] = existing["total_records"]
            results["success_requests"] = existing["success_responses"]
            results["resumed"] = True
            
            # Find which pages we still need
            existing_pages_query = """
                SELECT DISTINCT current_page 
                FROM psa.pncp_raw_responses 
                WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
            """
            existing_params = [endpoint["name"], data_date.date()]
            if modalidade is not None:
                existing_pages_query += " AND modalidade = ?"
                existing_params.append(modalidade)
            else:
                existing_pages_query += " AND modalidade IS NULL"
            
            existing_pages = {row[0] for row in self.conn.execute(existing_pages_query, existing_params).fetchall()}
            missing_pages = [p for p in range(1, total_pages + 1) if p not in existing_pages]
            
            if not missing_pages:
                console.print(f"    ✅ [cyan]Skipping {endpoint['name']} {data_date.date()}" + 
                            (f" modalidade={modalidade}" if modalidade else "") + 
                            f" - already complete ({pages_fetched}/{max_pages} pages)[/cyan]")
                results["skipped"] = True
                return results
            
            console.print(f"    🔄 [yellow]Resuming {endpoint['name']} {data_date.date()}" + 
                        (f" modalidade={modalidade}" if modalidade else "") + 
                        f" - fetching {len(missing_pages)} missing pages[/yellow]")
            
            start_page = min(missing_pages)
            
            # If we need page 1, make the first request
            if 1 in missing_pages:
                response_data = self._make_request(endpoint, params)
                results["total_requests"] += 1
                
                if response_data["status_code"] == 200:
                    results["success_requests"] += 1
                    # Update total_pages from fresh response
                    total_pages = response_data["total_pages"]
                else:
                    results["error_requests"] += 1
                    
                # Store first response
                self._store_response(endpoint, params, response_data, data_date, modalidade)
                
                missing_pages.remove(1)
            
            # Update missing pages list if total_pages changed
            if total_pages != max_pages:
                missing_pages = [p for p in range(start_page, total_pages + 1) if p not in existing_pages]
        
        # Get remaining pages
        remaining_pages = list(range(start_page, total_pages + 1)) if not results["resumed"] else missing_pages
        
        for page in remaining_pages:
            if page == 1:  # Already handled above
                continue
                
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
    
    def _extract_endpoint_modalidade_single(self, endpoint: Dict[str, Any], data_date: datetime, modalidade: Optional[int], force: bool = False) -> Dict[str, Any]:
        """Extract data from endpoint for a single date."""
        return self._extract_endpoint_modalidade(endpoint, data_date, modalidade, force)
    
    async def extract_all_data(self, start_date: datetime, end_date: datetime, force: bool = False) -> Dict[str, Any]:
        """Extract data from all endpoints for a date range."""
        console.print(Panel("🔄 Starting PNCP Data Extraction", style="bold blue"))
        console.print(f"📅 Date Range: {start_date.date()} to {end_date.date()}")
        console.print(f"🆔 Run ID: {self.run_id}")
        console.print(f"📊 Endpoints: {len(PNCP_ENDPOINTS)}")
        if force:
            console.print("⚠️ [yellow]Force mode enabled - will re-extract existing data[/yellow]")
        
        # Start extraction timer for RPS tracking
        self._start_extraction_timer()
        
        total_results = {
            "run_id": self.run_id,
            "start_date": start_date.date(),
            "end_date": end_date.date(),
            "total_requests": 0,
            "total_records": 0,
            "success_requests": 0,
            "error_requests": 0,
            "endpoints_processed": [],
            "dates_processed": [],
            "skipped_extractions": 0,
            "resumed_extractions": 0
        }
        
        # Calculate total date range chunks for progress bar
        total_chunks = 0
        temp_start = start_date
        while temp_start <= end_date:
            temp_end = min(temp_start + timedelta(days=364), end_date)
            total_chunks += 1
            temp_start = temp_end + timedelta(days=1)
        
        # Process in date range chunks (up to 365 days per chunk) with progress bar
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, MofNCompleteColumn
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=False
        ) as progress:
            
            # Main date range progress bar
            date_range_task = progress.add_task(
                "📅 Processing Date Ranges", 
                total=total_chunks
            )
            
            current_start = start_date
            chunk_number = 1
            
            while current_start <= end_date:
                # Calculate chunk end date (max 365 days or end_date, whichever is smaller)
                chunk_end = min(current_start + timedelta(days=364), end_date)  # 364 + 1 = 365 days
                days_in_chunk = (chunk_end - current_start).days + 1
                
                progress.update(date_range_task, description=f"📅 Processing Chunk {chunk_number}: {current_start.date()} to {chunk_end.date()} ({days_in_chunk} days)")
                
                chunk_results = {
                    "chunk_number": chunk_number,
                    "start_date": current_start.date(),
                    "end_date": chunk_end.date(),
                    "days_in_chunk": days_in_chunk,
                    "total_requests": 0,
                    "total_records": 0,
                    "success_requests": 0,
                    "error_requests": 0,
                    "endpoints": []
                }
                
                # Process each endpoint for this date range chunk with individual progress bars
                await self._extract_chunk_with_progress_bars(chunk_results, total_results, current_start, chunk_end, chunk_number, days_in_chunk, force)
                
                total_results["total_requests"] += chunk_results["total_requests"]
                total_results["total_records"] += chunk_results["total_records"]
                total_results["success_requests"] += chunk_results["success_requests"]
                total_results["error_requests"] += chunk_results["error_requests"]
                total_results["dates_processed"].append(chunk_results)
                
                # Update progress
                progress.update(date_range_task, advance=1)
                
                # Move to next chunk
                current_start = chunk_end + timedelta(days=1)
                chunk_number += 1
        
        self._print_summary(total_results)
        return total_results

    async def _extract_chunk_with_progress_bars(self, chunk_results: Dict[str, Any], total_results: Dict[str, Any], 
                                               start_date: datetime, end_date: datetime, chunk_number: int, days_in_chunk: int, force: bool = False):
        """Extract data with individual progress bars for each URL pattern."""
        
        # Step 1: Discover all URL patterns and create progress bars
        url_patterns = []
        
        for endpoint in PNCP_ENDPOINTS:
            if "modalidades" in endpoint and endpoint.get("modalidade_strategy") != "optional_unrestricted":
                # Create a pattern for each modalidade
                for modalidade in endpoint["modalidades"]:
                    pattern_id = f"{endpoint['name']}_mod_{modalidade}"
                    pattern_name = f"{endpoint['name']} (modalidade {modalidade})"
                    url_patterns.append({
                        "id": pattern_id,
                        "name": pattern_name,
                        "endpoint": endpoint,
                        "modalidade": modalidade
                    })
            else:
                # Single pattern for endpoint without modalidades
                pattern_id = endpoint['name']
                pattern_name = endpoint['name']
                url_patterns.append({
                    "id": pattern_id,
                    "name": pattern_name,
                    "endpoint": endpoint,
                    "modalidade": None
                })
        
        # Step 2: Process patterns sequentially for now (to avoid nested progress bar conflicts)
        console.print(f"\n📅 Processing Date Range Chunk {chunk_number}: {start_date.date()} to {end_date.date()} ({days_in_chunk} days)")
        
        # Process each pattern individually
        for pattern in url_patterns:
            endpoint = pattern["endpoint"]
            modalidade = pattern["modalidade"]
            
            # Extract this pattern using the existing method
            try:
                if endpoint.get("supports_date_range", True):
                    # Use date range extraction (async version)
                    result = await self._extract_endpoint_modalidade_range_async(endpoint, start_date, end_date, modalidade, force)
                else:
                    # Use single date extraction for endpoints that don't support ranges
                    result = self._extract_endpoint_modalidade(endpoint, end_date, modalidade, force)
                
                # Aggregate results
                chunk_results["total_requests"] += result.get("total_requests", 0)
                chunk_results["total_records"] += result.get("total_records", 0)
                chunk_results["success_requests"] += result.get("success_requests", 0)
                chunk_results["error_requests"] += result.get("error_requests", 0)
                
                # Track specific endpoint results
                endpoint_result = {
                    "endpoint_name": endpoint["name"],
                    "modalidade": modalidade,
                    "total_requests": result.get("total_requests", 0),
                    "total_records": result.get("total_records", 0),
                    "success_requests": result.get("success_requests", 0),
                    "error_requests": result.get("error_requests", 0),
                    "skipped": result.get("skipped", False),
                    "resumed": result.get("resumed", False)
                }
                chunk_results["endpoints"].append(endpoint_result)
                
                # Update global counters
                if result.get("skipped"):
                    total_results["skipped_extractions"] += 1
                if result.get("resumed"):
                    total_results["resumed_extractions"] += 1
                    
            except Exception as e:
                console.print(f"❌ [red]Error processing {pattern['name']}: {e}[/red]")
                chunk_results["error_requests"] += 1

    async def _discover_pattern_total(self, pattern: Dict[str, Any], start_date: datetime, end_date: datetime, progress, task_id) -> Dict[str, Any]:
        """Make first request to discover total pages for a URL pattern."""
        endpoint = pattern["endpoint"]
        modalidade = pattern["modalidade"]
        
        # Build parameters
        params = {
            "pagina": 1,
            "tamanhoPagina": endpoint["default_page_size"]
        }
        
        # Add date parameters
        if endpoint["supports_date_range"]:
            if "dataInicio" in endpoint["date_params"]:
                params["dataInicio"] = self._format_date(start_date)
                params["dataFim"] = self._format_date(end_date)
            else:
                params["dataInicial"] = self._format_date(start_date)
                params["dataFinal"] = self._format_date(end_date)
        else:
            params["dataFinal"] = self._format_date(end_date)
        
        # Add modalidade if required
        if modalidade is not None:
            params["codigoModalidadeContratacao"] = modalidade
        
        # Make discovery request
        response_data = await self._make_request_async(endpoint, params)
        
        if response_data["status_code"] == 200:
            total_pages = response_data["total_pages"]
            total_records = response_data["total_records"]
            
            # Update progress bar with discovered total
            progress.update(task_id, total=total_pages, completed=1)
            progress.start_task(task_id)
            
            # Show URL and stats
            endpoint_url = f"{PNCP_BASE_URL}{endpoint['path']}"
            sample_params = {k: v for k, v in params.items() if k != 'pagina'}
            sample_params['pagina'] = '[1-N]'
            url_pattern = f"{endpoint_url}?{'&'.join([f'{k}={v}' for k, v in sample_params.items()])}"
            console.print(f"🌐 [dim cyan]{pattern['name']}: {total_pages} pages, {total_records:,} records[/dim cyan]")
            console.print(f"   [dim]{url_pattern}[/dim]")
            
            # Store first response
            self._store_response_range(endpoint, params, response_data, start_date, end_date, modalidade)
            
            return {
                "total_pages": total_pages,
                "total_records": total_records,
                "params": params,
                "success": True
            }
        else:
            progress.update(task_id, total=1, completed=1)
            progress.start_task(task_id)
            console.print(f"❌ [red]{pattern['name']}: Failed with status {response_data['status_code']}[/red]")
            return {
                "total_pages": 1,
                "total_records": 0,
                "params": params,
                "success": False
            }

    async def _extract_pattern_pages(self, pattern: Dict[str, Any], start_date: datetime, end_date: datetime, 
                                   discovery_result: Dict[str, Any], progress, task_id) -> Dict[str, Any]:
        """Extract remaining pages for a URL pattern with progress updates."""
        endpoint = pattern["endpoint"]
        modalidade = pattern["modalidade"]
        total_pages = discovery_result["total_pages"]
        base_params = discovery_result["params"]
        
        results = {
            "total_requests": 1,  # Include discovery request
            "total_records": discovery_result["total_records"],
            "success_requests": 1 if discovery_result["success"] else 0,
            "error_requests": 0 if discovery_result["success"] else 1
        }
        
        if total_pages > 1:
            # Extract remaining pages concurrently
            remaining_pages = list(range(2, total_pages + 1))
            
            # Use semaphore to limit concurrent requests per pattern
            semaphore = asyncio.Semaphore(3)  # 3 concurrent requests per pattern
            
            async def fetch_single_page(page_num: int) -> Dict[str, Any]:
                async with semaphore:
                    params = base_params.copy()
                    params["pagina"] = page_num
                    
                    response_data = await self._make_request_async(endpoint, params)
                    self._store_response_range(endpoint, params, response_data, start_date, end_date, modalidade)
                    
                    # Update progress bar
                    progress.advance(task_id)
                    
                    return {
                        "status_code": response_data["status_code"],
                        "page": page_num
                    }
            
            # Execute all page requests concurrently
            tasks = [fetch_single_page(page) for page in remaining_pages]
            page_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate results
            for result in page_results:
                results["total_requests"] += 1
                if isinstance(result, dict) and result.get("status_code") == 200:
                    results["success_requests"] += 1
                else:
                    results["error_requests"] += 1
        
        return results
    
    def _print_summary(self, results: Dict[str, Any]):
        """Print extraction summary with performance metrics."""
        final_progress = self._get_progress_info()
        total_time = time.time() - self.extraction_start_time if self.extraction_start_time else 0
        
        table = Table(title="🔍 PNCP Data Extraction Summary")
        
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        
        table.add_row("Run ID", results["run_id"])
        table.add_row("Date Range", f"{results['start_date']} to {results['end_date']}")
        table.add_row("Total Requests", f"{results['total_requests']:,}")
        table.add_row("Total Pages", f"{final_progress['pages_processed']:,}")
        table.add_row("Total Records", f"{results['total_records']:,}")
        table.add_row("Success Requests", f"{results['success_requests']:,}")
        table.add_row("Error Requests", f"{results['error_requests']:,}")
        table.add_row("Avg RPS", f"{final_progress['rps']:.2f}")
        table.add_row("Total Time", f"{total_time:.1f}s")
        
        # Add resume/skip statistics
        if results.get("skipped_extractions", 0) > 0:
            table.add_row("Skipped (Complete)", f"{results['skipped_extractions']:,}", style="cyan")
        if results.get("resumed_extractions", 0) > 0:
            table.add_row("Resumed (Partial)", f"{results['resumed_extractions']:,}", style="yellow")
        
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
        if hasattr(self, 'async_client') and self.async_client:
            # Close async client if it exists
            try:
                asyncio.run(self.async_client.aclose())
            except Exception:
                pass  # May fail if event loop is already closed
        if hasattr(self, 'conn'):
            self.conn.close()


# CLI interface
app = typer.Typer()

@app.command()
def extract(
    start_date: str = typer.Option(
        "2021-01-01",
        help="Start date (YYYY-MM-DD) - defaults to 2021-01-01 for full historical extraction"
    ),
    end_date: str = typer.Option(
        datetime.now().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD) - defaults to today"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Force re-extraction even if data already exists in PSA"
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
    results = asyncio.run(extractor.extract_all_data(start_dt, end_dt, force=force))
    
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