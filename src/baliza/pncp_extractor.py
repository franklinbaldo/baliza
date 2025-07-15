#!/usr/bin/env python3
"""
PNCP Data Extractor V2 - True Async Architecture
Based on steel-man pseudocode: endpoint → 365-day ranges → async pagination
"""

import asyncio
import json
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Any, List, Tuple
import time

import duckdb
import httpx
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

console = Console()

# Configuration
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta"
CONCURRENCY = 8  # Concurrent requests limit
PAGE_SIZE = 500  # Maximum page size
REQUEST_TIMEOUT = 30
USER_AGENT = "BALIZA/3.0 (Backup Aberto de Licitacoes)"

# Data directory
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"

# Working endpoints (only the reliable ones)
PNCP_ENDPOINTS = [
    {
        "name": "contratos_publicacao",
        "path": "/v1/contratos",
        "description": "Contratos por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,
        "supports_date_range": True
    },
    {
        "name": "contratos_atualizacao", 
        "path": "/v1/contratos/atualizacao",
        "description": "Contratos por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,
        "supports_date_range": True
    },
    {
        "name": "atas_periodo",
        "path": "/v1/atas",
        "description": "Atas de Registro de Preço por Período de Vigência",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,
        "supports_date_range": True
    },
    {
        "name": "atas_atualizacao",
        "path": "/v1/atas/atualizacao",
        "description": "Atas por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,
        "supports_date_range": True
    }
]


class AsyncPNCPExtractor:
    """True async PNCP extractor with semaphore back-pressure."""
    
    def __init__(self, concurrency: int = CONCURRENCY):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.run_id = str(uuid.uuid4())
        self.client = None
        
        # Statistics
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_records = 0
        
        self._init_database()
        
    def _init_database(self):
        """Initialize DuckDB with PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(BALIZA_DB_PATH))
        
        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")
        
        # Create raw responses table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                request_parameters JSON,
                response_code INTEGER NOT NULL,
                response_content TEXT,
                response_headers JSON,
                data_date DATE,
                run_id VARCHAR,
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER
            )
        """)
        
        # Create indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_response_code ON psa.pncp_raw_responses(response_code)")
        
    async def _init_client(self):
        """Initialize HTTP client with optimal settings."""
        self.client = httpx.AsyncClient(
            base_url=PNCP_BASE_URL,
            timeout=REQUEST_TIMEOUT,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, br",
                "Accept": "application/json"
            },
            # http2=True,  # HTTP/2 support - requires h2 package
            limits=httpx.Limits(
                max_connections=self.concurrency * 2,
                max_keepalive_connections=self.concurrency
            )
        )
        
    async def _fetch_with_backpressure(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    self.total_requests += 1
                    response = await self.client.get(url, params=params)
                    
                    if response.status_code == 200:
                        self.successful_requests += 1
                        data = response.json()
                        return {
                            "success": True,
                            "status_code": response.status_code,
                            "data": data,
                            "headers": dict(response.headers),
                            "total_records": data.get("totalRegistros", 0),
                            "total_pages": data.get("totalPaginas", 1),
                            "content": response.text
                        }
                    else:
                        # Log error but don't retry 4xx errors
                        if response.status_code >= 400 and response.status_code < 500:
                            self.failed_requests += 1
                            return {
                                "success": False,
                                "status_code": response.status_code,
                                "error": f"HTTP {response.status_code}",
                                "content": response.text,
                                "headers": dict(response.headers)
                            }
                        
                        # Retry on 5xx errors
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                            
                        self.failed_requests += 1
                        return {
                            "success": False,
                            "status_code": response.status_code,
                            "error": f"HTTP {response.status_code} after {attempt + 1} attempts",
                            "content": response.text,
                            "headers": dict(response.headers)
                        }
                        
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    
                    self.failed_requests += 1
                    return {
                        "success": False,
                        "status_code": 0,
                        "error": str(e),
                        "content": "",
                        "headers": {}
                    }
                    
    def _format_date(self, date_obj: date) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")
        
    def _year_chunks(self, start_date: date, end_date: date, max_days: int = 365) -> List[Tuple[date, date]]:
        """Generate date chunks of max_days."""
        chunks = []
        current = start_date
        
        while current <= end_date:
            chunk_end = min(current + timedelta(days=max_days - 1), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
            
        return chunks
        
    def _check_page_exists(self, endpoint_name: str, data_date: date, page: int) -> bool:
        """Check if a specific page already exists with success status."""
        result = self.conn.execute("""
            SELECT COUNT(*) FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
        """, [endpoint_name, data_date, page]).fetchone()
        
        return (result[0] if result else 0) > 0
        
    def _batch_store_responses(self, responses: List[Dict[str, Any]]):
        """Store multiple responses in a single batch operation."""
        if not responses:
            return
            
        # Prepare batch data
        batch_data = []
        for resp in responses:
            batch_data.append([
                resp["endpoint_url"],
                resp["endpoint_name"],
                json.dumps(resp["request_parameters"]),
                resp["response_code"],
                resp["response_content"],
                json.dumps(resp["response_headers"]),
                resp["data_date"],
                resp["run_id"],
                resp["total_records"],
                resp["total_pages"],
                resp["current_page"],
                resp["page_size"]
            ])
            
        # Batch insert
        self.conn.executemany("""
            INSERT INTO psa.pncp_raw_responses (
                endpoint_url, endpoint_name, request_parameters,
                response_code, response_content, response_headers,
                data_date, run_id, total_records, total_pages,
                current_page, page_size
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_data)
        
    async def _crawl_endpoint_range(self, endpoint: Dict[str, Any], start_date: date, end_date: date, 
                                  progress: Progress, task_id: int, force: bool = False) -> Dict[str, Any]:
        """Crawl a single endpoint for a date range."""
        results = {
            "endpoint_name": endpoint["name"],
            "start_date": start_date,
            "end_date": end_date,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_records": 0,
            "pages_processed": 0,
            "pages_skipped": 0
        }
        
        # Build base parameters
        base_params = {
            "tamanhoPagina": PAGE_SIZE,
            "dataInicial": self._format_date(start_date),
            "dataFinal": self._format_date(end_date)
        }
        
        # Step 1: Fetch first page to discover total pages
        first_page_params = base_params.copy()
        first_page_params["pagina"] = 1
        
        # Check if we should skip this range
        if not force and self._check_page_exists(endpoint["name"], start_date, 1):
            progress.update(task_id, description=f"[blue]{endpoint['name']}[/blue] - Skipping (exists)")
            results["pages_skipped"] += 1
            return results
            
        # Fetch first page
        first_response = await self._fetch_with_backpressure(endpoint["path"], first_page_params)
        results["total_requests"] += 1
        
        if not first_response["success"]:
            results["failed_requests"] += 1
            progress.update(task_id, description=f"[red]{endpoint['name']}[/red] - Failed")
            return results
            
        total_pages = first_response["total_pages"]
        total_records = first_response["total_records"]
        results["successful_requests"] += 1
        results["total_records"] = total_records
        
        # Update progress bar
        progress.update(task_id, total=total_pages, description=f"[green]{endpoint['name']}[/green] - {total_records:,} records")
        
        # Prepare batch storage
        batch_responses = []
        
        # Store first page
        batch_responses.append({
            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
            "endpoint_name": endpoint["name"],
            "request_parameters": first_page_params,
            "response_code": first_response["status_code"],
            "response_content": first_response["content"],
            "response_headers": first_response["headers"],
            "data_date": start_date,
            "run_id": self.run_id,
            "total_records": first_response["total_records"],
            "total_pages": first_response["total_pages"],
            "current_page": 1,
            "page_size": PAGE_SIZE
        })
        
        results["pages_processed"] += 1
        progress.update(task_id, advance=1)
        
        # Step 2: Fetch remaining pages concurrently
        if total_pages > 1:
            # Determine which pages to fetch
            pages_to_fetch = []
            for page in range(2, total_pages + 1):
                if force or not self._check_page_exists(endpoint["name"], start_date, page):
                    pages_to_fetch.append(page)
                else:
                    results["pages_skipped"] += 1
                    progress.update(task_id, advance=1)
            
            # Fetch pages concurrently
            if pages_to_fetch:
                page_tasks = []
                for page in pages_to_fetch:
                    page_params = base_params.copy()
                    page_params["pagina"] = page
                    page_tasks.append(self._fetch_with_backpressure(endpoint["path"], page_params))
                
                # Wait for all pages
                page_responses = await asyncio.gather(*page_tasks)
                
                # Process results
                for page_num, response in zip(pages_to_fetch, page_responses):
                    results["total_requests"] += 1
                    
                    if response["success"]:
                        results["successful_requests"] += 1
                        results["pages_processed"] += 1
                        
                        # Add to batch
                        batch_responses.append({
                            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
                            "endpoint_name": endpoint["name"],
                            "request_parameters": {**base_params, "pagina": page_num},
                            "response_code": response["status_code"],
                            "response_content": response["content"],
                            "response_headers": response["headers"],
                            "data_date": start_date,
                            "run_id": self.run_id,
                            "total_records": response.get("total_records", 0),
                            "total_pages": response.get("total_pages", total_pages),
                            "current_page": page_num,
                            "page_size": PAGE_SIZE
                        })
                    else:
                        results["failed_requests"] += 1
                        
                        # Store failed response too
                        batch_responses.append({
                            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
                            "endpoint_name": endpoint["name"],
                            "request_parameters": {**base_params, "pagina": page_num},
                            "response_code": response["status_code"],
                            "response_content": response["content"],
                            "response_headers": response["headers"],
                            "data_date": start_date,
                            "run_id": self.run_id,
                            "total_records": 0,
                            "total_pages": total_pages,
                            "current_page": page_num,
                            "page_size": PAGE_SIZE
                        })
                    
                    progress.update(task_id, advance=1)
        
        # Batch store all responses
        self._batch_store_responses(batch_responses)
        
        return results
        
    async def extract_data(self, start_date: date, end_date: date, force: bool = False) -> Dict[str, Any]:
        """Main extraction method with true async architecture."""
        console.print("🚀 Starting Async PNCP Extraction")
        console.print(f"📅 Date Range: {start_date} to {end_date}")
        console.print(f"🔧 Concurrency: {self.concurrency}")
        console.print(f"🆔 Run ID: {self.run_id}")
        
        if force:
            console.print("⚠️ [yellow]Force mode enabled - will re-extract existing data[/yellow]")
            
        # Initialize client
        await self._init_client()
        
        start_time = time.time()
        
        # Create all endpoint-range combinations
        all_tasks = []
        for endpoint in PNCP_ENDPOINTS:
            date_chunks = self._year_chunks(start_date, end_date, endpoint["max_days"])
            for chunk_start, chunk_end in date_chunks:
                all_tasks.append((endpoint, chunk_start, chunk_end))
        
        # Create progress bars
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            
            # Create tasks with progress bars
            extraction_tasks = []
            for endpoint, chunk_start, chunk_end in all_tasks:
                task_id = progress.add_task(
                    f"[blue]{endpoint['name']}[/blue] {chunk_start} to {chunk_end}",
                    total=None
                )
                
                task = self._crawl_endpoint_range(endpoint, chunk_start, chunk_end, progress, task_id, force)
                extraction_tasks.append(task)
            
            # Run all tasks concurrently
            results = await asyncio.gather(*extraction_tasks)
        
        # Aggregate results
        total_results = {
            "run_id": self.run_id,
            "start_date": start_date,
            "end_date": end_date,
            "total_requests": sum(r["total_requests"] for r in results),
            "successful_requests": sum(r["successful_requests"] for r in results),
            "failed_requests": sum(r["failed_requests"] for r in results),
            "total_records": sum(r["total_records"] for r in results),
            "pages_processed": sum(r["pages_processed"] for r in results),
            "pages_skipped": sum(r["pages_skipped"] for r in results),
            "duration": time.time() - start_time,
            "endpoints": results
        }
        
        # Print summary
        console.print("\n🎉 Extraction Complete!")
        console.print(f"📊 Total Requests: {total_results['total_requests']:,}")
        console.print(f"✅ Successful: {total_results['successful_requests']:,}")
        console.print(f"❌ Failed: {total_results['failed_requests']:,}")
        console.print(f"📈 Total Records: {total_results['total_records']:,}")
        console.print(f"⏱️ Duration: {total_results['duration']:.1f}s")
        console.print(f"🚀 Avg RPS: {total_results['total_requests'] / total_results['duration']:.2f}")
        
        # Close client
        await self.client.aclose()
        
        return total_results
        
    def __del__(self):
        """Cleanup."""
        if hasattr(self, 'conn'):
            self.conn.close()


# CLI interface
app = typer.Typer()

@app.command()
def extract(
    start_date: str = typer.Option(
        "2021-01-01",
        help="Start date (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        datetime.now().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD)"
    ),
    concurrency: int = typer.Option(
        CONCURRENCY,
        help="Number of concurrent requests"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Force re-extraction even if data exists"
    )
):
    """Extract data using true async architecture."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        console.print("❌ Invalid date format. Use YYYY-MM-DD", style="bold red")
        raise typer.Exit(1)
    
    if start_dt > end_dt:
        console.print("❌ Start date must be before end date", style="bold red")
        raise typer.Exit(1)
    
    async def main():
        extractor = AsyncPNCPExtractor(concurrency=concurrency)
        results = await extractor.extract_data(start_dt, end_dt, force)
        
        # Save results
        results_file = DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"📄 Results saved to: {results_file}")
        
    asyncio.run(main())

@app.command()
def stats():
    """Show extraction statistics."""
    conn = duckdb.connect(str(BALIZA_DB_PATH))
    
    # Overall stats
    total_responses = conn.execute("SELECT COUNT(*) FROM psa.pncp_raw_responses").fetchone()[0]
    success_responses = conn.execute("SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200").fetchone()[0]
    
    console.print(f"📊 Total Responses: {total_responses:,}")
    console.print(f"✅ Successful: {success_responses:,}")
    console.print(f"❌ Failed: {total_responses - success_responses:,}")
    
    if total_responses > 0:
        console.print(f"📈 Success Rate: {success_responses / total_responses * 100:.1f}%")
    
    # Endpoint breakdown
    endpoint_stats = conn.execute("""
        SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM psa.pncp_raw_responses 
        WHERE response_code = 200
        GROUP BY endpoint_name
        ORDER BY total_records DESC
    """).fetchall()
    
    console.print("\n📋 Endpoint Statistics:")
    for name, responses, records in endpoint_stats:
        console.print(f"  {name}: {responses:,} responses, {records:,} records")
    
    conn.close()

if __name__ == "__main__":
    app()