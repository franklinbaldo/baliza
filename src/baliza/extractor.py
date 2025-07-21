"""PNCP Data Extractor V4 - Vectorized Engine

REWRITTEN: Implements a high-performance, stateless, two-pass reconciliation
process powered by vectorized Pandas operations. This approach eliminates the
legacy stateful task table (`psa.pncp_extraction_tasks`) and offers significant
performance gains over iterative methods.

Architecture:
- **Stateless & Resilient:** No longer relies on a stateful task table. The process
  can be restarted at any time and will automatically determine the missing data.
- **Two-Pass Reconciliation:**
  1. **Discovery Pass:** Concurrently fetches the first page of all potential
     requests to discover the `total_pages` for each.
  2. **Reconciliation & Execution Pass:** Vectorized operations are used to
     calculate the full set of required pages, anti-join against existing
     data in the database to find what's missing, and then concurrently
     fetch only the missing pages.
- **Vectorized Operations:** Leverages Pandas `explode` and `merge` for highly
  efficient in-memory data manipulation, avoiding slow Python loops.
"""

import asyncio
import logging
import signal
import sys
import time
import uuid
from datetime import date

import pandas as pd
import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from baliza.pncp_client import PNCPClient
from baliza.pncp_writer import PNCPWriter
from baliza.enums import ModalidadeContratacao

logger = logging.getLogger(__name__)
console = Console()


class AsyncPNCPExtractor:
    """
    High-performance, vectorized data extractor for PNCP.
    """

    def __init__(self, concurrency: int = 10, force_db: bool = False):
        """Initializes the vectorized extractor."""
        self.concurrency = concurrency
        self.force_db = force_db
        self.shutdown_event = asyncio.Event()
        self.run_id = str(uuid.uuid4())
        self.writer = None
        self.client = None
        self.write_queue = None
        self.writer_task = None

    async def __aenter__(self):
        """Initializes resources for the extractor."""
        self.writer = PNCPWriter(force_db=self.force_db)
        await self.writer.__aenter__()
        self.client = PNCPClient(concurrency=self.concurrency)
        await self.client.__aenter__()
        
        # Initialize write queue and start writer worker
        self.write_queue = asyncio.Queue()
        self.writer_task = asyncio.create_task(
            self.writer.writer_worker(self.write_queue)
        )
        
        self.setup_signal_handlers()
        logger.info(f"Vectorized Extractor initialized with run_id: {self.run_id}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleans up resources."""
        # Stop writer worker gracefully
        if hasattr(self, 'write_queue') and hasattr(self, 'writer_task'):
            await self.write_queue.put(None)  # Signal writer to stop
            await self.writer_task  # Wait for writer to finish
        
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)
        if self.writer:
            await self.writer.__aexit__(exc_type, exc_val, exc_tb)
        logger.info("Vectorized Extractor resources cleaned up.")

    def setup_signal_handlers(self):
        """Sets up aggressive shutdown handlers for immediate termination."""
        def signal_handler(signum, frame):
            console.print(f"\n🛑 [bold red]Signal {signum} received, STOPPING immediately![/bold red]")
            self.shutdown_event.set()
            
            # Cancel all running tasks aggressively
            try:
                # Get current event loop
                loop = asyncio.get_event_loop()
                
                # Cancel all running tasks
                tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
                if tasks:
                    console.print(f"🔄 [yellow]Cancelling {len(tasks)} running tasks...[/yellow]")
                    for task in tasks:
                        task.cancel()
                        
            except Exception as e:
                console.print(f"⚠️ [yellow]Signal cleanup error: {e}[/yellow]")
                
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _plan_initial_requests_df(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Generates a DataFrame of initial `pagina=1` requests for the discovery phase.
        Uses a vectorized approach with a cross-join.
        """
        console.print("📋 [bold]Phase 1: Planning Initial Requests[/bold]")
        
        # Create base endpoints. For 'contratacoes', we omit the `modalidade`
        # parameter to get the most comprehensive results, simplifying the logic.
        base_endpoints = [
            {'endpoint_name': 'contratos', 'modalidade': None},
            {'endpoint_name': 'atas', 'modalidade': None},
            {'endpoint_name': 'contratacoes', 'modalidade': None},
        ]
        
        endpoints_config = pd.DataFrame(base_endpoints)

        date_range = pd.date_range(start_date, end_date, freq='D')
        dates_df = pd.DataFrame(date_range, columns=['data_date'])

        # Perform a cross join
        initial_plan_df = dates_df.merge(endpoints_config, how='cross')
        initial_plan_df['pagina'] = 1

        console.print(f"✅ Generated {len(initial_plan_df):,} initial discovery tasks.")
        return initial_plan_df

    def _filter_existing_discoveries(self, plan_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out discovery requests that already exist in the database.
        For current month, ignore existing data to force refresh.
        """
        current_month_start = date.today().replace(day=1)
        
        existing_query = """
            SELECT DISTINCT
                endpoint_name,
                data_date,
                CASE 
                    WHEN json_extract(request_parameters, '$.modalidade') IS NULL THEN NULL
                    ELSE CAST(json_extract(request_parameters, '$.modalidade') AS VARCHAR)
                END as modalidade
            FROM psa.pncp_raw_responses 
            WHERE current_page = 1
                -- Qualquer response_code é válido (200, 404, etc.) - já foi processado
                -- Para o mês atual, ignorar dados existentes (forçar refresh)
                AND data_date < ?
        """
        
        try:
            existing_results = self.writer.conn.execute(
                existing_query, [current_month_start]
            ).fetchall()
            
            if not existing_results:
                return plan_df  # Nada existe, processar tudo
                
            # Converter para DataFrame para fazer anti-join
            existing_df = pd.DataFrame(
                existing_results, 
                columns=['endpoint_name', 'data_date', 'modalidade']
            )
            
            # Converter tipos para comparação
            existing_df['data_date'] = pd.to_datetime(existing_df['data_date']).dt.date
            plan_df['data_date'] = pd.to_datetime(plan_df['data_date']).dt.date
            
            # Padronizar tipos de modalidade (converter None/NaN para string vazia)
            # Remover aspas duplas do JSON e converter para consistência
            existing_df['modalidade'] = existing_df['modalidade'].replace('NaN', '').fillna('').astype(str)
            existing_df['modalidade'] = existing_df['modalidade'].str.replace('"', '', regex=False)  # Remove aspas
            plan_df['modalidade'] = plan_df['modalidade'].fillna('').astype(str)
            
            # Anti-join: pegar apenas o que NÃO existe
            merged_df = pd.merge(
                plan_df,
                existing_df,
                on=['endpoint_name', 'data_date', 'modalidade'],
                how='left',
                indicator=True
            )
            
            filtered_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
            return filtered_df
            
        except Exception as e:
            logger.error(f"Error filtering existing discoveries: {e}")
            return plan_df  # Em caso de erro, processar tudo

    async def _discover_total_pages_concurrently(self, initial_plan_df: pd.DataFrame):
        """
        Executes the 'Discovery Pass'. Fetches page 1 for all requests to find out
        the total number of pages and persists these initial results.
        Only processes requests that don't already exist in the database.
        """
        # First, filter out requests that already exist in the database
        filtered_plan_df = self._filter_existing_discoveries(initial_plan_df)
        total_tasks = len(filtered_plan_df)
        skipped_tasks = len(initial_plan_df) - total_tasks
        
        if skipped_tasks > 0:
            console.print(f"✅ [green]Reutilizando {skipped_tasks:,} descobertas existentes do banco[/green]")
            
            # Atualizar progress bar para refletir requests já "completas"
            if hasattr(self, '_progress') and hasattr(self, '_progress_task'):
                self._skipped_requests = skipped_tasks
                self._completed_requests = skipped_tasks  # Começar com as já completas
                
                http_percentage = (self._completed_requests / max(1, self._total_planned_requests)) * 100
                rate = 0.0  # Ainda não começamos requests reais
                
                self._progress.update(
                    self._progress_task,
                    http_completed=self._completed_requests,
                    http_percentage=http_percentage,
                    rate=rate
                )
        
        # Debug info
        logger.info(f"Discovery filter: {len(initial_plan_df)} planned -> {total_tasks} to process ({skipped_tasks} skipped)")
        
        if total_tasks == 0:
            console.print("✅ [green]Todas as descobertas já existem no banco. Discovery Pass ignorado.[/green]")
            return
            
        console.print(f"\n📥 [bold blue]Discovery Pass: Processando {total_tasks:,} novas requests...[/bold blue]")
        
        page_queue = asyncio.Queue()
        completed_count = 0
        
        # Progress tracking for console updates
        last_reported = 0
        report_interval = max(1, total_tasks // 20)  # Report every 5% or at least every request

        async def worker():
            nonlocal completed_count, last_reported
            while not self.shutdown_event.is_set():
                try:
                    row = await page_queue.get()
                    if row is None or self.shutdown_event.is_set():
                        break

                    params = {
                        "dataInicial": row.data_date.strftime('%Y%m%d'),
                        "dataFinal": row.data_date.strftime('%Y%m%d'),
                        "pagina": 1,
                        "tamanhoPagina": 500,
                    }
                    if row.modalidade is not None:
                        params['modalidade'] = row.modalidade

                    # Make the actual API request
                    endpoint_path = f"/v1/{row.endpoint_name}"
                    try:
                        response = await self.client.fetch_with_backpressure(endpoint_path, params)
                        
                        # Update HTTP request progress
                        # Somar 1 às requests reais (sem contar as reutilizadas)
                        actual_requests_made = (self._completed_requests - getattr(self, '_skipped_requests', 0)) + 1
                        self._completed_requests = getattr(self, '_skipped_requests', 0) + actual_requests_made
                        
                        elapsed = time.time() - self._request_start_time
                        rate = actual_requests_made / elapsed if elapsed > 0 else 0
                        if hasattr(self, '_progress') and self._progress_task is not None:
                            http_total = self._progress.tasks[0].fields.get('http_total', 1)
                            http_percentage = (self._completed_requests / max(1, http_total)) * 100
                            self._progress.update(
                                self._progress_task,
                                http_completed=self._completed_requests,
                                http_percentage=http_percentage,
                                rate=rate
                            )
                    except Exception as e:
                        # Show exact request details when error occurs
                        # Build full clickable URL
                        base_url = "https://pncp.gov.br/api/consulta"
                        full_url = f"{base_url}{endpoint_path}"
                        if params:
                            param_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
                            full_url = f"{full_url}?{param_str}"
                        
                        console.print(f"\n❌ [bold red]Request Error Details:[/bold red]")
                        console.print(f"   🔗 Full URL: {full_url}")
                        console.print(f"   📅 Date: {row.data_date}")
                        console.print(f"   🏷️  Modalidade: {row.modalidade}")
                        console.print(f"   ⚠️  Error: {str(e)}")
                        console.print()
                        
                        # Still update progress for failed requests
                        # Somar 1 às requests reais (sem contar as reutilizadas)
                        actual_requests_made = (self._completed_requests - getattr(self, '_skipped_requests', 0)) + 1
                        self._completed_requests = getattr(self, '_skipped_requests', 0) + actual_requests_made
                        
                        elapsed = time.time() - self._request_start_time
                        rate = actual_requests_made / elapsed if elapsed > 0 else 0
                        if hasattr(self, '_progress') and self._progress_task is not None:
                            http_total = self._progress.tasks[0].fields.get('http_total', 1)
                            http_percentage = (self._completed_requests / max(1, http_total)) * 100
                            self._progress.update(
                                self._progress_task,
                                http_completed=self._completed_requests,
                                http_percentage=http_percentage,
                                rate=rate
                            )
                        raise

                    # Transform response for writer persistence
                    writer_data = {
                        "extracted_at": None,  # Writer will set this
                        "endpoint_url": endpoint_path,
                        "endpoint_name": row.endpoint_name,
                        "request_parameters": params,
                        "response_code": response.get("status_code"),
                        "response_content": response.get("content", ""),
                        "response_headers": response.get("headers", {}),
                        "data_date": row.data_date,
                        "run_id": self.run_id,
                        "total_records": response.get("total_records", 0),
                        "total_pages": response.get("total_pages", 1),
                        "current_page": row.pagina,
                        "page_size": params.get("tamanhoPagina", 500)
                    }

                    # Queue transformed data for persistence by writer worker
                    await self.write_queue.put(writer_data)
                    
                    # Update progress counter
                    completed_count += 1
                    
                    # Report progress periodically
                    if completed_count - last_reported >= report_interval:
                        percentage = (completed_count / total_tasks) * 100
                        console.print(f"⏳ Progress: {completed_count:,}/{total_tasks:,} ({percentage:.1f}%)")
                        last_reported = completed_count
                    
                    logger.info(f"Fetched page 1 for {row.endpoint_name}, got {len(response.get('data', []))} items")

                finally:
                    page_queue.task_done()

        # Enqueue tasks (only the filtered ones)
        for row in filtered_plan_df.itertuples():
            await page_queue.put(row)

        # Create workers
        workers = [asyncio.create_task(worker()) for _ in range(self.concurrency)]

        # Wait for queue to be processed
        await page_queue.join()

        # Stop workers
        for _ in workers:
            await page_queue.put(None)
        await asyncio.gather(*workers)

        console.print("✅ Discovery Pass complete. All page 1 responses persisted.")

    def _expand_and_reconcile_from_db_vectorized(self) -> pd.DataFrame:
        """
        Performs the vectorized reconciliation.
        1. Fetches discovery results (page 1) from the database.
        2. Expands the DataFrame to represent all required pages.
        3. Anti-joins against all existing data to find what's missing.
        """
        console.print("\nReconciliation Pass: Calculating missing pages...")

        # 1. Fetch discovery results (page 1 with total_pages > 1) from actual database
        # Para o mês atual, sempre usar dados fresh da API (ignorar cache)
        current_month_start = date.today().replace(day=1)
        
        discovery_query = """
            SELECT DISTINCT 
                endpoint_name,
                data_date,
                CASE 
                    WHEN json_extract(request_parameters, '$.modalidade') IS NULL THEN NULL
                    ELSE CAST(json_extract(request_parameters, '$.modalidade') AS VARCHAR)
                END as modalidade,
                total_pages
            FROM psa.pncp_raw_responses 
            WHERE current_page = 1 
                AND total_pages > 1 
                AND response_code = 200
                AND (
                    -- Para meses anteriores, usar dados existentes (qualquer run_id)
                    data_date < ? 
                    -- Para o mês atual, apenas do run atual para forçar refresh
                    OR (data_date >= ? AND run_id = ?)
                )
        """
        
        try:
            discovery_results = self.writer.conn.execute(
                discovery_query, 
                [current_month_start, current_month_start, self.run_id]
            ).fetchall()
            
            if not discovery_results:
                console.print("✅ No multi-page results to expand. Reconciliation complete.")
                return pd.DataFrame()
                
            discovery_df = pd.DataFrame(discovery_results, columns=['endpoint_name', 'data_date', 'modalidade', 'total_pages'])
            
        except Exception as e:
            logger.error(f"Error fetching discovery results: {e}")
            console.print(f"⚠️ Error during reconciliation: {e}")
            return pd.DataFrame()

        # 2. Vectorized Expansion
        discovery_df['pagina'] = discovery_df['total_pages'].apply(lambda x: list(range(2, int(x) + 1)))
        expanded_df = discovery_df.explode('pagina').reset_index(drop=True)

        # 3. Fetch all existing requests for an anti-join
        # Para o mês atual, ignorar dados cached (forçar re-fetch)
        existing_query = """
            SELECT DISTINCT
                endpoint_name,
                data_date,
                CASE 
                    WHEN json_extract(request_parameters, '$.modalidade') IS NULL THEN NULL
                    ELSE CAST(json_extract(request_parameters, '$.modalidade') AS VARCHAR)
                END as modalidade,
                current_page as pagina
            FROM psa.pncp_raw_responses 
            WHERE response_code = 200
                -- Ignorar dados do mês atual para forçar refresh
                AND data_date < ?
        """
        
        try:
            existing_results = self.writer.conn.execute(
                existing_query, [current_month_start]
            ).fetchall()
            
            if existing_results:
                all_existing_df = pd.DataFrame(existing_results, columns=['endpoint_name', 'data_date', 'modalidade', 'pagina'])
                
                # Convert data_date to proper date objects for comparison
                all_existing_df['data_date'] = pd.to_datetime(all_existing_df['data_date']).dt.date
                expanded_df['data_date'] = pd.to_datetime(expanded_df['data_date']).dt.date
                
                # 4. Vectorized Anti-Join
                merged_df = pd.merge(
                    expanded_df,
                    all_existing_df,
                    on=['endpoint_name', 'data_date', 'modalidade', 'pagina'],
                    how='left',
                    indicator=True
                )
                
                missing_pages_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge', 'total_pages'])
            else:
                # No existing data, all expanded pages are missing
                missing_pages_df = expanded_df.drop(columns=['total_pages'])
                
        except Exception as e:
            logger.error(f"Error during anti-join reconciliation: {e}")
            console.print(f"⚠️ Error during reconciliation: {e}")
            return pd.DataFrame()

        console.print(f"✅ Reconciliation complete. Found {len(missing_pages_df):,} missing pages to fetch.")
        return missing_pages_df

    async def _execute_fetch_tasks(self, tasks_df: pd.DataFrame):
        """
        Executes the fetching of the final 'missing pages' work queue.
        """
        if tasks_df is None or tasks_df.empty:
            console.print("✅ No missing pages to fetch.")
            return

        total_tasks = len(tasks_df)
        
        console.print(f"\n📤 [bold green]Execution Pass: Processando {total_tasks:,} missing pages...[/bold green]")
        
        page_queue = asyncio.Queue()
        completed_count = 0
        
        # Progress tracking for console updates
        last_reported = 0
        report_interval = max(1, total_tasks // 20)  # Report every 5% or at least every request

        async def worker():
            nonlocal completed_count, last_reported
            while not self.shutdown_event.is_set():
                try:
                    row = await page_queue.get()
                    if row is None or self.shutdown_event.is_set():
                        break

                    params = {
                        "dataInicial": row.data_date.strftime('%Y%m%d'),
                        "dataFinal": row.data_date.strftime('%Y%m%d'),
                        "pagina": row.pagina,
                        "tamanhoPagina": 500,
                    }
                    if row.modalidade is not None:
                        params['modalidade'] = row.modalidade

                    endpoint_path = f"/v1/{row.endpoint_name}"
                    try:
                        response = await self.client.fetch_with_backpressure(endpoint_path, params)
                        
                        # Update HTTP request progress
                        # Somar 1 às requests reais (sem contar as reutilizadas)
                        actual_requests_made = (self._completed_requests - getattr(self, '_skipped_requests', 0)) + 1
                        self._completed_requests = getattr(self, '_skipped_requests', 0) + actual_requests_made
                        
                        elapsed = time.time() - self._request_start_time
                        rate = actual_requests_made / elapsed if elapsed > 0 else 0
                        if hasattr(self, '_progress') and self._progress_task is not None:
                            http_total = self._progress.tasks[0].fields.get('http_total', 1)
                            http_percentage = (self._completed_requests / max(1, http_total)) * 100
                            self._progress.update(
                                self._progress_task,
                                http_completed=self._completed_requests,
                                http_percentage=http_percentage,
                                rate=rate
                            )
                    except Exception as e:
                        # Show exact request details when error occurs
                        # Build full clickable URL
                        base_url = "https://pncp.gov.br/api/consulta"
                        full_url = f"{base_url}{endpoint_path}"
                        if params:
                            param_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
                            full_url = f"{full_url}?{param_str}"
                        
                        console.print(f"\n❌ [bold red]Request Error Details:[/bold red]")
                        console.print(f"   🔗 Full URL: {full_url}")
                        console.print(f"   📅 Date: {row.data_date}")
                        console.print(f"   🏷️  Modalidade: {row.modalidade}")
                        console.print(f"   📌 Page: {row.pagina}")
                        console.print(f"   ⚠️  Error: {str(e)}")
                        console.print()
                        
                        # Still update progress for failed requests
                        # Somar 1 às requests reais (sem contar as reutilizadas)
                        actual_requests_made = (self._completed_requests - getattr(self, '_skipped_requests', 0)) + 1
                        self._completed_requests = getattr(self, '_skipped_requests', 0) + actual_requests_made
                        
                        elapsed = time.time() - self._request_start_time
                        rate = actual_requests_made / elapsed if elapsed > 0 else 0
                        if hasattr(self, '_progress') and self._progress_task is not None:
                            http_total = self._progress.tasks[0].fields.get('http_total', 1)
                            http_percentage = (self._completed_requests / max(1, http_total)) * 100
                            self._progress.update(
                                self._progress_task,
                                http_completed=self._completed_requests,
                                http_percentage=http_percentage,
                                rate=rate
                            )
                        raise
                    
                    # Transform response for writer persistence
                    writer_data = {
                        "extracted_at": None,  # Writer will set this
                        "endpoint_url": endpoint_path,
                        "endpoint_name": row.endpoint_name,
                        "request_parameters": params,
                        "response_code": response.get("status_code"),
                        "response_content": response.get("content", ""),
                        "response_headers": response.get("headers", {}),
                        "data_date": row.data_date,
                        "run_id": self.run_id,
                        "total_records": response.get("total_records", 0),
                        "total_pages": response.get("total_pages", 1),
                        "current_page": row.pagina,
                        "page_size": params.get("tamanhoPagina", 500)
                    }
                    
                    # Queue transformed data for persistence by writer worker
                    await self.write_queue.put(writer_data)
                    
                    # Update progress counter
                    completed_count += 1
                    
                    # Report progress periodically
                    if completed_count - last_reported >= report_interval:
                        percentage = (completed_count / total_tasks) * 100
                        console.print(f"⏳ Progress: {completed_count:,}/{total_tasks:,} ({percentage:.1f}%)")
                        last_reported = completed_count
                    
                    logger.info(f"Fetched page {row.pagina} for {row.endpoint_name}, got {len(response.get('data', []))} items")

                finally:
                    page_queue.task_done()

        # Enqueue tasks
        for row in tasks_df.itertuples():
            await page_queue.put(row)

        # Create workers
        workers = [asyncio.create_task(worker()) for _ in range(self.concurrency)]

        # Wait for queue to be processed
        await page_queue.join()

        # Stop workers
        for _ in workers:
            await page_queue.put(None)
        await asyncio.gather(*workers)

        console.print("✅ Execution Pass complete. All missing pages have been fetched.")

    async def extract_data(self, start_date: date, end_date: date):
        """
        Orchestrates the new, resilient two-pass vectorized extraction flow.
        """
        console.print(f"🚀 [bold blue]Starting Vectorized Extraction for {start_date} to {end_date}[/bold blue]")
        start_time = time.time()
        
        with Progress(
            TextColumn("[bold blue]📋 Fases[/bold blue]"),
            BarColumn(bar_width=20, complete_style="blue", finished_style="bright_blue"),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TextColumn("•"),
            TextColumn("[bold]{task.description}[/bold]"),
            TextColumn(""),
            TextColumn("[bold green]🌐 Requests[/bold green]"),
            BarColumn(bar_width=25, complete_style="green", finished_style="bright_green"),
            TextColumn("[progress.percentage]{task.fields[http_percentage]:>3.1f}%"),
            TextColumn("•"),
            TextColumn("[white]{task.fields[http_completed]:,}[/white]/[bright_white]{task.fields[http_total]:,}[/bright_white]"),
            TextColumn("•"),
            TextColumn("[cyan]⚡ {task.fields[rate]:.1f} req/s[/cyan]"),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
            expand=True
        ) as progress:
            overall_task = progress.add_task(
                "[yellow]🔄 Inicializando...[/yellow]", 
                total=4,
                http_completed=0,
                http_total=0,
                http_percentage=0.0,
                rate=0.0
            )
            
            # Store progress objects for worker access
            self._progress = progress
            self._progress_task = overall_task

            # 1. Plan initial `pagina=1` requests
            progress.update(overall_task, description="[cyan]📋 Planejando requests...[/cyan]")
            initial_plan_df = self._plan_initial_requests_df(start_date, end_date)
            progress.update(overall_task, completed=1)
            
            # Initialize HTTP request tracking
            total_discovery_requests = len(initial_plan_df)
            progress.update(
                overall_task,
                http_total=total_discovery_requests,
                http_completed=0,
                http_percentage=0.0,
                rate=0.0
            )
            self._request_start_time = time.time()
            self._completed_requests = 0
            
            # Track requests that were skipped (reutilizados)
            self._total_planned_requests = total_discovery_requests
            self._skipped_requests = 0
            
            if self.shutdown_event.is_set():
                console.print("🛑 [red]Shutdown requested, stopping after planning phase[/red]")
                return

            # 2. Discover total pages and persist page 1 results
            progress.update(overall_task, description="[green]🔍 Descoberta (pág 1)...[/green]")
            await self._discover_total_pages_concurrently(initial_plan_df)
            progress.update(overall_task, completed=2)
            
            if self.shutdown_event.is_set():
                console.print("🛑 [red]Shutdown requested, stopping after discovery phase[/red]")
                return

            # 3. Vectorized reconciliation to find missing pages
            progress.update(overall_task, description="[yellow]⚙️ Reconciliação...[/yellow]")
            missing_pages_df = self._expand_and_reconcile_from_db_vectorized()
            progress.update(overall_task, completed=3)
            
            if self.shutdown_event.is_set():
                console.print("🛑 [red]Shutdown requested, stopping after reconciliation phase[/red]")
                return

            # 4. Execute fetching of the final work queue
            progress.update(overall_task, description="[blue]🚀 Executando faltantes...[/blue]")
            
            # Update HTTP progress for execution phase
            execution_requests = len(missing_pages_df) if missing_pages_df is not None and not missing_pages_df.empty else 0
            if execution_requests > 0:
                total_requests = total_discovery_requests + execution_requests
                progress.update(
                    overall_task,
                    http_total=total_requests
                )
            
            await self._execute_fetch_tasks(missing_pages_df)
            progress.update(overall_task, completed=4, description="[bright_green]✅ Completo![/bright_green]")
            
            # Final HTTP progress update
            final_rate = self._completed_requests / (time.time() - self._request_start_time) if time.time() - self._request_start_time > 0 else 0
            http_percentage = (self._completed_requests / max(1, progress.tasks[0].fields.get('http_total', 1))) * 100
            progress.update(
                overall_task,
                http_completed=self._completed_requests,
                http_percentage=http_percentage,
                rate=final_rate
            )

        duration = time.time() - start_time
        console.print(f"\n🎉 [bold green]Extraction Complete![/bold green]")
        console.print(f"⏱️  Total duration: {duration:.2f} seconds")
        console.print(f"📡 Total HTTP requests: {getattr(self, '_completed_requests', 0):,}")
        if hasattr(self, '_request_start_time'):
            final_rate = self._completed_requests / (time.time() - self._request_start_time) if time.time() - self._request_start_time > 0 else 0
            console.print(f"⚡ Average request rate: {final_rate:.1f} req/s")