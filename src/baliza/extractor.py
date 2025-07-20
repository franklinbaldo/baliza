"""PNCP Data Extractor V3 - Refactored Architecture

REFACTORED: Addresses architectural issues identified in issues/extractor.md:
- Removed complex 300+ line methods 
- Implemented dependency injection
- Separated concerns between orchestration and data processing
- Delegated complex orchestration to ExtractionCoordinator

Architecture:
- Focused on core data extraction responsibilities
- Clean delegation to specialized components
- Improved testability through dependency injection
"""

import asyncio
import calendar
import contextlib
import json
import logging
import re
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

# Import configuration from the config module
from baliza.config import settings
from baliza.pncp_client import PNCPClient
from baliza.pncp_task_planner import PNCPTaskPlanner
from baliza.pncp_writer import BALIZA_DB_PATH, DATA_DIR, PNCPWriter, connect_utf8
from baliza.extraction_coordinator import ExtractionCoordinator

# Configure standard logging com UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)

logger = logging.getLogger(__name__)
console = Console()


class AsyncPNCPExtractor:
    """
    REFACTORED: Simplified PNCP Data Extractor focused on core responsibilities.
    
    Complex orchestration methods have been moved to ExtractionCoordinator for better
    separation of concerns and improved maintainability.
    """

    def __init__(self, concurrency: int = 10, force_db: bool = False):
        """Initialize the extractor with dependency injection support."""
        self.concurrency = concurrency
        self.force_db = force_db
        self.shutdown_event = asyncio.Event()
        self.page_queue = asyncio.Queue()
        self.running_tasks = set()
        self.run_id = str(uuid.uuid4())
        self.writer = None  # Will be initialized in context manager
        self.writer_running = False

    async def __aenter__(self):
        """Async context manager entry with proper resource initialization."""
        # Initialize writer
        self.writer = PNCPWriter()
        await self.writer.__aenter__()
        
        # Setup signal handlers
        self.setup_signal_handlers()
        
        logger.info(f"AsyncPNCPExtractor initialized with concurrency={self.concurrency}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        # Stop writer if running
        self.writer_running = False
        
        # Cancel all running tasks
        for task in list(self.running_tasks):
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete with timeout
        if self.running_tasks:
            await asyncio.wait(self.running_tasks, timeout=30)
        
        # Clean up writer
        if self.writer:
            await self.writer.__aexit__(exc_type, exc_val, exc_tb)
        
        logger.info("AsyncPNCPExtractor cleanup complete")

    def setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        def signal_handler(signum, frame):
            console.print(f"\n⚠️ [yellow]Received signal {signum}, initiating graceful shutdown...[/yellow]")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def extract_dbt_driven(self, start_date: date, end_date: date, use_existing_plan: bool = True):
        """
        REFACTORED: dbt-driven extraction using ExtractionCoordinator.
        
        This method now delegates to ExtractionCoordinator for better separation of concerns
        and improved maintainability. The complex orchestration logic has been moved to
        dedicated phase classes.
        
        Args:
            start_date: Start date for extraction
            end_date: End date for extraction  
            use_existing_plan: Use existing plan if available, otherwise generate new
        """
        # Initialize coordinator with dependency injection
        coordinator = ExtractionCoordinator(
            writer=self.writer,
            concurrency=self.concurrency,
            force_db=self.force_db
        )
        
        # Delegate to coordinator for clean orchestration
        return await coordinator.extract_dbt_driven(start_date, end_date, use_existing_plan)

    # Legacy extract_data method preserved for backward compatibility
    async def extract_data(self, start_date: date, end_date: date, force: bool = False):
        """
        Legacy extraction method - preserved for backward compatibility.
        
        For new implementations, prefer extract_dbt_driven() which uses the
        improved architecture with proper separation of concerns.
        """
        console.print("⚠️ [yellow]Using legacy extraction method. Consider using extract_dbt_driven() for better performance and maintainability.[/yellow]")
        
        # Initialize task planner
        planner = PNCPTaskPlanner(start_date, end_date)
        
        # Simple extraction logic for backward compatibility
        start_time = time.time()
        total_records = 0
        
        console.print("📋 [blue]Starting legacy extraction...[/blue]")
        
        # Get tasks from planner
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            
            # Phase 1: Planning
            tasks = await planner.generate_tasks()
            console.print(f"📊 Generated {len(tasks):,} extraction tasks")
            
            if not force:
                # Filter out existing data (simple check)
                tasks = [task for task in tasks if not await self._task_already_processed(task)]
                console.print(f"📊 Filtered to {len(tasks):,} new tasks")
            
            if not tasks:
                console.print("✅ [green]No new data to extract![/green]")
                return {"total_records_extracted": 0, "run_id": self.run_id}
            
            # Phase 2: Execution
            extraction_progress = progress.add_task("[cyan]Extracting data", total=len(tasks))
            
            async with PNCPClient(self.concurrency) as client:
                for task in tasks:
                    if self.shutdown_event.is_set():
                        break
                        
                    try:
                        records = await self._extract_task_simple(client, task)
                        total_records += records
                        progress.update(extraction_progress, advance=1)
                    except Exception as e:
                        logger.error(f"Failed to extract task {task}: {e}")
                        continue
        
        duration = time.time() - start_time
        console.print(f"\n✅ [green]Legacy extraction complete![/green]")
        console.print(f"📊 Total records: {total_records:,}")
        console.print(f"⏱️  Duration: {duration:.1f}s")
        
        return {
            "total_records_extracted": total_records,
            "run_id": self.run_id,
            "duration": duration
        }

    async def _task_already_processed(self, task: dict) -> bool:
        """Simple check if task has already been processed."""
        # Simplified implementation - check if any data exists for this endpoint/date
        try:
            count = self.writer.conn.execute("""
                SELECT COUNT(*) FROM psa.pncp_requests 
                WHERE endpoint_name = ? AND data_date = ?
            """, (task.get('endpoint_name'), task.get('data_date'))).fetchone()[0]
            return count > 0
        except Exception:
            return False

    async def _extract_task_simple(self, client: PNCPClient, task: dict) -> int:
        """Simple task extraction for legacy compatibility."""
        endpoint_name = task.get('endpoint_name', '')
        data_date = task.get('data_date')
        
        # Build simple request
        url = f"/v1/{endpoint_name}"
        params = {
            "dataInicial": str(data_date),
            "dataFinal": str(data_date),
            "pagina": 1,
            "tamanhoPagina": 500
        }
        
        try:
            response = await client.fetch_with_backpressure(url, params)
            if response.get('success'):
                data = response.get('data', [])
                
                # Simple write to database
                if data:
                    await self._write_simple_data(endpoint_name, data_date, data)
                
                return len(data)
            else:
                logger.warning(f"Failed to fetch {endpoint_name} for {data_date}: {response.get('error')}")
                return 0
                
        except Exception as e:
            logger.error(f"Error extracting {endpoint_name} for {data_date}: {e}")
            return 0

    async def _write_simple_data(self, endpoint_name: str, data_date: date, data: list):
        """Simple data writing for legacy compatibility."""
        try:
            # Write to bronze layer with basic structure
            request_id = str(uuid.uuid4())
            
            for record in data:
                self.writer.conn.execute("""
                    INSERT INTO bronze.pncp_raw (
                        request_id, endpoint_name, data_date, 
                        record_data, extracted_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    request_id, endpoint_name, data_date,
                    json.dumps(record), datetime.now()
                ))
            
            self.writer.conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to write data for {endpoint_name}: {e}")

    # Additional utility methods for fetching pages (preserved from original)
    async def _fetch_with_backpressure(self, url: str, params: dict, task_id: str = None):
        """Fetch data with backpressure control."""
        async with PNCPClient(self.concurrency) as client:
            return await client.fetch_with_backpressure(url, params, task_id)

    def _create_progress_summary(self, stats: dict) -> Table:
        """Create a beautiful progress summary table."""
        table = Table(show_header=True, header_style="bold blue", title="🎯 Extraction Summary")
        
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Tasks", f"{stats.get('total_tasks', 0):,}")
        table.add_row("Completed", f"{stats.get('completed_tasks', 0):,}")
        table.add_row("Failed", f"{stats.get('failed_tasks', 0):,}")
        table.add_row("Records Extracted", f"{stats.get('total_records', 0):,}")
        
        return table