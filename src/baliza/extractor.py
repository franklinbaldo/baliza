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
import logging
import signal
import sys
import uuid
from datetime import date

from rich.console import Console

from .extraction_coordinator import ExtractionCoordinator
from .pncp_writer import PNCPWriter
from .task_claimer import TaskClaimer
from .dbt_runner import DbtRunner

# Configure standard logging com UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)

logger = logging.getLogger(__name__)
console = Console()


class AsyncPNCPExtractor:
    def __init__(self, concurrency: int = 10, force_db: bool = False):
        self.concurrency = concurrency
        self.force_db = force_db
        self.shutdown_event = asyncio.Event()
        self.run_id = str(uuid.uuid4())
        self.writer = PNCPWriter(force_db=self.force_db)
        self.task_claimer = TaskClaimer(db_path=self.writer.db_path)
        self.dbt_runner = DbtRunner()

    async def __aenter__(self):
        await self.writer.__aenter__()
        self.setup_signal_handlers()
        logger.info(f"AsyncPNCPExtractor initialized with concurrency={self.concurrency}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.writer.__aexit__(exc_type, exc_val, exc_tb)
        logger.info("AsyncPNCPExtractor cleanup complete")

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            console.print(f"\n⚠️ [yellow]Received signal {signum}, initiating graceful shutdown...[/yellow]")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def extract_dbt_driven(self, start_date: date, end_date: date, use_existing_plan: bool = True):
        coordinator = ExtractionCoordinator(
            writer=self.writer,
            task_claimer=self.task_claimer,
            dbt_runner=self.dbt_runner,
            concurrency=self.concurrency,
            force_db=self.force_db
        )
        return await coordinator.extract_dbt_driven(start_date, end_date, use_existing_plan)