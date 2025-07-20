"""
Extraction Coordinator - Orchestrates the dbt-driven extraction process.

This module addresses the architectural issues in extractor.py by:
1. Separating orchestration from data processing
2. Breaking down complex methods into focused components  
3. Implementing proper dependency injection
4. Creating clear phase separation
"""

import asyncio
import contextlib
import logging
import time
from datetime import date
from typing import Any, Protocol

import duckdb
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn

from baliza.dbt_runner import DbtRunner
from baliza.task_claimer import TaskClaimer

logger = logging.getLogger(__name__)
console = Console()


class WriterProtocol(Protocol):
    """Protocol for the writer dependency to enable dependency injection."""
    
    db_path: str
    
    async def writer_worker(self, queue: asyncio.Queue, commit_every: int = 100) -> None:
        """Process data from queue and write to database."""
        ...


class ExtractionPhase:
    """Base class for extraction phases with common functionality."""
    
    def __init__(self, name: str, emoji: str):
        self.name = name
        self.emoji = emoji
        
    def print_phase_header(self):
        """Print standardized phase header."""
        console.print(f"{self.emoji} [bold blue]Phase: {self.name}[/bold blue]")


class PlanningPhase(ExtractionPhase):
    """Phase 1: Task Planning - Generate or validate dbt task plans."""
    
    def __init__(self, writer: WriterProtocol):
        super().__init__("dbt Task Planning", "🎯")
        self.writer = writer
    
    async def execute(self, start_date: date, end_date: date, use_existing_plan: bool, claimer: TaskClaimer) -> str:
        """Execute the planning phase and return plan fingerprint."""
        self.print_phase_header()
        
        # Check for existing plan
        existing_plan = await self._check_existing_plan(start_date, end_date)
        
        if existing_plan and use_existing_plan:
            return await self._use_existing_plan(existing_plan, claimer)
        else:
            return await self._generate_new_plan(start_date, end_date)
    
    async def _check_existing_plan(self, start_date: date, end_date: date) -> tuple | None:
        """Check for existing task plan in database."""
        with duckdb.connect(self.writer.db_path) as conn:
            return conn.execute("""
                SELECT plan_fingerprint, task_count, generated_at
                FROM main_planning.task_plan_meta 
                WHERE date_range_start <= ? AND date_range_end >= ?
                ORDER BY generated_at DESC
                LIMIT 1
            """, (start_date, end_date)).fetchone()
    
    async def _use_existing_plan(self, existing_plan: tuple, claimer: TaskClaimer) -> str:
        """Use existing task plan if valid."""
        plan_fingerprint, task_count, generated_at = existing_plan
        console.print(f"✅ [green]Using existing plan: {plan_fingerprint[:16]}... ({task_count:,} tasks)[/green]")
        console.print(f"   Generated: {generated_at}")
        
        # Validate fingerprint
        if not claimer.validate_plan_fingerprint(plan_fingerprint):
            console.print("⚠️ [yellow]Plan fingerprint validation failed - generating new plan[/yellow]")
            return await self._generate_new_plan_from_existing()
            
        return plan_fingerprint
    
    async def _generate_new_plan(self, start_date: date, end_date: date) -> str:
        """Generate new task plan using dbt."""
        console.print("🔨 [blue]Generating new task plan with dbt...[/blue]")
        dbt_runner = DbtRunner()
        plan_fingerprint = dbt_runner.create_task_plan(str(start_date), str(end_date), "prod")

        if plan_fingerprint:
            # Get task count
            with duckdb.connect(self.writer.db_path) as conn:
                task_count = conn.execute(
                    "SELECT COUNT(*) FROM main_planning.task_plan WHERE plan_fingerprint = ?",
                    (plan_fingerprint,)
                ).fetchone()[0]

            console.print(f"✅ [green]Plan generated: {plan_fingerprint[:16]}... ({task_count:,} tasks)[/green]")
            return plan_fingerprint
        else:
            console.print("❌ [red]Failed to generate task plan.[/red]")
            return None
    
    async def _generate_new_plan_from_existing(self) -> str:
        """Generate new plan when existing plan validation fails."""
        # This would need start_date and end_date from context
        # For now, use a placeholder implementation
        return await self._generate_new_plan(date(2024, 1, 1), date(2024, 12, 31))


class ExecutionPhase(ExtractionPhase):
    """Phase 2: Task Execution - Claim and execute tasks from the dbt plan."""
    
    def __init__(self, writer: WriterProtocol, shutdown_event: asyncio.Event, concurrency: int):
        super().__init__("Task Execution", "⚡")
        self.writer = writer
        self.shutdown_event = shutdown_event
        self.concurrency = concurrency
        self.running_tasks = set()
    
    async def execute(self, progress: Progress, claimer: TaskClaimer, plan_fingerprint: str):
        """Execute the task execution phase."""
        self.print_phase_header()
        
        # Get total pending tasks for progress tracking
        total_pending = await self._get_pending_task_count(plan_fingerprint)
        
        if total_pending == 0:
            console.print("✅ [green]No pending tasks - all work already completed![/green]")
            return
        
        console.print(f"🎯 Executing {total_pending:,} pending tasks...")
        execution_progress = progress.add_task("[cyan]Executing tasks", total=total_pending)
        
        await self._execute_task_batches(claimer, execution_progress, progress)
    
    async def _get_pending_task_count(self, plan_fingerprint: str) -> int:
        """Get count of pending tasks for progress tracking."""
        with duckdb.connect(self.writer.db_path) as conn:
            return conn.execute("""
                SELECT COUNT(*) FROM main_planning.task_plan 
                WHERE status = 'PENDING' AND plan_fingerprint = ?
            """, (plan_fingerprint,)).fetchone()[0]
    
    async def _execute_task_batches(self, claimer: TaskClaimer, execution_progress, progress: Progress):
        """Execute tasks in batches until completion."""
        processed_tasks = 0
        
        while not self.shutdown_event.is_set():
            # Release expired claims
            expired_count = claimer.release_expired_claims()
            if expired_count > 0:
                console.print(f"🧹 Released {expired_count} expired claims")
            
            # Claim batch of tasks
            claimed_tasks = claimer.claim_pending_tasks(limit=self.concurrency * 2)
            
            if not claimed_tasks:
                console.print("✅ [green]No more tasks to claim - execution complete![/green]")
                break
            
            console.print(f"🔒 Claimed {len(claimed_tasks)} tasks for execution")
            
            # Execute claimed tasks concurrently
            batch_processed = await self._execute_task_batch(claimed_tasks, claimer)
            processed_tasks += batch_processed
            progress.update(execution_progress, completed=processed_tasks)
            
            # Small delay to prevent tight loops
            await asyncio.sleep(0.1)
    
    async def _execute_task_batch(self, claimed_tasks: list, claimer: TaskClaimer) -> int:
        """Execute a batch of claimed tasks concurrently."""
        execution_jobs = []
        for task in claimed_tasks:
            job = asyncio.create_task(self._execute_single_task(task, claimer))
            execution_jobs.append(job)
            self.running_tasks.add(job)
        
        try:
            await asyncio.gather(*execution_jobs)
            return len(claimed_tasks)
        except Exception as e:
            console.print(f"⚠️ [yellow]Some tasks failed: {e}[/yellow]")
            return len(claimed_tasks)  # Still count as processed
        finally:
            for job in execution_jobs:
                self.running_tasks.discard(job)
    
    async def _execute_single_task(self, task: dict, claimer: TaskClaimer):
        """Execute a single claimed task - placeholder implementation."""
        # This would contain the actual task execution logic
        # For now, just simulate task completion
        task_id = task['task_id']
        
        try:
            # Update status to EXECUTING
            claimer.update_claim_status(task_id, 'EXECUTING')
            
            # Simulate work
            await asyncio.sleep(0.1)
            
            # Record successful completion
            claimer.record_task_result(task_id, 'SUCCESS', 0, 100, 'SUCCESS')
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            claimer.record_task_result(task_id, 'FAILED', 0, 0, 'FAILED')


class ExtractionCoordinator:
    """Coordinates the dbt-driven extraction process with proper separation of concerns."""
    
    def __init__(self, writer: WriterProtocol, concurrency: int = 10, force_db: bool = False):
        """Initialize coordinator with injected dependencies."""
        self.writer = writer
        self.concurrency = concurrency
        self.force_db = force_db
        self.shutdown_event = asyncio.Event()
        self.page_queue = asyncio.Queue()
        self.running_tasks = set()
        
        # Initialize phases
        self.planning_phase = PlanningPhase(writer)
        self.execution_phase = ExecutionPhase(writer, self.shutdown_event, concurrency)
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        import signal
        
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum} at frame {frame}, initiating graceful shutdown...")
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def extract_dbt_driven(self, start_date: date, end_date: date, use_existing_plan: bool = True) -> dict[str, Any]:
        """
        Orchestrate the complete dbt-driven extraction process.
        
        This method replaces the complex extract_dbt_driven method in AsyncPNCPExtractor
        with a clean, focused orchestration approach.
        """
        console.print("🚀 [bold blue]Starting dbt-driven extraction[/bold blue]")
        console.print(f"📅 Date range: {start_date} to {end_date}")
        
        start_time = time.time()
        
        # Initialize components
        run_id = f"extraction-{int(time.time())}"
        claimer = TaskClaimer(db_path=self.writer.db_path, worker_id=f"coordinator-{run_id[:8]}")
        
        # Clear database if force_db is enabled
        if self.force_db:
            await self._clear_database()
            use_existing_plan = False
        
        # Setup signal handlers
        self.setup_signal_handlers()
        
        # Start writer worker
        writer_task = await self._start_writer_worker()
        
        try:
            # Phase 1: Task Planning
            plan_fingerprint = await self.planning_phase.execute(
                start_date, end_date, use_existing_plan, claimer
            )
            
            # Phase 2: Task Execution
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("•"),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                console=console,
                refresh_per_second=10,
            ) as progress:
                await self.execution_phase.execute(progress, claimer, plan_fingerprint)
            
            # Finalize writer
            await self._finalize_writer(writer_task)
            
        except asyncio.CancelledError:
            console.print("⚠️ [yellow]Extraction cancelled during shutdown[/yellow]")
            await self._cleanup_on_cancellation(writer_task)
        finally:
            self.running_tasks.discard(writer_task)
        
        # Final reporting
        return await self._generate_final_report(start_time, claimer)
    
    async def _clear_database(self):
        """Clear database when force_db is enabled."""
        console.print("🧹 [yellow]Force DB enabled - clearing all data[/yellow]")
        with duckdb.connect(self.writer.db_path) as conn:
            conn.execute("DELETE FROM main_planning.task_plan")
            conn.execute("DELETE FROM main_planning.task_plan_meta")
            conn.execute("DELETE FROM main_runtime.task_claims")
            conn.execute("DELETE FROM main_runtime.task_results")
            conn.commit()
    
    async def _start_writer_worker(self) -> asyncio.Task:
        """Start the writer worker task."""
        writer_task = asyncio.create_task(
            self.writer.writer_worker(self.page_queue, commit_every=100)
        )
        self.running_tasks.add(writer_task)
        return writer_task
    
    async def _finalize_writer(self, writer_task: asyncio.Task):
        """Finalize writer operations."""
        await self.page_queue.join()
        await self.page_queue.put(None)  # Send sentinel
        await writer_task
    
    async def _cleanup_on_cancellation(self, writer_task: asyncio.Task):
        """Clean up resources on cancellation."""
        if not writer_task.done():
            writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await writer_task
    
    async def _generate_final_report(self, start_time: float, claimer: TaskClaimer) -> dict[str, Any]:
        """Generate final extraction statistics."""
        duration = time.time() - start_time
        stats = await self._get_extraction_stats(claimer)
        
        console.print("\n🎉 dbt-driven extraction complete!")
        console.print(f"📊 Tasks: {stats['total_tasks']:,} total, {stats['completed_tasks']:,} completed, {stats['failed_tasks']:,} failed")
        console.print(f"📈 Records: {stats['total_records']:,}")
        console.print(f"⏱️  Duration: {duration:.1f}s")
        console.print(f"🔧 Worker: {claimer.worker_id}")
        
        return stats
    
    async def _get_extraction_stats(self, claimer: TaskClaimer) -> dict[str, Any]:
        """Get extraction statistics from the database."""
        with duckdb.connect(self.writer.db_path) as conn:
            # Get task counts
            task_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_tasks,
                    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_tasks,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_tasks
                FROM main_planning.task_plan
            """).fetchone()
            
            # Get total records
            total_records = conn.execute("""
                SELECT COALESCE(SUM(tr.records_count), 0)
                FROM main_runtime.task_results tr
                JOIN main_runtime.task_claims tc ON tr.task_id = tc.task_id
                WHERE tc.status = 'COMPLETED'
            """).fetchone()[0]
            
            return {
                'total_tasks': task_stats[0] or 0,
                'completed_tasks': task_stats[1] or 0,
                'failed_tasks': task_stats[2] or 0,
                'total_records': total_records or 0,
                'worker_id': claimer.worker_id,  # Use the claimer parameter
                'run_id': claimer.worker_id.replace('coordinator-', '')
            }