"""Atomic task claiming for dbt-driven task planning.

Implements atomic task claiming with expiration to prevent zombie claims
and enable concurrent worker safety.

ADR-009: dbt-Driven Task Planning Architecture
"""

import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
import duckdb
import logging

from baliza.plan_fingerprint import PlanFingerprint

logger = logging.getLogger(__name__)


class TaskClaimer:
    """Handles atomic task claiming for concurrent worker execution."""
    
    def __init__(self, db_path: str = "data/baliza.duckdb", worker_id: Optional[str] = None):
        """Initialize task claimer.
        
        Args:
            db_path: Path to DuckDB database
            worker_id: Unique identifier for this worker (auto-generated if None)
        """
        self.db_path = db_path
        self.worker_id = worker_id or f"worker-{str(uuid.uuid4())[:8]}"
        self.fingerprint = PlanFingerprint()
        
    def validate_plan_fingerprint(self, expected_fingerprint: str) -> bool:
        """Validate that the current configuration matches the plan fingerprint.
        
        This prevents execution of stale plans when configuration has changed.
        
        Args:
            expected_fingerprint: Fingerprint from task plan metadata
            
        Returns:
            True if fingerprints match, False if configuration drift detected
        """
        try:
            # Generate current fingerprint (environment will be detected from plan)
            with duckdb.connect(self.db_path) as conn:
                # Get environment from existing plan metadata
                result = conn.execute("""
                    SELECT environment, date_range_start, date_range_end 
                    FROM main_planning.task_plan_meta 
                    WHERE plan_fingerprint = ?
                    LIMIT 1
                """, (expected_fingerprint,)).fetchone()
                
                if not result:
                    logger.warning(f"No plan metadata found for fingerprint {expected_fingerprint[:16]}...")
                    return False
                
                environment, start_date, end_date = result
                current_fingerprint = self.fingerprint.generate_fingerprint(
                    date_range_start=str(start_date),
                    date_range_end=str(end_date),
                    environment=environment
                )
                
                return current_fingerprint == expected_fingerprint
                
        except Exception as e:
            logger.error(f"Fingerprint validation failed: {e}")
            return False
    
    def claim_pending_tasks(self, limit: int = 100, claim_timeout_minutes: int = 15) -> List[Dict[str, Any]]:
        """Atomically claim pending tasks for execution.
        
        Uses SELECT...FOR UPDATE for atomic claiming to prevent race conditions
        between concurrent workers.
        
        Args:
            limit: Maximum number of tasks to claim
            claim_timeout_minutes: Claim expiration timeout
            
        Returns:
            List of claimed task dictionaries
        """
        with duckdb.connect(self.db_path) as conn:
            try:
                conn.begin()
                
                # This query now uses a simplified approach for claiming
                available_tasks = conn.execute("""
                    UPDATE main_planning.task_plan
                    SET status = 'CLAIMED'
                    WHERE task_id IN (
                        SELECT task_id
                        FROM main_planning.task_plan
                        WHERE status = 'PENDING'
                        ORDER BY data_date ASC, endpoint_name ASC
                        LIMIT ?
                    )
                    RETURNING task_id, endpoint_name, data_date, modalidade, plan_fingerprint
                """, (limit,)).fetchall()

                if not available_tasks:
                    conn.commit()
                    return []
                
                expires_at = datetime.utcnow() + timedelta(minutes=claim_timeout_minutes)
                claimed_tasks = []
                for task_row in available_tasks:
                    task_id, endpoint_name, data_date, modalidade, plan_fingerprint = task_row
                    
                    conn.execute("""
                        INSERT INTO main_runtime.task_claims 
                        (claim_id, task_id, claimed_at, expires_at, worker_id, status)
                        VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, 'CLAIMED')
                        ON CONFLICT (task_id) DO UPDATE SET
                        claimed_at = CURRENT_TIMESTAMP,
                        expires_at = ?,
                        worker_id = ?,
                        status = 'CLAIMED'
                    """, (str(uuid.uuid4()), task_id, expires_at, self.worker_id, expires_at, self.worker_id))

                    claimed_tasks.append({
                        'task_id': task_id,
                        'endpoint_name': endpoint_name,
                        'data_date': data_date,
                        'modalidade': modalidade,
                        'plan_fingerprint': plan_fingerprint,
                        'claimed_at': datetime.utcnow(),
                        'expires_at': expires_at,
                        'worker_id': self.worker_id
                    })

                conn.commit()
                
                logger.info(f"🔒 Claimed {len(claimed_tasks)} tasks for worker {self.worker_id}")
                return claimed_tasks
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Task claiming failed: {e}")
                raise
    
    def update_claim_status(self, task_id: str, status: str) -> None:
        """Update the status of a claimed task.
        
        Args:
            task_id: Task identifier
            status: New status (EXECUTING, COMPLETED, FAILED)
        """
        valid_statuses = ['CLAIMED', 'EXECUTING', 'COMPLETED', 'FAILED']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")
        
        with duckdb.connect(self.db_path) as conn:
            # Update claim status
            conn.execute("""
                UPDATE main_runtime.task_claims 
                SET status = ?
                WHERE task_id = ? AND worker_id = ?
            """, (status, task_id, self.worker_id))
            
            # Update task plan status if completed or failed
            if status in ['COMPLETED', 'FAILED']:
                conn.execute("""
                    UPDATE main_planning.task_plan 
                    SET status = ?
                    WHERE task_id = ?
                """, (status, task_id))
    
    def record_task_result(self, task_id: str, request_id: str, page_number: int, records_count: int) -> None:
        """Record execution result for a task.
        
        Args:
            task_id: Task identifier
            request_id: PNCP request ID for traceability
            page_number: Page number in paginated response
            records_count: Number of records in this page
        """
        with duckdb.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO main_runtime.task_results 
                (result_id, task_id, request_id, page_number, records_count, completed_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (str(uuid.uuid4()), task_id, request_id, page_number, records_count))
    
    def release_expired_claims(self) -> int:
        """Release claims that have expired (claim reaper function).
        
        This is the simplified claim reaper that replaces complex heartbeat mechanisms.
        Should be called periodically (every 5 minutes) to clean up zombie claims.
        
        Returns:
            Number of expired claims released
        """
        with duckdb.connect(self.db_path) as conn:
            # Update expired claims to FAILED
            result = conn.execute("""
                UPDATE main_runtime.task_claims 
                SET status = 'FAILED'
                WHERE status IN ('CLAIMED', 'EXECUTING') 
                  AND expires_at < CURRENT_TIMESTAMP
            """)
            
            expired_count = result.fetchone()[0] if result else 0
            
            if expired_count > 0:
                # Also update corresponding task plan statuses
                conn.execute("""
                    UPDATE main_planning.task_plan 
                    SET status = 'PENDING'
                    WHERE task_id IN (
                        SELECT task_id FROM main_runtime.task_claims 
                        WHERE status = 'FAILED' 
                          AND expires_at < CURRENT_TIMESTAMP
                    )
                """)
                
                logger.info(f"🧹 Released {expired_count} expired claims")
            
            return expired_count
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """Get statistics for this worker's claimed tasks.
        
        Returns:
            Dictionary with worker statistics
        """
        with duckdb.connect(self.db_path) as conn:
            stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_claims,
                    COUNT(CASE WHEN status = 'CLAIMED' THEN 1 END) as pending_claims,
                    COUNT(CASE WHEN status = 'EXECUTING' THEN 1 END) as executing_claims,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_claims,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_claims
                FROM main_runtime.task_claims
                WHERE worker_id = ?
            """, (self.worker_id,)).fetchone()
            
            if stats:
                return {
                    'worker_id': self.worker_id,
                    'total_claims': stats[0],
                    'pending_claims': stats[1],
                    'executing_claims': stats[2],
                    'completed_claims': stats[3],
                    'failed_claims': stats[4]
                }
            else:
                return {
                    'worker_id': self.worker_id,
                    'total_claims': 0,
                    'pending_claims': 0,
                    'executing_claims': 0,
                    'completed_claims': 0,
                    'failed_claims': 0
                }

