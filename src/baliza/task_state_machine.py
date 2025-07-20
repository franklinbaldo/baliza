"""
Task State Machine for BALIZA extraction tasks.

Implements a proper state machine to replace hardcoded string-based status management
with validated transitions and clear state definitions.

Addresses architectural issue: "Lack of a Clear State Machine: The task status is 
managed with strings (PENDING, DISCOVERING, FETCHING, etc.). A more robust state 
machine implementation would make the logic clearer and less error-prone."
"""

from enum import Enum
from typing import Dict, Set, Optional, List
import logging

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Enumeration of all possible task statuses in BALIZA extraction pipeline."""
    
    # Task Plan statuses (main_planning.task_plan)
    PENDING = "PENDING"        # Task created, ready for claiming
    CLAIMED = "CLAIMED"        # Task claimed by a worker
    COMPLETED = "COMPLETED"    # Task successfully completed
    FAILED = "FAILED"         # Task failed (permanent failure)
    
    # Task Claim statuses (main_runtime.task_claims) 
    EXECUTING = "EXECUTING"    # Task currently being executed
    
    # Legacy statuses (for backward compatibility with extractor_legacy.py)
    DISCOVERING = "DISCOVERING"  # Discovery phase (getting metadata)
    FETCHING = "FETCHING"       # Execution phase (downloading pages)
    PARTIAL = "PARTIAL"         # Partially completed (some pages missing)
    

class TaskStateMachine:
    """State machine for managing task status transitions with validation."""
    
    # Define valid state transitions
    _VALID_TRANSITIONS: Dict[TaskStatus, Set[TaskStatus]] = {
        TaskStatus.PENDING: {
            TaskStatus.CLAIMED,      # Worker claims the task
            TaskStatus.FAILED,       # Direct failure during claiming
        },
        TaskStatus.CLAIMED: {
            TaskStatus.EXECUTING,    # Worker starts execution
            TaskStatus.PENDING,      # Claim expires/released, back to pending
            TaskStatus.FAILED,       # Failure during claim
        },
        TaskStatus.EXECUTING: {
            TaskStatus.COMPLETED,    # Successful execution
            TaskStatus.FAILED,       # Execution failure
            TaskStatus.PENDING,      # Claim expires, task returns to pending
        },
        TaskStatus.COMPLETED: {
            # Terminal state - no transitions allowed
        },
        TaskStatus.FAILED: {
            TaskStatus.PENDING,      # Manual retry/reset
        },
        
        # Legacy statuses for backward compatibility
        TaskStatus.DISCOVERING: {
            TaskStatus.FETCHING,     # Discovery completed
            TaskStatus.COMPLETED,    # No data found (empty endpoint)
            TaskStatus.FAILED,       # Discovery failed
        },
        TaskStatus.FETCHING: {
            TaskStatus.COMPLETED,    # All pages downloaded
            TaskStatus.PARTIAL,      # Some pages downloaded
            TaskStatus.FAILED,       # Fetching failed
        },
        TaskStatus.PARTIAL: {
            TaskStatus.FETCHING,     # Resume partial download
            TaskStatus.COMPLETED,    # All remaining pages downloaded
            TaskStatus.FAILED,       # Final failure
        },
    }
    
    def __init__(self):
        """Initialize the state machine."""
        self._current_status: Optional[TaskStatus] = None
        self._transition_history: List[tuple] = []
    
    @classmethod
    def is_valid_transition(cls, from_status: TaskStatus, to_status: TaskStatus) -> bool:
        """Check if a status transition is valid.
        
        Args:
            from_status: Current status
            to_status: Desired status
            
        Returns:
            True if transition is valid, False otherwise
        """
        if from_status not in cls._VALID_TRANSITIONS:
            logger.warning(f"Unknown source status: {from_status}")
            return False
            
        return to_status in cls._VALID_TRANSITIONS[from_status]
    
    @classmethod
    def get_valid_transitions(cls, from_status: TaskStatus) -> Set[TaskStatus]:
        """Get all valid transitions from a given status.
        
        Args:
            from_status: Current status
            
        Returns:
            Set of valid target statuses
        """
        return cls._VALID_TRANSITIONS.get(from_status, set())
    
    @classmethod
    def validate_transition(cls, from_status: TaskStatus, to_status: TaskStatus, 
                          task_id: str = None, raise_on_invalid: bool = True) -> bool:
        """Validate a status transition and optionally raise exception.
        
        Args:
            from_status: Current status
            to_status: Desired status
            task_id: Task identifier for logging
            raise_on_invalid: Whether to raise exception on invalid transition
            
        Returns:
            True if transition is valid
            
        Raises:
            ValueError: If transition is invalid and raise_on_invalid is True
        """
        if cls.is_valid_transition(from_status, to_status):
            return True
        
        error_msg = (
            f"Invalid status transition: {from_status.value} → {to_status.value}"
            f"{f' for task {task_id}' if task_id else ''}"
        )
        
        if raise_on_invalid:
            raise ValueError(error_msg)
        else:
            logger.error(error_msg)
            return False
    
    @classmethod
    def is_terminal_status(cls, status: TaskStatus) -> bool:
        """Check if a status is terminal (no further transitions).
        
        Args:
            status: Status to check
            
        Returns:
            True if status is terminal
        """
        return len(cls._VALID_TRANSITIONS.get(status, set())) == 0
    
    @classmethod
    def is_active_status(cls, status: TaskStatus) -> bool:
        """Check if a status represents an active/in-progress task.
        
        Args:
            status: Status to check
            
        Returns:
            True if status represents active work
        """
        active_statuses = {
            TaskStatus.CLAIMED,
            TaskStatus.EXECUTING,
            TaskStatus.DISCOVERING,
            TaskStatus.FETCHING,
        }
        return status in active_statuses
    
    @classmethod
    def get_status_categories(cls) -> Dict[str, Set[TaskStatus]]:
        """Get status categories for reporting and filtering.
        
        Returns:
            Dictionary of status categories
        """
        return {
            'pending': {TaskStatus.PENDING},
            'active': {
                TaskStatus.CLAIMED,
                TaskStatus.EXECUTING,
                TaskStatus.DISCOVERING,
                TaskStatus.FETCHING,
            },
            'partial': {TaskStatus.PARTIAL},
            'completed': {TaskStatus.COMPLETED},
            'failed': {TaskStatus.FAILED},
            'terminal': {TaskStatus.COMPLETED, TaskStatus.FAILED},
            'retryable': {TaskStatus.FAILED, TaskStatus.PARTIAL},
        }
    
    def transition_to(self, new_status: TaskStatus, task_id: str = None) -> bool:
        """Transition to a new status with validation.
        
        Args:
            new_status: Target status
            task_id: Task identifier for logging
            
        Returns:
            True if transition was successful
            
        Raises:
            ValueError: If transition is invalid
        """
        if self._current_status is None:
            # First status assignment
            self._current_status = new_status
            self._transition_history.append((None, new_status))
            return True
        
        if self.validate_transition(self._current_status, new_status, task_id):
            old_status = self._current_status
            self._current_status = new_status
            self._transition_history.append((old_status, new_status))
            logger.debug(f"Task {task_id or 'unknown'}: {old_status.value} → {new_status.value}")
            return True
        
        return False
    
    @property
    def current_status(self) -> Optional[TaskStatus]:
        """Get the current status."""
        return self._current_status
    
    @property
    def transition_history(self) -> List[tuple]:
        """Get the transition history."""
        return self._transition_history.copy()


def validate_status_value(status_str: str) -> TaskStatus:
    """Convert and validate a string status value.
    
    Args:
        status_str: Status as string
        
    Returns:
        TaskStatus enum value
        
    Raises:
        ValueError: If status string is invalid
    """
    try:
        return TaskStatus(status_str)
    except ValueError:
        valid_statuses = [s.value for s in TaskStatus]
        raise ValueError(
            f"Invalid status '{status_str}'. Valid statuses: {valid_statuses}"
        )


def get_db_status_constraint() -> str:
    """Get the database CHECK constraint for status columns.
    
    Returns:
        SQL CHECK constraint string for status validation
    """
    valid_values = [s.value for s in TaskStatus]
    values_str = "', '".join(valid_values)
    return f"CHECK (status IN ('{values_str}'))"


class TaskStatusUpdater:
    """Helper class for safe status updates with state machine validation."""
    
    def __init__(self, validate_transitions: bool = True):
        """Initialize the status updater.
        
        Args:
            validate_transitions: Whether to validate state transitions
        """
        self.validate_transitions = validate_transitions
        self.state_machine = TaskStateMachine()
    
    def update_status(self, current_status: str, new_status: str, 
                     task_id: str = None, force: bool = False) -> TaskStatus:
        """Update task status with validation.
        
        Args:
            current_status: Current status as string
            new_status: New status as string  
            task_id: Task identifier for logging
            force: Skip validation if True
            
        Returns:
            New status as TaskStatus enum
            
        Raises:
            ValueError: If status transition is invalid
        """
        current_enum = validate_status_value(current_status)
        new_enum = validate_status_value(new_status)
        
        if not force and self.validate_transitions:
            TaskStateMachine.validate_transition(current_enum, new_enum, task_id)
        
        return new_enum


# Backward compatibility: export status constants
PENDING = TaskStatus.PENDING.value
CLAIMED = TaskStatus.CLAIMED.value
EXECUTING = TaskStatus.EXECUTING.value
COMPLETED = TaskStatus.COMPLETED.value
FAILED = TaskStatus.FAILED.value
DISCOVERING = TaskStatus.DISCOVERING.value
FETCHING = TaskStatus.FETCHING.value
PARTIAL = TaskStatus.PARTIAL.value

# Export all status values for easy access
ALL_STATUSES = [s.value for s in TaskStatus]