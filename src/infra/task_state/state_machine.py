from __future__ import annotations

from dataclasses import dataclass

from models.task_status import TaskState

_ALLOWED_TRANSITIONS: dict[TaskState, set[TaskState]] = {
    TaskState.PENDING: {TaskState.RUNNING, TaskState.CANCELLED, TaskState.FAILED},
    TaskState.RUNNING: {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED},
    TaskState.SUCCEEDED: set(),
    TaskState.FAILED: set(),
    TaskState.CANCELLED: set(),
}


@dataclass(frozen=True)
class TaskStateMachine:
    """Finite-state machine enforcing task status transitions."""

    def can_transition(self, current: TaskState, target: TaskState) -> bool:
        """Return True if the transition is allowed."""
        return target in _ALLOWED_TRANSITIONS.get(current, set())

    def ensure_transition(self, current: TaskState, target: TaskState) -> None:
        """Raise ValueError if transition is not allowed."""
        if not self.can_transition(current, target):
            raise ValueError(f"Invalid transition: {current} -> {target}")
