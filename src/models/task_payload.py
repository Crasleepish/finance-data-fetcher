from __future__ import annotations

from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.pipeline.types import Arguments, Options
from core.pipeline.validation import validate_payload
from models.task_spec import TaskSpec


class IdempotencyInput(BaseModel):
    """Base input for idempotency key generation."""

    model_config = ConfigDict(frozen=True)

    spec: TaskSpec


class PipelineTask(IdempotencyInput):
    """Task payload for pipeline execution."""

    pipeline_id: str | None = None
    source: str
    task_type: str
    arguments: Arguments = Field(default_factory=lambda: cast(Arguments, {}))
    options: Options = Field(default_factory=lambda: cast(Options, {}))

    @field_validator("arguments", "options")
    @classmethod
    def _validate_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        validate_payload(value)
        return value
