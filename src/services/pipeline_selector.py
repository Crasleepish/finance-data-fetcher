from __future__ import annotations

from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from models.task_spec import TaskSpec


@dataclass(frozen=True)
class PipelineSelector:
    """Select pipeline candidates based on task spec mapping."""

    mapping: dict[TaskSpec, list[str]]

    def candidates_for(self, spec: TaskSpec) -> list[str]:
        """Return ordered pipeline candidates for the spec."""
        return list(self.mapping.get(spec, []))


def load_pipeline_mapping(path: str) -> dict[TaskSpec, list[str]]:
    """Load pipeline mapping from a Python file path."""
    mapping_path = Path(path)
    if not mapping_path.exists():
        raise FileNotFoundError(f"pipeline mapping not found: {mapping_path}")

    spec = spec_from_file_location("task_pipeline_mapping", mapping_path)
    if spec is None or spec.loader is None:
        raise ValueError("failed to load pipeline mapping module")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    raw = getattr(module, "TASK_PIPELINE_MAPPING", None)
    if not isinstance(raw, dict):
        raise ValueError("TASK_PIPELINE_MAPPING must be a dict")

    mapping: dict[TaskSpec, list[str]] = {}
    for key, value in raw.items():
        spec_key = TaskSpec(key) if not isinstance(key, TaskSpec) else key
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError("pipeline mapping values must be list[str]")
        mapping[spec_key] = list(value)

    return mapping
