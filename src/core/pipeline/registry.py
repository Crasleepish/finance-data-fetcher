from __future__ import annotations

from dataclasses import dataclass, field

from core.pipeline.pipeline import IngestionPipeline


@dataclass
class PipelineRegistry:
    """Registry for resolving pipelines by id."""

    _pipelines: dict[str, IngestionPipeline] = field(default_factory=dict)

    def register(self, pipeline_id: str, pipeline: IngestionPipeline) -> None:
        """Register a pipeline implementation by id."""
        self._pipelines[pipeline_id] = pipeline

    def get(self, pipeline_id: str) -> IngestionPipeline:
        """Return a pipeline for the id or raise KeyError."""
        return self._pipelines[pipeline_id]
