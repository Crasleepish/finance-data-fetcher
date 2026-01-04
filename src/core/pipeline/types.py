from __future__ import annotations

from typing import Any, Mapping, Sequence, TypedDict


class Arguments(TypedDict, total=False):
    """Lightweight, JSON-serializable task arguments."""

    params: dict[str, Any]


Options = dict[str, Any]
# Lightweight, JSON-serializable task options (arbitrary keys allowed).


class ChunkArgs(TypedDict, total=False):
    """Chunk arguments used by pipelines to fetch partial data."""

    cursor: str
    params: dict[str, Any]


RawBatch = Sequence[Mapping[str, Any]]
NormalizedBatch = Sequence[Mapping[str, Any]]
