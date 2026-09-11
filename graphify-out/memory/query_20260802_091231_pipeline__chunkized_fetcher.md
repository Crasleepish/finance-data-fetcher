---
type: "query"
date: "2026-08-02T09:12:31.354160+00:00"
question: "pipeline, chunkized fetcher"
contributor: "graphify"
outcome: "useful"
source_nodes: ["ChunkPolicy (chunked fetching)", "Fetcher", "IngestionPipeline", "PipelineRegistry"]
---

# Q: pipeline, chunkized fetcher

## Answer

Expanded from original query via graph vocab: [pipeline, chunked, fetcher]. BFS started at Fetcher, ChunkPolicy (chunked fetching), and PipelineRegistry and found 297 nodes. The graph places ChunkPolicy in src/core/pipeline/types.py:L16, Fetcher in src/core/fetch/fetcher.py:L8, IngestionPipeline in src/core/pipeline/pipeline.py:L8, and PipelineRegistry in src/core/pipeline/registry.py:L9. The traversal was truncated before enumerating relationships, so it does not establish a specific Fetcher-to-ChunkPolicy edge.

## Outcome

- Signal: useful

## Source Nodes

- ChunkPolicy (chunked fetching)
- Fetcher
- IngestionPipeline
- PipelineRegistry