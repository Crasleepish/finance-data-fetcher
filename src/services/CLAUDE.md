# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Scope

- Orchestration layer: coordinate pipelines, workflows, and infra adapters.
- Services may call `src/infra/` and `src/core/`, but avoid API or UI concerns.

## Key locations

- `pipelines/`: pipeline implementations bound to specs.
- `workflow_engine.py`: workflow orchestration.
- `worker_handler.py`: task execution handler.
