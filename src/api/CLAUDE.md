# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Scope

- FastAPI entrypoints only. Keep HTTP-layer concerns here (request/response models, routing).
- Delegate orchestration to `src/services/`; avoid business logic in this layer.

## Key locations

- `routers/`: API route modules. Keep handlers thin and call services.
