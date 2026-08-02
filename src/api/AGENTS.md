# AGENTS.md

## Scope

- FastAPI entrypoints only. Keep HTTP-layer concerns here (request/response models, routing).
- Delegate orchestration to `src/services/`; avoid business logic in this layer.

## Key locations

- `routers/`: API route modules. Keep handlers thin and call services.
