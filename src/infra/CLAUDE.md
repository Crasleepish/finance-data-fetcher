# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Scope

- Infrastructure adapters: DB, logging, external clients, fetcher implementations.
- This layer can depend on `core`, but should avoid service orchestration.

## Key locations

- `fetcher/`: concrete fetcher implementations for external data sources.
- `db/`: SQLAlchemy Core engine, tables, repository helpers.
- `http_client/`: HTTP client adapters used by fetchers.
- `queue/`: task queue abstractions/implementations.
- `worker_runtime/`: background worker runtime.
