# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Scope

- Framework-agnostic business logic: pipeline contracts, fetch/clean abstractions, chunking, workflow policies, and analytics helpers.
- This layer must not import `api` or `services`.

## Key subdomains

- `pipeline/`: pipeline interfaces, registry, validation, and types.
- `fetch/`: fetcher interfaces, retry policies, and fetch errors.
- `clean/`: cleaners and validation helpers.
- `workflow/`: workflow orchestration and failover policies.
- `chunking/`: chunk policies for splitting arguments.
