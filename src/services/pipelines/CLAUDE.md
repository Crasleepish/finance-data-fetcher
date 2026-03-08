# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Scope

- Pipeline implementations mapped by `config/task_pipeline_mapping.py` (spec → pipeline).
- Each pipeline should orchestrate fetch → clean → persist using core/infra modules.

## Related modules

- Fetch interfaces: `src/core/fetch/`
- Cleaning interfaces: `src/core/clean/`
- Pipeline contracts: `src/core/pipeline/`
