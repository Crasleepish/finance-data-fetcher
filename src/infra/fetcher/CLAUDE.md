# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Scope

- Concrete fetcher implementations (Akshare, Tushare, etc.).
- Must follow interfaces in `src/core/fetch/` and avoid embedding business logic.

## Related modules

- HTTP adapters: `src/infra/http_client/`
- Token helpers: `src/infra/tushare/`, `src/infra/xueqiu_token_*`
