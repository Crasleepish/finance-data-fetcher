#!/usr/bin/env sh
set -eu

uv run uvicorn api.main:app --app-dir src "$@"
