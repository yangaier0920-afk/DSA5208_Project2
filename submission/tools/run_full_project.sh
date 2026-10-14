#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."
PY="${PROJECT_PYTHON:-$PWD/.venv/bin/python}"
if [[ ! -x "$PY" ]]; then PY="$(command -v python3)"; fi
exec "$PY" submission/tools/run_project.py "$@"
