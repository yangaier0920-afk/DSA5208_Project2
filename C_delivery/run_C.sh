#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME="${JAVA_HOME:-$PWD/.runtime/java17/jdk-17.0.20.1+1/Contents/Home}"
RUN="${C_RUN_DIR:-$PWD/C_delivery/results/C_v1_20261012}"
DATA_ROOT="${C_DATA_ROOT:-$PWD/.runtime/frozen_root}"
FEATURES="${C_FEATURES:-$PWD/B_delivery/results/features_rerun_20261012}"
mkdir -p "$RUN/logs"
for stage in pilot develop final; do
  if [[ -f "$RUN/${stage}_run_status.json" ]] && .venv/bin/python -c 'import json,sys;sys.exit(json.load(open(sys.argv[1]))["status"]!="PASS")' "$RUN/${stage}_run_status.json"; then
    echo "Already passed: $stage"
  else
    .venv/bin/python C_delivery/src/C_train.py --stage "$stage" --root "$DATA_ROOT" --features "$FEATURES" --output "$RUN" > "$RUN/logs/$stage.log" 2>&1
  fi
done
if [[ ! -f "$RUN/model_reload_validation.json" ]]; then
  .venv/bin/python C_delivery/src/C_package_model.py --run "$RUN" --root "$DATA_ROOT" > "$RUN/logs/package.log" 2>&1
fi
.venv/bin/python C_delivery/src/C_make_report.py --run "$RUN" > "$RUN/logs/report.log" 2>&1
.venv/bin/python C_delivery/src/C_finalize.py --run "$RUN" > "$RUN/logs/finalize.log" 2>&1
