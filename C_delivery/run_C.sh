#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${C_PYTHON:-}" ]]; then PY="$C_PYTHON"; elif [[ -x .venv/bin/python ]]; then PY="$PWD/.venv/bin/python"; else PY="$(command -v python3)"; fi
if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then export JAVA_HOME="$(/usr/libexec/java_home -v 17)"; fi
RUN="${C_RUN_DIR:-$PWD/C_delivery/results/C_$(date +%Y%m%d_%H%M%S)}"
DATA_ROOT="${C_DATA_ROOT:-$PWD/.runtime/frozen_root}"
FEATURES="${C_FEATURES:-$PWD/B_delivery/results/features_rerun_20261012}"
mkdir -p "$RUN/logs"
for stage in pilot develop final; do
  if [[ -f "$RUN/${stage}_run_status.json" ]] && "$PY" -c 'import json,sys;sys.exit(json.load(open(sys.argv[1]))["status"]!="PASS")' "$RUN/${stage}_run_status.json"; then
    echo "Already passed: $stage"
  else
    "$PY" C_delivery/src/C_train.py --stage "$stage" --root "$DATA_ROOT" --features "$FEATURES" --output "$RUN" > "$RUN/logs/$stage.log" 2>&1
  fi
done
if [[ ! -f "$RUN/model_reload_validation.json" ]]; then
  "$PY" C_delivery/src/C_package_model.py --run "$RUN" --root "$DATA_ROOT" > "$RUN/logs/package.log" 2>&1
fi
"$PY" C_delivery/src/C_make_report.py --run "$RUN" > "$RUN/logs/report.log" 2>&1
"$PY" C_delivery/src/C_finalize.py --run "$RUN" > "$RUN/logs/finalize.log" 2>&1
echo "PASS: isolated C run $RUN; model/report/lock/manifest are under this run"
