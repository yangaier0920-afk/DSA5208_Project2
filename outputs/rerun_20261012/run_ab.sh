#!/bin/bash
set -euo pipefail
cd /Users/kendallshao/Downloads/DSA5208_Project2
export JAVA_HOME="$PWD/.runtime/java17/jdk-17.0.20.1+1/Contents/Home"
export MPLCONFIGDIR="$PWD/.runtime/matplotlib"
export PYTHONPATH="$PWD/B_delivery/src"
PY="$PWD/.venv/bin/python"
ROOT="$PWD/.runtime/frozen_root"
OUT="$PWD/outputs/rerun_20261012"
TASK="$PWD/B_delivery/results/task1_rerun_20261012"
FEAT="$PWD/B_delivery/results/features_rerun_20261012"
JOIN="$PWD/B_delivery/results/join_audit_rerun_20261012"
run_stage() {
 echo "$(date '+%F %T') START $1" | tee -a "$OUT/stages.log"
 local stage="$1"; shift
 if "$@" > "$OUT/$stage.log" 2>&1; then
  echo "$(date '+%F %T') PASS $stage" | tee -a "$OUT/stages.log"
 else
  echo "$(date '+%F %T') FAIL $stage" | tee -a "$OUT/stages.log"
  tail -40 "$OUT/$stage.log"
  exit 1
 fi
}
run_stage prepare "$PY" .runtime/prepare_frozen_root.py
run_stage A_full "$PY" scripts/read_data_release.py --root "$ROOT" --member B --scope full --receipt "$OUT/B_full_retry.json"
run_stage B_task1 "$PY" B_delivery/src/B_task1_analysis.py --root "$ROOT" --output "$TASK"
run_stage B_plots "$PY" B_delivery/src/B_plot_results.py --input "$TASK"
run_stage B_features "$PY" B_delivery/src/B_build_features.py --root "$ROOT" --output "$FEAT"
run_stage B_feature_api "$PY" B_delivery/src/B_validate_feature_api.py --root "$ROOT" --features "$FEAT"
run_stage B_join "$PY" B_delivery/src/B_join_audit.py --root "$ROOT" --features "$FEAT" --output "$JOIN"
echo "Compute stages complete; report and final metadata pending" | tee -a "$OUT/stages.log"
