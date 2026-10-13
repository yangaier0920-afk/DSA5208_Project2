#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${C_PYTHON:-}" ]]; then PY="$C_PYTHON"; elif [[ -x .venv/bin/python ]]; then PY="$PWD/.venv/bin/python"; else PY="$(command -v python3)"; fi
if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then export JAVA_HOME="$(/usr/libexec/java_home -v 17)"; fi
RUN="${C_REVISION_RUN:-$PWD/C_delivery/results/C_revision_20261013}"
SOURCE="${C_SOURCE_RUN:-$PWD/C_delivery/results/C_v1_20261012}"
DATA="${C_DATA_ROOT:-$PWD/.runtime/frozen_root}"
FEATURES="${C_FEATURES:-$PWD/B_delivery/results/features_rerun_20261012}"
mkdir -p "$RUN/logs"
ANCHORS="${C_INPUT_ANCHORS:-$SOURCE}"
if [[ ! -d "$ANCHORS/fold1/anchors" ]]; then
  ANCHORS="$PWD/C_delivery/results/rebuilt_inputs_$(basename "$RUN")"
  "$PY" C_delivery/src/C_revision_rebuild_anchors.py --root "$DATA" --features "$FEATURES" --reference-run "$SOURCE" --output "$ANCHORS" > "$RUN/logs/rebuild_anchors.log" 2>&1
fi
"$PY" C_delivery/src/C_test_distribution.py > "$RUN/logs/distribution_contracts.log" 2>&1
"$PY" C_delivery/src/C_revision_experiments.py --source-run "$SOURCE" --input-anchors "$ANCHORS" --root "$DATA" --features "$FEATURES" --output "$RUN" > "$RUN/logs/development.log" 2>&1
for stage in select refit reload guard; do
  if [[ "$stage" == reload && -f "$RUN/reload_validation.json" ]]; then continue; fi
  "$PY" C_delivery/src/C_revision_select_refit.py --run "$RUN" --source-run "$SOURCE" --input-anchors "$ANCHORS" --root "$DATA" --stage "$stage" > "$RUN/logs/$stage.log" 2>&1
done
"$PY" C_delivery/src/C_revision_validate.py --run "$RUN" > "$RUN/logs/evidence_validation.log" 2>&1
"$PY" C_delivery/src/C_revision_report.py --run "$RUN" --source-run "$SOURCE" > "$RUN/logs/report.log" 2>&1
"$PY" C_delivery/src/C_finalize.py --run "$RUN" > "$RUN/logs/finalize.log" 2>&1
echo "PASS: C development revision; 2024 was not rescored. All outputs isolated under $RUN"
