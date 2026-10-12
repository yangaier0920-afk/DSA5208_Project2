#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
PY="$PYSPARK_PYTHON"
ID="${PROJECT_RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
ROOT="$PWD/.runtime/frozen_root"
TASK="$PWD/B_delivery/results/task1_$ID"
FEATURES="$PWD/B_delivery/results/features_$ID"
JOIN="$PWD/B_delivery/results/join_audit_$ID"
OUT="$PWD/outputs/reproduction_$ID"
mkdir -p "$OUT"
"$PY" submission/tools/prepare_frozen_runtime.py
"$PY" scripts/read_data_release.py --root "$ROOT" --member C --scope full --receipt "$OUT/full_read.json"
"$PY" scripts/reproduce_a_sample.py --root "$ROOT" --output "$OUT/A_sample"
"$PY" B_delivery/src/B_task1_analysis.py --root "$ROOT" --output "$TASK"
"$PY" B_delivery/src/B_plot_results.py --input "$TASK"
"$PY" B_delivery/src/B_build_features.py --root "$ROOT" --output "$FEATURES"
"$PY" B_delivery/src/B_validate_feature_api.py --root "$ROOT" --features "$FEATURES"
"$PY" B_delivery/src/B_join_audit.py --root "$ROOT" --features "$FEATURES" --output "$JOIN"
"$PY" C_delivery/src/C_test_contracts.py
C_DATA_ROOT="$ROOT" C_FEATURES="$FEATURES" C_RUN_DIR="$PWD/C_delivery/results/C_$ID" bash C_delivery/run_C.sh
echo "PASS: new independent run C_$ID; report outputs regenerated from that run"
