# Portable execution and input identity

Use Python 3.11, PySpark 3.5.7 (requirements-spark.txt), Java 17, and the report dependencies already listed in the project setup documentation. Install a Java distribution for the host OS/architecture; set JAVA_HOME to its home directory. The runner discovers the executing interpreter and installed Spark rather than assuming a Unix or Windows virtual environment layout. Windows also requires the verified native components installed by `python scripts/setup_hadoop_windows.py`.

From the project root, with the project environment activated:

```sh
python submission/tools/run_project.py --preflight-only --run-id environment_check
python submission/tools/run_project.py --run-id reproduction_001
```

Use a new run ID for changed code, data or configuration. For an operational failure with unchanged completed-stage inputs/outputs, `--resume --run-id <same-id>` preserves the previous failure receipt and explicitly labels completed stages as reused. A resumed run is not a fresh execution of those stages. On macOS/Linux, `bash submission/tools/run_full_project.sh` delegates to the same Python entry point. Paths containing spaces are passed as argument lists. This entry point does not create ZIP archives. C finalization runs with `--no-archive` to validate the model directory and save dependency/inventory metadata.

Obtain the formal A data release according to README before a full run. Frozen release metadata is restored and the full input checksum is checked. A is replayed on its representative sample; this is not a fresh full raw A collection/cleaning run. B analysis, features and joins are regenerated. C preserves the locked protocol: 30 historical model/fold cells are explicitly reused, six leaf-size cells are newly trained, then selection/refit/reload/validation and reports run. It does not train all 36 cells from scratch or perform a new 2024 evaluation.

Every stage writes a log and `outputs/reproduction_<run-id>/run_status.json`. An environment preflight is not a complete pipeline PASS. Missing data, dependencies, incompatible runtimes and changed C inputs cause failure; C signature disagreement is recorded as BLOCKED_INPUT_IDENTITY. No rounding, automatic schema casts or relaxed signature acceptance is used.

## First-fold disagreement on another computer

Reference signature receipt: `C_delivery/results/C_leaf_study_20261014/rebuilt_anchor_comparison.json`, SHA-256 `dba848f712d2f9a5018537d1620b4c8bd5d61574fbdde13a754132eac15efd4f`.

Original first-fold anchors are available on the reference computer at `C_delivery/results/C_v1_20261012/fold1/anchors`. Copy the directory as a directory if exact keyed comparison is needed; full anchors are not included in Git. Preserve the old directory and receipts.

```sh
python C_delivery/src/C_anchor_signature_diagnostics.py --anchors NEW_FOLD1_ANCHORS --role fold1_role --config C_delivery/results/C_v1_20261012/experiment_config.json --reference-diagnostics C_delivery/results/signature_diagnostics/reference_fold1.json --output NEW_DIAGNOSTIC.json
python C_delivery/src/C_compare_anchor_values.py --reference-anchors ORIGINAL_FOLD1_ANCHORS --actual-anchors NEW_FOLD1_ANCHORS --role fold1_role --config C_delivery/results/C_v1_20261012/experiment_config.json --output NEW_KEYED_COMPARISON.json
```

Both commands are read-only. Per-field key-paired hashes localize candidate differences; the full outer keyed comparison records changed values, schema types, missing keys and exact counts. Equal row counts and equal target marginal distributions do not establish keyed equivalence. Calendar corrections did not explain the reported complete mismatch; its cause remains unresolved until the other computer's keyed comparison is available.

Actual Windows execution has not been verified on this macOS host. Supported runtime configuration and diagnostics cannot guarantee execution on arbitrary hardware or claim cross-host acceptance before the identity failure is resolved.

## Recorded local verification

On macOS arm64, the current pipeline passed formal full A read/checks, representative A replay, fresh B aggregates/features/contracts/full join audit, all C reference signatures, six new model/fold cells, selection/refit/reload/guard/compact validation, no-archive model inventory and integrated report generation. Two operational failures and their subsequent resumptions remain recorded; completed stages are explicitly labelled reused during continuation. Reports contain 10 C pages and 25 integrated pages and were rendered for visual review. Compact evidence is in `C_delivery/results/signature_diagnostics/PORTABILITY_VALIDATION.json` and `portable_run_acceptance.json`. This remains a local current-protocol execution, not a fresh full raw-A run, all 36 cells from scratch, or Windows/cross-host acceptance.
