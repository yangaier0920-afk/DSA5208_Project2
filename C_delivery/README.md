# C: current Spark conditional rainfall model

The selected model is results/C_leaf_study_20261014/trained_model/, candidate cdf_persistence_d8_leaf1000. It uses 22 recent/persistence features from the B 26-column API and supports finite nonnegative Decimal(38,18) threshold queries with strict exceedance. Development mean Brier 0.01679503 is 6.38% below the strong persistence baseline. Nine evaluated cuts lie in 0..5 mm; far-tail reliability is unverified. No new 2024 scoring occurred. The historical five-cut classifier retains its own known 2024 result.

See ../README.md for installation, formal-data acquisition, predictor API, full reproduction and document packaging. CURRENT_EVIDENCE.md indexes successful receipts. docs/C_REVISION_PROTOCOL.md and docs/C_REVIEW_IMPLEMENTATION_CHECKLIST.md specify the comparisons and feature ablation.

```bash
python C_delivery/src/C_test_contracts.py
python C_delivery/src/C_test_distribution.py
# For current full C reconstruction and tuning, use the explicit commands below.
```

The runner needs full formal A data and B features. Missing original anchors are reconstructed for 2017–2023 only and recorded with new joins/counts; the supplementary completed run used freshly reconstructed anchors whose counts and two role-wise row-hash sums match historical inputs. Set C_PYTHON, C_DATA_ROOT and C_FEATURES as required. Outputs remain run-local: report, trained_model, package, lock and manifests. C_finalize reads installed dependency metadata from its running interpreter; shell wrappers are Unix and Python stages can be run separately on Windows (not tested here). Model loading validates logical file hashes; inference uses complete past history and never future eligibility. AI use, assumptions and development-only limitations are declared in the main report.

## Required leaf-size completion and C reconstruction

C_leaf_study.json freezes leaf sizes 250/500/1000 on the already selected 22-feature depth-8 model; compare primary five-cut/year mean Brier, with fixed 1e-5 tolerance and no known-2024 retuning. C_run_leaf_study.py uses the running interpreter and explicit paths. Compact CDF evidence is included in the current source package; multi-GB intermediate inputs are not.

```bash
python C_delivery/src/C_revision_rebuild_anchors.py --root .runtime/frozen_root --features B_delivery/results/features_reproduction --reference-run C_delivery/results/C_v1_20261012 --output C_delivery/results/rebuilt_inputs_reproduction
python C_delivery/src/C_run_leaf_study.py --base-run C_delivery/results/C_revision_20261013 --source-run C_delivery/results/C_v1_20261012 --input-anchors C_delivery/results/rebuilt_inputs_reproduction --root .runtime/frozen_root --features B_delivery/results/features_reproduction --config C_delivery/configs/C_leaf_study.json --output C_delivery/results/C_leaf_reproduction --expected-anchor-signatures C_delivery/results/C_leaf_study_20261014/rebuilt_anchor_comparison.json
python C_delivery/src/C_revision_report.py --run C_delivery/results/C_leaf_reproduction --source-run C_delivery/results/C_v1_20261012
python C_delivery/src/C_finalize.py --run C_delivery/results/C_leaf_reproduction
```

With original full intermediate inputs available, --check-against-original-anchors compares both sides directly. With the compact package, --expected-anchor-signatures compares against saved role-wise signatures instead. Neither signature mode is claimed as a cryptographic row-equality proof. Regenerated Parquet bytes can differ; counts/new joins/roles and source identities must still pass. This workflow supports C acceptance without claiming fresh A/B generation, another machine or Windows execution.
