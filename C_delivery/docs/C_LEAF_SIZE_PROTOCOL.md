# Required C completion: supplementary leaf-size protocol

Freeze before fitting new candidates: depth 8, 22 recent/persistence features, min leaf anchor counts 250/500/1000, maxBins 32, seed 20261012 and leaf prior 50. Reuse the three original E2 development folds and deterministic 10% TRAIN/full eligible VALIDATION rows. All five primary cuts determine equal-cut/equal-year Brier; four held-out cuts remain diagnostics. Preserve the original 1e-5 absolute tie tolerance and simplicity order, then leaf1000 before leaf250. No 2024 score or later-test claim.

Thirty prior candidate/fold diagnostics, including leaf500, remain reference evidence; six added fold cells fit leaf250/leaf1000. This is an incremental development search following review, not nested cross-validation or an untouched external test. Feature ablation remains a separate fixed-leaf500 experiment.

C reconstruction executes from an extracted source directory against existing verified A data and B features. Rebuilt fold anchors and final TRAIN through 2023 are checked against historical counts and two role-wise row-hash sums over all keys/targets/features. Signatures are diagnostics, not cryptographic equality proofs; Parquet bytes can differ. Added candidates and the final refit use rebuilt inputs, followed by past-only reload and compact checks. This accepts C stages without claiming a fresh-machine full A/B run or Windows execution.

Use C_run_leaf_study.py with explicit --base-run, --source-run, --input-anchors, --root, --features, --config and --output paths. It executes Python stages using sys.executable; --check-against-original-anchors requires those historical intermediate Parquet files and is optional when reproducing a compact source archive. All output stays run-local.
