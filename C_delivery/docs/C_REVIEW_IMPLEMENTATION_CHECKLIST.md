# Review recommendations: C implementation checklist

Source: the user's objective evaluation and optimization recommendation document. All current-run candidate experiments and narrow reload/finalizer/evidence checks are complete. The independent leaf-size search and extracted-source C input reconstruction/model stages are completed. Full fresh-machine A/B/C and Windows execution remain untested. See ../../submission/requirements_and_review_acceptance.md for the full acceptance distinction. C reconstruction from the extracted source was executed against existing formal A/B data. New candidate/refit stages use rebuilt inputs; historical baseline/feature-ablation cells are reused transparently. The run-local revision report and CURRENT_EVIDENCE.md are the final authority for completed results.

| Review item | Implementation | Verification artifact |
|---|---|---|
| Resolve fixed-five threshold restriction | Conditional training-only empirical CDF, strict >, any finite nonnegative Decimal(38,18) query; evaluated domain disclosed | Held-out-cut CSV, monotonicity counts, C_test_distribution.py, reload_validation.json |
| Investigate low-risk threshold discrimination | Leaf distributions give threshold-specific survival; depth 5/8 and discrete deeper-tree/RF comparisons | Primary Brier/PR-AUC/log loss; fixed-bin ECE; 0..5% reliability zoom with support |
| Never calibrate using validation/test rates | All CDF counts and fixed smoothing prior are trained within each training block; no held-out outcome writeback | Frozen experiment config, per-candidate training-only completion receipt |
| Stronger climate/persistence baselines | Smoothed station/month/hour and recent-rain-bin/current-wet empirical distributions | Same-fold, same-anchor baseline metrics |
| Feature ablation | Recent -> persistence -> cyclic time -> station; d8/minLeaf500/prior50 and order fixed | ablation_summary.csv and per-fold metrics |
| Few grouped evaluations | Twelve months, five first-fold-training-selected stations, dry/wet complete history | Group Brier, approximate PR-AUC, eligibility/observed coverage CSVs |
| Honest sampling/coverage | Reuse 10% train/full validation anchors; observed denominator, unknown-history ledger | sample_identity.json, group_coverage.json, original join receipt hashes |
| Preserve known 2024 interpretation | No new test scoring or choices; retain original v1 2024 result | Original hash guard/check; explicit development-only selection/refit policy |
| Portable finalization | importlib.metadata via executing interpreter; records sys.executable; no uv/pip binary path | package/requirements.lock.txt, environment_capture.json, extracted-runtime finalizer smoke check |
| Independent run outputs | Every new model/report/ZIP/lock/manifest stays within run root | run/package, run/report; original preservation check |
| Explicit report evidence input | Integrated builder has --task1/--c-run/--c-report/--revision-run/--output | integration_sources.json; no mixed shared figures |
| Model checksum portability | New model manifest checks logical Spark/CDF files and excludes derived Hadoop .crc sidecars; ZIP may retain them | Core artifact hashes and ZIP checks |
| True short summary | Extracts selected findings/caveats rather than copying full report | Separate concise summary, byte inequality check |
| Current evidence navigation | Keep history and point to current successful receipts | CURRENT_EVIDENCE.md |

Optional uncertainty intervals were not added. Window/station correlation is disclosed; no independent-row significance claim is made. Live latency remains unverified. Continuous input acceptance is a model design property, while predictive validation remains finite (nine cuts in 0..5 mm); empirical far-tail probabilities are not a calibrated deployment guarantee.
