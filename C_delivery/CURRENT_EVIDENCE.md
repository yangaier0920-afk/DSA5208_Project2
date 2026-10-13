# Current valid C evidence

Updated 14 October 2026. Historical FAIL/PENDING records remain for traceability; they do not replace the successful evidence below. The revision run ID denotes its 13 October protocol/start date.

| Scope | Current evidence | Interpretation |
|---|---|---|
| Original v1 result | results/C_v1_20261012/selection.json, final_test.json, model_reload_validation.json | Known 2024 original tree result, not new-model test evidence |
| Original preservation | results/C_leaf_study_20261014/original_preservation_check.json | 6,256 original experiment files checked; model/2024 bytes unchanged |
| Reconstructed input identity | rebuilt_anchor_comparison.json, compact_signature_mode_validation.json, reused_anchor_hash_verification.json and each fold sample_identity.json | Rebuilt anchor/data-sidecar hashes verified; counts and two role-wise row-hash sums match historical inputs |
| New development protocol | results/C_leaf_study_20261014/experiment_config.json | Three E2 folds, same 10% training/full validation; primary five cuts; held-out four cuts |
| All candidate comparisons | development_metrics.csv, candidate_probability_diagnostics.csv, selection.json | 36 completed model/fold evidence cells: 30 reused and six new; selected cdf_persistence_d8_leaf1000; development mean Brier 0.01679503 |
| Strong baselines | frequency_cdf/climatology_cdf/persistence_cdf fold CSVs | Training-only global, station/month/hour and recent-rain distributions |
| Feature contribution | ablation_summary.csv | Recent 0.01684576; +persistence 0.01681224; +time 0.01684208; +station 0.01684210; no causal/significance claim |
| Probability diagnostics | selected_calibration_summary.csv, reliability_plot_exact_counts_2023.csv | Weighted fixed-bin ECE and exact support for each low-risk plot point |
| Group/coverage diagnostics | selected_group_metrics.csv, sample_flow.csv, fold*/group_coverage.json | Month, five training-selected stations and dry/wet history; observed-anchor denominator |
| Independent reconciliation | compact_evidence_validation.json | 300 threshold metric cells; bin counts/positive counts/mean probabilities reconcile; tiny floating mean error recorded |
| New final refit | refit_status.json | 4,071,679 sampled 2017–2023 anchors, no 2024 scoring |
| New persisted model/API | trained_model/, reload_validation.json | 100 anchors/1,000 probabilities rebuilt from past-only observations; arbitrary 3.14159 cut; monotonic and invalid-input tests |
| Legacy compatibility | legacy_model_compatibility_check.json | Original model loads and keeps its original five-cut guard |
| Portable packaging | finalizer_portability_validation.json; package/environment_capture.json | Current finalizer works in an extracted source/model directory without private uv/venv paths |
| Current C report/model | results/C_leaf_study_20261014/report/C_report.pdf; package/C_revision_20261013_trained_model.zip | Run-local English report and continuous model; original shared outputs preserved |
| Integrated revision | ../submission/ | Current coherent main report and delivery; original submission archived separately |

The 2024 mean Brier 0.01773592 and 38.59% skill remain only the original tree's known test result. The selected 0.01679503 is development validation, not a new 2024 score. Its reduction versus the stronger persistence baseline (0.01793895) is 6.38%; versus original development tree (0.01758544) it is 4.49%. Model input accepts nonnegative Decimal(38,18) cuts; predictive validation is finite at nine cuts in 0..5 mm, and empirical far-tail reliability remains unverified. The survival curve is stepwise and structurally nonincreasing.

Only 22 recent/persistence numeric columns enter the selected tree. Station selects its observed series and prediction time selects the exact history anchor, even though the selected tree does not use station one-hot or calendar features. No future truth/role/eligibility enters inference.

Leaf-size comparison and executed C reconstruction: results/C_leaf_study_20261014/leaf_size_comparison.csv, rebuilt_anchor_comparison.json, compact_signature_mode_validation.json and C_pipeline_acceptance.json. C required work is summarized in ../submission/C_REQUIRED_WORK_COMPLETION.md; other members actions are in the separately supplied group handoff. The feature-group ablation remains leaf500 evidence.
