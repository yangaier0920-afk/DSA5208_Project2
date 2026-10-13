# C required work completion

C's outstanding non-optional model changes are completed. Optional block bootstrap, wider-tail validation and a later real-time study were not performed.

| Required C item | Completed work | Evidence |
|---|---|---|
| Limited leaf-size comparison | Leaf250/500/1000, depth8 and 22 features fixed; three development folds, same rows and primary cuts | leaf_size_comparison.csv, selection.json, protocol.json |
| Preserve honest selection/test interpretation | Selected cdf_persistence_d8_leaf1000; development Brier 0.01679503; original tolerance/order preserved; no new 2024 evaluation | Frozen config, selection, original preservation receipt |
| C input reconstruction | Executed from extracted source; reconstructed all three development TRAIN/VALIDATION inputs and final TRAIN only | rebuilt_anchor_comparison.json, compact_signature_mode_validation.json, rebuilt metadata/joins |
| C model refit and past-only API | Refit 4,071,679 TRAIN anchors through 2023; independent reload checks 100 anchors / 1,000 probabilities, maximum error zero | refit_status.json, reload_validation.json |
| Independent compact checks | 300 metric cells, calibration/CDF/coverage reconciliation; zero continuous probability inversions | compact_evidence_validation.json |
| Run-local artifacts and preservation | New report/model/package/config isolated; 6,256 original files and 263 previous compact/deployed evidence files retain hashes | original_preservation_check.json, previous_run_preservation_check.json |
| Portable C commands and packaging | Python runner uses sys.executable; finalizer uses importlib.metadata; compact CDFs retained for source-only validation | C_run_leaf_study.py, finalizer_portability_validation.json and cold_model_validation.json |

Leaf250 mean Brier: 0.01686547; leaf500: 0.01681224; leaf1000: 0.01679503. Selected Brier is 6.38% below the stronger persistence baseline. The improvement versus leaf500 is small; no statistical significance or universal ranking advantage is claimed. The feature-group ablation is still the controlled leaf500 experiment and is not relabelled an ablation on the tuned model.

Thirty earlier model/fold cells were reused transparently; six additional cells were newly fitted/scored on rebuilt inputs. These are incremental experiments on already-observed development years. Rebuilt Parquet bytes differ; two row-hash sums are diagnostics, not cryptographic equality proofs. A complete fresh-machine A/B/C run and Windows runtime execution remain team/environment work, not achievements claimed here.

Current report/model/evidence are in C_delivery/results/C_leaf_study_20261014. Canonical integrated materials are in submission/. A/B and group actions are in the separately supplied Chinese group handoff. Source/model/report archive contents and manifests are checked during packaging.
