# Assignment and review recommendation acceptance

This checklist assesses actual source, saved experiments and current delivery files. COMPLETE means implemented with the evidence below; it does not mean independently rerunning all raw processing/training on a new machine. This audit checks current archives/model loading and independently reconciles compact experiment metrics. Original full-data results and earlier validation receipts retain their stated scope.

## intro.txt requirements

| Requirement | Status | Evidence and limits |
|---|---|---|
| Use annual 2017–2024 rainfall and Apache Spark | COMPLETE | A cleaning/targets, B aggregates/features, annual counts and signed provenance; 48,538,627 retained observations |
| Task 1 within-year weather patterns | COMPLETE | Month/hour occurrence, nonzero intensity, coverage rules and figures in the main report |
| Task 1 changes across years | COMPLETE | Four fixed eight-year same-month panels; all 84 adjacent-year cells, 63 supported; changing annual panels are descriptive |
| Task 1 appropriate analysis, plots and discussion | COMPLETE | Nine Task 1 figures including supported-panel station map and official climate context; no unsupported national-trend claim |
| Task 2 probability of strict future 30-minute exceedance | COMPLETE | Six exact future slots; conditional training-only CDF; equality is negative, unknown truth excluded |
| Model inputs: station, prediction time and given threshold | COMPLETE WITH DISCLOSED DOMAIN | Past-only predictor; supported stations, complete exact history and five-minute grid; nonnegative finite Decimal(38,18) thresholds; performance checked at nine cuts 0..5 mm, not every possible threshold |
| Select features and evaluate with Spark MLlib | COMPLETE | Spark regression/classification, three expanding temporal folds, ten candidates, stronger baselines and controlled feature ablation; selected model uses 22 features |
| Explain cleaning, weather exploration, features and model development | COMPLETE | Main report methods and tables; detailed source and feature definitions |
| Describe computing environment/cluster configuration | COMPLETE | A/B/C execution records distinguished; Python/Spark/Java, local threads, heap/shuffle settings and timing limits |
| Present/discuss both tasks and predictive performance | COMPLETE | Brier, log loss, PR/ROC-AUC, reliability/ECE and groups; current model development validation is distinct from historical 2024 test |
| Declare AI use | COMPLETE | Codex implementation/execution/checks/drafting declared; members should supplement any additional tools actually used |
| Discuss limitations/insights/additional work | COMPLETE | Missingness, event-time availability, station composition, correlation, empirical tail and development-only evidence |
| At most three members | COMPLETE | Three members, matching members.json and PDF cover |
| Complete source plus running README | PREPARED; FULL FRESH-MACHINE REPLAY NOT ACCEPTED | Current source ZIP includes A/B/C, configs, compact evidence and fixtures. Formal data are downloadable; missing anchors have reconstruction code. Default scripts documented; notebook hardcoded path and unexecuted full reconstruction remain limits |
| Trained model | COMPLETE/PREPARED | Model ZIP logical hashes and fresh extracted-source past-only inference pass |
| PDF naming every member and student ID | COMPLETE/PREPARED | 24-page English PDF; three supplied names/IDs checked |
| Submit to Canvas by 18 October 2026 | NOT PERFORMED | Report/source/model bundle is prepared. Upload must still occur |

The assignment does not prescribe 2024 as an untouched test. The current model has development evidence only; historical 2024 performance belongs to the historical classifier. Prepared files do not prove Canvas submission or universal-environment reproducibility.

## Review: B recommendations

| Recommendation | Status | Evidence |
|---|---|---|
| Add weather meaning with official context | COMPLETE | Main report links MSS climate background; distinguishes November/afternoon observations from physical attribution and official rain-day measures |
| Station map using metadata history | COMPLETE | 21 October-panel stations, latest coordinate convention; S113 separation 38.59 m, relocation versus metadata correction unknown; station_map_validation.json |
| Local interannual comparisons | COMPLETE | Seven predefined adjacent-year pairs x 12 months; 63/84 supported; 35 extra supported cells outside the fixed four months; adjacent_year_validation.json |
| Correct 90%/95% sensitivity heading and formula in generator | COMPLETE | Explicit maximum absolute same-year panel-mean difference across years; formula and generator updated; Markdown table pipes escaped |
| Do not equate station ID with invariant measuring conditions | COMPLETE | Report discloses coordinate/device uncertainty and descriptive scope; no invented relocation date, homogenization or arbitrary deletion/clipping |

## Review: C recommendations

| Recommendation | Status | Evidence and qualifications |
|---|---|---|
| Resolve five-threshold API restriction by a real model design | COMPLETE | Training-only conditional empirical CDF, strict right lookup and arbitrary representable nonnegative queries; original five-cut guard retained only for historical model |
| Check unseen threshold cuts and monotonicity | COMPLETE | Four selection-excluded cuts 0.25/0.75/1.5/3 plus five primary cuts; all continuous candidates have zero inversion counts; fresh query 3.14159 and exact 18-decimal boundary pass |
| Limited tree-depth/leaf-size and ensemble experiments | PARTIALLY COMPLETE | Depth 5/8 CDF comparisons, deeper threshold tree and 16-tree RF completed; new comparisons fix min leaf size at 500. No independent leaf-size search or GBT run; RF is the requested RF-or-GBT alternative |
| Structurally separate threshold-specific probabilities | COMPLETE | Exact leaf distributions and fixed training-only global prior; threshold queried after tree prediction, giving monotone stepwise survival |
| Use train-only temporal calibration if a calibrator is fitted | NOT APPLICABLE; LEAKAGE RULE OBSERVED | No separate calibrator fitted. Empirical CDF counts/smoothing use training only; no validation/test rates written into predictions |
| Low-probability reliability zoom and per-point sample support | COMPLETE | 0..5% zoom, support-sized points and exact 2023 plotted-point counts appendix/CSV |
| Weighted calibration metric or Brier decomposition | COMPLETE | Fixed-bin ECE added; Brier decomposition was not added, as the suggestion allows either |
| Stronger station/month/hour climate baseline | COMPLETE | Training-only smoothed group distributions, prior 200; same folds/anchors |
| Stronger recent-rain persistence baseline | COMPLETE | Recent-total bins/current-wet distributions estimated within training; mean Brier 0.01793895 |
| Controlled feature-group ablation | COMPLETE | Recent 14 -> persistence 22 -> calendar 26 -> station coding; fixed shared settings/order. Persistence gives ~0.199% relative gain; calendar/station additions do not improve Brier; no causal claim |
| Month/station/history-state evaluation | COMPLETE | Twelve months, five first-training-fold selected stations, dry/wet complete history; Brier, approximate PR/ROC-AUC, ECE, prevalence and coverage |
| Sample-flow plot and observed-anchor denominator | COMPLETE | sample_flow.csv and diagram; observed denominator distinguished from ideal calendar; unknown history stays in ledger |
| Block bootstrap if uncertainty is added | OPTIONAL, NOT IMPLEMENTED | No uncertainty intervals or significance claims; correlation disclosed. No independent-row bootstrap used |
| Preserve known 2024 test and avoid new test-driven choices | COMPLETE | Development-only selection/refit, original preservation receipt for 6,256 files; original score explicitly historical |

Current selected development Brier is 0.0168122356: 6.2808% lower than the stronger persistence baseline, not a 6.28% accuracy improvement. The arbitrary threshold interface does not establish far-tail calibration. Diagnostic RF results were retained even though worse, rather than omitted.

## Review: reproduction and delivery recommendations

| Recommendation | Status | Evidence and limits |
|---|---|---|
| P0 remove private uv/Python path dependency in finalizer | IMPLEMENTED AND NARROWLY VALIDATED | importlib.metadata and sys.executable; extracted-source finalizer receipt passes without private uv/venv. Windows path use is explained but not executed |
| P0 full fresh-machine end-to-end completion | NOT YET ACCEPTED | Fresh model loading and finalization pass; no complete fresh download -> B features -> reconstructed anchors -> all models -> report chain was rerun; Windows untested |
| P1 independent run model/report/ZIP/lock/metadata | COMPLETE | Run-local paths, frozen config and original preservation checks; final integration selects evidence directories explicitly |
| Correct logical model hash handling | COMPLETE | Spark/CDF logical files checked; derived Hadoop .crc excluded from required model manifest; ZIP checks pass |
| P2 genuine shorter summary | COMPLETE | Summary is distinct and less than half the full report size |
| P2 current successful evidence index while keeping failures | COMPLETE | C_delivery/CURRENT_EVIDENCE.md points to successful receipts; historical failures kept |
| P2 distinguish notebook viewing from full computation and frozen runtime | PARTIALLY COMPLETE | README now names scripts/frozen root/full-data needs and notebook switches. Task 1 notebook still contains a machine-specific PROJECT_ROOT and needs editing; default notebook execution is not full recomputation acceptance |
| Retain retrospective event-time interpretation | COMPLETE | Available-current-slot assumption and unverified publication latency disclosed; no live-deployment assertion |
| Freeze coherent report/source/model/README/manifests and validate files | COMPLETE FOR PREPARED MATERIALS | Main PDF and C report version labels removed; report sources explicit; source/model manifests, ZIP CRC and extracted-model checks pass. Canvas upload remains separate |

## Current audit evidence

- verification/current_requirements_audit.json: archive/manifest/source payload, independent candidate Brier reconciliation and genuine-summary checks.
- verification/current_delivery_model.json: fresh extracted-source model checksums, past-only inference, arbitrary query and exact decimal precision.
- C_delivery/results/C_revision_20261013/compact_evidence_validation.json: 246 metric cells plus calibration/CDF/coverage reconciliation.
- C_delivery/results/C_revision_20261013/reload_validation.json: 100 real anchors / 1,000 probability comparisons, maximum error zero.
- B_delivery/results/task1_v2/station_map_validation.json and adjacent_year_validation.json: map/panel and paired-change evidence.

No full retraining, new 2024 scoring, fresh raw-data reprocessing, Windows runtime test or Canvas submission was performed during this acceptance audit.
