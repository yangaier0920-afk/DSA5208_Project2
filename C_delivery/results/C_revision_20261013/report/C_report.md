# Rainfall probabilities, stronger baselines and feature ablation

The selected conditional-distribution model is evaluated on development folds. The historical classifier and its known 2024 test are retained as separate evidence.

## Findings

Selected continuous candidate: **cdf_persistence_d8**, development equal-threshold/year Brier **0.016812**. Strong persistence baseline: 0.017939; climate baseline: 0.028989; global frequency: 0.029194; historical tree: 0.017585. Relative Brier reduction is 6.28% versus persistence and 42.41% versus frequency. These are probability-error reductions relative to named baselines, not accuracy improvements or superiority on every metric.

## Data, selection and test policy

Reuse exactly the original 10% fixed-hash training/full eligible validation anchors for each E2 fold. No new 2024 labels or scores are used. Original post-filter join and sample signatures are recorded; all candidates share keys. Selection uses five original cuts and equal year weights; only continuous-CDF candidates are deployment-eligible. Discrete tree/RF results are openly reported diagnostics. Held-out cuts 0.25/0.75/1.5/3 are not used to fit or select. A new 2017–2023 refit is saved after lock, but its evidence is development validation; there is no claim of a new untouched final test.

| Candidate | 2021 | 2022 | 2023 | Mean Brier | Continuous eligible |
|---|---|---|---|---|---|
| frequency_cdf | 0.028542 | 0.029113 | 0.029928 | 0.029194 | True |
| climatology_cdf | 0.028416 | 0.028968 | 0.029584 | 0.028989 | True |
| persistence_cdf | 0.017216 | 0.018134 | 0.018467 | 0.017939 | True |
| cdf_recent_d8 | 0.016166 | 0.017101 | 0.017271 | 0.016846 | True |
| cdf_persistence_d8 | 0.016121 | 0.017084 | 0.017232 | 0.016812 | True |
| cdf_calendar_d8 | 0.016202 | 0.017073 | 0.017251 | 0.016842 | True |
| cdf_full_d5 | 0.016259 | 0.017175 | 0.017448 | 0.016961 | True |
| cdf_full_d8 | 0.016202 | 0.017073 | 0.017251 | 0.016842 | True |
| threshold_tree_d8 | 0.016579 | 0.017585 | 0.017895 | 0.017353 | False |
| random_forest_small | 0.018098 | 0.019147 | 0.019618 | 0.018954 | False |

![Development comparison](figures/01_revision_brier.png)

| Candidate | Mean Brier | Mean log loss | Mean PR-AUC | Mean ROC-AUC |
|---|---|---|---|---|
| frequency_cdf | 0.029194 | 0.130588 | 0.030478 | 0.500000 |
| climatology_cdf | 0.028989 | 0.128169 | 0.049425 | 0.610753 |
| persistence_cdf | 0.017939 | 0.079660 | 0.544264 | 0.816221 |
| cdf_recent_d8 | 0.016846 | 0.074189 | 0.574488 | 0.846418 |
| cdf_persistence_d8 | 0.016812 | 0.074096 | 0.575936 | 0.846505 |
| cdf_calendar_d8 | 0.016842 | 0.072624 | 0.562884 | 0.894917 |
| cdf_full_d5 | 0.016961 | 0.073453 | 0.558996 | 0.885623 |
| cdf_full_d8 | 0.016842 | 0.072624 | 0.562883 | 0.894933 |
| threshold_tree_d8 | 0.017353 | 0.080014 | 0.559288 | 0.828428 |
| random_forest_small | 0.018954 | 0.082222 | 0.556613 | 0.839224 |

Additional ranking/log-loss metrics are diagnostics, not replacement selection criteria. A lower Brier does not imply superiority for every cut/year/ranking measure. Complete per-cut/year metrics are retained.

## Arbitrary threshold design

Training-only conditional empirical distributions provide P(S>h)=1-F(h) for finite nonnegative Decimal(38,18) thresholds. Right-sided CDF lookup makes equality negative. For tree candidates Spark DecisionTreeRegressor fits log1p future amount using past features, then exact training amounts form leaf CDFs. Fixed prior weights are 50 anchors for leaves and 200 for baseline groups. No validation outcomes are written back into predictions. This naturally gives threshold-specific probabilities and nonincreasing stepwise survival curves without post-hoc isotonic correction; arbitrary input is not a claim of a smooth curve. Nine checked cuts lie in 0..5 mm; arbitrary input acceptance and monotonicity do not prove every threshold or extreme-tail reliability. Beyond maximum training amount, empirical probability is zero with no proven tail guarantee.

| Untrained/selection-excluded cut (mm) | Mean Brier | Mean PR-AUC | Mean log loss |
|---|---|---|---|
| 0.25 | 0.022548 | 0.679550 | 0.097408 |
| 0.75 | 0.017303 | 0.627999 | 0.077030 |
| 1.5 | 0.012823 | 0.555037 | 0.059076 |
| 3 | 0.008773 | 0.474793 | 0.042362 |

## Controlled feature contribution experiment

All depth-8 CDF ablations hold rows, seed, min leaf size, bins, prior and shared feature ordering fixed. The threshold is a CDF query, not a tree input. Sequential differences test recent amount -> persistence -> periodic time -> station. Negative Brier delta means adding that group helps under this design; positive means it does not. These are model/data-conditional associations, not causal attribution. Impurity weights are separately saved but are not used as contribution evidence.

| Feature group | Mean Brier | Delta from previous | Relative reduction |
|---|---|---|---|
| Recent amount features only (14) | 0.016846 | NA | NA |
| Add wetness/persistence (22) | 0.016812 | -0.000034 | 0.001990 |
| Add cyclic time (26) | 0.016842 | 0.000030 | -0.001775 |
| Add training-only station coding (26 + station) | 0.016842 | 0.000000 | -0.000001 |

## Calibration and low-risk discrimination

![Low-probability development reliability](figures/02_low_probability_reliability.png)

The 2023 plot zooms to 0..5% and point size shows support. Complete bin counts are in each fold calibration CSV; fixed-bin ECE and low-region rates are in selected_calibration_summary.csv. The appendix gives the exact n for every point shown in the 2023 low-probability plot. Low-region membership depends on each model prediction, so its support can differ; it is not a common held-fixed subgroup comparison. Dry/wet history group tables provide that common covariate-defined comparison. No held-out calibrator is fitted.

| Cut/mm | Original ECE | Selected ECE | Selected low mean p | Selected low observed rate |
|---|---|---|---|---|
| 0 | 0.016184 | 0.003363 | 0.020037 | 0.020952 |
| 0.5 | 0.004827 | 0.002311 | 0.010158 | 0.010823 |
| 1 | 0.003437 | 0.002333 | 0.007631 | 0.008323 |
| 2 | 0.005694 | 0.001619 | 0.005965 | 0.006479 |
| 5 | 0.009469 | 0.000852 | 0.003248 | 0.003580 |

## Group performance and coverage

![Observed-anchor sample flow](figures/04_sample_flow.png)

![Group Brier](figures/03_group_brier.png)

Groups are 12 months, five stations chosen from first-fold training counts, and complete-history all-dry/any-wet states. Exact per-year/cut rows include n, positives, Brier, mean p, fixed-bin ECE, approximate PR/ROC-AUC, eligible count and observed denominator. Group AUC uses 1,000 equal-probability histogram bins and is explicitly approximate; overall AUC uses Spark MLlib numBins=1000. No subgroup is chosen using test outcomes. Coverage is of observed anchors, not the ideal station calendar; unknown history is retained in the coverage ledger but is unscored.

| Group | Equal-cut/year Brier | Approximate PR-AUC | Mean observed coverage |
|---|---|---|---|
| history:dry | 0.009236 | 0.504682 | 0.997436 |
| history:wet | 0.096012 | 0.743264 | 0.996209 |
| month:1 | 0.022048 | 0.587031 | 0.986302 |
| month:10 | 0.015081 | 0.535226 | 0.991655 |
| month:11 | 0.020855 | 0.583308 | 0.991760 |
| month:12 | 0.021248 | 0.537261 | 0.988788 |
| month:2 | 0.015334 | 0.496253 | 0.988228 |
| month:3 | 0.017706 | 0.598740 | 0.991018 |
| month:4 | 0.016954 | 0.547442 | 0.992813 |
| month:5 | 0.012893 | 0.534878 | 0.992972 |
| month:6 | 0.016508 | 0.520768 | 0.994474 |
| month:7 | 0.013478 | 0.545833 | 0.991547 |
| month:8 | 0.017239 | 0.556444 | 0.991386 |
| month:9 | 0.013901 | 0.616816 | 0.992004 |
| station:S113 | 0.015187 | 0.547103 | 0.995828 |
| station:S120 | 0.017769 | 0.580334 | 0.995401 |
| station:S77 | 0.016764 | 0.573382 | 0.972816 |
| station:S88 | 0.017748 | 0.566842 | 0.990772 |
| station:S94 | 0.016102 | 0.560061 | 0.995448 |

## Reproduction and immutable evidence

Fresh-process model reload reconstructs 100 anchors from only past observations and compares 1,000 probabilities at ten cuts, including 3.14159 mm. Maximum error: 0.000000. Invalid station/time/history/threshold handling and monotonic examples pass. The model, report, ZIP, requirements lock and metadata stay inside this run. Finalization reads the executing Python environment via importlib.metadata, without private uv or OS-specific virtual-environment paths. All original source-run file hashes, including 2024 scores/model, pass preservation checks. Current evidence navigation identifies successful receipts without deleting historical failures.

## Computing environment and timing

Python 3.11.17, Spark 3.5.7, Java 17.0.20.1; macOS-14.1-arm64-arm-64bit; 14 logical CPUs. Each process: local[2], 6g heap, 128 shuffle partitions, 32 MiB reads. At most two fold processes overlapped. Peaks were not measured; timings are not benchmarks. Candidate timing includes fit/CDF estimation, saves, scoring and metrics; excludes shared hashes/startup, reports and packaging. Training timing includes preprocessing. Exact fold JSON and candidate_timing_summary.csv define the measured scopes.

## Remaining limitations and AI use

Development results motivated by review are now known and should not be relabelled untouched. Fixed train sampling and small candidate set limit conclusions. Overlapping windows/weather across stations are correlated; no independent-row bootstrap or significance claims are used. Observed gaps, composition changes and training leaf support affect performance. Event-time availability and official interval meaning remain assumptions; live deployment latency is unverified. OpenAI Codex assisted implementation, execution, tests, figures and drafting. All scores come from actual saved Spark outputs; member review is still required.

Official model references: [Spark DecisionTreeRegressor](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.regression.DecisionTreeRegressor.html) and [Spark RandomForestClassifier](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.classification.RandomForestClassifier.html).

<!-- PAGEBREAK -->

## Appendix: selected continuous model, 2021

| Cut/mm | n | Event rate | Brier | Log loss | PR-AUC | ROC-AUC |
|---|---|---|---|---|---|---|
| 0.0 | 6976044 | 0.060931 | 0.030065 | 0.126697 | 0.708883 | 0.842429 |
| 0.25 | 6976044 | 0.042799 | 0.021313 | 0.092896 | 0.691867 | 0.861718 |
| 0.5 | 6976044 | 0.035763 | 0.018606 | 0.082267 | 0.664838 | 0.861190 |
| 0.75 | 6976044 | 0.031126 | 0.016784 | 0.075100 | 0.641438 | 0.859054 |
| 1.0 | 6976044 | 0.025885 | 0.014754 | 0.066834 | 0.607284 | 0.855207 |
| 1.5 | 6976044 | 0.021097 | 0.012667 | 0.058338 | 0.569913 | 0.850451 |
| 2.0 | 6976044 | 0.017239 | 0.010894 | 0.051177 | 0.533070 | 0.844222 |
| 3.0 | 6976044 | 0.012973 | 0.008634 | 0.041846 | 0.487846 | 0.838366 |
| 5.0 | 6976044 | 0.008877 | 0.006286 | 0.031184 | 0.431938 | 0.840369 |

<!-- PAGEBREAK -->

## Appendix: selected continuous model, 2022

| Cut/mm | n | Event rate | Brier | Log loss | PR-AUC | ROC-AUC |
|---|---|---|---|---|---|---|
| 0.0 | 7072758 | 0.064804 | 0.033501 | 0.139131 | 0.687444 | 0.831974 |
| 0.25 | 7072758 | 0.044063 | 0.023178 | 0.100155 | 0.663440 | 0.850643 |
| 0.5 | 7072758 | 0.035889 | 0.019627 | 0.086630 | 0.634773 | 0.849584 |
| 0.75 | 7072758 | 0.030773 | 0.017331 | 0.077715 | 0.612955 | 0.847752 |
| 1.0 | 7072758 | 0.025229 | 0.014914 | 0.068126 | 0.579162 | 0.842910 |
| 1.5 | 7072758 | 0.020505 | 0.012662 | 0.059082 | 0.544686 | 0.837830 |
| 2.0 | 7072758 | 0.016969 | 0.010851 | 0.051645 | 0.515843 | 0.834222 |
| 3.0 | 7072758 | 0.013176 | 0.008830 | 0.042836 | 0.474682 | 0.832629 |
| 5.0 | 7072758 | 0.009109 | 0.006524 | 0.031970 | 0.413762 | 0.838988 |

<!-- PAGEBREAK -->

## Appendix: selected continuous model, 2023

| Cut/mm | n | Event rate | Brier | Log loss | PR-AUC | ROC-AUC |
|---|---|---|---|---|---|---|
| 0.0 | 6686625 | 0.066713 | 0.033359 | 0.137338 | 0.706400 | 0.843920 |
| 0.25 | 6686625 | 0.045559 | 0.023154 | 0.099173 | 0.683343 | 0.862185 |
| 0.5 | 6686625 | 0.037479 | 0.019885 | 0.086546 | 0.655057 | 0.862460 |
| 0.75 | 6686625 | 0.032254 | 0.017793 | 0.078275 | 0.629604 | 0.861374 |
| 1.0 | 6686625 | 0.026297 | 0.015347 | 0.068634 | 0.591675 | 0.857637 |
| 1.5 | 6686625 | 0.021309 | 0.013139 | 0.059809 | 0.550513 | 0.852940 |
| 2.0 | 6686625 | 0.017291 | 0.011240 | 0.052198 | 0.509063 | 0.847619 |
| 3.0 | 6686625 | 0.012949 | 0.008854 | 0.042404 | 0.461852 | 0.842480 |
| 5.0 | 6686625 | 0.008702 | 0.006330 | 0.031068 | 0.399851 | 0.844842 |

<!-- PAGEBREAK -->

## Appendix: exact sample count for every plotted low-probability point (2023)

| Model | Cut/mm | Bin | n | Mean p | Observed rate |
|---|---|---|---|---|---|
| Historical tree | 0 | 3 | 6,273,227 | 0.010323 | 0.026913 |
| Selected model | 0 | 4 | 6,080,591 | 0.020324 | 0.021151 |
| Historical tree | 0.5 | 3 | 6,273,227 | 0.010323 | 0.011734 |
| Selected model | 0.5 | 2 | 6,080,591 | 0.009796 | 0.010196 |
| Selected model | 0.5 | 4 | 131,565 | 0.037973 | 0.046403 |
| Historical tree | 1 | 3 | 6,273,227 | 0.010323 | 0.008240 |
| Selected model | 1 | 2 | 6,083,547 | 0.007069 | 0.007347 |
| Selected model | 1 | 3 | 78,833 | 0.017879 | 0.022998 |
| Selected model | 1 | 4 | 141,115 | 0.031200 | 0.036290 |
| Historical tree | 2 | 3 | 6,273,227 | 0.010323 | 0.005691 |
| Selected model | 2 | 2 | 6,090,249 | 0.005025 | 0.005146 |
| Selected model | 2 | 3 | 199,332 | 0.014067 | 0.017689 |
| Selected model | 2 | 4 | 170,562 | 0.034507 | 0.042172 |
| Historical tree | 5 | 3 | 6,273,227 | 0.010323 | 0.002857 |
| Selected model | 5 | 1 | 6,140,717 | 0.002591 | 0.002648 |
| Selected model | 5 | 2 | 180,870 | 0.006881 | 0.008094 |
| Selected model | 5 | 3 | 128,921 | 0.014317 | 0.018872 |
| Selected model | 5 | 4 | 97,200 | 0.030665 | 0.033158 |
