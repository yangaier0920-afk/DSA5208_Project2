# C revision protocol: 13 October 2026

This revision implements C's items in the supplied objective review: arbitrary-threshold support, low-probability diagnostics, a limited candidate comparison, stronger baselines, controlled feature ablation, group evaluation and portable isolated outputs. It does not alter A's data rules or B's 26 feature calculations. B's newly pulled map/adjacent-year results remain intact.

## Evidence policy

Original C_v1_20261012 is immutable, including its known 2024 test. New candidate choices and comparisons use only the three original E2 development folds (2017–2020/2021, 2017–2021/2022, 2017–2022/2023). The already materialized post-filter 10% TRAIN and full VALIDATION anchors are reused exactly, with source hashes and original join receipts. Every candidate in a fold sees the same anchor keys. No new 2024 scoring or selection is performed. After selection a new model may refit 2017–2023; its performance evidence is development validation, not a newly untouched final test.

## Prespecified candidates and probability design

Stronger baselines are a station/month/hour climatology and recent-rain persistence (15-minute bins 0,0.1,0.5,2,5,10 mm plus current wetness). Each estimates a training-only empirical rainfall CDF with a fixed 200-anchor global prior mixture. A global empirical CDF reproduces the frequency baseline at the five original cuts.

Continuous models fit Spark MLlib DecisionTreeRegressor to log1p future amount, without a threshold feature. Each leaf then estimates the exact Decimal rainfall empirical CDF from the same training anchors, with a fixed 50-anchor global-prior mixture. Predictions are P(S>h)=1-F(h), using right-sided CDF lookup so equality is non-exceedance. This is a conditional-distribution estimator, not merely removal of the v1 threshold guard. Structural monotonicity holds for any finite nonnegative Decimal(38,18) threshold. Arbitrary threshold input does not mean a smooth probability curve: the empirical survival function is stepwise at observed amounts. Large thresholds beyond observed training amounts return empirical zero, not a proven extreme-tail absence.

Depth-8 ablation holds seed, training rows, tree settings, shrinkage and column ordering fixed:

1. Recent levels/lags/totals/changes/variability (14 numeric features).
2. Add eight wetness/persistence/censor features (22).
3. Add four cyclic time features (26).
4. Add training-only station one-hot coding (26 plus station).

A full depth-5 distribution tree is also compared with depth 8. minInstancesPerNode=500, maxBins=32. Discrete diagnostic candidates are a depth-8 threshold classifier and a 16-tree depth-6 RF with the same five threshold-expanded labels. They are comparison diagnostics, not eligible for continuous-interface deployment.

Primary selection averages Brier equally over original thresholds 0,0.5,1,2,5 and the three development years. Only continuous-CDF candidates are eligible. Absolute tie tolerance is 1e-5 with the recorded simplicity order. Additional cuts 0.25,0.75,1.5,3 are unseen threshold queries: neither cut-specific labels nor these evaluation scores fit the distribution models or select the configuration. Nine cuts in 0..5 give finite validation evidence, not an assertion that every threshold/tail is equally well validated.

## Diagnostics and limitations

Report Brier, log loss, Spark MLlib PR/ROC-AUC and fixed-bin ECE. Reliability plots include a <=5% probability zoom and point sizes/counts. Compare low-region prediction/observed rates without writing validation outcomes back into predictions.

Group diagnostics cover 12 months, complete-history all-dry versus any-wet, and five stations selected by largest first-fold training counts before metrics. Report primary-cut Brier, positive rate, approximate group PR/ROC-AUC and eligibility/observed coverage. Group AUC uses 1,000 equal-probability histogram bins; it is labelled separately from the primary Spark evaluator. Unknown-history anchors appear in the coverage ledger but are not scored. Observed-anchor coverage does not imply coverage of an ideal station calendar.

Ablation differences describe predictive contribution conditional on this model/data, not causality. Overlapping temporal windows and shared weather across stations violate independent-row assumptions; no rowwise bootstrap or significance claim is used. Event-time availability, interval semantics and publication latency limitations remain. CDF leaf estimates use training outcomes, never validation or test rates; there is no held-out temporal probability calibrator.

## Output isolation and reproduction

All experiment config, models, scores, reports, dependency locks and ZIPs live below the requested run root. Existing incomplete candidates require inspection; successful ones can resume. Finalization uses importlib.metadata from sys.executable, requiring no private uv path or assumed Windows/Linux virtual-environment layout. Explicit evidence directories drive integration; no automatic replacement of submission occurs.

Sources: Spark 3.5.7 DecisionTreeRegressor (leafCol) and RandomForestClassifier official API documentation. The supplied review is task context, not proof of new empirical results.
