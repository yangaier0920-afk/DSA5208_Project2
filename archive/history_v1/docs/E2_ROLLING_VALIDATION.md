# E2 expanding-window validation protocol

Historical protocol, confirmed before formal modelling. A prepares truth and role fields; B computes past-only features; C implements sampling, threshold expansion and Spark MLlib evaluation. The protocol itself does not claim that training had already been executed at this historical stage.

| Fold | Training years | Validation years |
|---|---|---|
| fold1 | 2017–2020 | 2021 |
| fold2 | 2017–2021 | 2022 |
| fold3 | 2017–2022 | 2023 |
| Final, after selection | 2017–2023 | 2024 TEST |

Use explicit temporal filters, not ordinary random-fold CrossValidator. Require full past 60 minutes (12 exact slots) and full future 30 minutes (six exact slots). Exclude anchors crossing the actual partition boundary, including a label endpoint equal to the next block start. Internal calendar-year boundaries inside a training block remain valid when context is complete. Keep EXCLUDED_BOUNDARY and UNUSED out of that fold.

Pilot on 1% deterministic hash-sampled anchors; formal training on 10%. Score validation and test at full eligible scope. Sample anchors before expanding all thresholds, keep all threshold versions of an anchor together, and do not sample by outcome. Candidate thresholds are 0,0.5,1,2,5 mm, supported and frozen using only the earliest 2017–2020 training block. Both classes must have adequate training support. Numeric threshold is an input to the shared model; inference is restricted to supported thresholds and training-known stations.

Candidates are a threshold-specific training-frequency baseline, logistic regression and a shallow decision tree. Fit encoding, scaling and any learned feature transformations only on each fold's training data. No future truth, completeness, eligibility or role fields may enter the vector. Compare Brier with equal threshold weight within year, then equal weight across the three validation years. Report log loss, PR-AUC, ROC-AUC and reliability as additional diagnostics. Predefine candidate configurations and a simplicity preference for effectively tied scores.

Lock selection before the final 2017–2023 refit. Use 2024 only for the final evaluation; never choose the model, thresholds, features or calibration based on its scores. An optional E1 comparison (train 2017–2022, validate 2023) is diagnostic and must not use 2024 to choose a protocol. Save per-threshold/year metrics, support counts, computational settings, model and reload/inference evidence. The completed implementation is documented in C_delivery and the integrated report.
