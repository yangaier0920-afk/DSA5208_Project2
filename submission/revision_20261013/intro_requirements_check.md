# intro.txt requirements and current evidence

Checked against the integrated report and selected model.

| Requirement | Evidence and status |
|---|---|
| 2017–2024 data and Spark processing | COMPLETE: A ledger and B full-data results, preserved signed provenance |
| Task 1 within/across-year patterns | COMPLETE: nine figures, coverage sensitivity, seasonality, hourly/spatial and adjacent-year discussion |
| Task 2 station/time/threshold probability with MLlib | COMPLETE: persisted conditional-CDF tree, 22 selected features, three temporal folds, stronger baselines and feature ablation |
| Given amount of rainfall | IMPLEMENTED: finite nonnegative Decimal(38,18) queries; nine development cuts 0..5 mm evaluated, arbitrary tails unvalidated |
| Cleaning, features, modelling and performance | COMPLETE: report methods, full metrics, calibration and grouped coverage/error |
| Computing environment | COMPLETE: separate A/B/C environments and measured-scope limitations |
| AI use and limitations | DECLARED: Codex assistance, event-time assumptions, correlated windows and development-only current model |
| Complete source and run README | PREPARED: source/README provided; fresh-machine full training and Windows untested; notebook machine path still requires editing |
| Trained model | PREPARED: Trained_Model.zip, logical hashes and fresh-process past-only reload validation |
| PDF with at most three named members and IDs | COMPLETE: all three supplied members on cover |
| Canvas submission by 18 October 2026 | NOT PERFORMED: materials prepared for member review/upload |

The assignment does not prescribe an untouched test year. Current model performance is explicitly development validation, with no new 2024 test. The preserved original 2024 score belongs only to the historical classifier and must not be credited to the revised model. Input support and monotonicity do not establish continuous-domain predictive accuracy or live publication availability.

Detailed review acceptance, partial items and audit receipts: requirements_and_review_acceptance.md.
