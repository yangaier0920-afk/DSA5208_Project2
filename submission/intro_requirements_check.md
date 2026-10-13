# Verification against intro.txt

Checked on 12 October 2026 against the local assignment text. The project has completed the required analysis, Spark MLlib modelling and submission materials within the explicitly documented prediction domain. Canvas submission is not complete. If the assignment is interpreted as requiring arbitrary continuous rainfall thresholds, the five-threshold interface is a remaining scope restriction; the project must not claim unlimited threshold support.

| Assignment requirement | Finding | Evidence |
|---|---|---|
| Annual 2017–2024 Singapore rainfall datasets | Complete: all eight annual exports represented; 48,538,713 raw records, 48,538,627 retained | manifests/raw_manifest.json; A annual cleaning/target ledger; report section 2 |
| Structured processing using Apache Spark | Complete: audit, cleaning, exact windows, aggregates and features use Spark | src/01_audit_raw.py, 02_clean_standardize.py, 03_build_targets.py; B source |
| Task 1 within-year and interannual exploration | Complete: coverage-aware monthly occurrence/intensity, hourly/station differences and matched same-month comparisons | B task1_rerun_20261012 summaries, seven figures; report section 4 |
| Discuss Task 1 findings using appropriate visualizations | Complete, with unsupported panels/NA retained and composition limitations disclosed | report figures 1–7 and discussion |
| Task 2 probability of strictly more than a given amount over next 30 minutes | Implemented/evaluated within five frozen thresholds 0,0.5,1,2,5 mm; exact Decimal > comparison, equality negative | C_train.py, C_predict.py, C contract tests; report sections 5–8 |
| Model accepts station, prediction time and threshold | Complete within supported station/grid/history/threshold domain; rejects unsupported inputs | RainfallPredictor.predict; past-only and reload evidence |
| Feature selection and Spark MLlib evaluation | Complete: 26 past features, numeric threshold, training-only station coding; baseline/LR/tree E2 comparison | C experiment config, development_metrics.csv, selection.json |
| Describe cleaning/features/modelling | Complete | report sections 2,3,5,6 |
| Describe compute environment and VM/cluster configuration | Complete: original Windows A and current Mac B/C distinguished; local threads/heap/shuffle/storage/timing limits documented | report computing section; environment/timing JSON |
| Present both tasks and predictive performance, figures/tables | Complete: ten figures, development and final probability metrics, reliability, support/coverage | Final_Report.pdf; final_test.json; metrics appendices |
| Declare AI usage | Complete: OpenAI Codex assistance disclosed; automated checks distinguished from human review | report AI-use paragraph and README |
| Discuss additional work/observations/limitations | Complete: missingness, composition, latency/interval assumptions, overlapping windows, restricted thresholds, sampling/candidate scope | report limitations and conclusion |
| Group at most three | Complete: three user-supplied members | members.json and PDF cover |
| Complete source and run README | Prepared and packaged; cold extraction smoke verification recorded separately | Source_Code.zip, README.md, tools/run_full_project.sh; verification receipts |
| Trained model | Prepared and saved; independent reload/past-only inference passed | Trained_Model.zip; model_reload_validation.json |
| PDF with every member name/student ID | Complete: SHAO YUQING A0330603W; YANG AIER A0354958J; ZHU YIXU A0355117H | Final_Report.pdf cover |
| Upload to Canvas by 18 October 2026 | NOT PERFORMED; deadline remains | User must upload the prepared materials |

The five-threshold choice is scientifically evaluated and prominently disclosed, but intro.txt does not explicitly limit the requested input to a fixed set. Therefore the audit distinguishes completed modelling from a stronger arbitrary-threshold claim. Expanding that domain would require prespecified development-only support/evaluation and cannot be justified by retuning on the already observed 2024 test.

English conversion is a documentation/source-message revision. It does not rerun training, change data, alter selected parameters or replace numerical results. Historical execution identities refer to original bytes; their immutable provenance snapshot is separate from active English materials. Automated verification does not establish that each group member has independently reviewed every line or that Canvas upload happened.
