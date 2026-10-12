# Project responsibilities and delivery plan

Use the 2017–2024 Singapore five-minute rainfall datasets with Apache Spark. Task 1 explores changes within and across years with visual evidence. Task 2 predicts P(next-30-minute accumulated rainfall > input threshold) for a station and prediction time, using Spark MLlib.

| Member role | Responsibility | Deliverable |
|---|---|---|
| A | Audit raw annual CSV, clean and standardize observations, preserve provenance, construct exact future-window truth and time splits | Versioned Parquet, coverage tables, data contract, environment and replay evidence |
| B | Coverage-aware seasonal, hourly, station and interannual analysis; past-only features | Spark analysis source, figures, summaries, feature contract and Parquet |
| C | Threshold expansion, temporal validation, frequency/LR/tree candidates, probability evaluation and inference | Saved model, evaluation tables, input contract and reload checks |

The members jointly review assumptions, reproduce interfaces, integrate the report and submit complete code with README, the trained model and a PDF listing all names and student IDs. The deadline in the course instructions is 18 October 2026; the planned development period is 8–18 October. At most three members are allowed.

Start B/C interface work with small fixtures while A performs full processing. Freeze common time, missingness, key and partition rules before final experiments. Compare coverage-qualified stations, never nationwide sums over changing station sets. Compare a training-frequency baseline, logistic regression and a shallow tree; choose using development data and reserve 2024 for the final test. Record AI use, limitations and measured computing resources. Check data identity, unique joins, no future leakage, model persistence, past-only inference and complete submission materials.
