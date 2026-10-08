# A report section draft: data preparation and reproducibility

Data release: `rainfall_v1`; `data_version=v1`; data configuration SHA-256: `e5258b99925606c8ba88207cb44a6756514fa84cd60f37adea3f9db64103248b`. This section describes A's executed work. B/C should supply the weather-analysis, feature-development, modelling and evaluation sections before submission.

## Data and cleaning

The eight annual data.gov.sg rainfall exports for 2017–2024 contain 48,538,713 records according to Spark parsing and occupy 8,047,044,701 bytes. Annual dataset links and local full-file SHA-256 hashes are recorded in `manifests/raw_manifest.json`. Download completeness was confirmed by the user; these local hashes are not presented as server-provided reference checksums. The records contain five-minute rainfall measurements in millimetres, station metadata and event/update timestamps.

We retained the original fields, record text and provenance. For 2017 only, observations whose original epoch remainder modulo 300 was 299 were shifted forward by one second to the five-minute endpoint, while retaining the original event timestamp. This affected 1,775,327 source records and produced 86 pairs of aligned-key collisions with equal rainfall. The original exact-grid observation was retained; rainfall was neither summed nor averaged. Both sides of each collision were preserved in a 172-row lineage table, including two retained observations with metadata conflicts. Conflicting rainfall would quarantine the entire key group, but no such groups occurred in these data.

Finite nonnegative measurements, including genuine zeros and extremes, were retained without arbitrary truncation or rounding to 0.2 mm. Exact cumulative rainfall uses Decimal(38,18); observed source precision required at most five effective decimal places. This storage capacity does not imply additional measurement precision. Missing timestamps remain unknown rather than becoming zero-rainfall observations. The source data have 63 days with no network records, including 4–31 August 2018; their physical cause cannot be inferred from these files.

The cleaning ledger closes as 48,538,713 = 48,538,627 retained observations + 86 merged records + 0 quarantined records. A zero quarantine count does not establish time-series completeness or meteorological quality. The observations table has a unique station/endpoint key and preserves 47,121,647 genuine zero measurements. Rainfall is interpreted as the five-minute interval ending at the aligned timestamp; calendar aggregation for Task 1 uses the interval start, including midnight contributions to the preceding day.

## Future-window truth and completeness

Each observation supplies one prediction anchor. We require observations at all six exact offsets from t+300 to t+1800 seconds for the future 30-minute total. Incomplete totals remain null; complete dry windows contain Decimal zero. History completeness requires all 12 positions from t−3300 to t. Adjacent-year context is included before anchors are selected, preventing artificial truncation at file boundaries.

There are 47,636,833 complete future windows and 901,794 unknown future totals. Historical inputs are complete for 47,347,833 anchors and incomplete for 1,190,794. Both windows are complete for 47,048,872 anchors; further fold-boundary and station-support restrictions remain the responsibility of downstream processing. History and future exclusions overlap and must not be added as disjoint losses. All anchors, including invalid ones, remain in the target table. Threshold labels must use the strict Decimal comparison `future_30m_mm > threshold_mm`; equality is a negative label, while an unknown total is not a negative example.

| Year | Raw records | Observations | Merged records | Future complete | Future unknown | Both windows complete |
|---|---|---|---|---|---|---|
| 2017 | 5,256,106 | 5,256,020 | 86 | 5,057,559 | 198,461 | 4,822,618 |
| 2018 | 4,594,246 | 4,594,246 | 0 | 4,079,741 | 514,505 | 3,981,287 |
| 2019 | 5,026,862 | 5,026,862 | 0 | 4,991,346 | 35,516 | 4,935,783 |
| 2020 | 6,334,473 | 6,334,473 | 0 | 6,292,286 | 42,187 | 6,234,155 |
| 2021 | 7,044,646 | 7,044,646 | 0 | 7,013,455 | 31,191 | 6,976,422 |
| 2022 | 7,133,230 | 7,133,230 | 0 | 7,108,318 | 24,912 | 7,073,178 |
| 2023 | 6,742,890 | 6,742,890 | 0 | 6,719,652 | 23,238 | 6,687,003 |
| 2024 | 6,406,260 | 6,406,260 | 0 | 6,374,476 | 31,784 | 6,338,426 |

E2 role fields support training through 2020/validation in 2021, training through 2021/validation in 2022, and training through 2022/validation in 2023. After configuration selection, the final refit uses 2017–2023 and the final test uses 2024. A generated truth/completeness and role fields without fitting a model, expanding thresholds or learning features. Future totals and future-dependent eligibility flags are prohibited as model inputs or prior inference conditions.

## Computing environment and observed runtime

The local machine uses Windows 11, a 12th Gen Intel(R) Core(TM) i7-12700H CPU (14 physical cores and 20 logical processors), and approximately 31.73 GiB of system-visible physical memory. Python 3.11.5, Spark/PySpark 3.5.7, Py4J 0.10.9.7 and Java 17.0.20.1 were used with the Asia/Singapore session timezone. Full stages used `local[4]`, a configured 4g driver heap and 48 shuffle partitions. Local threads are not cluster nodes, and the heap setting is not peak resident memory.

Original full-run wall times were 31.48 minutes for auditing, 34.27 minutes for cleaning, and 9.82 minutes for target construction, calculated from each stage's original start/finish receipts. They include that stage's lifecycle and built-in checks but exclude dependency installation, separately executed validators and human work between stages. No full-run peak RSS, CPU-utilisation or temporary-disk peak measurements were collected. Formal Parquet payloads occupy 4,678,105,303 bytes, which is not a peak-disk requirement.

## Verification, limitations and AI assistance

Existing full-data validation includes count reconciliation, unique keys, time-grid/schema checks, root-directory readback and file hashes. Target checks include independent Decimal recomputation for 29 real cases covering 522 expected positions. The additional P1-6 replay uses 1,297 preserved raw source records and 114 selected anchors, covering all 86 collisions and the development-period manual cases. Headers/newlines are reconstructed, while record text is unchanged. The frozen production functions are rerun; all cleaned observations, collision lineage and selected targets are compared against full-run reference rows after Parquet writing/reading, and every selected target is independently recomputed by timestamp-key lookup. The initial local replay took approximately 72.58 seconds with `local[2]`, 2g and eight shuffle partitions. It is a functional check, not a representative performance benchmark. A separately extracted support-package replay is documented by its receipt.

Missing coverage limits monthly/annual interpretation and evaluation scope. Alignment and interval interpretation are explicit project assumptions. Metadata first/last appearances do not establish continuous validity periods. Under the retrospective event-time availability assumption, delayed updates are audited but are not predictors; results should not be claimed as demonstrated real-time deployment performance. The eight-year record supports descriptive interannual comparisons, not an established long-term climate trend. A's checks use the same host and configured runtime; B/C and macOS acceptance, and final-model reload/prediction validation, remain pending.

Suggested AI disclosure for A, to be reviewed and supplemented by the team: OpenAI Codex assisted with planning, implementation, documentation, quality checks and reproduction workflows. Reported A-stage counts, hashes and execution status were checked against actual outputs. B/C should disclose their own AI use and verification separately. This draft does not claim that all code has been manually reviewed line by line or that modelling is complete.
