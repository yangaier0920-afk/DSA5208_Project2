# Project reference material

Daily run instructions are in README.md. Historical status is identified by stage; completed A/B/C conclusions are in submission/Final_Report_EN.md.



# Project responsibilities and delivery plan

Use the 2017–2024 Singapore five-minute rainfall datasets with Apache Spark. Task 1 explores changes within and across years with visual evidence. Task 2 predicts P(next-30-minute accumulated rainfall > input threshold) for a station and prediction time, using Spark MLlib.

| Member role | Responsibility | Deliverable |
|---|---|---|
| A | Audit raw annual CSV, clean and standardize observations, preserve provenance, construct exact future-window truth and time splits | Versioned Parquet, coverage tables, data contract, environment and replay evidence |
| B | Coverage-aware seasonal, hourly, station and interannual analysis; past-only features | Spark analysis source, figures, summaries, feature contract and Parquet |
| C | Threshold expansion, temporal validation, frequency/LR/tree candidates, probability evaluation and inference | Saved model, evaluation tables, input contract and reload checks |

The members jointly review assumptions, reproduce interfaces, integrate the report and submit complete code with README, the trained model and a PDF listing all names and student IDs. The deadline in the course instructions is 18 October 2026; the planned development period is 8–18 October. At most three members are allowed.

Start B/C interface work with small fixtures while A performs full processing. Freeze common time, missingness, key and partition rules before final experiments. Compare coverage-qualified stations, never nationwide sums over changing station sets. Compare a training-frequency baseline, logistic regression and a shallow tree; choose using development data and reserve 2024 for the final test. Record AI use, limitations and measured computing resources. Check data identity, unique joins, no future leakage, model persistence, past-only inference and complete submission materials.


# Released data contract

Data version v1; observations p0_3_v1; targets p0_4_v1. Original releases rainfall_v1/r2/r3 changed delivery documentation without changing Parquet rules. The immutable original-byte snapshot and checksums are retained separately after English translation.

Formal observations and targets each have 48,538,627 rows. Keys are station_id+slot_end_epoch_s and station_id+prediction_epoch_s respectively. Read using scripts/data_release.py and match both station and epoch; do not join only station/date. Use Asia/Singapore, Unix seconds and rainfall mm. Exact rainfall is Decimal(38,18).

Only 2017 epoch remainder 299 is shifted +1 second. Equal-rain collisions prefer the original standard-grid record, retaining two-sided lineage; unequal-rain groups are quarantined. Valid finite nonnegative rainfall, zeros and extremes are retained. Missingness remains unknown. No future metadata backfill. Metadata first/last appearance is not a continuous validity interval.

The project assumes an interval endpoint t with R(t) available at t. Task 1 calendar uses t−300 seconds. Future truth is the six exact endpoints t+300,...,t+1800; full history is 12 endpoints t−3300,...,t. Load boundary context before filtering years. Unknown future truth stays null; strict exceedance uses >, not >=. Coverage comparison uses the same qualified stations for each supported month, full calendar denominators, main 90% and sensitivity 95%; it does not globally filter Task 2.

E2 development: 2017–2020/2021, 2017–2021/2022, 2017–2022/2023. Lock selection then refit 2017–2023/test 2024. Filter actual role boundaries and joint window eligibility for supervision; do not remove every cross-year anchor. Fit learned encoding/scaling only on training, sample anchors before threshold expansion. Future values, eligibility and role are not features or inference gates.

Record data_version, release_id, config_hash (SHA-256 of exact original project.json bytes) and experiment_config_hash in result metadata. Do not silently replace historical identities. New data/rules require a new data version; new delivery documentation requires a documented revision. Original frozen bytes must pass their original checksums. New translated documents have their own translation inventory. Paired 1,309-row fixtures and 1,297-record/114-anchor replay are interface tests, not representative training data.

See p0_3_data_contract.md, p0_4_data_contract.md, field_dictionary.md and E2_ROLLING_VALIDATION.md for details. Full source, trained model and integrated report are now in submission; historical A-stage status files retain their original scope.


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


# Field dictionary for the formal rainfall_v1 data

Timestamps are displayed in Asia/Singapore; epochs use Unix seconds. Parquet nullable is a storage-schema property, not permission for every field to be missing. Observation validity and label-null constraints are defined in data_contract. Raw/audit columns support lineage and must not be passed wholesale to a model.

## observations

| Field | Spark type | schema nullable | Definition and purpose |
|---|---|---|---|
| raw_line | string | true | Original CSV record, preserved verbatim for lineage; not a model input |
| field_count | integer | true | Number of fields in the original CSV record; expected to be 12 |
| odd_quotes | boolean | true | Whether the original record has an odd quote count; parsing audit flag |
| raw_date | string | true | Original date, preserved without substitution for lineage |
| raw_timestamp | string | true | Original observation timestamp text, preserved without substitution for lineage |
| raw_update_timestamp | string | true | Original update timestamp text, preserved without substitution for lineage |
| raw_station_id | string | true | Original station ID, preserved without substitution for lineage |
| raw_station_name | string | true | Original station name, preserved without substitution for lineage |
| raw_station_device_id | string | true | Original device ID, preserved without substitution for lineage |
| raw_location_longitude | string | true | Original longitude text, preserved without substitution for lineage |
| raw_location_latitude | string | true | Original latitude text, preserved without substitution for lineage |
| raw_reading_update_timestamp | string | true | Original reading-update timestamp text, preserved without substitution for lineage |
| raw_reading_value | string | true | Original rainfall text, in source unit mm, preserved without substitution for lineage |
| raw_reading_type | string | true | Original reading-type text, preserved without substitution for lineage |
| raw_reading_unit | string | true | Original unit text; expected mm, preserved without substitution for lineage |
| _corrupt_record | string | true | Original text captured by Spark for a parsing failure; null for valid observations |
| source_year | integer | true | Year of the original annual source file |
| source_file | string | true | Original filename or source-path identifier |
| source_sha256 | string | true | SHA-256 of the complete original source file |
| raw_record_sha256 | string | true | SHA-256 of the original record; lineage identifier, not a replacement observation key |
| station_id | string | true | Preserved original station ID, including S113; used in the primary key and station grouping |
| station_name | string | true | Station name associated with this record; historical changes are retained |
| station_device_id | string | true | Device ID associated with this record |
| event_ts | timestamp | true | Parsed original observation timestamp, before the 2017 alignment |
| update_ts | timestamp | true | Parsed original update_timestamp; audit only, not a weather predictor |
| reading_update_ts | timestamp | true | Parsed original reading_update_timestamp; audit only, not a weather predictor |
| rain_mm | double | true | Five-minute interval rainfall as double, in mm; usable for features, while exact threshold comparisons use Decimal |
| rain_decimal_mm | decimal(38,18) | true | Exact five-minute rainfall as Decimal(38,18), in mm; used for label accumulation |
| longitude | double | true | Longitude in the original row, in degrees; never backfilled with a future location |
| latitude | double | true | Latitude in the original row, in degrees; never backfilled with a future location |
| event_epoch_s | long | true | Unix seconds of the original observation timestamp |
| original_offset_s | long | true | Original epoch modulo 300, in seconds |
| alignment_offset_s | integer | true | Seconds actually added during alignment (0 or 1) |
| alignment_rule | string | true | Identifier of the applied alignment rule |
| slot_end_epoch_s | long | true | Aligned five-minute interval endpoint in Unix seconds; timestamp component of the observation key |
| slot_end_ts | timestamp | true | Aligned interval endpoint or prediction anchor t on the five-minute grid |
| rain_period_start_ts | timestamp | true | Interval start, slot_end_ts minus 300 seconds; calendar attribution for Task 1 |
| observation_date | date | true | Singapore calendar date of the interval endpoint |
| rain_period_date | date | true | Singapore calendar date of the interval start |
| rain_period_hour | integer | true | Singapore hour of the interval start, from 0 to 23 |
| update_delay_s | long | true | Update timestamp minus original observation time, in seconds; audit only |
| reading_update_delay_s | long | true | Reading-update timestamp minus original observation time, in seconds; audit only |
| quarantine_reasons | array<string> | true | Quarantine-reason array; empty for valid observations |
| metadata_warning | boolean | true | Warning for missing name/device, invalid coordinates or unparsed update timestamps; valid rainfall remains retained |
| source_effective_decimal_scale | integer | true | Effective decimal places in the source number; lossless-representation audit |
| decimal_representation_error | boolean | true | Flag for failure to represent a value exactly with the selected Decimal; false for valid observations |
| rules_version | string | true | Cleaning-rule version identifier |
| aligned_source_count | long | true | Number of source rows mapped to the same aligned key |
| metadata_conflict | boolean | true | Metadata differs within an aligned-key collision; equal rainfall may still be retained |
| year | integer | true | Singapore year of the aligned endpoint/prediction anchor; partition key |
| month | integer | true | Singapore month of the aligned endpoint/prediction anchor; partition key from 1 to 12 |

## quarantine

| Field | Spark type | schema nullable | Definition and purpose |
|---|---|---|---|
| raw_line | string | true | Original CSV record, preserved verbatim for lineage; not a model input |
| field_count | integer | true | Number of fields in the original CSV record; expected to be 12 |
| odd_quotes | boolean | true | Whether the original record has an odd quote count; parsing audit flag |
| raw_date | string | true | Original date, preserved without substitution for lineage |
| raw_timestamp | string | true | Original observation timestamp text, preserved without substitution for lineage |
| raw_update_timestamp | string | true | Original update timestamp text, preserved without substitution for lineage |
| raw_station_id | string | true | Original station ID, preserved without substitution for lineage |
| raw_station_name | string | true | Original station name, preserved without substitution for lineage |
| raw_station_device_id | string | true | Original device ID, preserved without substitution for lineage |
| raw_location_longitude | string | true | Original longitude text, preserved without substitution for lineage |
| raw_location_latitude | string | true | Original latitude text, preserved without substitution for lineage |
| raw_reading_update_timestamp | string | true | Original reading-update timestamp text, preserved without substitution for lineage |
| raw_reading_value | string | true | Original rainfall text, in source unit mm, preserved without substitution for lineage |
| raw_reading_type | string | true | Original reading-type text, preserved without substitution for lineage |
| raw_reading_unit | string | true | Original unit text; expected mm, preserved without substitution for lineage |
| _corrupt_record | string | true | Original text captured by Spark for a parsing failure; null for valid observations |
| source_year | integer | true | Year of the original annual source file |
| source_file | string | true | Original filename or source-path identifier |
| source_sha256 | string | true | SHA-256 of the complete original source file |
| raw_record_sha256 | string | true | SHA-256 of the original record; lineage identifier, not a replacement observation key |
| station_id | string | true | Preserved original station ID, including S113; used in the primary key and station grouping |
| station_name | string | true | Station name associated with this record; historical changes are retained |
| station_device_id | string | true | Device ID associated with this record |
| event_ts | timestamp | true | Parsed original observation timestamp, before the 2017 alignment |
| update_ts | timestamp | true | Parsed original update_timestamp; audit only, not a weather predictor |
| reading_update_ts | timestamp | true | Parsed original reading_update_timestamp; audit only, not a weather predictor |
| rain_mm | double | true | Five-minute interval rainfall as double, in mm; usable for features, while exact threshold comparisons use Decimal |
| rain_decimal_mm | decimal(38,18) | true | Exact five-minute rainfall as Decimal(38,18), in mm; used for label accumulation |
| longitude | double | true | Longitude in the original row, in degrees; never backfilled with a future location |
| latitude | double | true | Latitude in the original row, in degrees; never backfilled with a future location |
| event_epoch_s | long | true | Unix seconds of the original observation timestamp |
| original_offset_s | long | true | Original epoch modulo 300, in seconds |
| alignment_offset_s | integer | true | Seconds actually added during alignment (0 or 1) |
| alignment_rule | string | true | Identifier of the applied alignment rule |
| slot_end_epoch_s | long | true | Aligned five-minute interval endpoint in Unix seconds; timestamp component of the observation key |
| slot_end_ts | timestamp | true | Aligned interval endpoint or prediction anchor t on the five-minute grid |
| rain_period_start_ts | timestamp | true | Interval start, slot_end_ts minus 300 seconds; calendar attribution for Task 1 |
| observation_date | date | true | Singapore calendar date of the interval endpoint |
| rain_period_date | date | true | Singapore calendar date of the interval start |
| rain_period_hour | integer | true | Singapore hour of the interval start, from 0 to 23 |
| update_delay_s | long | true | Update timestamp minus original observation time, in seconds; audit only |
| reading_update_delay_s | long | true | Reading-update timestamp minus original observation time, in seconds; audit only |
| quarantine_reasons | array<string> | true | Quarantine-reason array; empty for valid observations |
| metadata_warning | boolean | true | Warning for missing name/device, invalid coordinates or unparsed update timestamps; valid rainfall remains retained |
| source_effective_decimal_scale | integer | true | Effective decimal places in the source number; lossless-representation audit |
| decimal_representation_error | boolean | true | Flag for failure to represent a value exactly with the selected Decimal; false for valid observations |
| rules_version | string | true | Cleaning-rule version identifier |
| aligned_source_count | long | true | Number of source rows mapped to the same aligned key |
| metadata_conflict | boolean | true | Metadata differs within an aligned-key collision; equal rainfall may still be retained |
| year | integer | true | Original source-file year; partition key |

## collision_lineage

| Field | Spark type | schema nullable | Definition and purpose |
|---|---|---|---|
| station_id | string | true | Preserved original station ID, including S113; used in the primary key and station grouping |
| slot_end_epoch_s | long | true | Aligned five-minute interval endpoint in Unix seconds; timestamp component of the observation key |
| raw_line | string | true | Original CSV record, preserved verbatim for lineage; not a model input |
| field_count | integer | true | Number of fields in the original CSV record; expected to be 12 |
| odd_quotes | boolean | true | Whether the original record has an odd quote count; parsing audit flag |
| raw_date | string | true | Original date, preserved without substitution for lineage |
| raw_timestamp | string | true | Original observation timestamp text, preserved without substitution for lineage |
| raw_update_timestamp | string | true | Original update timestamp text, preserved without substitution for lineage |
| raw_station_id | string | true | Original station ID, preserved without substitution for lineage |
| raw_station_name | string | true | Original station name, preserved without substitution for lineage |
| raw_station_device_id | string | true | Original device ID, preserved without substitution for lineage |
| raw_location_longitude | string | true | Original longitude text, preserved without substitution for lineage |
| raw_location_latitude | string | true | Original latitude text, preserved without substitution for lineage |
| raw_reading_update_timestamp | string | true | Original reading-update timestamp text, preserved without substitution for lineage |
| raw_reading_value | string | true | Original rainfall text, in source unit mm, preserved without substitution for lineage |
| raw_reading_type | string | true | Original reading-type text, preserved without substitution for lineage |
| raw_reading_unit | string | true | Original unit text; expected mm, preserved without substitution for lineage |
| _corrupt_record | string | true | Original text captured by Spark for a parsing failure; null for valid observations |
| source_year | integer | true | Year of the original annual source file |
| source_file | string | true | Original filename or source-path identifier |
| source_sha256 | string | true | SHA-256 of the complete original source file |
| raw_record_sha256 | string | true | SHA-256 of the original record; lineage identifier, not a replacement observation key |
| station_name | string | true | Station name associated with this record; historical changes are retained |
| station_device_id | string | true | Device ID associated with this record |
| event_ts | timestamp | true | Parsed original observation timestamp, before the 2017 alignment |
| update_ts | timestamp | true | Parsed original update_timestamp; audit only, not a weather predictor |
| reading_update_ts | timestamp | true | Parsed original reading_update_timestamp; audit only, not a weather predictor |
| rain_mm | double | true | Five-minute interval rainfall as double, in mm; usable for features, while exact threshold comparisons use Decimal |
| rain_decimal_mm | decimal(38,18) | true | Exact five-minute rainfall as Decimal(38,18), in mm; used for label accumulation |
| longitude | double | true | Longitude in the original row, in degrees; never backfilled with a future location |
| latitude | double | true | Latitude in the original row, in degrees; never backfilled with a future location |
| event_epoch_s | long | true | Unix seconds of the original observation timestamp |
| original_offset_s | long | true | Original epoch modulo 300, in seconds |
| alignment_offset_s | integer | true | Seconds actually added during alignment (0 or 1) |
| alignment_rule | string | true | Identifier of the applied alignment rule |
| slot_end_ts | timestamp | true | Aligned interval endpoint or prediction anchor t on the five-minute grid |
| rain_period_start_ts | timestamp | true | Interval start, slot_end_ts minus 300 seconds; calendar attribution for Task 1 |
| observation_date | date | true | Singapore calendar date of the interval endpoint |
| rain_period_date | date | true | Singapore calendar date of the interval start |
| rain_period_hour | integer | true | Singapore hour of the interval start, from 0 to 23 |
| update_delay_s | long | true | Update timestamp minus original observation time, in seconds; audit only |
| reading_update_delay_s | long | true | Reading-update timestamp minus original observation time, in seconds; audit only |
| quarantine_reasons | array<string> | true | Quarantine-reason array; empty for valid observations |
| metadata_warning | boolean | true | Warning for missing name/device, invalid coordinates or unparsed update timestamps; valid rainfall remains retained |
| source_effective_decimal_scale | integer | true | Effective decimal places in the source number; lossless-representation audit |
| decimal_representation_error | boolean | true | Flag for failure to represent a value exactly with the selected Decimal; false for valid observations |
| rules_version | string | true | Cleaning-rule version identifier |
| aligned_source_count | long | true | Number of source rows mapped to the same aligned key |
| rain_variants | long | true | Number of distinct exact rainfall values in a collision group |
| metadata_variants | long | true | Number of distinct metadata combinations in a collision group |
| collision_rank | integer | true | Deterministic collision rank; 1 identifies the preferred row |
| retained_record_sha256 | string | true | SHA-256 of the original record retained from the collision group |
| disposition | string | true | Disposition of a collision record, such as RETAINED or MERGED_DUPLICATE |
| year | integer | true | Original source-file year; partition key |

## station_metadata_history

| Field | Spark type | schema nullable | Definition and purpose |
|---|---|---|---|
| station_id | string | true | Preserved original station ID, including S113; used in the primary key and station grouping |
| station_name | string | true | Station name associated with this record; historical changes are retained |
| station_device_id | string | true | Device ID associated with this record |
| raw_location_longitude | string | true | Original longitude text, preserved without substitution for lineage |
| raw_location_latitude | string | true | Original latitude text, preserved without substitution for lineage |
| raw_rows | long | true | Source-row count for this station/metadata combination in the source year, before duplicate merging |
| first_event_ts | timestamp | true | First original appearance of this station/metadata combination; not the start of a continuous validity period |
| last_event_ts | timestamp | true | Last original appearance of this combination; not the end of a continuous validity period |
| source_file | string | true | Original filename or source-path identifier |
| year | integer | true | Original source-file year; partition key |

## targets

| Field | Spark type | schema nullable | Definition and purpose |
|---|---|---|---|
| station_id | string | true | Preserved original station ID, including S113; used in the primary key and station grouping |
| prediction_epoch_s | long | true | Prediction anchor t in Unix seconds; equal to slot_end_epoch_s |
| historical_n_valid | long | true | Valid observations among the 12 historical slots through t, from 0 to 12 |
| prediction_ts | timestamp | true | Prediction anchor t, displayed in Asia/Singapore |
| label_end_epoch_s | long | true | t+1800 seconds; right endpoint of the supervised label window |
| label_end_ts | timestamp | true | Right endpoint of the future 30-minute label window |
| future_missing_offsets_s | array<integer> | true | Missing expected future-slot offsets from t, in seconds; label audit only |
| historical_missing_offsets_s | array<integer> | true | Missing expected historical-slot offsets from t, in seconds |
| future_n_valid | long | true | Valid observations among the six future slots, from 0 to 6; label audit only |
| target_valid | boolean | true | Whether all six future slots are present; supervision filter, prohibited as a predictor |
| historical_valid | boolean | true | Whether all 12 H1 historical slots are present; may check an inference prerequisite |
| future_30m_mm | decimal(38,18) | true | Exact total over six future slots in mm; null if invalid; prohibited as a feature |
| model_anchor_eligible | boolean | true | target_valid AND historical_valid; base supervision eligibility, not an inference prerequisite |
| year_boundary_safe | boolean | true | Whether label end and t lie in the same calendar year; diagnostic only, never a global cross-year filter |
| invalid_reason | array<string> | true | Future-label invalidity reasons, potentially multiple; unknown is not dry |
| historical_invalid_reason | array<string> | true | Historical-input completeness failure reasons |
| fold1_role | string | true | E2 fold 1: TRAIN 2017-2020, VALIDATION 2021, or UNUSED/EXCLUDED_BOUNDARY |
| fold2_role | string | true | E2 fold 2: TRAIN 2017-2021, VALIDATION 2022, or UNUSED/EXCLUDED_BOUNDARY |
| fold3_role | string | true | E2 fold 3: TRAIN 2017-2022, VALIDATION 2023, or UNUSED/EXCLUDED_BOUNDARY |
| final_role | string | true | After configuration lock: TRAIN 2017-2023, TEST 2024, or EXCLUDED_BOUNDARY |
| targets_version | string | true | Target-table version p0_4_v1 |
| observations_version | string | true | Source-observation version p0_3_v1 |
| year | integer | true | Singapore year of the aligned endpoint/prediction anchor; partition key |
| month | integer | true | Singapore month of the aligned endpoint/prediction anchor; partition key from 1 to 12 |


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


The native Windows Hadoop helpers were pinned community 3.3.5 binaries; Spark bundled Hadoop Java 3.3.4. The tested workload passed native Parquet write/read; this is not a general compatibility certification. Alignment affected 1,775,327 raw records and 1,775,241 retained observations. Original replay time (72.58 s) is distinguished from subsequent Mac replay (about 15 s).


# Course instructions

The Historical Rainfall across Singapore datasets, available at https://data.gov.sg/collections/2279/viewLinks to an external site., provide rainfall measurements recorded at five-minute intervals by monitoring stations across Singapore. Use the annual datasets from 2017 to 2024. Each record includes the observation time, station information, and rainfall amount (mm).

The project involves structured data processing and machine learning using Apache Spark. You may use any programming language supported by Spark. The two tasks are as follows:

Task 1: Explore weather patterns. Explore the weather patterns reflected in the rainfall data and how they change within a year and across the years 2017–2024. Present and discuss your findings using appropriate analysis and visualizations.
Task 2: Predict rainfall. Build a machine learning model that estimates the probability that a monitoring station will accumulate more than a given amount of rainfall in the next 30 minutes. The model should take the station, prediction time, and rainfall threshold as inputs. Select suitable features and evaluate your model using Spark MLlib.
The project requires you to write a report describing your approach and results. In the report, you must

Explain how you cleaned the data, investigated weather patterns and their changes, selected features, and developed your machine learning model.
Describe your computing environment (e.g., a local machine, Google Cloud, or Databricks), including the relevant VM or cluster configuration.
Present and discuss the results of both tasks, preferably with figures and tables, and evaluate your model’s predictive performance.
Declare any AI usages.
Discuss any additional work, observations, limitations, or insights you would like to share.
This is again a group project, and each group can have at most three people. Please submit the following materials to Canvas no later than 18 Oct 2026: 

Your complete source code and a README explaining how to run it.
Your trained model.
A PDF report listing the names and student IDs of all group members.