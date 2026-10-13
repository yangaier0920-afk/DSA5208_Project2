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
