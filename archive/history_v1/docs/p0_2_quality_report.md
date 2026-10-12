# P0-2 full quality audit results

Generated at: 2026-10-08T15:02:54.947318+08:00。

**All 2017–2024 annual exports were actually audited using Spark; eight-year reconciliation and normal Java shutdown passed. Raw CSV files were not modified.**

Spark attempted to parse **48,538,713** records; structurally valid **48,538,713**; annual unique station/time keys total **48,538,713**. Full-period stations **91**. Candidate valid records **48,538,713**; this is not yet the formal cleaned delivery count.

Candidate valid five-minute observations with positive rain total **1,416,983** rows, proportion **2.92%**. This is single-slot wet frequency, not future-30-minute exceedance prevalence; C must recompute class support using complete truth.

## 1. Full annual counts and main issues

| Year | Spark rows | Stations | Unique time keys | Duplicate excess rows | Conflicting-rain keys | Off-grid rows | Adjacent gaps >300s ||
|---|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 5,256,106 | 62 | 5,256,106 | 0 | 0 | 1,775,327 | 44,825 |
| 2018 | 4,594,246 | 56 | 4,594,246 | 0 | 0 | 0 | 113,440 |
| 2019 | 5,026,862 | 54 | 5,026,862 | 0 | 0 | 0 | 6,328 |
| 2020 | 6,334,473 | 80 | 6,334,473 | 0 | 0 | 0 | 8,086 |
| 2021 | 7,044,646 | 76 | 7,044,646 | 0 | 0 | 0 | 6,506 |
| 2022 | 7,133,230 | 72 | 7,133,230 | 0 | 0 | 0 | 5,305 |
| 2023 | 6,742,890 | 74 | 6,742,890 | 0 | 0 | 0 | 4,584 |
| 2024 | 6,406,260 | 70 | 6,406,260 | 0 | 0 | 0 | 6,479 |

| Check | Eight-year affected rows ||
|---|---:|
| `csv_corrupt` | 0 |
| `csv_field_count_mismatch` | 0 |
| `csv_odd_quotes` | 0 |
| `missing_station_id` | 0 |
| `missing_rainfall` | 0 |
| `invalid_rainfall_number` | 0 |
| `nonfinite_rainfall` | 0 |
| `negative_rainfall` | 0 |
| `unexpected_unit` | 0 |
| `unexpected_reading_type` | 0 |
| `invalid_timestamp` | 0 |
| `missing_timestamp` | 0 |
| `event_year_mismatch` | 0 |
| `date_mismatch_or_missing` | 0 |
| `invalid_coordinates` | 0 |

Checks overlap; affected rows cannot be added as cleaning losses. Annual timestamp, missing device/name and extreme-value details are in annual_quality.csv. Full Spark counts agree annually with the physical-line inventory; manifests/raw_manifest.json identifies input source/bytes.

## 2. Timestamp phases and cadence

- 2017：offset=0 seconds: 3,480,779 rows；offset=299 seconds: 1,775,327 rows; adjacent gaps below 300 seconds 129; nonmultiples of 300 seconds 139 occurrences.
- 2018：offset=0 seconds: 4,594,246 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.
- 2019：offset=0 seconds: 5,026,862 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.
- 2020：offset=0 seconds: 6,334,473 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.
- 2021：offset=0 seconds: 7,044,646 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.
- 2022：offset=0 seconds: 7,133,230 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.
- 2023：offset=0 seconds: 6,742,890 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.
- 2024：offset=0 seconds: 6,406,260 rows; adjacent gaps below 300 seconds 0; nonmultiples of 300 seconds 0 occurrences.

2017 offsets appear in the full data from 2017-01-01 to 2017-04-25. Daily distribution is in daily_timestamp_offsets.csv. Appearance range does not establish simultaneous station transitions.

Station-month tables contain **43** excess unique-second times over occupied diagnostic slots. Flooring is diagnostic only; retain original times, identify stations/transitions before choosing alignment. The audit did not shift every 2017 timestamp.

30-minute truth requires exact time-span completeness, never simply the next six rows. Preserve original/candidate-aligned times and gaps. Cross-year cadence is in year_boundary_gaps.csv.

### Supplemental 2017 alignment audit (no cleaning performed)

Station-wise ordering confirmed **139** phase transitions and **129** adjacent observation pairs below 300 seconds; all small-table records are retained.
Adding one second only for offset=299 would produce **86** duplicate target keys, **86** excess duplicate rows; including **0** rain conflicts and **2** metadata conflicts. Another **6,191** records would change local calendar date.

Alignment and collision rules must be designed jointly. All collision groups/raw rows/phase transitions are in time_alignment_diagnostic/. This audit did not adopt the candidate rule or choose retained collision rows.

## 3. Station coverage and metadata

Station-month table has **6,528** rows including zero-observation months for stations present that year. Zero-coverage station-months **533**; below 95% **2,010**. Counts include station start/stop scope, not only equipment missed readings.

Stations appearing in all eight years total **38**: S08, S104, S106, S107, S108, S109, S111, S112, S113, S114, S115, S116, S118, S119, S120, S121, S123, S24, S33, S35, S40, S43, S44, S50, S60, S66, S69, S71, S77, S78, S79, S81, S84, S88, S89, S90, S900, S94. This is not yet a confirmed fixed comparison panel.

Three candidate sensitivity panels for B require station presence in all eight years and qualifying complete-calendar coverage in each of 96 months.

| Minimum monthly coverage | Candidate count | Candidate stations ||
|---|---:|---|
| 90% | 0 | None |
| 95% | 0 | None |
| 99% | 0 | None |

**All three strict all-month panels are empty, so an eight-year fixed station set with every month >=90% is unsupported.**

Lowest-coverage months among 38 common stations follow; denominator is 38 stations times complete-calendar monthly slots:

| Year-month | Common-station slot coverage ||
|---|---:|
| 2018-08 | 7.47% |
| 2018-02 | 60.31% |
| 2023-01 | 65.77% |
| 2018-04 | 67.19% |
| 2018-03 | 71.40% |
| 2020-08 | 76.30% |
| 2019-11 | 78.52% |
| 2024-08 | 80.21% |

The local raw 2018 CSV has **3 days** with any August station records: 2018-08-01, 2018-08-02, 2018-08-03。4–31 August has no records; this does not mean no rainfall.Full-period total **63 calendar days** with no station record in the corresponding annual CSV; see daily_network_record_counts.csv.

**Source completeness was confirmed by the user** in configs/source_confirmation.json. Treat absent dates as gaps in the supplied official export, preserving original files/manifest. This is human confirmation, not a remote official-file hash comparison; the physical cause remains unknown.

B must analyze observed periods, compute frequency/intensity from valid readings, display coverage and use comparable months/periods. Do not zero-fill gaps, call incomplete sums true totals or repair them by dividing by coverage. C truth and lag features require continuous exact-time segments; windows near gaps are incomplete.

These are candidate comparability/sensitivity checks, not confirmed filters. Review coordinates, unusual rain and seasonal gaps even with adequate coverage. Small panels may require explicit within-station or period comparisons. Do not compare annual sums across all changing stations.

| Full-period metadata check | Stations ||
|---|---:|
| Multiple original station names | 26 |
| Multiple original device IDs | 0 |
| Multiple numeric coordinate combinations | 1 |

Stations with coordinate variants: S113。
Combination first/last appearances and counts are in station_metadata_history.csv; coordinate variation does not establish physical relocation. Join historical metadata by station_id, not name, and never overwrite all history with latest names/coordinates.

## 4. Update differences and rainfall extremes

| Year | Update >1 day rows | Reading update >1 day | Negative update delay | Negative reading delay | Maximum finite rain/mm ||
|---|---:|---:|---:|---:|---:|
| 2017 | 17,899 | 17,899 | 0 | 0 | 18.0 |
| 2018 | 84,835 | 84,835 | 0 | 0 | 15.6 |
| 2019 | 145,739 | 145,637 | 0 | 0 | 15.4 |
| 2020 | 185,142 | 185,142 | 0 | 0 | 20.6 |
| 2021 | 12,078 | 12,078 | 0 | 0 | 26.4 |
| 2022 | 68,026 | 68,026 | 0 | 0 | 37.2 |
| 2023 | 179,651 | 179,651 | 0 | 0 | 17.622 |
| 2024 | 63,929 | 63,929 | 0 | 0 | 15.912 |

Update minus event time is a field difference, not proof of first real-time availability. Retain both updates and valid delayed rain. C must disclose retrospective availability assumptions and exclude future update status from features.

See largest_rainfall_examples.csv. Values not divisible by 0.2, long decimals and extremes cannot be removed on appearance alone. An anomaly cutoff requires instrument/accumulation semantics, adjacent-time/station evidence and a documented rule/impact.

## 5. Next A/B/C interfaces

- **A/P0-3**: confirm phase/collision, duplicate/conflict, metadata and validity rules. Write standardized Parquet with source year/original time/quality and raw-to-quarantine/dedup/retained ledgers. Preserve gaps; construct future truth in the next stage.
- **B**: use station-month/metadata tables for coverage plots and comparison policy. Adopt explicit interannual panels/coverage control; compute formal seasonal/hourly/interannual results after cleaned facts are frozen.
- **C**: implement baseline, time partitions and Spark MLlib probability interface. Review training label/station/month support after A windows arrive. Gapped future windows are not dry negatives. Agree train/validation/test interfaces separately; this audit does not lock them automatically.

B/C machine checks were deferred by the user without blocking A full audit. This stage created no cleaned data, targets or models.

## 6. Delivery and reproduction evidence

Result root: `outputs/p0_2_audit/p0_2_v1/`。

- annual_quality.csv / summary.json: full counts and annual quality.
- station_month_coverage.csv: station/month coverage; station_summary.csv / station_full_period_summary.csv: annual/full-period station summaries.
- common_station_month_network_coverage.csv / daily_network_record_counts.csv: common-network monthly coverage and full-calendar daily raw counts, including zero-record dates.
- station_metadata_history.csv: observed metadata combination history.
- timestamp_offsets.csv / daily_timestamp_offsets.csv / cadence_gap_counts.csv / year_boundary_gaps.csv: phases and continuity.
- units_types.csv / update_delay_buckets.csv: units, types and update-field differences.
- anomaly_examples.csv / duplicate_key_examples.csv / cadence_gap_examples.csv / largest_rainfall_examples.csv: diagnostic examples.
- Annual audit.json: metrics, small tables and ten reconciliation checks; run_status.json: resources, code/input signatures and process exit.
- outputs/p0_2_audit/self_test_v1/self_test.json: synthetic anomaly checks.

Program: src/01_audit_raw.py; lifecycle: scripts/run_spark.py and spark_session.py. See [p0_2_methodology.md](p0_2_methodology.md) for policy/reproduction.
