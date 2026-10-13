# P0-3 standardized observation contract

Status at this historical stage: cleaning complete, E2 confirmed, targets and models not yet generated. Formal observations are in data/processed/p0_3_v1/observations/, partitioned by aligned endpoint year/month. Quality evidence is in outputs/p0_3_clean/p0_3_v1/.

Primary key is station_id + slot_end_epoch_s. Timezone is Asia/Singapore, epoch unit seconds, rainfall unit mm. Retain source year, all original 12 text columns, original event_ts, raw line/file SHA-256 and audit fields. For source year 2017 only, epoch mod 300=299 is shifted +1 second. slot_end_ts is the project-assumed interval endpoint; rain_period_start_ts=slot_end_ts−300 seconds supplies Task 1 calendar. A midnight endpoint belongs to the previous date, including a 2018-01-01 endpoint contribution to 2017-12-31. This interval meaning is an explicit assumption, not official proof.

Keep finite nonnegative values including true zeros and valid extremes. Do not round to 0.2 mm or insert zero observations at gaps. Exact rainfall uses Decimal(38,18); unsupported precision stops processing instead of silently rounding. The observed source effective precision is at most five decimals.

Aligned equal-rain collisions retain the originally standard-grid row deterministically without summing or averaging. Unequal-rain collisions quarantine the whole group. collision_lineage records both retained and merged sides; station_metadata_history retains pre-merge source combinations. Name/device/coordinate warnings do not invalidate otherwise valid rainfall. S113 remains present; metadata first/last appearances do not establish continuous validity periods and latest coordinates must not overwrite history.

| Output | Rows | Use |
|---|---:|---|
| observations | 48,538,627 | Task 1 and past features |
| quarantine | 0, readable schema retained | Invalid/conflicting source rows |
| collision_lineage | 172 | 86 retained/86 merged provenance |

Ledger: 48,538,713 raw = 48,538,627 observations + 86 merged + 0 quarantine. Both endpoint-calendar and interval-calendar coverage are exported. Cross-month/year contributions are combined before coverage comparison. Missingness remains unknown. Task 1 uses 38 common candidates with same-month intersections at 90%, plus 95% sensitivity. This panel does not filter the global observation table or Task 2.

```python
from data_release import read_tables
observations = read_tables(spark, root, scope='full')['observations']
```

Historical processing entry points are src/02_clean_standardize.py, scripts/run_spark.py, scripts/verify_p0_3_root.py and scripts/validate_p0_3_outputs.py; consult their --help for workspace and evidence arguments. Windows uses .venv\Scripts\python.exe; macOS uses .venv/bin/python with Java 17. Run synthetic tests before full processing and re-read each annual Parquet plus merged roots. Use new output/evidence paths: input/code signatures and existing receipts intentionally block silent overwrite. Stage P0-4 subsequently constructs exact historical/future windows and E2 roles. Complete field definitions are in field_dictionary.md.
