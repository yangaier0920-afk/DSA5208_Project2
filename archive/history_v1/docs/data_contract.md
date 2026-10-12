# Released data contract

Data version v1; observations p0_3_v1; targets p0_4_v1. Original releases rainfall_v1/r2/r3 changed delivery documentation without changing Parquet rules. The immutable original-byte snapshot and checksums are retained separately after English translation.

Formal observations and targets each have 48,538,627 rows. Keys are station_id+slot_end_epoch_s and station_id+prediction_epoch_s respectively. Read using scripts/data_release.py and match both station and epoch; do not join only station/date. Use Asia/Singapore, Unix seconds and rainfall mm. Exact rainfall is Decimal(38,18).

Only 2017 epoch remainder 299 is shifted +1 second. Equal-rain collisions prefer the original standard-grid record, retaining two-sided lineage; unequal-rain groups are quarantined. Valid finite nonnegative rainfall, zeros and extremes are retained. Missingness remains unknown. No future metadata backfill. Metadata first/last appearance is not a continuous validity interval.

The project assumes an interval endpoint t with R(t) available at t. Task 1 calendar uses t−300 seconds. Future truth is the six exact endpoints t+300,...,t+1800; full history is 12 endpoints t−3300,...,t. Load boundary context before filtering years. Unknown future truth stays null; strict exceedance uses >, not >=. Coverage comparison uses the same qualified stations for each supported month, full calendar denominators, main 90% and sensitivity 95%; it does not globally filter Task 2.

E2 development: 2017–2020/2021, 2017–2021/2022, 2017–2022/2023. Lock selection then refit 2017–2023/test 2024. Filter actual role boundaries and joint window eligibility for supervision; do not remove every cross-year anchor. Fit learned encoding/scaling only on training, sample anchors before threshold expansion. Future values, eligibility and role are not features or inference gates.

Record data_version, release_id, config_hash (SHA-256 of exact original project.json bytes) and experiment_config_hash in result metadata. Do not silently replace historical identities. New data/rules require a new data version; new delivery documentation requires a documented revision. Original frozen bytes must pass their original checksums. New translated documents have their own translation inventory. Paired 1,309-row fixtures and 1,297-record/114-anchor replay are interface tests, not representative training data.

See p0_3_data_contract.md, p0_4_data_contract.md, field_dictionary.md and E2_ROLLING_VALIDATION.md for details. Full source, trained model and integrated report are now in submission; historical A-stage status files retain their original scope.
