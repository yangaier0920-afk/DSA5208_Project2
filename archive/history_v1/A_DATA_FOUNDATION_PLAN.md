# A work packages and data-foundation plan

Historical planning document. A owns shared data engineering, immutable data contracts, quality evidence and replay/report support. B owns Task 1 and historical features; C owns threshold expansion, Spark MLlib models/evaluation and inference. The shared target is strict next-30-minute rainfall exceedance probability, using 2017–2024 annual exports.

## Initial dataset inspection

| Year | File size (GB, approximate) | Sampled rows | Sample stations |
|---|---:|---:|---:|
| 2017 | 0.881 | 37,508 | 59 |
| 2018 | 0.770 | 37,520 | 56 |
| 2019 | 0.843 | 37,484 | 54 |
| 2020 | 1.049 | 37,931 | 80 |
| 2021 | 1.159 | 38,232 | 76 |
| 2022 | 1.173 | 38,263 | 72 |
| 2023 | 1.107 | 38,274 | 73 |
| 2024 | 1.066 | 37,792 | 70 |

These preliminary samples do not establish full-data rates. Rainfall examples include 9.60001, 5.628 and 13.869 mm; decimal appearance is not sufficient reason to remove values or round to 0.2. A 2020-01-01 observation was updated on 2020-01-05 17:47:38, motivating an availability audit. S06 naming variants include Paya Lebar with consistent coordinates; S215 includes case variations. S113 coordinates include (103.9104,1.30648) and (103.91074,1.30655), so preserve historical metadata instead of guessing relocation time.

## Work packages

P0-1: pin Python 3.11/Java 17/PySpark 3.5.7, inventory raw inputs and validate a shared sample. P0-2: full Spark structural/numeric/time/key/coverage/metadata audit. P0-3: confirm options, standardize endpoints, resolve collisions deterministically, write observations/quarantine/lineage/metadata with provenance. P0-4: exact future truth, past completeness and temporal role fields. P1-5: freeze release identities and paired interface fixtures. P1-6: representative real raw replay, resource/ledger tables and English report support.

Use partitioned Parquet for formal observations and targets, separate from B's features and C's expanded labels. Store station_id and epoch as shared keys; source year and calendar partition year may differ. Retain raw text, hashes, original event/update times, aligned endpoint/start and quality flags. Keep raw facts, cleaned facts, metadata history, quality/coverage helpers and target supervision distinct. Provide versioned configs/manifests/docs/sample roots with read helpers and per-member receipts.

## Cleaning and coverage rules

Confirm the 2017 phase correction rather than blindly shifting every timestamp. Equal-rain aligned collisions are not additive observations; preserve lineage and isolate conflicting rainfall. Keep finite nonnegative zeros/extremes, exact Decimal precision and sparse unknown gaps. Use complete-calendar denominators, report station lifetime/coverage changes and compare eligible station panels. Name is not a primary key; coordinates/first-last metadata appearances do not prove continuous station validity. Delayed update fields belong to audit, not model predictors.

## Window and temporal interfaces

For endpoint t, require exact future positions t+300,...,t+1800 and past positions t−3300,...,t. Load adjacent-year context before selecting anchor partitions. No next-six-row shortcut, no zero-filled future, and no partial total labelled negative. Equality is not strict exceedance. H1 uses complete 60-minute history. E2 trains through 2020/2021/2022 and validates 2021/2022/2023, then locks and refits 2017–2023 for 2024. Purge only actual partition boundaries. Candidate thresholds and transforms are supported/fitted using training only; anchor sampling precedes threshold expansion. Never place future truth/eligibility/role into features or prediction gates.

## Local Spark execution and collaboration

Process annually with required boundary context, avoid full observation caching, and tune shuffle/input partitions to measured local limits. Original full A defaults were local[4]/4g/48; these are local execution settings, not a cluster or peak memory. Record startup, annual processing, readback, shutdown and separate validator timing. B/C can implement interfaces with fixtures while A audits full data; fixtures are not formal training data. Use new workspace/evidence paths and hash inputs/code before each accepted run.

## Schedule and acceptance

Begin the environment/audit work on 8 October; advance cleaning only after source completeness and decision options are confirmed, then construct truth and handoff. Before the 18 October submission, require count closure, unique keys, schema/readback, exact-window synthetic/manual checks, frozen hashes, member receipts, replay, feature/model nonleakage, saved-model reload and complete source/README/model/PDF with member names, environment, limitations and AI declaration. Original evidence remains in manifests and outputs; current completed A/B/C results are integrated in submission.
