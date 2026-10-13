# P0-4 continuity and future-total acceptance report

PASS. A generated targets without fitting models, expanding thresholds or computing B features. All 48,538,627 observation anchors remain. Future complete 47,636,833; unknown 901,794; history complete 47,347,833; both complete 47,048,872; final-role joint eligibility 47,048,494. These eligibility counts overlap and are not final sampled training counts.

Future endpoints are t+300 through t+1800, history t−3300 through t, checked exactly with adjacent-year context. Accumulation uses Decimal(38,18). Unknown truth stays null, true dry truth is zero, equality to a threshold is negative. Only actual E2 block boundaries are purged; internal year crossings remain eligible. 2024 is UNUSED in development roles. EXCLUDED_BOUNDARY retains truth for audit.

All 30 synthetic window/Decimal/E2/Parquet checks, 15 annual checks, 19 root checks and 43 delivery checks passed with normal Java exit. Independent manual reconstruction covered 29 real development-period cases and 522 expected slots: wet/dry, missing future/history, date/month/year crossings and the 2017 phase transition. Cases use 2017–2020, not test labels. Exact target/observation key matching passed. All 96 input observation Parquet hashes were checked before processing and size/mtime remained unchanged.

At this historical stage, 2024 was inspected for completeness, boundaries and structure only; no threshold-specific test distribution or model scores guided selection. B/C subsequently implement features, supported thresholds, sampling, training and evaluation. Future truth/completeness must never enter prediction features or prior request eligibility. See p0_4_data_contract.md and annual evidence tables.
