# P0-3 cleaning acceptance report

PASS. P0-2 passed 32 delivery checks before eight annual Spark cleaning runs and annual/root Parquet readback. E2 was confirmed; labels and models were not created at this stage.

48,538,713 raw = 48,538,627 observations + 86 merged duplicates + 0 quarantined. Empty quarantine retains its schema. The 172 lineage rows contain both sides of 86 collisions and are not counted as another loss.

T2 shifted 1,775,327 raw 2017 offset rows +1 second, with 6,191 raw date changes; post-merge shifted rows are 1,775,241. C1 retained originally standard-grid rows with equal rainfall and flagged S113's two metadata conflicts. Finite nonnegative values, zeros and extremes remain; sparse gaps are not zero-filled. Decimal(38,18) preserves source effective precision up to five decimals. Endpoint interpretation and interval-start calendar remain project assumptions.

All 21 synthetic/Parquet checks, nine annual readback checks, 11 root checks and 32 delivery checks passed, with normal Java exit. Eight raw file SHA-256 values were rechecked before processing; size/mtime were unchanged afterwards. Raw CSV was not modified; output hashes were recorded separately.

| Supported same-month panel | 90% stations | 95% stations |
|---|---:|---:|
| May | 11 | 9 |
| September | 20 | 17 |
| October | 21 | 18 |
| December | 18 | 15 |

The common candidate set remains 38. Other months have empty qualified eight-year intersections and cannot be forced into comparable panels. Do not sum the four supported months into an annual total. Interval-calendar coverage includes contributions across source-year boundaries; missing 2025 context remains a dataset-edge limitation. Coverage filters apply to Task 1 comparisons, not all observations or Task 2. See p0_3_data_contract.md and annual evidence for quantities and reproduction.
