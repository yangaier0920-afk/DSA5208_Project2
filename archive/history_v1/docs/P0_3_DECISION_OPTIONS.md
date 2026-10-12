# P0-3 and downstream decision options

The confirmed combination is T2/C1/L1/D1/M1/H1/R1/S1/B1/K1/A1/E2/P1/V1/I1. These are project choices, not additional course requirements. Counts below are audit evidence or prospective estimates before cleaning.

| Decision | Confirmed option | Alternatives and implications |
|---|---|---|
| T: 2017 time | T2: add one second only to epoch remainder 299 | T1 preserves native phases and requires exact 300-second continuity; T3 models only the standard phase, retaining other data for descriptive analysis. T2 affects 1,775,327 source rows and 6,191 date changes. |
| C: aligned collision | C1: equal rain retains the originally standard-grid row; unequal rain quarantines the whole group | 86 pairs (43 stations, 2017-04-25 11:20/11:25), 172 source rows, equal rain including 0.2 and 1.4 mm. S113 has two metadata conflicts. C1 yields 48,538,627 observations. C2 would quarantine all 172, yielding 48,538,541; C3 merges 84 and quarantines four, yielding 48,538,625. Never sum duplicate rain. |
| L: interval semantics | L1: endpoint t, future endpoints t+5,...,t+30 minutes | L2 treats timestamps as starts, changing future to t,...,t+25 and excluding an unfinished current observation from history. Official semantics are not independently proven. |
| D: calendar | D1: interval-start date/hour | D2 uses timestamp calendar and changes midnight attribution. |
| M: missingness | M1: sparse observations, gaps remain unknown | M2 creates a null grid for all 91 stations, about 76,579,776 rows over eight years; this does not establish true station operating lifetimes. Never fill missing rain with zero. |
| H: history | H1: all 12 exact slots in the last 60 minutes | H2 requires six 30-minute slots and drops 60-minute inputs. H3 permits partial sums with counts and training-only imputation; future truth must still be complete. |
| R: values | R1: finite nonnegative observations, true zeros and extremes retained, no 0.2-mm rounding | R2 retains original truth while optionally transforming or winsorizing training features; this requires a separately documented rule. |
| S: metadata | S1: keep station ID including S113 and historical metadata | S2 excludes S113 from the common Task 1 set, reducing 38 to 37; never backfill future coordinates. |
| B: comparisons | B1: 38 common candidates, qualified same-month intersection across years | B2 describes all qualified stations with changing composition. B3 keeps 38 with low-coverage flags and sensitivity checks. Do not call incomplete sums full annual rain. |
| K: monthly coverage | K1: 90% main, 95% sensitivity | K2 uses 95%; K3 uses 80%. Original candidate support was 2,825/3,648, 2,654/3,648 and 3,102/3,648 respectively. Denominator is the full calendar, not first-to-last observed time. |
| A: availability | A1: retrospective event-time availability assumption | A2 requires reading_update<=t as a proxy, which cannot establish first publication time. A3 uses calendar/station/threshold only and excludes rainfall history. |
| E: evaluation | E2: three expanding folds, final refit and 2024 test | E1 trains 2017–2022, validates 2023, tests 2024. E2 averages threshold/year Brier; no test-driven choice. |
| P: scale | P1: 1% pilot, 10% train, full validation/test | P2 uses 20% training; P3 full training. Sampling must be deterministic, anchor-first and independent of outcome. |
| V: threshold | V1: numeric threshold shared model; candidates 0/0.5/1/2/5 mm | V2 fits separate threshold-specific models. Freeze support from training only. |
| I: inference | I1: reject unknown stations, unsupported thresholds, off-grid times or incomplete history | A permissive I2 interface needs explicitly trained and separately validated fallback handling. Future completeness never gates an inference request. |

A begins stage 03 after full stage 02 passes; B/C interfaces use station_id plus epoch, one-to-one joins before threshold expansion. Persist the confirmed configuration, original input identities and decision receipt. Further changes require a documented new experiment or release rather than silent edits to frozen data rules.
