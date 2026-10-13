# Default numeric feature definitions (B_v2)

R(t) is rainfall over (t-5min,t] under the project L1/D1 assumption, not independently verified official semantics. Original event_ts is retained; prediction keys use aligned slot_end_epoch_s. Exact endpoints are matched by epoch seconds and missing positions are never zero-filled.

The following 26 rows follow B_feature_api.NUMERIC_FEATURES order. Station ID and numeric threshold are additional C inputs.

| Name | Definition/formula | Unit | Exact historical endpoints | Includes current slot | Missing/special values |
|---|---|---|---|---|---|
| current_5m_mm | Rainfall in the last completed five-minute interval | mm | Endpoint t; interval (t-5min,t] | yes | An observed valid current anchor is required. |
| lag_5m_mm | Rainfall at the exact endpoint t-5min | mm | Endpoint t-5min and its preceding five-minute interval | no | Null if the exact endpoint is absent; do not substitute the preceding row. |
| rain_sum_15m_mm | Sum over the complete past 15-minute interval | mm | Exact endpoints through t; interval (t-15min,t] | yes | All 3 exact endpoints are required; otherwise null, never a partial sum. |
| rain_sum_30m_mm | Sum over the complete past 30-minute interval | mm | Exact endpoints through t; interval (t-30min,t] | yes | All 6 exact endpoints are required; otherwise null, never a partial sum. |
| rain_sum_60m_mm | Sum over the complete past 60-minute interval | mm | Exact endpoints through t; interval (t-60min,t] | yes | All 12 exact endpoints are required; otherwise null, never a partial sum. |
| rain_sum_previous_15m_mm | Sum over the complete previous 15-minute interval | mm | t-25min,t-20min,t-15min; interval (t-30min,t-15min] | no | All 3 exact endpoints are required; otherwise null, never a partial sum. |
| rain_change_15m_mm | Past 15-minute sum minus the preceding 15-minute sum; not physical acceleration | mm | Two adjacent 15-minute windows through t | yes | Null if either operand is missing. |
| rain_max_60m_mm | Maximum five-minute rainfall in the complete past hour | mm | 12 exact endpoints t-55min,...,t | yes | Null unless the required exact historical slots exist; never replace missing rainfall with zero. |
| rain_std_60m_mm | Population standard deviation of the 12 past-hour rainfall values (divide by 12) | mm | 12 exact endpoints t-55min,...,t | yes | Null unless the required exact historical slots exist; never replace missing rainfall with zero. |
| wet_slots_60m | Number of positive-rainfall slots in the complete past hour, from 0 to 12 | slots | 12 exact endpoints t-55min,...,t | yes | Null unless the required exact historical slots exist; never replace missing rainfall with zero. |
| hour_sin | sin(2*pi*h/24); h is Singapore hour + minute/60, m is month 1-12 | dimensionless | Singapore calendar at prediction time t; no rainfall lookup | not applicable (time only) | Computed for a valid anchor time; independent of missing rainfall history. |
| hour_cos | cos(2*pi*h/24); h is Singapore hour + minute/60, m is month 1-12 | dimensionless | Singapore calendar at prediction time t; no rainfall lookup | not applicable (time only) | Computed for a valid anchor time; independent of missing rainfall history. |
| month_sin | sin(2*pi*(m-1)/12); h is Singapore hour + minute/60, m is month 1-12 | dimensionless | Singapore calendar at prediction time t; no rainfall lookup | not applicable (time only) | Computed for a valid anchor time; independent of missing rainfall history. |
| month_cos | cos(2*pi*(m-1)/12); h is Singapore hour + minute/60, m is month 1-12 | dimensionless | Singapore calendar at prediction time t; no rainfall lookup | not applicable (time only) | Computed for a valid anchor time; independent of missing rainfall history. |
| lag_10m_mm | Rainfall at the exact endpoint t-10min | mm | Endpoint t-10min and its preceding five-minute interval | no | Null if the exact endpoint is absent; do not substitute the preceding row. |
| lag_15m_mm | Rainfall at the exact endpoint t-15min | mm | Endpoint t-15min and its preceding five-minute interval | no | Null if the exact endpoint is absent; do not substitute the preceding row. |
| lag_30m_mm | Rainfall at the exact endpoint t-30min | mm | Endpoint t-30min and its preceding five-minute interval | no | Null if the exact endpoint is absent; do not substitute the preceding row. |
| wet_current | 1 if current_5m_mm>0, otherwise 0 | 0/1 | Endpoint t | yes | Null for a missing lag; genuine observed zero gives 0. |
| wet_lag_5m | 1 if lag_5m_mm>0, otherwise 0 | 0/1 | Exact endpoint t-5min | no | Null for a missing lag; genuine observed zero gives 0. |
| wet_lag_10m | 1 if lag_10m_mm>0, otherwise 0 | 0/1 | Exact endpoint t-10min | no | Null for a missing lag; genuine observed zero gives 0. |
| consecutive_wet_slots_60m | Trailing positive-rainfall slot count from t backwards; 0 when currently dry; capped at 12 | slots | t-55min,...,t | yes | Null for incomplete history; all 12 wet gives 12 with the left-censor flag; this does not establish continuous rain within each slot. |
| wet_run_left_censored_60m | 1 when all 12 slots are wet, indicating the wet run may have started before the observed hour; otherwise 0 | 0/1 | t-55min,...,t | yes | Null unless the required exact historical slots exist; never replace missing rainfall with zero. |
| minutes_since_last_rain_60m_capped | Minutes from t to the latest observed wet-slot endpoint; use code 60 when none is observed | minutes/censoring code | t-55min,...,t; observed ages 0,5,...,55 | yes | Null for incomplete history; complete but all dry gives unknown-gap code 60, paired with no_rain_observed_60m. |
| no_rain_observed_60m | 1 when all 12 observed slots are dry; 0 when at least one is wet | 0/1 | t-55min,...,t | yes | Null for incomplete history; 1 does not imply there has never been rainfall. |
| rain_change_5m_mm | current_5m_mm minus lag_5m_mm | mm | Exact endpoints t-5min and t | yes | Null if either operand is missing. |
| rain_ratio_15m_30m | Past 15-minute rainfall sum divided by past 30-minute sum | dimensionless, 0-1 | t-25min,...,t | yes | All 6 slots required; divide when the denominator is positive; complete all-dry gives 0; missing slots give null. |

## Audit columns outside the default model inputs

n_valid_15m/n_valid_30m/n_valid_60m/n_valid_previous_15m count valid historical positions; history_complete_60m tests all 12. The uncapped minutes_since_last_rain_60m is null when no rain is seen in the hour and is not a default model feature. Prediction timestamps/year are keys or partition fields, not wholesale numeric model inputs. Twelve wet slots do not establish the true storm start or uninterrupted within-slot rainfall. Inference requires complete history and does not use future-label validity.
