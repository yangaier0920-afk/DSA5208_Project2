# Prediction time and rainfall interval contract

## Project assumptions and evidence boundary

The frozen project selects L1/D1: an aligned slot_end_ts is the endpoint of a five-minute interval. Original event_ts remains available; only 2017 epochs with remainder 299 modulo 300 are advanced by one second. This interval meaning is a disclosed engineering assumption, not an independently verified official definition.

The anchor is t = prediction_epoch_s = slot_end_epoch_s, in Asia/Singapore. Task 1 groups by rain_period_start_ts = slot_end_ts - 300 seconds. A midnight-ending interval starts at 23:55 on the preceding date.

## Last observation available at t

Under retrospective event-time availability, R(t) over (t-5min,t] is completed and available at t. No input endpoint may exceed t. Twelve exact endpoints t-55min,...,t cover (t-60min,t]. Publication/update delay is not reconstructed as an as-of historical version. If R(t) is unavailable at real prediction time, the cutoff, features and evaluation must be redesigned; the current result is not a demonstrated live-deployment result.

## Future labels

| Slot | Endpoint | Rainfall interval |
|---|---|---|
| 1 | t+5min | (t,t+5min] |
| 2 | t+10min | (t+5min,t+10min] |
| 3 | t+15min | (t+10min,t+15min] |
| 4 | t+20min | (t+15min,t+20min] |
| 5 | t+25min | (t+20min,t+25min] |
| 6 | t+30min | (t+25min,t+30min] |

future_30m_mm = R(t+300)+...+R(t+1800), covering (t,t+30min] and excluding R(t). All six positions are required, otherwise the total is null. Decimal totals are compared strictly with threshold_mm: equality is negative; unknown is not negative.

At 2019-05-15 14:30 SGT, the latest input interval is (14:25,14:30]; the future spans (14:30,14:35] through (14:55,15:00]. Use exact timestamp positions, not the previous 12/next 6 rows, and load cross-year context.

## Implementation status

A's frozen labels and B's offline/past-only features use this endpoint/six-slot contract. Cross-year, missing-slot and all-26-feature checks pass. C's actual post-sample joins, strict labels and saved-model past-only prediction interface now also pass. This verifies implementation consistency; official timestamp meaning and real publication availability remain assumptions to review before live deployment.
