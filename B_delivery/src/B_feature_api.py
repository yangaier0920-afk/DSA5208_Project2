"""Reusable prediction-time feature adapter for C (no target table needed)."""
from datetime import datetime
from B_build_features import SGT, make_features

# C fits station encoding and any scaling separately inside each training fold.
NUMERIC_FEATURES = [
    'current_5m_mm', 'lag_5m_mm', 'rain_sum_15m_mm', 'rain_sum_30m_mm',
    'rain_sum_60m_mm', 'rain_sum_previous_15m_mm', 'rain_change_15m_mm',
    'rain_max_60m_mm', 'rain_std_60m_mm', 'wet_slots_60m',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    'lag_10m_mm', 'lag_15m_mm', 'lag_30m_mm',
    'wet_current', 'wet_lag_5m', 'wet_lag_10m',
    'consecutive_wet_slots_60m', 'wet_run_left_censored_60m',
    'minutes_since_last_rain_60m_capped', 'no_rain_observed_60m',
    'rain_change_5m_mm', 'rain_ratio_15m_30m'
]
KEY_COLUMNS = ['station_id', 'prediction_epoch_s']
HISTORY_AUDIT_COLUMNS = ['n_valid_15m','n_valid_30m','n_valid_60m',
    'n_valid_previous_15m','history_complete_60m']

def features_at(observations, station_id, prediction_epoch_s):
    """Build one anchor using only history <=t. C checks learned station support.

    Requires the published observation schema, including partition column year.
    Does not require future observations, labels, role fields or thresholds.
    """
    from pyspark.sql import functions as F
    t=int(prediction_epoch_s)
    if t!=prediction_epoch_s or t%300:
        raise ValueError('Prediction epoch must be integer seconds on the 5-minute grid')
    year=datetime.fromtimestamp(t,SGT).year
    history=observations.filter((F.col('station_id')==station_id)&F.col('slot_end_epoch_s').between(t-3300,t))
    result=make_features(history,year).filter(F.col('prediction_epoch_s')==t)
    check=result.select('history_complete_60m').collect()
    if len(check)!=1 or not check[0][0]:
        raise ValueError('An observed anchor and all 12 exact historical slots are required')
    return result
