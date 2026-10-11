"""Small fixtures exercise missing slots, censoring, exact lags and year context."""
import argparse,json
from pathlib import Path
from b_common import PACKAGE,dump,start
from B_build_features import epoch,make_features

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True)
    p.add_argument('--output',type=Path,default=PACKAGE/'results/features_v2');a=p.parse_args()
    spark=start(a.root,'B v2 persistence edge cases')
    try:
        from pyspark.sql import functions as F
        t=epoch(2020,1,1,0,15)
        cases={'all_wet':[1.0]*12,'all_dry':[0.0]*12,'last_three':[0.0]*9+[1.0]*3,
               'stopped':[1.0]*10+[0.0]*2,'alternating':[float(i%2) for i in range(12)],
               'missing_lag10':[1.0]*12}
        source=[]
        for station,values in cases.items():
            for i,v in enumerate(values):
                ts=t-3300+i*300
                if station=='missing_lag10' and ts==t-600:continue
                source.append((station,ts,v))
        obs=spark.createDataFrame(source,'station_id string, slot_end_epoch_s long, rain_mm double')
        obs=obs.withColumn('slot_end_ts',F.timestamp_seconds('slot_end_epoch_s')).withColumn('year',F.year('slot_end_ts'))
        found={r['station_id']:r.asDict() for r in make_features(obs,2020).filter(F.col('prediction_epoch_s')==t).collect()}
        assert found['all_wet']['consecutive_wet_slots_60m']==12
        assert found['all_wet']['wet_run_left_censored_60m']==1
        assert found['all_wet']['minutes_since_last_rain_60m']==0
        assert found['all_wet']['rain_ratio_15m_30m']==.5
        assert found['all_dry']['consecutive_wet_slots_60m']==0
        assert found['all_dry']['minutes_since_last_rain_60m'] is None
        assert found['all_dry']['minutes_since_last_rain_60m_capped']==60
        assert found['all_dry']['no_rain_observed_60m']==1
        assert found['all_dry']['rain_ratio_15m_30m']==0
        assert found['last_three']['consecutive_wet_slots_60m']==3
        assert found['last_three']['rain_ratio_15m_30m']==1
        assert found['stopped']['consecutive_wet_slots_60m']==0
        assert found['stopped']['minutes_since_last_rain_60m']==10
        assert found['alternating']['consecutive_wet_slots_60m']==1
        missing=found['missing_lag10']
        assert missing['n_valid_60m']==11 and not missing['history_complete_60m']
        assert missing['lag_10m_mm'] is None and missing['wet_lag_10m'] is None
        assert missing['consecutive_wet_slots_60m'] is None
        assert missing['minutes_since_last_rain_60m_capped'] is None
        assert missing['lag_30m_mm']==1  # exact offset is present despite a later missing slot
        assert all(found[s]['n_valid_60m']==12 for s in cases if s!='missing_lag10')
        dump(a.output/'synthetic_edge_validation.json',{'status':'PASS','cases':list(cases),
            'cross_year_context':True,'missing_slot_not_replaced_by_preceding_row':True,
            'right_and_left_censor_semantics':True})
        print('PASS: six edge cases, including cross-year history, gaps and censoring')
    finally:spark.stop()

if __name__=='__main__':main()
