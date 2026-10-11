"""Check the prediction-time adapter against saved features, without future rows."""
import argparse, math
from pathlib import Path
from b_common import PACKAGE,dump,release,start
from B_build_features import epoch
from B_feature_api import NUMERIC_FEATURES,features_at

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True)
    p.add_argument('--features',type=Path,default=PACKAGE/'results/features_v2');a=p.parse_args()
    d=release(a.root);spark=start(a.root,'B prediction adapter validation')
    try:
        from pyspark.sql import functions as F
        obs=d.read_tables(spark,a.root,'full',verify=False)['observations']
        saved=spark.read.parquet(str(a.features/'parquet'))
        # Intentionally provide no observations after t to the inference adapter.
        station='S24';t=epoch(2019,5,15,14,30)
        past_only=obs.filter((F.col('station_id')==station)&F.col('slot_end_epoch_s').between(t-3300,t))
        predicted=features_at(past_only,station,t).first().asDict()
        expected=saved.filter((F.col('year')==2019)&(F.col('station_id')==station)&(F.col('prediction_epoch_s')==t)).first().asDict()
        for column in NUMERIC_FEATURES:
            assert predicted[column] is not None and expected[column] is not None,column
            assert math.isclose(predicted[column],expected[column],rel_tol=1e-10,abs_tol=1e-8),column
        try:features_at(past_only,station,t+1)
        except ValueError:pass
        else:raise AssertionError('Off-grid prediction was accepted')
        try:features_at(past_only.filter(F.col('slot_end_epoch_s')!=t-300),station,t)
        except ValueError:pass
        else:raise AssertionError('Missing history was accepted')
        dump(a.features/'inference_adapter_validation.json',{'status':'PASS','station_id':station,'prediction_epoch_s':t,
            'compared_features':NUMERIC_FEATURES,'future_rows_supplied':False,'missing_history_rejected':True,'off_grid_rejected':True})
        print('Prediction-time adapter PASS; all 26 inputs match offline features without any future rows')
    finally:spark.stop()

if __name__=='__main__':main()
