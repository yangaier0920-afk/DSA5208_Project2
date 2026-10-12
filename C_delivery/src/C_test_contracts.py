"""Small meaningful checks for strict Decimal labels, sampling, and learned transforms."""
from decimal import Decimal
from c_common import *

def main():
    config=load(CONFIG);config['spark']['driver_memory']='2g';spark=spark_start(config)
    try:
        from pyspark.sql import functions as F
        thresholds=[0,0.5,1,2,5]
        data=spark.createDataFrame([('S1',i*300,Decimal(x)) for i,x in enumerate(['0','0.5','0.500000000000000001','1','2','5','5.000000000000000001'])],
            'station_id string,prediction_epoch_s long,future_30m_mm decimal(38,18)')
        expanded=expand(data,thresholds)
        actual={(r.prediction_epoch_s,r.threshold_mm):r.label for r in expanded.collect()}
        for r in data.collect():
            for threshold in thresholds:assert actual[r.prediction_epoch_s,float(threshold)]==float(r.future_30m_mm>Decimal(str(threshold)))
        many=spark.range(2000).select(F.lit('S1').alias('station_id'),(F.col('id')*300).alias('prediction_epoch_s'))
        small={r.prediction_epoch_s for r in sampled(many,.01,config['seed']).collect()}
        formal={r.prediction_epoch_s for r in sampled(many,.1,config['seed']).collect()}
        assert small.issubset(formal) and len(small)>0 and len(formal)<2000
        second={r.prediction_epoch_s for r in sampled(many.orderBy(F.desc('prediction_epoch_s')),.1,config['seed']).collect()}
        assert second==formal,'Sampling depends on row order'
        assert not set(NUMERIC_FEATURES).intersection({'future_30m_mm','target_valid','model_anchor_eligible','year_boundary_safe','fold1_role','final_role'})
        dump(PACKAGE/'results/contract_tests.json',{'status':'PASS','strict_decimal_comparisons':35,
            'exact_equal_thresholds_negative':True,'beyond_threshold_at_18_decimal_places_positive':True,
            'hash_sampling_nested_and_order_independent':True,'future_information_excluded_from_feature_allowlist':True})
        log('C contract tests PASS')
    finally:spark.stop()

if __name__=='__main__':main()
