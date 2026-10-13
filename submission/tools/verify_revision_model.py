"""Cold-source/model load and past-only arbitrary-threshold fixture verification."""
import argparse,sys,json
from pathlib import Path
from decimal import Decimal
ap=argparse.ArgumentParser();ap.add_argument('--project-root',required=True,type=Path);ap.add_argument('--artifact',required=True,type=Path);ap.add_argument('--receipt',required=True,type=Path);a=ap.parse_args()
root=a.project_root.resolve();sys.path.insert(0,str(root/'C_delivery/src'))
from c_common import spark_start,load,dump,KEYS
from C_predict import RainfallPredictor
from pyspark.sql import functions as F
cfg=load(a.artifact/'experiment_config.json');cfg['spark']['driver_memory']='2g';spark=spark_start(cfg)
try:
    predictor=RainfallPredictor(spark,a.artifact)
    assert predictor.distribution is not None
    model=predictor.distribution.model
    if model is not None:
        candidate=predictor.contract['selected_model']
        expected_leaf=cfg.get('candidate_overrides',{}).get(candidate,{}).get('minInstancesPerNode',cfg['tree_cdf']['minInstancesPerNode'])
        assert model.stages[-1].getMinInstancesPerNode()==expected_leaf
        assert len(predictor.contract['used_numeric_features'])==22 if candidate.startswith('cdf_persistence_d8') else True
    fixture=root/'data/samples/p1_6_v1/expected';target=spark.read.parquet(str(fixture/'targets'))
    row=target.filter(F.col('historical_valid')&F.col('station_id').isin(predictor.contract['station_ids'])).orderBy(*KEYS).first();assert row is not None
    obs=spark.read.parquet(str(fixture/'observations'));past=obs.filter((F.col('station_id')==row.station_id)&F.col('slot_end_epoch_s').between(row.prediction_epoch_s-3300,row.prediction_epoch_s))
    thresholds=sorted([Decimal(str(v)) for v in [0,.25,.5,.75,1,1.5,2,3,3.14159,5]]+[Decimal('0.500000000000000001')])
    results=[predictor.predict(past,row.station_id,row.prediction_epoch_s,h) for h in thresholds]
    assert all(0<=r['probability']<=1 for r in results) and all(x['probability']>=y['probability']-1e-15 for x,y in zip(results,results[1:]))
    precise=next(r for r in results if r['threshold_decimal_mm']=='0.500000000000000001');assert precise is not None
    dump(a.receipt,{'status':'PASS','extracted_source_and_model':True,'logical_artifact_checksums_verified_by_predictor':True,
        'only_historical_fixture_observations_supplied':True,'arbitrary_threshold_3_14159_accepted':True,'exact_18_decimal_query_preserved':True,
        'monotone_probabilities':True,'selected_model':predictor.contract['selected_model'],'minimum_leaf_anchors':model.stages[-1].getMinInstancesPerNode() if model else None,'predictions':results})
    print('PASS: cold revision artifact checksums/load/past-only arbitrary thresholds and exact decimal query')
finally:spark.stop()
