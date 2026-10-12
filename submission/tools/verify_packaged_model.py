"""Load the deployed model from an extracted source archive and use historical fixtures."""
import argparse,sys,json
from pathlib import Path
ap=argparse.ArgumentParser();ap.add_argument('--project-root',required=True,type=Path);ap.add_argument('--receipt',required=True,type=Path);args=ap.parse_args()
root=args.project_root.resolve();sys.path.insert(0,str(root/'C_delivery/src'))
from c_common import spark_start,load,CONFIG,dump
from C_predict import RainfallPredictor
from pyspark.sql import functions as F
spark=spark_start(load(CONFIG))
try:
 artifact=root/'C_delivery/results/C_v1_20261012/trained_model'
 predictor=RainfallPredictor(spark,artifact)
 fixture=root/'data/samples/p1_6_v1/expected'
 targets=spark.read.parquet(str(fixture/'targets'))
 row=targets.filter(F.col('historical_valid')&F.col('station_id').isin(predictor.contract['station_ids'])).orderBy('station_id','prediction_epoch_s').first()
 assert row is not None
 observations=spark.read.parquet(str(fixture/'observations'))
 past=observations.filter((F.col('station_id')==row.station_id)&F.col('slot_end_epoch_s').between(row.prediction_epoch_s-3300,row.prediction_epoch_s))
 answers=[predictor.predict(past,row.station_id,row.prediction_epoch_s,t) for t in predictor.contract['supported_thresholds_mm']]
 assert all(0<=a['probability']<=1 for a in answers)
 dump(args.receipt,{'status':'PASS','model_loaded_from_extracted_source_archive':True,'past_only_fixture_predictions':answers})
 print('PASS: extracted model loaded; five past-only threshold predictions')
finally:spark.stop()
