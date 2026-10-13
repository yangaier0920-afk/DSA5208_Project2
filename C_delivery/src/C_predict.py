"""Reload final artifact and predict P(future 30-minute rainfall > threshold)."""
import argparse, math
from pathlib import Path
from c_common import *

class RainfallPredictor:
    def __init__(self, spark, artifact):
        self.spark=spark;self.artifact=Path(artifact)
        self.contract=load(self.artifact/'prediction_contract.json')
        self.model=None
        if self.contract['selected_model']!='frequency_baseline':
            from pyspark.ml import PipelineModel
            self.model=PipelineModel.load(str(self.artifact/'spark_model'))

    def predict(self, observations, station_id, prediction_epoch_s, threshold_mm):
        from pyspark.sql import functions as F
        from pyspark.ml.functions import vector_to_array
        if station_id not in self.contract['station_ids']:
            raise ValueError('Station was not observed in the final sampled training data')
        if isinstance(threshold_mm,bool) or not isinstance(threshold_mm,(float,int)) or not math.isfinite(threshold_mm):
            raise ValueError('Threshold must be a finite numeric value')
        threshold=float(threshold_mm)
        if threshold not in self.contract['supported_thresholds_mm']:
            raise ValueError('Threshold outside frozen evaluated support; supported: '+str(self.contract['supported_thresholds_mm']))
        # Pure historical feature API; no targets, eligibility or future columns.
        features=features_at(observations,station_id,prediction_epoch_s).select(*KEYS,*NUMERIC_FEATURES)
        if self.model is None:
            result=features.count();assert result==1
            p=next(r['p'] for r in self.contract['baseline_probabilities'] if r['threshold_mm']==threshold)
        else:
            p=self.model.transform(features.withColumn('threshold_mm',F.lit(threshold))).select(
                vector_to_array('probability')[1].alias('p')).first()['p']
        assert math.isfinite(p) and 0<=p<=1
        return {'station_id':station_id,'prediction_epoch_s':int(prediction_epoch_s),
                'threshold_mm':threshold,'probability':p,'event':'future_30m_mm > threshold_mm',
                'information_availability':'retrospective event-time assumption; no live availability guarantee'}

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--artifact',required=True,type=Path)
    ap.add_argument('--observations',required=True,type=Path)
    ap.add_argument('--station',required=True);ap.add_argument('--epoch',required=True,type=int)
    ap.add_argument('--threshold',required=True,type=float);a=ap.parse_args()
    spark=spark_start(load(CONFIG))
    try:
        frame=spark.read.parquet(str(a.observations))
        print(json.dumps(RainfallPredictor(spark,a.artifact).predict(frame,a.station,a.epoch,a.threshold),indent=2))
    finally:spark.stop()

if __name__=='__main__':main()
