"""Package final model and verify persisted probabilities in a fresh Spark process."""
import argparse, shutil
from c_common import *

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',required=True,type=Path)
    ap.add_argument('--root',required=True,type=Path);a=ap.parse_args()
    selection=load(a.run/'selection.json');final=load(a.run/'final_test.json');assert final['status']=='PASS'
    artifact=a.run/'trained_model';artifact.mkdir(exist_ok=False)
    winner=selection['selected_model'];thresholds=selection['thresholds']
    contract={'selected_model':winner,'station_ids':load(a.run/'final/status.json')['station_ids'],
        'supported_thresholds_mm':thresholds,'numeric_features':NUMERIC_FEATURES,
        'threshold_as_numeric_input':True,'history_offsets_s':list(range(-3300,1,300)),
        'label_rule':'future_30m_mm > threshold_mm','timezone':'Asia/Singapore',
        'time_contract':'interval ends at t; current interval (t-300,t] assumed available; future six ends t+300..t+1800',
        'inference_scope':'supported training station, 5-minute grid, all 12 history endpoints, frozen discrete thresholds',
        'baseline_probabilities':load(a.run/'final/baseline.json'),'experiment_config_hash':sha256(CONFIG),
        'selection_sha256':sha256(a.run/'selection.json'),**provenance(a.root)}
    dump(artifact/'prediction_contract.json',contract)
    shutil.copy2(a.run/'experiment_config.json',artifact/'experiment_config.json')
    if winner!='frequency_baseline':shutil.copytree(a.run/'final/models'/winner,artifact/'spark_model')
    spark=spark_start(load(CONFIG))
    try:
        from pyspark.sql import functions as F
        from pyspark.ml.functions import vector_to_array
        from pyspark.ml import PipelineModel
        from data_release import load_release
        from C_predict import RainfallPredictor
        inputs=spark.read.parquet(str(a.run/'final/anchors')).filter(F.col('final_role')=='TEST')
        # 100 deterministic full-history test anchors; compare saved scorer vs reload.
        probes=inputs.orderBy(*KEYS).limit(100).drop('final_role')
        from C_train import score
        reloaded=RainfallPredictor(spark,artifact)
        if reloaded.model is not None:
            assert sorted(reloaded.model.stages[0].labels)==sorted(contract['station_ids'])
            classifier=reloaded.model.stages[-1]
            attrs=reloaded.model.transform(probes.withColumn('threshold_mm',F.lit(thresholds[0]))).schema['features'].metadata['ml_attr']['attrs']
            names={entry['idx']:entry['name'] for group in attrs.values() for entry in group}
            values=classifier.coefficients if winner=='logistic_regression' else classifier.featureImportances
            csv_write(a.run/'final/model_feature_weights.csv',[{'feature':names.get(i,str(i)),
                'weight':float(value),'kind':'coefficient' if winner=='logistic_regression' else 'tree_impurity_importance'}
                for i,value in enumerate(values)])
            if winner=='decision_tree':(a.run/'final/tree_rules.txt').write_text(classifier.toDebugString)
        new=score(winner,reloaded.model,probes,thresholds,contract['baseline_probabilities'])
        saved=spark.read.parquet(str(a.run/'final/predictions'/winner))
        comparison=new.alias('n').join(saved.alias('s'),KEYS+['threshold_mm'])
        stats=comparison.agg(F.count('*').alias('rows'),F.max(F.abs(F.col('n.p')-F.col('s.p'))).alias('max_abs_error')).first().asDict()
        assert stats['rows']==100*len(thresholds) and stats['max_abs_error']<=1e-12,stats
        row=probes.orderBy(*KEYS).first();station=row['station_id'];t=row['prediction_epoch_s']
        manifest=load_release(a.root)
        obs=spark.read.parquet(str(a.root/manifest['tables']['observations']['path']))
        past=obs.filter((F.col('station_id')==station)&F.col('slot_end_epoch_s').between(t-3300,t))
        answers=[]
        for threshold in thresholds:
            answer=reloaded.predict(past,station,t,threshold)
            expected=new.filter((F.col('station_id')==station)&(F.col('prediction_epoch_s')==t)&(F.col('threshold_mm')==threshold)).first()['p']
            assert abs(answer['probability']-expected)<=1e-10
            answers.append(answer)
        rejected={}
        for key,args in [('unknown_station',('__UNSEEN_STATION__',t,thresholds[0])),
                         ('unsupported_threshold',(station,t,3.14159)),('off_grid_time',(station,t+1,thresholds[0]))]:
            try:reloaded.predict(past,*args)
            except ValueError:rejected[key]=True
            else:raise AssertionError(key+' accepted')
        try:reloaded.predict(past.filter(F.col('slot_end_epoch_s')!=t-300),station,t,thresholds[0])
        except ValueError:rejected['missing_history']=True
        else:raise AssertionError('missing history accepted')
        dump(a.run/'model_reload_validation.json',{'status':'PASS','fresh_process':True,'probe_anchor_count':100,
             'compared_threshold_probabilities':stats['rows'],'max_abs_probability_error':stats['max_abs_error'],
             'prediction_api_uses_past_only':True,'example_predictions':answers,'invalid_input_rejections':rejected})
        dump(artifact/'artifact_manifest.json',{'status':'PASS',**provenance(a.root),
            'files':[{'path':p.relative_to(artifact).as_posix(),'bytes':p.stat().st_size,'sha256':sha256(p)}
                     for p in sorted(artifact.rglob('*')) if p.is_file() and p.name!='artifact_manifest.json']})
        log('Final artifact reload and past-only prediction API PASS')
    finally:spark.stop()

if __name__=='__main__':main()
