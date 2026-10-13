"""Lock development-only selection, refit through 2023 and verify independent reload."""
import argparse,shutil,time
from pathlib import Path
from zoneinfo import ZoneInfo
from c_common import *
from C_distribution import fit_tree,fit_cdf,score_cdf,DistributionPredictor,validated_threshold,features_for

def select(run):
    config=load(run/'experiment_config.json');names=config['continuous_candidates']+list(config['diagnostic_classifiers']);metrics=[]
    for fold in range(1,4):
        for name in names:
            assert load(run/f'fold{fold}/{name}_complete.json')['status']=='PASS'
            metrics+=load(run/f'fold{fold}/{name}_evaluation.json')['metrics']
    csv_write(run/'development_metrics.csv',metrics);scores=[]
    for name in names:
        years={}
        for year in [2021,2022,2023]:
            rows=[r for r in metrics if r['model']==name and r['year']==year and r['threshold_mm'] in config['primary_thresholds_mm']]
            assert len(rows)==5
            years[str(year)]=sum(r['brier'] for r in rows)/5
        scores.append({'model':name,'year_scores':years,'mean_brier':sum(years.values())/3,'eligible_for_continuous_deployment':name in config['continuous_candidates']})
    best=min(s['mean_brier'] for s in scores if s['eligible_for_continuous_deployment'])
    winner=next(n for n in config['simplicity_order'] if next(s['mean_brier'] for s in scores if s['model']==n)<=best+config['tie_absolute_tolerance'])
    selection={'status':'LOCKED','selected_model':winner,'scores':scores,'experiment_config_hash':sha256(run/'experiment_config.json'),
        'selected_at_utc':datetime.now(timezone.utc).isoformat(),'selection_rule':config['selection'],'test_labels_or_metrics_used':False,
        '2024_policy':'No new scoring; original v1 test already observed and retained','primary_thresholds_mm':config['primary_thresholds_mm'],
        'held_out_thresholds_excluded_from_selection':config['held_out_thresholds_mm']}
    target=run/'selection.json'
    if target.exists():assert load(target)['selected_model']==winner and load(target)['scores']==scores
    else:dump(target,selection)
    log(f'LOCKED continuous candidate {winner}, development Brier '+str(next(s['mean_brier'] for s in scores if s['model']==winner)))
    return selection

def refit(spark,run,source,root):
    from pyspark.sql import functions as F
    from data_release import load_release
    config=load(run/'experiment_config.json');selection=load(run/'selection.json');winner=selection['selected_model'];artifact=run/'trained_model'
    if (run/'refit_status.json').exists():assert load(run/'refit_status.json')['status']=='PASS';return
    if artifact.exists():raise FileExistsError('Incomplete trained_model; inspect failure before a fresh refit')
    artifact.mkdir();begin=time.monotonic()
    # Partition pruning reads the TRAIN partition only; no 2024 targets or prediction scores are used.
    train=spark.read.parquet(str(source/'final/anchors')).filter(F.col('final_role')=='TRAIN')
    assert train.filter(F.col('year')>2023).limit(1).count()==0
    n=train.count();stations=sorted(r.station_id for r in train.select('station_id').distinct().collect())
    kind={'frequency_cdf':'frequency','climatology_cdf':'climatology','persistence_cdf':'persistence'}.get(winner,'leaf');model=None
    if kind=='leaf':model=fit_tree(train,winner,config);model.write().save(str(artifact/'spark_model'))
    cdf=fit_cdf(model.transform(train) if model else train,kind,config['tree_cdf']['leaf_prior_anchors'] if model else (0 if kind=='frequency' else config['baselines']['prior_anchors']))
    dump(artifact/'conditional_cdf.json',cdf);shutil.copyfile(run/'experiment_config.json',artifact/'experiment_config.json')
    contract={'model_family':'conditional_empirical_cdf','selected_model':winner,'station_ids':stations,'numeric_features':NUMERIC_FEATURES,
        'used_numeric_features':features_for(winner)[0] if model else None,
        'uses_station_one_hot':features_for(winner)[1] if model else False,
        'station_selects_history':True,'prediction_time_is_exact_history_anchor':True,
        'history_offsets_s':list(range(-3300,1,300)),'threshold_domain':{'minimum_mm':0,'maximum_mm':None,'numeric_scale':18},
        'thresholds_checked_in_development_mm':sorted(config['primary_thresholds_mm']+config['held_out_thresholds_mm']),
        'threshold_as_numeric_input':False,'threshold_operation':'Strict empirical conditional survival query; global training-prior mixture',
        'label_rule':'future_30m_mm > threshold_mm','timezone':'Asia/Singapore','train_years':[2017,2023],
        'time_contract':'Current interval (t-300,t] assumed available at t; future endpoints t+300..t+1800',
        'evaluation_scope':'Three full development years; no new 2024 test. Nine cuts in 0..5 mm; empirical higher tails unvalidated',
        'experiment_config_hash':sha256(run/'experiment_config.json'),'selection_sha256':sha256(run/'selection.json'),**provenance(root)}
    dump(artifact/'prediction_contract.json',contract)
    probes=train.orderBy(*KEYS).limit(100)
    cuts=sorted(config['primary_thresholds_mm']+config['held_out_thresholds_mm']+[3.14159])
    score_cdf(spark,probes,cuts,cdf,model).write.parquet(str(run/'reload_probe_scores'))
    probe_ids=[r.asDict() for r in probes.select(*KEYS).collect()];dump(run/'reload_probe_keys.json',probe_ids)
    dump(artifact/'artifact_manifest.json',{'status':'PASS','checksum_scope':'Logical model files; Hadoop .crc sidecars are derived filesystem metadata, optional in source control','files':[{'path':p.relative_to(artifact).as_posix(),'bytes':p.stat().st_size,'sha256':sha256(p)} for p in sorted(artifact.rglob('*')) if p.is_file() and p.name!='artifact_manifest.json' and p.suffix!='.crc']})
    dump(run/'refit_status.json',{'status':'PASS','selected_model':winner,'train_anchors':n,'train_years':[2017,2023],'training_seconds':time.monotonic()-begin,'2024_read_or_scored':False,'source_train_anchor_partition':str(source/'final/anchors/final_role=TRAIN')})
    log('Refit through 2023 PASS, '+str(n)+' anchors')

def reload_check(spark,run,root):
    from pyspark.sql import functions as F
    from C_distribution import grouping,conditional_survival
    from C_predict import RainfallPredictor
    from data_release import load_release
    predictor=RainfallPredictor(spark,run/'trained_model');inner=predictor.distribution
    keys=load(run/'reload_probe_keys.json');reference=spark.read.parquet(str(run/'reload_probe_scores')).collect()
    # Rebuild only past features from observations, independently of train materialization.
    manifest=load_release(root);obs=spark.read.parquet(str(root/manifest['tables']['observations']['path']))
    ids=spark.createDataFrame([(r['station_id'],r['prediction_epoch_s']) for r in keys],KEYS)
    from B_build_features import make_features
    years=sorted(set(datetime.fromtimestamp(r['prediction_epoch_s'],ZoneInfo('Asia/Singapore')).year for r in keys))
    min_epoch=min(r['prediction_epoch_s'] for r in keys)-3300;max_epoch=max(r['prediction_epoch_s'] for r in keys)
    history=obs.filter(F.col('slot_end_epoch_s').between(min_epoch,max_epoch)&F.col('station_id').isin(sorted(set(r['station_id'] for r in keys))))
    frame=None
    for year in years:
        features=make_features(history,year).join(ids,KEYS)
        frame=features if frame is None else frame.unionByName(features)
    frame=grouping(inner.model.transform(frame) if inner.model else frame,inner.cdf['kind'])
    groups={(r.station_id,r.prediction_epoch_s):r.cdf_group for r in frame.select(*KEYS,'cdf_group').collect()}
    diffs=[abs(conditional_survival(inner.cdf,groups[(r.station_id,r.prediction_epoch_s)],r.threshold_mm)-r.p) for r in reference]
    assert len(diffs)==1000 and max(diffs)<=1e-12,(len(diffs),max(diffs))
    key=keys[0];station=key['station_id'];t=key['prediction_epoch_s'];past=obs.filter((F.col('station_id')==station)&F.col('slot_end_epoch_s').between(t-3300,t))
    cuts=[0,.25,.5,.75,1,1.5,2,3,3.14159,5];answers=[predictor.predict(past,station,t,h) for h in cuts]
    assert all(a['probability']>=b['probability']-1e-15 for a,b in zip(answers,answers[1:]))
    rejected={}
    for name,s,e,h in [('unknown_station','__UNKNOWN__',t,1),('negative_threshold',station,t,-1),('nan_threshold',station,t,float('nan')),('off_grid',station,t+1,1)]:
        try:predictor.predict(past,s,e,h)
        except ValueError:rejected[name]=True
        else:raise AssertionError(name)
    try:predictor.predict(past.filter(F.col('slot_end_epoch_s')!=t-300),station,t,1)
    except ValueError:rejected['missing_history']=True
    else:raise AssertionError('Missing history accepted')
    dump(run/'reload_validation.json',{'status':'PASS','fresh_spark_process':True,'probe_anchors':100,'threshold_queries_per_anchor':10,
        'compared_probabilities':len(diffs),'maximum_error':max(diffs),'past_only_feature_reconstruction':True,
        'arbitrary_nontraining_threshold_accepted':3.14159,'monotone_example':True,'example_predictions':answers,'invalid_input_rejections':rejected})
    log('Fresh model reload and arbitrary-threshold past-only inference PASS')

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',required=True,type=Path);ap.add_argument('--source-run',required=True,type=Path);ap.add_argument('--root',required=True,type=Path);ap.add_argument('--input-anchors',type=Path);ap.add_argument('--stage',choices=['select','refit','reload','guard'],required=True);a=ap.parse_args()
    run=a.run.resolve()
    if a.stage=='select':select(run);return
    if a.stage=='guard':
        guard=load(run/'original_run_guard.json')
        for item in guard['files']:
            p=Path(guard['source_run'])/item['path'];assert p.stat().st_size==item['bytes'] and sha256(p)==item['sha256'],str(p)
        dump(run/'original_preservation_check.json',{'status':'PASS','original_files_checked':len(guard['files']),'original_2024_and_models_unchanged':True});return
    spark=spark_start(load(run/'experiment_config.json'))
    try:
        if a.stage=='refit':refit(spark,run,(a.input_anchors or a.source_run).resolve(),a.root.resolve())
        else:reload_check(spark,run,a.root.resolve())
    finally:spark.stop()
if __name__=='__main__':main()
