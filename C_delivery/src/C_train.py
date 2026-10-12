"""Explicit E2 development and locked final test; no random-fold CrossValidator."""
import argparse, time
from pathlib import Path
from c_common import *

def fit_preprocessor(train):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler
    return Pipeline(stages=[StringIndexer(inputCol='station_id',outputCol='station_index',handleInvalid='error'),
        OneHotEncoder(inputCols=['station_index'],outputCols=['station_ohe'],handleInvalid='error'),
        VectorAssembler(inputCols=NUMERIC_FEATURES+['threshold_mm','station_ohe'],outputCol='features',handleInvalid='error')]).fit(train)

def fit_candidate(name, train, config):
    from pyspark.ml.classification import LogisticRegression,DecisionTreeClassifier
    if name=='logistic_regression':return LogisticRegression(**config['candidates'][name]).fit(train)
    return DecisionTreeClassifier(seed=config['seed'],**config['candidates'][name]).fit(train)

def baseline(train):
    from pyspark.sql import functions as F
    return [r.asDict() for r in train.groupBy('threshold_mm').agg(F.avg('label').alias('p'),F.count('*').alias('n')).orderBy('threshold_mm').collect()]

def score(model_name, model, anchors, thresholds, probabilities):
    from pyspark.sql import functions as F
    from pyspark.ml.functions import vector_to_array
    expanded=expand(anchors,thresholds)
    if model_name=='frequency_baseline':
        vals=F.create_map(*[v for r in probabilities for v in (F.lit(r['threshold_mm']),F.lit(r['p']))])
        return expanded.withColumn('p',vals[F.col('threshold_mm')]).select(*KEYS,'threshold_mm','label','p')
    return model.transform(expanded).select(*KEYS,'threshold_mm','label',vector_to_array('probability')[1].alias('p'))

def evaluate(spark, path, config, out, name, year):
    from pyspark.sql import functions as F
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark import StorageLevel
    frame=spark.read.parquet(str(path)).persist(StorageLevel.DISK_ONLY)
    clip=config['log_loss_clip'];p=F.greatest(F.lit(clip),F.least(F.lit(1-clip),F.col('p')))
    invalid=frame.filter(F.col('p').isNull()|F.isnan('p')|~F.col('p').between(0,1)|F.col('label').isNull()).limit(1).count()
    assert invalid==0,'Invalid probability/label'
    stats=frame.groupBy('threshold_mm').agg(F.count('*').alias('n'),F.sum('label').alias('positive'),
         F.avg('p').alias('mean_probability'),F.avg((F.col('p')-F.col('label'))**2).alias('brier'),
         F.avg(-F.col('label')*F.log(p)-(1-F.col('label'))*F.log(1-p)).alias('log_loss')).orderBy('threshold_mm').collect()
    metrics=[]
    for r in stats:
        m={'model':name,'year':year,**r.asDict()};m['positive']=int(m['positive']);m['positive_rate']=m['positive']/m['n']
        subset=frame.filter(F.col('threshold_mm')==m['threshold_mm'])
        if 0<m['positive']<m['n']:
            evaluator=BinaryClassificationEvaluator(labelCol='label',rawPredictionCol='p',numBins=config['auc_num_bins'])
            m['roc_auc']=evaluator.setMetricName('areaUnderROC').evaluate(subset)
            m['pr_auc']=evaluator.setMetricName('areaUnderPR').evaluate(subset)
        else:m.update(roc_auc=None,pr_auc=None)
        metrics.append(m)
    edges=config['calibration_bin_edges'];bincol=F.lit(len(edges)-2)
    for i in reversed(range(len(edges)-1)):
        bincol=F.when((F.col('p')>=edges[i])&(F.col('p')<edges[i+1]),F.lit(i)).otherwise(bincol)
    bins=frame.withColumn('bin',bincol).groupBy('threshold_mm','bin').agg(F.count('*').alias('n'),
         F.avg('p').alias('mean_probability'),F.avg('label').alias('observed_rate')).orderBy('threshold_mm','bin').collect()
    csv_write(out/f'{name}_calibration.csv',[{'model':name,'year':year,**r.asDict()} for r in bins])
    # Raw model diagnostics; no test-driven calibration/monotonic correction.
    from pyspark.sql.window import Window
    w=Window.partitionBy(*KEYS).orderBy('threshold_mm')
    violations=frame.withColumn('previous_p',F.lag('p').over(w)).filter(F.col('p')>F.col('previous_p')+1e-12).count()
    csv_write(out/f'{name}_metrics.csv',metrics)
    dump(out/f'{name}_evaluation.json',{'status':'PASS','metrics':metrics,'threshold_probability_increasing_pairs':violations,
        'auc':'Spark MLlib approximate curves with numBins='+str(config['auc_num_bins']),
        'expanded_score_rows':sum(m['n'] for m in metrics)})
    frame.unpersist();return metrics

def prepare_fold(spark, targets, features, config, thresholds, role, eval_role, year, fraction, out):
    from pyspark.sql import functions as F
    base=targets.filter(F.col('year')<=year).filter(F.col('model_anchor_eligible')).select(
        *KEYS,'year','future_30m_mm',role)
    train=sampled(base.filter(F.col(role)=='TRAIN'),fraction,config['seed'])
    stations=[r['station_id'] for r in train.groupBy('station_id').count().filter(
        F.col('count')>=config['station_support_min_training_anchors']).collect()]
    assert stations
    train=train.filter(F.col('station_id').isin(stations))
    eligible_validation=base.filter(F.col(role)==eval_role)
    validation=eligible_validation.filter(F.col('station_id').isin(stations))
    if fraction==config['pilot_anchor_fraction']:validation=sampled(validation,fraction,config['seed'])
    ntrain=train.count();nvalidation=validation.count()
    all_targets_year=targets.filter(F.col('year')==year)
    count=all_targets_year.agg(F.count('*').alias('all_observed_anchors'),
        F.sum(F.col('historical_valid').cast('long')).alias('history_valid'),
        F.sum(F.col('model_anchor_eligible').cast('long')).alias('eligible_history_and_future'),
        F.sum((F.col(role)==eval_role).cast('long')).alias('role_before_eligibility')).first().asDict()
    count.update(sampled_train_anchors=ntrain,eligible_eval_before_station_filter=eligible_validation.count(),
        unknown_station_eval_anchors=eligible_validation.filter(~F.col('station_id').isin(stations)).count(),
        scored_eval_anchors=nvalidation,station_ids=sorted(stations),train_fraction=fraction)
    count['scored_fraction_of_observed']=nvalidation/count['all_observed_anchors']
    dump(out/'coverage.json',count)
    # Actual inputs AFTER role, eligibility, station support and train sampling, BEFORE thresholds.
    actual=train.unionByName(validation)
    right=features.filter(F.col('year')<=year).select(*KEYS,*NUMERIC_FEATURES)
    log(f'{out.name}: auditing actual sampled train + evaluation anchors against B features')
    audit=audit_anchor_join(actual,right,scope=f'C_{out.name}_actual_post_filter_post_sample_pre_threshold')
    dump(out/'actual_input_join_audit.json',audit);assert audit['status']=='PASS'
    joined=actual.join(right,KEYS,'inner')
    assert joined.count()==audit['left_rows']==ntrain+nvalidation
    # Materialize skinny fold inputs once; no full observational cache.
    joined.write.partitionBy(role).parquet(str(out/'anchors'))
    saved=spark.read.parquet(str(out/'anchors'))
    train=saved.filter(F.col(role)=='TRAIN');validation=saved.filter(F.col(role)==eval_role)
    check_expansion(expand(train,thresholds),ntrain,thresholds,out/'train_expansion.json')
    check_expansion(expand(validation,thresholds),nvalidation,thresholds,out/'eval_expansion.json')
    return train,validation,stations

def run_fold(spark, targets, features, config, thresholds, role, eval_role, year, fraction, out, names):
    from pyspark.ml import PipelineModel
    from pyspark import StorageLevel
    out.mkdir(parents=True,exist_ok=True);begin=time.monotonic()
    train,val,stations=prepare_fold(spark,targets,features,config,thresholds,role,eval_role,year,fraction,out)
    expanded=expand(train,thresholds);probabilities=baseline(expanded)
    dump(out/'baseline.json',probabilities)
    log(f'{out.name}: fitting training-only station encoder and feature assembler')
    prep=fit_preprocessor(expanded);prep.write().save(str(out/'preprocessor'))
    # Persist only projected vectors; Spark disk cache prevents a 6g heap bottleneck.
    trainvec=prep.transform(expanded).select('label','features').persist(StorageLevel.DISK_ONLY)
    trainvec.count();all_metrics=[];times={}
    for name in names:
        start=time.monotonic();model=None
        log(f'{out.name}: fit {name}')
        if name!='frequency_baseline':
            classifier=fit_candidate(name,trainvec,config)
            model=PipelineModel(stages=prep.stages+[classifier]);model.write().save(str(out/'models'/name))
            detail={'class':classifier.__class__.__name__,'parameters':{p.name:v for p,v in classifier.extractParamMap().items()}}
            if name=='logistic_regression':detail.update(iterations=classifier.summary.totalIterations,objective_history=classifier.summary.objectiveHistory)
            else:detail.update(num_nodes=classifier.numNodes,depth=classifier.depth)
            dump(out/f'{name}_fit.json',detail)
        fit_seconds=time.monotonic()-start
        log(f'{out.name}: scoring {name} on all supported evaluation anchors')
        scores=out/'predictions'/name
        score(name,model,val,thresholds,probabilities).write.partitionBy('threshold_mm').parquet(str(scores))
        all_metrics+=evaluate(spark,scores,config,out,name,year)
        times[name]={'fit_seconds':fit_seconds,'fit_score_evaluate_seconds':time.monotonic()-start}
        log(f'{out.name}: {name} completed')
    trainvec.unpersist()
    dump(out/'timing.json',{'elapsed_seconds':time.monotonic()-begin,'candidates':times})
    dump(out/'status.json',{'status':'PASS','year':year,'models':names,'station_ids':stations})
    return all_metrics

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--stage',choices=['pilot','develop','final'],required=True)
    ap.add_argument('--root',type=Path,required=True);ap.add_argument('--features',type=Path,required=True)
    ap.add_argument('--output',type=Path,required=True);a=ap.parse_args()
    config=load(CONFIG);out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
    # Record fixed candidates and tie tolerance before any validation scores.
    frozen=out/'experiment_config.json'
    if frozen.exists():assert load(frozen)==config,'Experiment config changed; use a new run'
    else:dump(frozen,config)
    integrity=verify_release(a.root,'full');bmeta=load(a.features/'result_metadata.json')
    for item in bmeta['outputs']:
        p=a.features/item['path'];assert p.is_file() and p.stat().st_size==item['bytes'] and sha256(p)==item['sha256'],str(p)
    dump(out/'input_integrity.json',{'A_release':integrity,'B_feature_metadata_sha256':sha256(a.features/'result_metadata.json'),
        'B_feature_files_verified':len(bmeta['outputs']),'B_features':str(a.features.resolve()),'experiment_config_hash':sha256(CONFIG)})
    spark=spark_start(config);begin=time.monotonic()
    try:
        from pyspark.sql import functions as F
        from data_release import load_release
        manifest=load_release(a.root)
        targets=spark.read.parquet(str(a.root/manifest['tables']['targets']['path']))
        features=spark.read.parquet(str(a.features/'parquet'))
        dump(out/'environment.json',environment(spark))
        support_file=out/'threshold_support.json'
        if not support_file.exists():
            earliest=targets.filter((F.col('fold1_role')=='TRAIN')&F.col('model_anchor_eligible'))
            counts=expand(earliest,config['threshold_candidates_mm']).groupBy('threshold_mm').agg(
                F.count('*').alias('n'),F.sum('label').alias('positive')).orderBy('threshold_mm').collect()
            records=[r.asDict() for r in counts];thresholds=[r['threshold_mm'] for r in records if
                r['positive']>=config['threshold_support']['min_positive_anchors'] and r['n']-r['positive']>=config['threshold_support']['min_negative_anchors']]
            assert thresholds
            dump(support_file,{'status':'FROZEN','thresholds':thresholds,'training_years':[2017,2020],
                'source':'full eligible first-fold TRAIN only; no validation/test labels','counts':records})
        thresholds=load(support_file)['thresholds'];names=list(config['candidates'])
        if a.stage=='pilot':
            metrics=run_fold(spark,targets,features,config,thresholds,'fold1_role','VALIDATION',2021,
                config['pilot_anchor_fraction'],out/'pilot',names)
            dump(out/'pilot_status.json',{'status':'PASS','purpose':'implementation check; not formal selection','metrics':metrics})
        elif a.stage=='develop':
            assert load(out/'pilot_status.json')['status']=='PASS'
            metrics=[]
            for i,year in enumerate([2021,2022,2023],1):
                folder=out/f'fold{i}'
                if (folder/'status.json').exists():
                    assert load(folder/'status.json')['status']=='PASS'
                    for name in names:metrics+=load(folder/f'{name}_evaluation.json')['metrics']
                else:metrics+=run_fold(spark,targets,features,config,thresholds,f'fold{i}_role','VALIDATION',year,
                    config['formal_anchor_fraction'],folder,names)
            csv_write(out/'development_metrics.csv',metrics)
            scores=[]
            for name in names:
                yr=[sum(m['brier'] for m in metrics if m['model']==name and m['year']==y)/len(thresholds) for y in [2021,2022,2023]]
                scores.append({'model':name,'mean_brier':sum(yr)/3,'best_year_brier':min(yr),'worst_year_brier':max(yr),'year_range':max(yr)-min(yr),'year_scores':dict(zip([2021,2022,2023],yr))})
            best=min(s['mean_brier'] for s in scores)
            winner=next(name for name in config['simplicity_order'] if next(s['mean_brier'] for s in scores if s['model']==name)<=best+config['tie_absolute_tolerance'])
            # Persist selection and config hash BEFORE any 2024 scoring.
            dump(out/'selection.json',{'status':'LOCKED','selected_model':winner,'scores':scores,'thresholds':thresholds,
                'experiment_config_hash':sha256(CONFIG),'selected_at_utc':datetime.now(timezone.utc).isoformat(),
                'test_labels_or_metrics_used':False,'selection_rule':config['selection'],'tie_absolute_tolerance':config['tie_absolute_tolerance']})
            log(f'LOCKED selection: {winner}; mean Brier '+str(next(s['mean_brier'] for s in scores if s['model']==winner)))
        else:
            selection=load(out/'selection.json');assert selection['status']=='LOCKED' and selection['experiment_config_hash']==sha256(CONFIG)
            assert not (out/'final/status.json').exists(),'Final test already completed; do not tune/retest'
            winner=selection['selected_model']
            metrics=run_fold(spark,targets,features,config,thresholds,'final_role','TEST',2024,
                config['formal_anchor_fraction'],out/'final',list(dict.fromkeys(['frequency_baseline',winner])))
            dump(out/'final_test.json',{'status':'PASS','selected_model':winner,'metrics':metrics,
                'selection_sha256':sha256(out/'selection.json'),'selection_locked_before_test':True,
                'final_train_years':[2017,2023],'test_year':2024,'mean_brier':sum(m['brier'] for m in metrics if m['model']==winner)/len(thresholds)})
        dump(out/f'{a.stage}_run_status.json',{'status':'PASS','elapsed_seconds':time.monotonic()-begin,'environment':environment(spark)})
    except Exception as e:
        dump(out/f'{a.stage}_run_status.json',{'status':'FAIL','elapsed_seconds':time.monotonic()-begin,'error':repr(e)})
        raise
    finally:spark.stop()

if __name__=='__main__':main()
