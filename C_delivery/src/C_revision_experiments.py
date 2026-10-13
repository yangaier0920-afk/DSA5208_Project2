"""Prespecified development-only C revision, stronger baselines and grouped ablations."""
import argparse,shutil,time,math
from pathlib import Path
from c_common import *
from C_distribution import fit_tree,fit_cdf,score_cdf,enrich,features_for
from C_train import fit_preprocessor,evaluate

REVISION_CONFIG=PACKAGE/'configs/C_revision_20261013.json'

def add_group_columns(frame,stations):
    from pyspark.sql import functions as F
    return enrich(frame).withColumn('history_state',F.when(F.col('rain_sum_60m_mm')==0,'dry').otherwise('wet'))

def grouped_evaluate(spark,predictions,coverage_rows,config,out,name,year,stations):
    from pyspark.sql import functions as F
    frame=spark.read.parquet(str(predictions))
    cuts=config['primary_thresholds_mm'];frame=frame.filter(F.col('threshold_mm').isin(cuts))
    bins=config['group_evaluation']['auc_histogram_bins'];edges=config['calibration_bin_edges'];bincol=F.lit(len(edges)-2)
    for i in reversed(range(len(edges)-1)):
        bincol=F.when((F.col('p')>=edges[i])&(F.col('p')<edges[i+1]),F.lit(i)).otherwise(bincol)
    group_arrays=F.array(F.concat(F.lit('month:'),F.col('prediction_month').cast('string')),
                        F.concat(F.lit('history:'),F.col('history_state')),
                        F.when(F.col('station_id').isin(stations),F.concat(F.lit('station:'),F.col('station_id'))))
    data=frame.withColumn('group',F.explode(group_arrays)).filter(F.col('group').isNotNull()).withColumn('cal_bin',bincol).withColumn('rank_bin',F.least(F.lit(bins-1),F.floor(F.col('p')*bins).cast('int')))
    cells=data.groupBy('group','threshold_mm','cal_bin','rank_bin').agg(F.count('*').alias('n'),F.sum('label').alias('positive'),
        F.sum('p').alias('sum_p'),F.sum((F.col('p')-F.col('label'))**2).alias('sum_brier')).collect()
    from collections import defaultdict
    grouped=defaultdict(list)
    for r in cells:grouped[(r['group'],r.threshold_mm)].append(r.asDict())
    results=[]
    for (group,t),rr in sorted(grouped.items()):
        n=sum(r['n'] for r in rr);pos=sum(r['positive'] for r in rr);neg=n-pos;cal=defaultdict(lambda:[0,0,0]);rank=defaultdict(lambda:[0,0])
        for r in rr:
            for i,v in enumerate([r['n'],r['positive'],r['sum_p']]):cal[r['cal_bin']][i]+=v
            rank[r['rank_bin']][0]+=r['positive'];rank[r['rank_bin']][1]+=r['n']-r['positive']
        ece=sum(abs(v[2]-v[1]) for v in cal.values())/n;pr=roc=None
        if pos>0 and neg>0:
            tp=fp=0;last_recall=last_fpr=0;last_precision=1;pr=roc=0
            for _,(pcount,ncount) in sorted(rank.items(),reverse=True):
                tp+=pcount;fp+=ncount;recall=tp/pos;fpr=fp/neg;precision=tp/(tp+fp)
                pr+=(recall-last_recall)*(precision+last_precision)/2;roc+=(fpr-last_fpr)*(recall+last_recall)/2
                last_recall=recall;last_fpr=fpr;last_precision=precision
        observed=coverage_rows[group]['observed'];eligible=coverage_rows[group]['eligible']
        assert n==eligible,(name,group,n,eligible)
        results.append({'model':name,'year':year,'group':group,'threshold_mm':t,'n':n,'positive':int(pos),'positive_rate':pos/n,
            'brier':sum(r['sum_brier'] for r in rr)/n,'mean_probability':sum(r['sum_p'] for r in rr)/n,
            'ece_fixed_bins':ece,'pr_auc_binned':pr,'roc_auc_binned':roc,'observed_anchors':observed,'eligible_anchors':eligible,
            'coverage_fraction':eligible/observed,'auc_method':f'{bins} equal-probability histogram bins; approximate descriptive group diagnostic'})
    csv_write(out/f'{name}_group_metrics.csv',results);return results

def coverage_by_group(spark,targets,features,year,role,stations,supported):
    from pyspark.sql import functions as F
    observed=targets.filter(F.col('year')==year).select(*KEYS,'historical_valid','model_anchor_eligible',role).join(
        features.filter(F.col('year')==year).select(*KEYS,'rain_sum_60m_mm'),KEYS)
    x=enrich(observed).withColumn('history_state',F.when(~F.col('historical_valid'),'unknown').when(F.col('rain_sum_60m_mm')==0,'dry').otherwise('wet'))
    x=x.withColumn('eligible',((F.col(role)=='VALIDATION')&F.col('model_anchor_eligible')&F.col('station_id').isin(supported)).cast('long'))
    groups=F.array(F.concat(F.lit('month:'),F.col('prediction_month').cast('string')),F.concat(F.lit('history:'),F.col('history_state')),
        F.when(F.col('station_id').isin(stations),F.concat(F.lit('station:'),'station_id')))
    rows=x.withColumn('group',F.explode(groups)).filter(F.col('group').isNotNull()).groupBy('group').agg(F.count('*').alias('observed'),F.sum('eligible').alias('eligible')).collect()
    return {r['group']:{'observed':r.observed,'eligible':r.eligible} for r in rows}

def calibration_summary(out,name,metrics):
    rows=list(__import__('csv').DictReader((out/f'{name}_calibration.csv').open()));summary=[]
    for m in metrics:
        rr=[r for r in rows if float(r['threshold_mm'])==m['threshold_mm']]
        ece=sum(int(r['n'])*abs(float(r['mean_probability'])-float(r['observed_rate'])) for r in rr)/m['n']
        low=[r for r in rr if float(r['mean_probability'])<=.05];n=sum(int(r['n']) for r in low)
        summary.append({'model':name,'year':m['year'],'threshold_mm':m['threshold_mm'],'ece_fixed_bins':ece,
            'low_probability_region_n':n,'low_probability_region_fraction':n/m['n'],
            'low_region_mean_p':sum(int(r['n'])*float(r['mean_probability']) for r in low)/n if n else None,
            'low_region_observed_rate':sum(int(r['n'])*float(r['observed_rate']) for r in low)/n if n else None})
    csv_write(out/f'{name}_calibration_summary.csv',summary)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--source-run',required=True,type=Path);ap.add_argument('--root',required=True,type=Path)
    ap.add_argument('--features',required=True,type=Path);ap.add_argument('--output',required=True,type=Path)
    ap.add_argument('--config',type=Path,default=REVISION_CONFIG);ap.add_argument('--input-anchors',type=Path,help='Optional reconstructed source produced by C_revision_rebuild_anchors.py');ap.add_argument('--fold',type=int,choices=[1,2,3]);ap.add_argument('--models',nargs='+');a=ap.parse_args()
    config=load(a.config);out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
    config['experiment_version']=out.name
    config_file=out/'experiment_config.json'
    if config_file.exists():assert load(config_file)==config,'Config changed; use a new output run'
    else:dump(config_file,config)
    source=a.source_run.resolve();anchor_source=(a.input_anchors or source).resolve();assert source!=out and anchor_source!=out
    # Freeze all original experiment bytes, including the already-seen 2024 result, before new work.
    guard=out/'original_run_guard.json'
    if not guard.exists():
        dump(guard,{'source_run':str(source),'files':[{'path':p.relative_to(source).as_posix(),'bytes':p.stat().st_size,'sha256':sha256(p)} for p in sorted(source.rglob('*')) if p.is_file() and 'logs' not in p.parts]})
    original_metadata=load(anchor_source/'result_metadata.json')
    verified=0
    for item in original_metadata['output_files']:
        rel=item['path']
        if '/anchors/' in rel or rel.startswith('anchors/'):
            if original_metadata.get('file_path_scope')=='relative_to_source':file=anchor_source/rel
            elif rel.startswith('results/'+anchor_source.name+'/'):file=PACKAGE/rel
            else:continue
            assert file.is_file() and file.stat().st_size==item['bytes'] and sha256(file)==item['sha256'],str(file)
            verified+=1
    assert verified>0
    dump(out/'reused_anchor_hash_verification.json',{'status':'PASS','verified_parquet_and_sidecar_files':verified,'reference':'Source anchor metadata; original byte hashes or explicitly regenerated source','training_sample_and_validation_inputs_unchanged':True})
    integrity=verify_release(a.root,'full');dump(out/'input_integrity.json',{'A_release':integrity,'source_run':str(source),'B_features':str(a.features.resolve()),
        'sampling':'Exactly reuse materialized v1 post-filter 10% TRAIN/full VALIDATION anchors; compare identical keys across candidates','source_config_sha256':sha256(a.config),'experiment_config_hash':sha256(config_file)})
    spark=spark_start(config);start=time.monotonic()
    try:
        from pyspark.sql import functions as F
        from pyspark.ml import PipelineModel
        from pyspark.ml.classification import DecisionTreeClassifier,RandomForestClassifier
        from pyspark.ml.functions import vector_to_array
        from pyspark import StorageLevel
        manifest=load(a.root/'manifests/release_manifest.json');targets=spark.read.parquet(str(a.root/manifest['tables']['targets']['path']))
        features=spark.read.parquet(str(a.features/'parquet'));dump(out/'environment.json',environment(spark))
        first=spark.read.parquet(str(anchor_source/'fold1/anchors')).filter(F.col('fold1_role')=='TRAIN')
        station_file=out/'group_station_selection.json'
        if not station_file.exists():
            selected=[r.station_id for r in first.groupBy('station_id').count().orderBy(F.desc('count'),'station_id').limit(5).collect()]
            dump(station_file,{'station_ids':selected,'rule':'Five largest first-fold TRAIN sample counts, ties station ID; chosen before model metrics','source':'2017..2020 TRAIN only'})
        stations=load(station_file)['station_ids'];thresholds=sorted(set(config['primary_thresholds_mm']+config['held_out_thresholds_mm']))
        names=a.models or config['continuous_candidates']+list(config['diagnostic_classifiers'])
        for i,year in enumerate([2021,2022,2023],1):
            if a.fold and a.fold!=i:continue
            folder=out/f'fold{i}';folder.mkdir(exist_ok=True);role=f'fold{i}_role';anchorpath=anchor_source/f'fold{i}/anchors'
            data=spark.read.parquet(str(anchorpath));train=data.filter(F.col(role)=='TRAIN');val=data.filter(F.col(role)=='VALIDATION')
            counts={'train_anchors':train.count(),'validation_anchors':val.count()}
            old_counts=load(source/f'fold{i}/coverage.json')
            assert counts['train_anchors']==old_counts['sampled_train_anchors'] and counts['validation_anchors']==old_counts['scored_eval_anchors']
            assert load(source/f'fold{i}/actual_input_join_audit.json')['status']=='PASS'
            dump(folder/'sample_identity.json',{'status':'PASS','source_path':str(anchorpath),
                'source_actual_join_audit_sha256':sha256(anchor_source/f'fold{i}/actual_input_join_audit.json'),**counts,'all_candidates_use_same_materialized_anchor_keys':True})
            supported=load(source/f'fold{i}/status.json')['station_ids'];val=add_group_columns(val,stations)
            coverage_file=folder/'group_coverage.json'
            if not coverage_file.exists():dump(coverage_file,coverage_by_group(spark,targets,features,year,role,stations,supported))
            coverage=load(coverage_file)
            for name in names:
                completed=folder/f'{name}_complete.json'
                if completed.exists():assert load(completed)['status']=='PASS';log(f'Already completed {folder.name}/{name}');continue
                begin=time.monotonic();log(f'{folder.name}: training {name}');model=None;cdf=None;mdir=folder/'models'/name
                if mdir.exists():raise FileExistsError(f'Incomplete candidate directory: {mdir}; inspect previous error and use a new name/run')
                mdir.mkdir(parents=True)
                if name in config['continuous_candidates']:
                    kind={'frequency_cdf':'frequency','climatology_cdf':'climatology','persistence_cdf':'persistence'}.get(name,'leaf')
                    if kind=='leaf':model=fit_tree(train,name,config);model.write().save(str(mdir/'spark_model'))
                    cdf=fit_cdf(model.transform(train) if model else train,kind,config['tree_cdf']['leaf_prior_anchors'] if model else (0 if kind=='frequency' else config['baselines']['prior_anchors']))
                    dump(mdir/'conditional_cdf.json',cdf)
                    if model:
                        imp=model.stages[-1].featureImportances
                        attrs=model.transform(train.limit(1)).schema['features'].metadata['ml_attr']['attrs'];attr={e['idx']:e['name'] for g in attrs.values() for e in g}
                        csv_write(folder/f'{name}_feature_weights.csv',[{'feature':attr.get(k,str(k)),'weight':float(v),'kind':'regression_tree_impurity_not_causal'} for k,v in enumerate(imp)])
                    scored=score_cdf(spark,val,thresholds,cdf,model,['prediction_month','history_state'])
                else:
                    expanded=expand(train,config['primary_thresholds_mm']);prep=fit_preprocessor(expanded);vector=prep.transform(expanded).select('label','features').persist(StorageLevel.DISK_ONLY);vector.count()
                    params=config['diagnostic_classifiers'][name];klass=DecisionTreeClassifier if name=='threshold_tree_d8' else RandomForestClassifier
                    classifier=klass(seed=config['seed'],maxBins=32,**params).fit(vector);model=PipelineModel(stages=prep.stages+[classifier]);model.write().save(str(mdir/'spark_model'));vector.unpersist()
                    scored=model.transform(expand(val,config['primary_thresholds_mm'])).select(*KEYS,'threshold_mm','label',vector_to_array('probability')[1].alias('p'),'prediction_month','history_state')
                train_seconds=time.monotonic()-begin;pred=folder/'predictions'/name
                log(f'{folder.name}: scoring/evaluating {name}');scored.write.partitionBy('threshold_mm').parquet(str(pred))
                metrics=evaluate(spark,pred,config,folder,name,year)
                if cdf:assert load(folder/f'{name}_evaluation.json')['threshold_probability_increasing_pairs']==0
                calibration_summary(folder,name,metrics)
                grouped_evaluate(spark,pred,coverage,config,folder,name,year,stations)
                dump(completed,{'status':'PASS','training_seconds':train_seconds,'elapsed_seconds':time.monotonic()-begin,
                    'model':name,'year':year,'train_anchors':counts['train_anchors'],'validation_anchors':counts['validation_anchors'],
                    'thresholds':thresholds if cdf else config['primary_thresholds_mm'],'source_2024_used':False,'training_only_fit':True})
                log(f'{folder.name}: PASS {name}, elapsed {time.monotonic()-begin:.1f}s')
        dump(out/'latest_execution.json',{'status':'PASS','elapsed_seconds':time.monotonic()-start,'fold':a.fold,'models':names,'2024_scored':False})
    except Exception as exc:
        dump(out/'latest_execution.json',{'status':'FAIL','elapsed_seconds':time.monotonic()-start,'error':repr(exc)});raise
    finally:spark.stop()
if __name__=='__main__':main()
