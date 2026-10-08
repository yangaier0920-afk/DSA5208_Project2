"""P0-4: full-coverage time-window truth and E2 roles; no model fitting."""
import argparse
import csv
import hashlib
import json
import sys
import time
from datetime import datetime,timedelta,timezone
from decimal import Decimal,localcontext
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from spark_session import start_spark,stop_spark

VERSION='p0_4_v1'
SGT=timezone(timedelta(hours=8))
SOURCE_VERSION='p0_3_v1'
FUTURE=[300,600,900,1200,1500,1800]
HISTORY=list(range(-3300,1,300))


def now():
    return datetime.now(SGT).isoformat()


def epoch(year):
    return int(datetime(year,1,1,tzinfo=SGT).timestamp())


def digest(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''):
            value.update(block)
    return value.hexdigest()


def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')
    temp.replace(path)


def write_csv(path,rows,columns=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns or list(rows[0]));writer.writeheader();writer.writerows(rows)


def collect(df):
    return [r.asDict(recursive=True) for r in df.collect()]


def build_targets(obs):
    """Actual epoch windows plus explicit expected-slot verification on clean keys."""
    from pyspark.sql import functions as F,Window
    data=obs.select('station_id','slot_end_epoch_s','rain_decimal_mm')
    order=Window.partitionBy('station_id').orderBy('slot_end_epoch_s')
    future=order.rangeBetween(300,1800)
    history=order.rangeBetween(-3300,0)
    data=(data.withColumn('_future_epochs',F.collect_set('slot_end_epoch_s').over(future))
        .withColumn('_future_count',F.count('rain_decimal_mm').over(future))
        .withColumn('_future_sum',F.sum('rain_decimal_mm').over(future))
        .withColumn('_history_epochs',F.collect_set('slot_end_epoch_s').over(history))
        .withColumn('historical_n_valid',F.count('rain_decimal_mm').over(history)))
    # Prove membership of all six/twelve expected positions, rather than merely
    # trusting the number of following records or treating absent slots as zero.
    missing=lambda offsets,column:F.filter(F.array(*[
        F.when(~F.array_contains(F.col(column),F.col('slot_end_epoch_s')+offset),F.lit(offset))
        for offset in offsets]),lambda item:item.isNotNull())
    data=(data.withColumnRenamed('slot_end_epoch_s','prediction_epoch_s')
        .withColumn('slot_end_epoch_s',F.col('prediction_epoch_s'))
        .withColumn('prediction_ts',F.col('prediction_epoch_s').cast('timestamp'))
        .withColumn('label_end_epoch_s',F.col('prediction_epoch_s')+1800)
        .withColumn('label_end_ts',F.col('label_end_epoch_s').cast('timestamp'))
        .withColumn('future_missing_offsets_s',missing(FUTURE,'_future_epochs'))
        .withColumn('historical_missing_offsets_s',missing(HISTORY,'_history_epochs'))
        .withColumn('future_n_valid',F.col('_future_count'))
        .withColumn('target_valid',(F.size('future_missing_offsets_s')==0)&(F.col('future_n_valid')==6)&F.col('_future_sum').isNotNull())
        .withColumn('historical_valid',(F.size('historical_missing_offsets_s')==0)&(F.col('historical_n_valid')==12))
        .withColumn('future_30m_mm',F.when(F.col('target_valid'),F.col('_future_sum')))
        .withColumn('model_anchor_eligible',F.col('target_valid')&F.col('historical_valid'))
        .withColumn('year',F.year('prediction_ts')).withColumn('month',F.month('prediction_ts'))
        .withColumn('year_boundary_safe',F.year('prediction_ts')==F.year('label_end_ts')))
    missing_end=F.exists(F.col('future_missing_offsets_s'),lambda off:F.col('prediction_epoch_s')+off>=epoch(2025))
    missing_start=F.exists(F.col('historical_missing_offsets_s'),lambda off:F.col('prediction_epoch_s')+off<epoch(2017))
    reasons=lambda conditions:F.filter(F.array(*[F.when(condition,F.lit(name)) for name,condition in conditions]),lambda item:item.isNotNull())
    data=(data.withColumn('invalid_reason',reasons([
        ('MISSING_FUTURE_SLOTS',~F.col('target_valid')),
        ('DATASET_END_CONTEXT_UNAVAILABLE',~F.col('target_valid')&missing_end)]))
        .withColumn('historical_invalid_reason',reasons([
        ('MISSING_HISTORY_SLOTS',~F.col('historical_valid')),
        ('DATASET_START_CONTEXT_UNAVAILABLE',~F.col('historical_valid')&missing_start)])))
    # Internal calendar-year boundaries inside a training block remain usable.
    # Purge only the actual fold's train/validation boundary, including equality.
    for index,val_year in enumerate([2021,2022,2023],1):
        role=(F.when(F.col('year')<val_year,
                    F.when(F.col('label_end_epoch_s')<epoch(val_year),'TRAIN').otherwise('EXCLUDED_BOUNDARY'))
              .when(F.col('year')==val_year,
                    F.when(F.col('label_end_epoch_s')<epoch(val_year+1),'VALIDATION').otherwise('EXCLUDED_BOUNDARY'))
              .otherwise('UNUSED'))
        data=data.withColumn(f'fold{index}_role',role)
    data=data.withColumn('final_role',F.when(F.col('year')<2024,
        F.when(F.col('label_end_epoch_s')<epoch(2024),'TRAIN').otherwise('EXCLUDED_BOUNDARY'))
        .otherwise(F.when(F.col('label_end_epoch_s')<epoch(2025),'TEST').otherwise('EXCLUDED_BOUNDARY')))
    return (data.withColumn('targets_version',F.lit(VERSION)).withColumn('observations_version',F.lit(SOURCE_VERSION))
        .drop('slot_end_epoch_s','rain_decimal_mm','_future_epochs','_history_epochs','_future_count','_future_sum'))


def metrics_and_checks(targets,expected_rows):
    from pyspark.sql import functions as F
    invalid=~F.col('target_valid'); history_bad=~F.col('historical_valid')
    expressions=[F.count('*').alias('anchors'),F.countDistinct(F.struct('station_id','prediction_epoch_s')).alias('unique_keys'),
        F.sum(F.col('target_valid').cast('long')).alias('future_valid'),
        F.sum(invalid.cast('long')).alias('future_invalid'),
        F.sum(F.col('historical_valid').cast('long')).alias('history_valid'),
        F.sum(history_bad.cast('long')).alias('history_invalid'),
        F.sum(F.col('model_anchor_eligible').cast('long')).alias('base_model_eligible'),
        F.sum((F.col('target_valid')&history_bad).cast('long')).alias('future_valid_history_invalid'),
        F.sum((~F.col('year_boundary_safe')).cast('long')).alias('cross_calendar_year_anchors'),
        F.sum((invalid&F.col('future_30m_mm').isNotNull()).cast('long')).alias('invalid_nonnull_future'),
        F.sum((F.col('target_valid')&(F.col('future_30m_mm').isNull()|(F.col('future_30m_mm')<0))).cast('long')).alias('bad_valid_future'),
        F.sum((F.col('future_n_valid')+F.size('future_missing_offsets_s')!=6).cast('long')).alias('bad_future_slot_account'),
        F.sum((F.col('historical_n_valid')+F.size('historical_missing_offsets_s')!=12).cast('long')).alias('bad_history_slot_account'),
        F.sum((F.col('target_valid')!=(F.size('future_missing_offsets_s')==0)).cast('long')).alias('bad_target_flag'),
        F.sum((F.col('historical_valid')!=(F.size('historical_missing_offsets_s')==0)).cast('long')).alias('bad_history_flag'),
        F.sum((F.col('model_anchor_eligible')!=(F.col('target_valid')&F.col('historical_valid'))).cast('long')).alias('bad_eligibility_flag'),
        F.sum(((F.col('label_end_epoch_s')!=F.col('prediction_epoch_s')+1800)|
               (F.col('label_end_ts').cast('long')!=F.col('label_end_epoch_s'))).cast('long')).alias('bad_label_end'),
        F.sum(((F.year('prediction_ts')!=F.col('year'))|(F.month('prediction_ts')!=F.col('month'))).cast('long')).alias('bad_partition')]
    for index,val_year in enumerate([2021,2022,2023],1):
        role=F.col(f'fold{index}_role')
        bad=((role=='TRAIN')&((F.col('year')>=val_year)|(F.col('label_end_epoch_s')>=epoch(val_year))))|(
              (role=='VALIDATION')&((F.col('year')!=val_year)|(F.col('label_end_epoch_s')>=epoch(val_year+1))))|(
              (F.col('year')==2024)&(role!='UNUSED'))
        expressions.append(F.sum(bad.cast('long')).alias(f'bad_fold{index}_role'))
    final_bad=((F.col('final_role')=='TRAIN')&((F.col('year')>=2024)|(F.col('label_end_epoch_s')>=epoch(2024))))|(
        (F.col('final_role')=='TEST')&((F.col('year')!=2024)|(F.col('label_end_epoch_s')>=epoch(2025))))
    expressions.append(F.sum(final_bad.cast('long')).alias('bad_final_role'))
    m=targets.agg(*expressions).first().asDict()
    checks={'all_observations_have_anchor':m['anchors']==expected_rows,
        'primary_key_unique':m['anchors']==m['unique_keys'],
        'future_count_balance':m['anchors']==m['future_valid']+m['future_invalid'],
        'history_count_balance':m['anchors']==m['history_valid']+m['history_invalid'],
        'unknown_future_is_null':not m['invalid_nonnull_future'],'valid_future_nonnegative':not m['bad_valid_future'],
        'six_future_positions_accounted':not m['bad_future_slot_account'],'twelve_history_positions_accounted':not m['bad_history_slot_account'],
        'target_flags_consistent':not m['bad_target_flag'],'history_flags_consistent':not m['bad_history_flag'],
        'base_eligibility_consistent':not m['bad_eligibility_flag'],'label_endpoint_correct':not m['bad_label_end'],
        'partitions_correct':not m['bad_partition'],'all_e2_roles_safe':not any(m[f'bad_fold{i}_role'] for i in [1,2,3]) and not m['bad_final_role']}
    assert all(checks.values()),checks
    return m,checks


def manual_recompute(obs,targets,evidence,year):
    """Small independent stdlib Decimal oracle using actual timestamp lookups."""
    from pyspark.sql import functions as F
    # Manual examples use development/training years; never inspect 2024 label
    # distributions to choose a model, threshold or temporal protocol.
    if year>2020:
        return []
    candidates=[]
    conditions=[('valid_nonzero',F.col('target_valid')&(F.col('future_30m_mm')>0)),
                ('valid_zero',F.col('target_valid')&(F.col('future_30m_mm')==0)),
                ('internal_missing_future',~F.col('target_valid')),
                ('cross_day',F.to_date('prediction_ts')!=F.to_date('label_end_ts')),
                ('cross_month',F.month('prediction_ts')!=F.month('label_end_ts')),
                ('cross_year',~F.col('year_boundary_safe')),
                ('incomplete_history',~F.col('historical_valid'))]
    if year==2017:
        conditions.append(('alignment_transition',F.col('prediction_ts').between('2017-04-25 11:00:00','2017-04-25 11:25:00')&(F.col('station_id')=='S113')))
    for name,condition in conditions:
        for r in targets.filter(condition).orderBy('station_id','prediction_epoch_s').limit(1).collect():
            candidates.append((name,r.asDict(recursive=True)))
    if not candidates:
        raise RuntimeError('No manual examples found')
    wanted=[]
    for _,r in candidates:
        for off in HISTORY+FUTURE:
            wanted.append((r['station_id'],r['prediction_epoch_s']+off))
    # Semi-join a tiny list, keeping the large source scan entirely in Spark.
    keys=targets.sparkSession.createDataFrame(sorted(set(wanted)),['station_id','slot_end_epoch_s'])
    raw=obs.join(F.broadcast(keys),['station_id','slot_end_epoch_s'],'left_semi')
    lookup={(r['station_id'],r['slot_end_epoch_s']):r['rain_decimal_mm'] for r in raw.collect()}
    result=[];details=[]
    with localcontext() as context:
        context.prec=50
        for name,r in candidates:
            s=r['station_id'];t=r['prediction_epoch_s']
            missing_future=[off for off in FUTURE if (s,t+off) not in lookup]
            missing_history=[off for off in HISTORY if (s,t+off) not in lookup]
            total=None if missing_future else sum((lookup[(s,t+off)] for off in FUTURE),Decimal(0))
            passed=(total==r['future_30m_mm'] and missing_future==r['future_missing_offsets_s'] and missing_history==r['historical_missing_offsets_s']
                and (not missing_future)==r['target_valid'] and (not missing_history)==r['historical_valid'])
            assert passed,(name,r,total,missing_future,missing_history)
            result.append({'case':name,'station_id':s,'prediction_epoch_s':t,'prediction_ts':str(r['prediction_ts']),
                'future_30m_mm':total,'future_missing_offsets_s':missing_future,'historical_missing_offsets_s':missing_history,'passed':passed})
            for off in HISTORY+FUTURE:
                details.append({'case':name,'station_id':s,'prediction_epoch_s':t,'offset_s':off,
                    'slot_epoch_s':t+off,'present':(s,t+off) in lookup,'rain_decimal_mm':lookup.get((s,t+off))})
    write_csv(evidence/str(year)/'manual_recomputed_examples.csv',result)
    write_csv(evidence/str(year)/'manual_example_slots.csv',details)
    return result


def build_year(spark,year,source,out,evidence,expected):
    from pyspark.sql import functions as F
    started=time.monotonic()
    print(f'{now()} YEAR {year}: read minimal columns with prior/next-year context',flush=True)
    root=spark.read.parquet(source.as_posix())
    pruned=(F.col('year')==year)|((F.col('year')==year-1)&(F.col('month')==12))|((F.col('year')==year+1)&(F.col('month')==1))
    obs=root.filter(pruned).filter(F.col('slot_end_epoch_s').between(epoch(year)-3300,epoch(year+1)+1800))
    targets=build_targets(obs).filter(F.col('prediction_epoch_s').between(epoch(year),epoch(year+1)-1))
    path=out/'targets'/f'year={year}'
    targets.drop('year').repartition(24,'month').write.mode('overwrite').partitionBy('month').option('compression','snappy').parquet(path.as_posix())
    print(f'{now()} YEAR {year}: re-read targets, count/slot/role checks',flush=True)
    saved=spark.read.parquet(path.as_posix()).withColumn('year',F.lit(year))
    m,checks=metrics_and_checks(saved,expected)
    monthly=collect(saved.groupBy('station_id','month').agg(F.count('*').alias('anchors'),
        F.sum(F.col('target_valid').cast('long')).alias('future_valid'),
        F.sum(F.col('historical_valid').cast('long')).alias('history_valid'),
        F.sum(F.col('model_anchor_eligible').cast('long')).alias('base_model_eligible')).orderBy('station_id','month'))
    write_csv(evidence/str(year)/'station_month_target_quality.csv',[{'year':year,**r} for r in monthly])
    reasons=collect(saved.select(F.explode('invalid_reason').alias('reason')).groupBy('reason').count().orderBy('reason'))
    write_csv(evidence/str(year)/'future_invalid_reasons.csv',reasons,['reason','count'])
    history_reasons=collect(saved.select(F.explode('historical_invalid_reason').alias('reason')).groupBy('reason').count().orderBy('reason'))
    write_csv(evidence/str(year)/'history_invalid_reasons.csv',history_reasons,['reason','count'])
    roles=[]
    for name in ['fold1_role','fold2_role','fold3_role','final_role']:
        for r in collect(saved.groupBy(F.col(name).alias('role')).agg(F.count('*').alias('anchors'),
                    F.sum(F.col('model_anchor_eligible').cast('long')).alias('base_model_eligible'))):
            roles.append({'year':year,'protocol':name,**r})
    write_csv(evidence/str(year)/'e2_role_counts.csv',roles)
    examples=collect(saved.filter(~F.col('target_valid')|~F.col('historical_valid')).orderBy('station_id','prediction_epoch_s').limit(10))
    write_json(evidence/str(year)/'invalid_examples.json',examples)
    manual=manual_recompute(obs,saved,evidence,year)
    checks['manual_examples_pass']=all(r['passed'] for r in manual)
    result={'status':'PASS','year':year,'metrics':m,'checks':checks,'manual_cases':len(manual),
            'elapsed_seconds':round(time.monotonic()-started,2)}
    write_json(evidence/str(year)/'targets.json',result)
    write_json(evidence/'schemas'/'targets.json',saved.schema.jsonValue())
    print(f'{now()} YEAR {year}: PASS {m["anchors"]:,} anchors, {m["future_valid"]:,} complete future windows',flush=True)
    return result


def self_test(spark,out,evidence):
    from pyspark.sql import functions as F,types as T
    rows=[]
    def sequence(station,start,values):
        base=int(datetime.fromisoformat(start).replace(tzinfo=SGT).timestamp())
        rows.extend((station,base+300*i,Decimal(str(value))) for i,value in enumerate(values) if value is not None)
        return base
    normal=sequence('NORMAL','2020-06-01T12:00:00',[0]*12+['0.1','0.2',0,0,0,0])
    gap=sequence('GAP','2020-06-01T12:00:00',[0]*12+['0.1',None,'0.2',0,0,0,9])
    hist=sequence('HIST','2020-06-01T12:00:00',[0]*5+[None]+[0]*6+[0]*6)
    year2020=sequence('YEAR2020','2020-12-31T22:35:00',[0]*25)
    year2019=sequence('YEAR2019','2019-12-31T22:35:00',[0]*25)
    zero=sequence('ZERO','2020-06-01T12:00:00',[0]*18)
    start=sequence('START','2017-01-01T00:00:00',[0]*7)
    end=sequence('END','2024-12-31T23:00:00',[0]*12)
    schema=T.StructType([T.StructField('station_id',T.StringType()),T.StructField('slot_end_epoch_s',T.LongType()),
                         T.StructField('rain_decimal_mm',T.DecimalType(38,18))])
    obs=spark.createDataFrame(rows,schema)
    targets=build_targets(obs)
    targets.write.mode('overwrite').partitionBy('year','month').parquet((out/'targets').as_posix())
    saved=spark.read.parquet((out/'targets').as_posix()).cache();saved.count()
    m,checks=metrics_and_checks(saved,len(rows))
    lookup={(r['station_id'],r['prediction_epoch_s']):r.asDict(recursive=True) for r in saved.collect()}
    a=lookup[('NORMAL',normal+3300)];g=lookup[('GAP',gap+3300)];h=lookup[('HIST',hist+3300)]
    y=lookup[('YEAR2020',year2020+3300)];internal=lookup[('YEAR2019',year2019+3300)]
    z=lookup[('ZERO',zero+3300)];begin=lookup[('START',start)];tail=lookup[('END',end+3300)]
    checks.update({'exact_six_future_slots':a['future_n_valid']==6 and a['future_missing_offsets_s']==[],
        'decimal_sum_exact':a['future_30m_mm']==Decimal('0.3'),
        'strict_threshold_equality_negative':not(a['future_30m_mm']>Decimal('0.3')),
        'strict_threshold_above_positive':a['future_30m_mm']>Decimal('0.299999999999999999'),
        'future_gap_not_next_six_rows':g['future_n_valid']==5 and g['future_missing_offsets_s']==[600] and g['future_30m_mm'] is None,
        'history_gap_separate_from_truth':h['target_valid'] and not h['historical_valid'] and not h['model_anchor_eligible'],
        'zero_is_valid_observation':z['target_valid'] and z['future_30m_mm']==Decimal(0),
        'train_fold_boundary_purged':y['fold1_role']=='EXCLUDED_BOUNDARY',
        'same_anchor_other_fold_training':y['fold2_role']=='TRAIN' and y['fold3_role']=='TRAIN',
        'internal_training_year_boundary_kept':internal['fold1_role']=='TRAIN' and internal['model_anchor_eligible'],
        'calendar_year_crossing_truth_available':y['target_valid'] and not y['year_boundary_safe'],
        'dataset_start_history_flagged':'DATASET_START_CONTEXT_UNAVAILABLE' in begin['historical_invalid_reason'],
        'dataset_end_future_unknown':tail['future_30m_mm'] is None and 'DATASET_END_CONTEXT_UNAVAILABLE' in tail['invalid_reason'],
        'test_never_developer_fold':tail['fold1_role']==tail['fold2_role']==tail['fold3_role']=='UNUSED'})
    # Independent Python epoch-key oracle checks EVERY synthetic anchor, including
    # sparsity, midnight/year transitions and the record at the window endpoint.
    source={(s,t):v for s,t,v in rows}
    with localcontext() as context:
        context.prec=50
        for (s,t),r in lookup.items():
            fm=[off for off in FUTURE if (s,t+off) not in source]
            hm=[off for off in HISTORY if (s,t+off) not in source]
            truth=None if fm else sum((source[(s,t+off)] for off in FUTURE),Decimal(0))
            assert truth==r['future_30m_mm'] and fm==r['future_missing_offsets_s'] and hm==r['historical_missing_offsets_s']
    checks['all_synthetic_anchors_independently_recomputed']=True
    # Last minute at a fold boundary must be excluded even when the endpoint is
    # EXACTLY the next year; the corresponding rows are not globally deleted.
    exact=lookup[('YEAR2020',year2020+3300)]
    assert exact['label_end_epoch_s']==epoch(2021)
    checks['boundary_equality_excluded']=exact['fold1_role']=='EXCLUDED_BOUNDARY'
    assert all(checks.values()),checks
    write_json(evidence/'self_test.json',{'status':'PASS','checks':checks,'synthetic_anchors':len(rows)})
    saved.unpersist()
    print(f'PASS: {len(checks)} synthetic window/Decimal/E2/Parquet checks',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,default=ROOT/'data/processed/p0_3_v1/observations')
    parser.add_argument('--output',type=Path,default=ROOT/'data/processed'/VERSION)
    parser.add_argument('--evidence',type=Path,default=ROOT/'outputs/p0_4_targets'/VERSION)
    parser.add_argument('--self-test',action='store_true');parser.add_argument('--resume',action='store_true')
    args=parser.parse_args();source=args.source.resolve();out=args.output.resolve();evidence=args.evidence.resolve()
    for target in [out,evidence]:
        if not target.is_relative_to(ROOT) or target==ROOT or target.is_relative_to(ROOT/'Raw_Dataset') or target.is_relative_to(ROOT/'data/processed/p0_3_v1'):
            raise RuntimeError('Unsafe output directory: '+str(target))
    cfg=ROOT/'configs/project.json';config=json.loads(cfg.read_text(encoding='utf-8'))
    assert config['selected']['E']=='E2' and config['windows']['future_offsets_seconds']==FUTURE and config['windows']['historical_offsets_seconds']==HISTORY
    prior=ROOT/'outputs/p0_3_clean/p0_3_v1';previous=json.loads((prior/'summary.json').read_text(encoding='utf-8'))
    validation=json.loads((prior/'validation.json').read_text(encoding='utf-8'))
    assert validation['status']=='PASS' and all(validation['checks'].values())
    assert source==ROOT/'data/processed/p0_3_v1/observations'
    signature={'version':VERSION,'code_sha256':digest(Path(__file__)),'config_sha256':digest(cfg),
        'source_output_manifest_sha256':digest(prior/'output_manifest.json'),'source_validation_sha256':digest(prior/'validation.json'),
        'session_helper_sha256':digest(ROOT/'scripts/spark_session.py'),'runtime_helper_sha256':digest(ROOT/'scripts/spark_runtime.py'),
        'output':out.as_posix(),'source':source.as_posix(),'self_test':args.self_test}
    state_path=evidence/'run_status.json'
    if state_path.exists():
        old=json.loads(state_path.read_text(encoding='utf-8'))
        if not args.resume or old['signature']!=signature:
            raise RuntimeError('Existing output requires unchanged --resume, or new output AND evidence folders')
    elif out.exists() and any(out.iterdir()):
        raise RuntimeError('Output exists without matching run state')
    source_files=[]
    if not args.self_test:
        print(f'{now()} Verify all source observation Parquet SHA256',flush=True)
        manifest=json.loads((prior/'output_manifest.json').read_text(encoding='utf-8'))
        for entry in manifest['files']:
            path=ROOT/entry['path']
            if path.is_relative_to(source) and path.suffix=='.parquet':
                assert path.stat().st_size==entry['bytes'] and digest(path)==entry['sha256'],'Input changed: '+str(path)
                source_files.append({**entry,'mtime_ns':path.stat().st_mtime_ns})
        assert len(source_files)==96
        assert {p.resolve() for p in source.rglob('*.parquet')}=={(ROOT/r['path']).resolve() for r in source_files}
    state={'status':'RUNNING','started_at_sgt':now(),'signature':signature,'completed_years':[],'source_files':source_files}
    write_json(state_path,state)
    if not args.self_test:
        receipt_path=ROOT/'configs/processing_status.json';receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
        receipt['P0-4']={'status':'RUNNING','owner':'A','run_status':state_path.relative_to(ROOT).as_posix()}
        write_json(receipt_path,receipt)
    spark=None;hooks=[]
    try:
        spark,hooks=start_spark('P0-4-time-window-targets')
        if args.self_test:
            self_test(spark,out,evidence)
        else:
            annual=[]
            expected={r['year']:r['metrics']['observations'] for r in previous['annual_results']}
            for year in range(2017,2025):
                checkpoint=evidence/str(year)/'targets.json'
                if args.resume and checkpoint.exists():
                    result=json.loads(checkpoint.read_text(encoding='utf-8'))
                    assert result['status']=='PASS' and result['metrics']['anchors']==expected[year]
                    print(f'{now()} YEAR {year}: reuse completed checkpoint',flush=True)
                else:
                    result=build_year(spark,year,source,out,evidence,expected[year])
                annual.append(result);state['completed_years'].append(year);write_json(state_path,state)
            names=['anchors','future_valid','future_invalid','history_valid','history_invalid','base_model_eligible','future_valid_history_invalid','cross_calendar_year_anchors']
            totals={k:sum(r['metrics'][k] for r in annual) for k in names}
            checks={'all_years_pass':all(all(r['checks'].values()) for r in annual),
                'all_observation_anchors_preserved':totals['anchors']==previous['totals']['observations'],
                'future_count_balance':totals['anchors']==totals['future_valid']+totals['future_invalid'],
                'input_files_unchanged':all((ROOT/r['path']).stat().st_size==r['bytes'] and (ROOT/r['path']).stat().st_mtime_ns==r['mtime_ns'] for r in source_files)}
            assert all(checks.values()),checks
            write_json(evidence/'summary.json',{'status':'PASS','version':VERSION,'signature':signature,'annual_results':annual,
                'totals':totals,'checks':checks,'scope':'Truth/completeness/E2 roles only; no threshold expansion, features or model fitting'})
        state['status']='PASS'
    except BaseException as exc:
        state.update(status='FAILED',error=repr(exc));raise
    finally:
        if spark is not None:
            try:
                state['shutdown']=stop_spark(spark,hooks)
            except BaseException as exc:
                state.update(status='FAILED',shutdown_error=repr(exc));write_json(state_path,state);raise
        state['finished_at_sgt']=now();write_json(state_path,state)


if __name__=='__main__':
    main()
