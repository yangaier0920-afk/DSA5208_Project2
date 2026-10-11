"""Audit one-to-one anchor joins before sampling/threshold expansion. Never silently deduplicate."""
import argparse,time
from pathlib import Path
from b_common import PACKAGE,csv_write,dump,log,release,start
KEYS=['station_id','prediction_epoch_s']

def audit_anchor_join(left,right,scope='C_pre_expansion',require_all_left=True,require_all_right=False):
    """left = C supervised anchors; right = B features. Invoke on actual C inputs.

    Audit precedes threshold expansion: repeated anchor/threshold rows are not valid inputs here.
    Returns evidence; caller performs the join only after PASS. No rain/target values are collected.
    """
    from pyspark.sql import functions as F
    def describe(frame):
        keys=frame.select(*KEYS)
        nulls=keys.filter(F.col(KEYS[0]).isNull()|F.col(KEYS[1]).isNull()).count()
        counts=keys.groupBy(*KEYS).count()
        info=counts.agg(F.count('*').alias('unique_keys'),F.sum('count').alias('rows'),
            F.sum(F.when(F.col('count')>1,F.lit(1)).otherwise(0)).alias('duplicate_key_groups'),
            F.sum(F.col('count')-1).alias('duplicate_excess_rows')).first().asDict()
        return keys,{k:int(v or 0) for k,v in info.items()}|{'null_key_rows':nulls}
    lk,ls=describe(left);rk,rs=describe(right)
    result={'scope':scope,**{'left_'+k:v for k,v in ls.items()},**{'right_'+k:v for k,v in rs.items()}}
    clean=ls['null_key_rows']==rs['null_key_rows']==ls['duplicate_excess_rows']==rs['duplicate_excess_rows']==0
    if not clean:
        result.update(status='FAIL',reason='Null or duplicate keys; join was not executed')
        return result
    unmatched_left=lk.join(rk,KEYS,'left_anti').count()
    unmatched_right=rk.join(lk,KEYS,'left_anti').count()
    inner_rows=lk.join(rk,KEYS,'inner').count()
    left_join_rows=lk.join(rk,KEYS,'left').count()
    result.update(left_unmatched_rows=unmatched_left,right_unmatched_rows=unmatched_right,
        matched_left_rows=ls['rows']-unmatched_left,inner_join_rows=inner_rows,left_join_rows=left_join_rows,
        left_match_rate=(ls['rows']-unmatched_left)/ls['rows'] if ls['rows'] else None,
        right_match_rate=(rs['rows']-unmatched_right)/rs['rows'] if rs['rows'] else None,
        row_inflation=left_join_rows-ls['rows'])
    ok=(inner_rows==ls['rows']-unmatched_left and left_join_rows==ls['rows']
        and (not require_all_left or unmatched_left==0) and (not require_all_right or unmatched_right==0))
    result['status']='PASS' if ok else 'FAIL'
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path)
    p.add_argument('--features',type=Path,default=PACKAGE/'results/features_v2')
    p.add_argument('--output',type=Path,default=PACKAGE/'results/join_audit')
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True);begin=time.monotonic()
    d=release(a.root);verification=d.verify_release(a.root,'full');spark=start(a.root,'B anchor join audit')
    try:
        from pyspark.sql import functions as F
        targets=d.read_tables(spark,a.root,'full',verify=False)['targets']
        features=spark.read.parquet(str(a.features/'parquet'))
        # Partitioned audits are globally valid only if each key's epoch year equals its partition year.
        for name,frame in [('targets',targets),('features',features)]:
            invalid=frame.filter(F.col('year').isNull()|~F.col('year').between(2017,2024)
                |F.col('prediction_epoch_s').isNull()
                |(F.year(F.from_unixtime('prediction_epoch_s').cast('timestamp'))!=F.col('year'))).limit(1).count()
            assert invalid==0,(name,'invalid year/key partition')
        evidence=[]
        for year in range(2017,2025):
            log(f'A targets / B features join audit: {year}')
            record=audit_anchor_join(targets.filter(F.col('year')==year),features.filter(F.col('year')==year),
                scope=f'published_targets_vs_B_features_{year}',require_all_right=True)
            record['year']=year;evidence.append(record);csv_write(a.output/'yearly_join_audit.csv',evidence)
            assert record['status']=='PASS',record
        integer_fields=['left_rows','right_rows','left_unique_keys','right_unique_keys','left_duplicate_key_groups',
            'right_duplicate_key_groups','left_duplicate_excess_rows','right_duplicate_excess_rows',
            'left_null_key_rows','right_null_key_rows','left_unmatched_rows','right_unmatched_rows',
            'inner_join_rows','left_join_rows','row_inflation']
        total={k:sum(r[k] for r in evidence) for k in integer_fields}
        assert total['left_rows']==targets.count() and total['right_rows']==features.count()
        total.update(left_match_rate=1.0,right_match_rate=1.0,status='PASS')
        dump(a.output/'validation.json',{'status':'PASS','scope':'published_A_targets_vs_B_features_pre_sampling_pre_threshold',
            'C_actual_training_input_status':'PENDING_NOT_AVAILABLE_LOCALLY','totals':total,'year_partition_consistency':True,
            'input_integrity':verification,'elapsed_seconds':time.monotonic()-begin,
            'timing':'includes integrity, Spark startup, reads and all join audits; excludes Spark shutdown'})
        log('Full base-table join audit PASS; C must audit its actual assembled inputs separately')
    finally:spark.stop()

if __name__=='__main__':main()
