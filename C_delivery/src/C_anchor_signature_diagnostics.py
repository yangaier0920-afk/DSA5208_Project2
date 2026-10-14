"""Explain exact row-hash mismatches without relaxing the acceptance rule."""
import argparse
from c_common import load,dump,spark_start,environment,KEYS
from C_check_rebuilt_anchors import signature

def diagnose(frame,role):
    from pyspark.sql import functions as F
    from pyspark.sql.types import FloatType,DoubleType
    columns=sorted(c for c in frame.columns if c!=role)
    numeric_float=[f.name for f in frame.schema.fields if isinstance(f.dataType,(FloatType,DoubleType)) and f.name!=role]
    stable=[c for c in columns if c not in numeric_float]
    aggregates=[F.count('*').alias('rows')]
    for i,c in enumerate(columns):
        # Key-paired single-column signatures localize differences; they are not equality proofs.
        keys=[k for k in KEYS if k!=c]
        aggregates += [F.sum(F.xxhash64(*keys,F.col(c)).cast('decimal(38,0)')).alias(f'h{i}'),F.sum(F.col(c).isNull().cast('long')).alias(f'z{i}')]
    values=frame.groupBy(role).agg(*aggregates).collect()
    byrole={r[role]:{'n':r['rows'],'columns':{c:{'key_paired_hash_sum':str(r[f'h{i}']),'nulls':r[f'z{i}']} for i,c in enumerate(columns)}} for r in values}
    return {'schema':frame.schema.jsonValue(),'sorted_hashed_columns':columns,'floating_columns':sorted(numeric_float),
        'exact_full_row_signature':signature(frame,role),'nonfloating_row_signature':signature(frame.select(role,*stable),role),
        'key_target_signature':signature(frame.select(role,*[c for c in ['station_id','prediction_epoch_s','year','future_30m_mm'] if c in frame.columns]),role),
        'role_column_diagnostics':byrole,'interpretation':'Exact typed hashes are sensitive to column set, data types and floating-point bits. Column hashes are diagnostics only; no tolerance-based PASS is granted.'}

def compare_diagnostics(reference,actual):
    before={f['name']:f['type'] for f in reference['schema']['fields']};after={f['name']:f['type'] for f in actual['schema']['fields']}
    fields=sorted(set(before)|set(after))
    changed={role:[c for c in actual['sorted_hashed_columns'] if reference.get('role_column_diagnostics',{}).get(role,{}).get('columns',{}).get(c)!=values['columns'][c]] for role,values in actual['role_column_diagnostics'].items()}
    return {'column_set_matches':set(before)==set(after),'type_differences':{c:{'expected':before.get(c),'actual':after.get(c)} for c in fields if before.get(c)!=after.get(c)},
        'full_row_hashes_match':reference['exact_full_row_signature']==actual['exact_full_row_signature'],
        'key_target_hashes_match':reference['key_target_signature']==actual['key_target_signature'],
        'nonfloating_hashes_match':reference['nonfloating_row_signature']==actual['nonfloating_row_signature'],
        'columns_with_key_paired_hash_or_null_count_differences':changed,'acceptance_granted':False}

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--anchors',required=True);ap.add_argument('--role',required=True);ap.add_argument('--config',required=True);ap.add_argument('--output',required=True);ap.add_argument('--reference-diagnostics');a=ap.parse_args()
    spark=spark_start(load(a.config))
    try:
        frame=spark.read.parquet(a.anchors);result=diagnose(frame,a.role)
        result.update(status='DIAGNOSTIC_ONLY',environment=environment(spark),timezone=spark.conf.get('spark.sql.session.timeZone'))
        if a.reference_diagnostics:
            result['comparison']=compare_diagnostics(load(a.reference_diagnostics),result);print(__import__('json').dumps(result['comparison'],indent=2))
        dump(a.output,result);print('Saved schema/key-target/per-column diagnostics; acceptance unchanged')
    finally:spark.stop()
if __name__=='__main__':main()
