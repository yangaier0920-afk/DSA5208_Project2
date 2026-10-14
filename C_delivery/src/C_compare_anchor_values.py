"""Read-only keyed comparison of reference/rebuilt anchors; never relax identity checks."""
import argparse
from c_common import load,dump,spark_start,environment,KEYS
from C_check_rebuilt_anchors import signature

def compare(spark,reference,actual,role,sample_rows=3):
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType
    from pyspark import StorageLevel
    schemas=lambda frame:{f.name:f.dataType.simpleString() for f in frame.schema.fields}
    before,after=schemas(reference),schemas(actual)
    report={'status':'FAIL','environment':environment(spark),'reference_schema':before,'actual_schema':after,'read_only':True,'tolerance_based_acceptance':False}
    if before!=after:
        report.update(reason='Column set/type mismatch; no automatic cast',schema_differences={c:{'reference':before.get(c),'actual':after.get(c)} for c in sorted(set(before)|set(after)) if before.get(c)!=after.get(c)})
        return report
    def key_stats(frame):
        r=frame.agg(F.count('*').alias('n'),F.countDistinct(F.struct(*KEYS)).alias('unique'),F.sum((F.col(KEYS[0]).isNull()|F.col(KEYS[1]).isNull()).cast('long')).alias('null_keys')).first().asDict()
        return r
    report['reference_keys']=key_stats(reference);report['actual_keys']=key_stats(actual)
    if any(r['n']!=r['unique'] or r['null_keys'] for r in [report['reference_keys'],report['actual_keys']]):
        report['reason']='Duplicate/null comparison keys';return report
    columns=sorted(c for c in reference.columns if c not in KEYS)
    old=reference.select(*KEYS,*[F.col(c).alias('ref_'+c) for c in columns]).withColumn('_ref_exists',F.lit(True))
    new=actual.select(*KEYS,*[F.col(c).alias('new_'+c) for c in columns]).withColumn('_new_exists',F.lit(True))
    joined=old.join(new,KEYS,'full').persist(StorageLevel.DISK_ONLY)
    matched=F.col('_ref_exists').isNotNull()&F.col('_new_exists').isNotNull()
    try:
        expressions=[F.count('*').alias('joined_rows'),F.sum(F.col('_ref_exists').isNull().cast('long')).alias('actual_only'),F.sum(F.col('_new_exists').isNull().cast('long')).alias('reference_only')]
        numeric={f.name for f in reference.schema.fields if isinstance(f.dataType,NumericType)}
        for i,c in enumerate(columns):
            x,y=F.col('ref_'+c),F.col('new_'+c);different=matched&~x.eqNullSafe(y)
            expressions += [F.sum(different.cast('long')).alias(f'd{i}'),F.sum((matched&(x.isNull()!=y.isNull())).cast('long')).alias(f'z{i}')]
            if c in numeric:expressions += [F.max(F.when(matched,F.abs(x.cast('double')-y.cast('double')))).alias(f'a{i}')]
        counts=joined.agg(*expressions).first().asDict();report['key_set_comparison']={c:counts[c] for c in ['joined_rows','actual_only','reference_only']};details=[]
        for i,c in enumerate(columns):
            count=counts[f'd{i}'];item={'column':c,'type':before[c],'exact_unequal_rows':count,'null_mismatch_rows':counts[f'z{i}']}
            if c in numeric:item['maximum_absolute_difference_double_diagnostic']=counts[f'a{i}']
            if count:
                rows=joined.filter(matched&~F.col('ref_'+c).eqNullSafe(F.col('new_'+c))).select(*KEYS,F.col('ref_'+role).alias('role'),F.col('ref_'+c).alias('reference_value'),F.col('new_'+c).alias('actual_value')).limit(sample_rows).collect()
                item['examples']=[r.asDict() for r in rows]
            details.append(item)
        report['column_comparison']=details
    finally:joined.unpersist()
    report['reference_signature']=signature(reference,role);report['actual_signature']=signature(actual,role)
    equal=not counts['actual_only'] and not counts['reference_only'] and not any(r['exact_unequal_rows'] for r in details)
    hashes=report['reference_signature']==report['actual_signature']
    report['status']='PASS' if equal and hashes else 'FAIL'
    report['reason']='Exact keyed values and original signatures match' if equal and hashes else ('Values match but original typed signatures differ; do not grant acceptance' if equal else 'Keyed column values or key sets differ; investigate changed fields')
    return report

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--reference-anchors',required=True);ap.add_argument('--actual-anchors',required=True);ap.add_argument('--role',default='fold1_role');ap.add_argument('--config',required=True);ap.add_argument('--output',required=True);ap.add_argument('--sample-rows',type=int,default=3);a=ap.parse_args()
    spark=spark_start(load(a.config))
    try:
        report=compare(spark,spark.read.parquet(a.reference_anchors),spark.read.parquet(a.actual_anchors),a.role,a.sample_rows)
        dump(a.output,report);print('Read-only keyed comparison: '+report['status']);print('Saved differences: '+a.output)
        if report['status']!='PASS':raise SystemExit(2)
    finally:spark.stop()
if __name__=='__main__':main()
