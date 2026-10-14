"""Compare deterministic role-wise row signatures of rebuilt and historical inputs."""
import argparse
from c_common import *

def signature(frame,role):
    from pyspark.sql import functions as F
    columns=sorted(c for c in frame.columns if c!=role)
    # Two row-hash sum signatures are diagnostics, not cryptographic proof of equality.
    hashed=frame.select(role,F.xxhash64(*columns).cast('decimal(38,0)').alias('h1'),F.xxhash64(*columns,F.lit(20261014)).cast('decimal(38,0)').alias('h2'))
    return {r[role]:{'n':r.n,'hash_sum_1':str(r.h1),'hash_sum_2':str(r.h2)} for r in hashed.groupBy(role).agg(F.count('*').alias('n'),F.sum('h1').alias('h1'),F.sum('h2').alias('h2')).collect()}

def main():
    ap=argparse.ArgumentParser();group=ap.add_mutually_exclusive_group(required=True);group.add_argument('--original',type=Path);group.add_argument('--reference-receipt',type=Path);ap.add_argument('--rebuilt',type=Path,required=True);ap.add_argument('--receipt',type=Path,required=True);ap.add_argument('--config',type=Path);ap.add_argument('--fold',choices=['fold1','fold2','fold3','final']);a=ap.parse_args()
    config=load(a.config or (a.original or a.rebuilt)/'experiment_config.json');spark=spark_start(config);results=[]
    expected={r['fold']:r['role_signatures'] for r in load(a.reference_receipt)['folds']} if a.reference_receipt else None
    try:
        from pyspark.sql import functions as F
        for folder,role in [(f'fold{i}',f'fold{i}_role') for i in range(1,4)]+[('final','final_role')]:
            if a.fold and folder!=a.fold:continue
            new=spark.read.parquet(str(a.rebuilt/folder/'anchors'))
            if expected is None:
                old=spark.read.parquet(str(a.original/folder/'anchors'))
                if folder=='final':old=old.filter(F.col(role)=='TRAIN')
                assert sorted(old.columns)==sorted(new.columns)
                one=signature(old,role)
            else:one=expected[folder]
            two=signature(new,role)
            if one!=two:
                dump(a.receipt,{'status':'FAIL','reason':'Exact typed row signature mismatch; counts alone do not establish input identity','fold':folder,'expected':one,'actual':two,'actual_schema':new.schema.jsonValue(),'sorted_hashed_columns':sorted(c for c in new.columns if c!=role),'environment':environment(spark),'2024_test_scored':False,'diagnostic_command':'Run C_anchor_signature_diagnostics.py on this fold and compare with the separate reference diagnostics'})
                raise AssertionError('Exact input signature mismatch; expected/actual/schema saved to '+str(a.receipt))
            results.append({'fold':folder,'role_signatures':two});log('Rebuilt row signature PASS: '+folder)
        dump(a.receipt,{'status':'PASS','counts_and_two_rolewise_row_hash_sums_match':True,'all_key_target_feature_columns_included':True,'signature_not_cryptographic_equality_proof':True,'Parquet_bytes_expected_to_differ':True,'final_comparison_TRAIN_only':True,'2024_test_scored':False,'folds':results,'scope':'single_fold' if a.fold else 'three_development_folds_and_final_TRAIN'})
    finally:spark.stop()
if __name__=='__main__':main()
