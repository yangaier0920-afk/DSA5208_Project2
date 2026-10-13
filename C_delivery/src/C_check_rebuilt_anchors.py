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
    ap=argparse.ArgumentParser();group=ap.add_mutually_exclusive_group(required=True);group.add_argument('--original',type=Path);group.add_argument('--reference-receipt',type=Path);ap.add_argument('--rebuilt',type=Path,required=True);ap.add_argument('--receipt',type=Path,required=True);a=ap.parse_args()
    config=load((a.original or a.rebuilt)/'experiment_config.json');spark=spark_start(config);results=[]
    expected={r['fold']:r['role_signatures'] for r in load(a.reference_receipt)['folds']} if a.reference_receipt else None
    try:
        from pyspark.sql import functions as F
        for folder,role in [(f'fold{i}',f'fold{i}_role') for i in range(1,4)]+[('final','final_role')]:
            new=spark.read.parquet(str(a.rebuilt/folder/'anchors'))
            if expected is None:
                old=spark.read.parquet(str(a.original/folder/'anchors'))
                if folder=='final':old=old.filter(F.col(role)=='TRAIN')
                assert sorted(old.columns)==sorted(new.columns)
                one=signature(old,role)
            else:one=expected[folder]
            two=signature(new,role);assert one==two,(folder,one,two)
            results.append({'fold':folder,'role_signatures':two});log('Rebuilt row signature PASS: '+folder)
        dump(a.receipt,{'status':'PASS','counts_and_two_rolewise_row_hash_sums_match':True,'all_key_target_feature_columns_included':True,'signature_not_cryptographic_equality_proof':True,'Parquet_bytes_expected_to_differ':True,'final_comparison_TRAIN_only':True,'2024_test_scored':False,'folds':results})
    finally:spark.stop()
if __name__=='__main__':main()
