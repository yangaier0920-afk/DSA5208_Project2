"""Regenerate v1-compatible development/refit TRAIN anchors from formal A/B tables only."""
import argparse
from pathlib import Path
from c_common import *
from C_train import prepare_fold

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',required=True,type=Path);ap.add_argument('--features',required=True,type=Path);ap.add_argument('--reference-run',required=True,type=Path);ap.add_argument('--output',required=True,type=Path);a=ap.parse_args()
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=True);config=load(a.reference_run/'experiment_config.json');verify_release(a.root,'full')
    spark=spark_start(config)
    try:
        from pyspark.sql import functions as F
        manifest=load(a.root/'manifests/release_manifest.json');targets=spark.read.parquet(str(a.root/manifest['tables']['targets']['path']));features=spark.read.parquet(str(a.features/'parquet'))
        for i,year in enumerate([2021,2022,2023],1):
            dest=out/f'fold{i}'
            if (dest/'anchors').exists():continue
            dest.mkdir(exist_ok=True)
            train,val,stations=prepare_fold(spark,targets,features,config,[0,.5,1,2,5],f'fold{i}_role','VALIDATION',year,.1,dest)
            old=load(a.reference_run/f'fold{i}/coverage.json');new=load(dest/'coverage.json')
            assert new['sampled_train_anchors']==old['sampled_train_anchors'] and new['scored_eval_anchors']==old['scored_eval_anchors']
            dump(dest/'status.json',{'status':'PASS','station_ids':stations,'year':year})
        # Only final TRAIN is rebuilt. No 2024 test anchors or labels are selected.
        if not (out/'final/anchors').exists():
            train=sampled(targets.filter((F.col('year')<=2023)&F.col('model_anchor_eligible')&(F.col('final_role')=='TRAIN')).select(*KEYS,'year','future_30m_mm','final_role'),.1,config['seed'])
            right=features.filter(F.col('year')<=2023).select(*KEYS,*NUMERIC_FEATURES)
            audit=audit_anchor_join(train,right,scope='Reconstructed_2017_2023_final_TRAIN_only');assert audit['status']=='PASS'
            joined=train.join(right,KEYS);assert joined.count()==load(a.reference_run/'final/coverage.json')['sampled_train_anchors']
            joined.write.partitionBy('final_role').parquet(str(out/'final/anchors'));dump(out/'final/actual_input_join_audit.json',audit)
        # Copy immutable compact reference evidence, not its missing intermediate hashes.
        import shutil
        for p in a.reference_run.rglob('*'):
            if not p.is_file():continue
            rel=p.relative_to(a.reference_run)
            if any(x in rel.parts for x in ['anchors','predictions','models','preprocessor','trained_model','package','report','logs']):continue
            if p.suffix not in ['.json','.csv','.txt']:continue
            if p.name in ['result_metadata.json','original_run_guard.json']:continue
            target=out/rel
            if not target.exists():target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target)
        files=[{'path':p.relative_to(out).as_posix(),'bytes':p.stat().st_size,'sha256':sha256(p)} for p in out.rglob('*') if p.is_file() and 'anchors' in p.parts]
        # Rebuilt Parquet bytes differ from original writes; deterministic row counts/roles and new joins are verified.
        dump(out/'result_metadata.json',{'status':'RECONSTRUCTED_SOURCE_ANCHORS','output_files':files,'reference_run':str(a.reference_run.resolve()),'file_path_scope':'relative_to_source',
             '2024_rebuilt_or_scored':False,'byte_identical_to_original_parquet':False,'deterministic_sampling_and_counts_match_reference':True})
        log('Development anchors plus final 2017–2023 TRAIN rebuilt; no new 2024 scoring')
    finally:spark.stop()
if __name__=='__main__':main()
