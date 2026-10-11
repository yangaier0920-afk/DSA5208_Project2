"""Record output hashes after plots/report generation; retain measured stage times."""
import argparse,json,platform
from pathlib import Path
from b_common import PACKAGE,CONFIG,dump,metadata,release

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path)
    p.add_argument('--task1',type=Path,default=PACKAGE/'results/task1_v2')
    p.add_argument('--features',type=Path,default=PACKAGE/'results/features_v2')
    p.add_argument('--report',type=Path,default=PACKAGE/'report_zh')
    a=p.parse_args();d=release(a.root);verification=d.verify_release(a.root,'full')
    code=[{'path':p.relative_to(PACKAGE).as_posix(),'sha256':d.sha256(p)} for p in sorted((PACKAGE/'src').glob('*.py'))]
    for path in [a.task1,a.features]:
        old=json.loads((path/'result_metadata.json').read_text())
        metadata(a.root,path,old['command'],old['elapsed_seconds'],
            {'input_integrity':verification,'source_code_hashes':code,'finalized':True,'feature_set_version':'B_v2',
             'aggregate_provenance': 'Task1 v1 verified Spark aggregates reused; v2 derived analyses regenerated' if path==a.task1 else 'v2 full feature recomputation',
             'environment':old['environment'],
             'documentation_revision':'2026-10-11',
             'hardware_inventory':'results/hardware_inventory.json (post-run inventory; peak not measured)',
             'elapsed_definition':('from integrity check before Spark startup through aggregate CSV writes; excludes validation JSON, hashing, charts/report and Spark shutdown' if path==a.task1 else 'from integrity check through first feature validation/schema writes; includes read/write; excludes hashing, Spark shutdown and later 513-probe/adapter/join reruns')})
    metadata(a.root,PACKAGE/'results/report_metadata','python src/B_make_report.py; python src/B_finalize.py',None,
        {'source_code_hashes':code,'environment':{'python':platform.python_version(),'format':'Chinese Markdown only; original charts referenced'},
             'report_scope':'B contribution only',
             'report_file':str((a.report/'B_report_zh.md').resolve()),
             'report_sha256':d.sha256(a.report/'B_report_zh.md'),
            'input_integrity':verification})
    topfiles=[p for p in PACKAGE.rglob('*') if p.is_file()
        and 'parquet' not in p.relative_to(PACKAGE).parts and 'runtime' not in p.relative_to(PACKAGE).parts
        and 'archive' not in p.relative_to(PACKAGE).parts and '.ipynb_checkpoints' not in p.parts and '__pycache__' not in p.parts and p.name!='delivery_manifest.json' and not p.name.startswith('.')]
    features=list((a.features/'parquet').rglob('*.parquet'))
    dump(PACKAGE/'delivery_manifest.json',{
        **d.provenance(a.root),'member':'B','status':'COMPLETE_B_CONTRIBUTION',
        'config_hash_B':d.sha256(CONFIG),'feature_parquet_files':len(features),
        'feature_parquet_bytes':sum(p.stat().st_size for p in features),
        'feature_file_hashes':'results/features_v2/result_metadata.json',
        'small_deliverables':[{'path':p.relative_to(PACKAGE).as_posix(),'bytes':p.stat().st_size,'sha256':d.sha256(p)} for p in sorted(topfiles)]})
    print('All B result metadata and delivery hashes finalized')

if __name__=='__main__':main()
