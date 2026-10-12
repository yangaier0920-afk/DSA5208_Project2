"""Capture actual C artifacts, checks, config, code and input provenance."""
import argparse,subprocess,zipfile
from c_common import *

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',type=Path,required=True);a=ap.parse_args();run=a.run.resolve()
    required=['pilot_run_status.json','develop_run_status.json','final_run_status.json','final_test.json','model_reload_validation.json']
    assert all(load(run/name)['status']=='PASS' for name in required)
    assert load(PACKAGE/'results/contract_tests.json')['status']=='PASS'
    for name in ['fold1','fold2','fold3','final']:
        for test in ['actual_input_join_audit.json','train_expansion.json','eval_expansion.json']:
            assert load(run/name/test)['status']=='PASS'
    report=PACKAGE/'report_zh/C_report_zh.md';pdf=PACKAGE/'output/pdf/C_report_zh.pdf'
    assert report.is_file() and pdf.is_file()
    config_hash=sha256(CONFIG);assert config_hash==load(run/'selection.json')['experiment_config_hash']
    dependency_versions=subprocess.check_output([str(ROOT/'.runtime/bin/uv'),'pip','freeze','--python',str(ROOT/'.venv/bin/python')],text=True)
    (PACKAGE/'requirements-C.lock.txt').write_text(dependency_versions)
    artifact=run/'trained_model';archive=PACKAGE/'output/trained_model_C_v1.zip'
    archive.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as zipped:
        for p in sorted(artifact.rglob('*')):
            if p.is_file():zipped.write(p,Path('trained_model')/p.relative_to(artifact))
    with zipfile.ZipFile(archive) as zipped:
        assert zipped.testzip() is None
        for item in load(artifact/'artifact_manifest.json')['files']:
            assert hashlib.sha256(zipped.read('trained_model/'+item['path'])).hexdigest()==item['sha256']
    files=[]
    for p in sorted(PACKAGE.rglob('*')):
        if not p.is_file():continue
        rel=p.relative_to(PACKAGE)
        if '__pycache__' in rel.parts or 'runtime' in rel.parts or 'tmp' in rel.parts or any('failed' in part for part in rel.parts):continue
        if 'logs' in rel.parts and p.name=='finalize.log':continue  # This command's live output is not a stable artifact.
        if p.name in ['result_metadata.json','delivery_manifest.json']:continue
        files.append({'path':rel.as_posix(),'bytes':p.stat().st_size,'sha256':sha256(p)})
    summary={'member':'C','status':'COMPLETE_C_CONTRIBUTION','experiment_version':'C_v1_20261012',
        'experiment_config_hash':config_hash,'input_integrity':load(run/'input_integrity.json'),
        'command':'bash C_delivery/run_C.sh; C_make_report.py; C_finalize.py',
        'environment':load(run/'environment.json'),'selected_model':load(run/'selection.json')['selected_model'],
        'final_test':load(run/'final_test.json'),'checks':'contract tests, actual post-sample joins, threshold key cardinality, persisted-model reload, past-only API',
        'source_code_hashes':[{'path':p.relative_to(PACKAGE).as_posix(),'sha256':sha256(p)} for p in sorted((PACKAGE/'src').glob('*.py'))],
        'output_files':files,'report_file':str(report),'pdf_file':str(pdf),
        'full_group_submission_status':'A/B/C report merge and human names/student IDs/review remain outside C contribution',
        'created_at_utc':datetime.now(timezone.utc).isoformat()}
    dump(run/'result_metadata.json',summary)
    dump(PACKAGE/'delivery_manifest.json',{'status':'COMPLETE_C_CONTRIBUTION','run':str(run),
        'result_metadata_sha256':sha256(run/'result_metadata.json'),'files':files})
    log('C evidence and artifact hashes finalized')

if __name__=='__main__':main()
