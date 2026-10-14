"""Portable, run-local C artifact inventory and packaging; never replace another run."""
import argparse,sys,zipfile,importlib.metadata
from c_common import *

def dependency_versions():
    """Read the executing interpreter's installed metadata; no pip/uv executable required."""
    rows={(d.metadata.get('Name') or '').lower():d.version for d in importlib.metadata.distributions() if d.metadata.get('Name')}
    return '\n'.join(f'{name}=={version}' for name,version in sorted(rows.items()))+'\n'

def finalize(run,report=None,create_archive=True):
    run=Path(run).resolve();config=load(run/'experiment_config.json');version=config['experiment_version']
    revision=(run/'refit_status.json').exists()
    if revision:
        assert load(run/'refit_status.json')['status']=='PASS'
        assert load(run/'reload_validation.json')['status']=='PASS'
        assert load(run/'selection.json')['status']=='LOCKED'
    else:
        required=['pilot_run_status.json','develop_run_status.json','final_run_status.json','final_test.json','model_reload_validation.json']
        assert all(load(run/name)['status']=='PASS' for name in required)
        for name in ['fold1','fold2','fold3','final']:
            for test in ['actual_input_join_audit.json','train_expansion.json','eval_expansion.json']:assert load(run/name/test)['status']=='PASS'
    selection=load(run/'selection.json');config_hash=sha256(run/'experiment_config.json')
    if 'experiment_config_hash' in selection and config_hash!=selection['experiment_config_hash']:
        # v1 locked the byte hash of its original config; the run's normalized JSON has the same values.
        candidates=[run/'trained_model/experiment_config.json',PACKAGE/'configs/C_experiment.json']
        matches=[p for p in candidates if p.is_file() and sha256(p)==selection['experiment_config_hash'] and load(p)==config]
        assert not revision and len(matches)>0,'Original locked config bytes unavailable or values differ'
        config_hash=sha256(matches[0])
    report=Path(report).resolve() if report else run/'report'
    pdf=report/'C_report.pdf';assert pdf.is_file(),f'Generate the run-local PDF before finalizing: {pdf}'
    package=run/'package';package.mkdir(exist_ok=True)
    lock=package/'requirements.lock.txt';lock.write_text(dependency_versions())
    dump(package/'environment_capture.json',{'python_executable':sys.executable,'dependency_capture':'importlib.metadata in the executing interpreter; no machine-specific uv/pip path','python':platform.python_version()})
    artifact=run/'trained_model';archive=None
    for item in load(artifact/'artifact_manifest.json')['files']:
        assert sha256(artifact/item['path'])==item['sha256']
    if create_archive:
        archive=package/f'{version}_trained_model.zip'
        with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
            for p in sorted(artifact.rglob('*')):
                if p.is_file():z.write(p,Path('trained_model')/p.relative_to(artifact))
        with zipfile.ZipFile(archive) as z:
            assert z.testzip() is None
            for item in load(artifact/'artifact_manifest.json')['files']:
                assert hashlib.sha256(z.read('trained_model/'+item['path'])).hexdigest()==item['sha256']
    files=[]
    for p in sorted(run.rglob('*')):
        if not p.is_file():continue
        rel=p.relative_to(run)
        if any(x in rel.parts for x in ['anchors','predictions','preprocessor','models','runtime','tmp','__pycache__','logs']):continue
        if p.name in ['delivery_manifest.json','result_metadata.json'] or p.name=='.DS_Store':continue
        files.append({'path':rel.as_posix(),'bytes':p.stat().st_size,'sha256':sha256(p)})
    summary={'member':'C','status':'COMPLETE_C_CONTRIBUTION','experiment_version':version,'experiment_config_hash':config_hash,
        'python_executable':sys.executable,'selected_model':selection['selected_model'],'source_code_hashes':[{'path':p.relative_to(PACKAGE).as_posix(),'sha256':sha256(p)} for p in sorted((PACKAGE/'src').glob('*.py'))],
        'output_files':files,'report_file':str(pdf),'model_archive':str(archive) if archive else None,'model_directory':str(artifact),'archive_created':bool(archive),'created_at_utc':datetime.now(timezone.utc).isoformat(),
        'test_policy':config.get('test_policy','Original locked v1 final 2024 evaluation'),'shared_delivery_outputs_overwritten':False}
    dump(run/'result_metadata.json',summary);dump(run/'delivery_manifest.json',{'status':'COMPLETE_C_CONTRIBUTION','run_id':version,'files':files,'result_metadata_sha256':sha256(run/'result_metadata.json')})
    log('Portable run-local C finalization PASS: '+str(package));return archive

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',type=Path,required=True);ap.add_argument('--report',type=Path);ap.add_argument('--no-archive',action='store_true');a=ap.parse_args();finalize(a.run,a.report,create_archive=not a.no_archive)
if __name__=='__main__':main()
