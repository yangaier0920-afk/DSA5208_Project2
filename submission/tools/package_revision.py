"""Package an explicitly selected C revision, preserving the original submission directory."""
import argparse,hashlib,json,shutil,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def load(p):return json.loads(p.read_text())
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',required=True,type=Path);ap.add_argument('--report-dir',required=True,type=Path);a=ap.parse_args();run=a.run.resolve();out=a.report_dir.resolve();out.mkdir(parents=True,exist_ok=True)
    selection=load(run/'selection.json');assert load(run/'reload_validation.json')['status']=='PASS';assert load(run/'compact_evidence_validation.json')['status']=='PASS'
    assert load(run/'original_preservation_check.json')['status']=='PASS';version=load(run/'experiment_config.json')['experiment_version']
    model=run/'package'/f'{version}_trained_model.zip';assert model.is_file()
    shutil.copyfile(model,out/'Trained_Model.zip')
    files={}
    for folder in ['src','scripts','configs','manifests','docs','data/samples','B_delivery','C_delivery','outputs','archive/history_v1/docs']:
        for p in (ROOT/folder).rglob('*'):
            if not p.is_file():continue
            rel=p.relative_to(ROOT)
            if any(x in rel.parts for x in ['__pycache__','runtime','tmp','anchors','predictions','preprocessor','reload_probe_scores','.ipynb_checkpoints','logs']):continue
            if 'models' in rel.parts and p.name!='conditional_cdf.json':continue
            if any('failed' in x for x in rel.parts) or p.name=='.DS_Store' or p.suffix=='.zip' or (p.suffix=='.crc' and 'trained_model' not in rel.parts):continue
            if 'C_delivery' in rel.parts and any(x.startswith('rebuilt_inputs_') for x in rel.parts):continue
            if rel.parts[0]=='B_delivery' and 'parquet' in rel.parts:continue
            files[rel.as_posix()]=p
    for folder in ['tools','reproduction','verification']:
        for p in (ROOT/'submission'/folder).rglob('*'):
            if p.is_file() and p.name!='.DS_Store' and '__pycache__' not in p.parts:files[p.relative_to(ROOT).as_posix()]=p
    for name in ['intro.txt','requirements-spark.txt','.gitignore','.gitattributes']:files[name]=ROOT/name
    files['README.md']=out/'README.md'
    for p in out.rglob('*'):
        if p.is_file() and 'archive' not in p.relative_to(out).parts and 'revision_20261013' not in p.relative_to(out).parts and 'C_completion' not in p.relative_to(out).parts and p.suffix!='.zip' and p.name not in ['submission_manifest.json','submission_status.json']:files[p.relative_to(ROOT).as_posix()]=p
    entries=[{'path':n,'bytes':p.stat().st_size,'sha256':sha(p)} for n,p in sorted(files.items())]
    with zipfile.ZipFile(out/'Source_Code.zip','w',zipfile.ZIP_DEFLATED) as z:
        for n,p in sorted(files.items()):z.write(p,'DSA5208_Project2/'+n)
        z.writestr('DSA5208_Project2/SOURCE_PAYLOAD_MANIFEST.json',json.dumps({'files':entries,'omitted':'Formal multi-GB data, B full feature Parquet, intermediate training anchors/predictions/models/caches; original and revision deployed models/compact metrics retained'},indent=2))
    with zipfile.ZipFile(out/'Source_Code.zip') as z:
        assert z.testzip() is None
        for i in entries:assert hashlib.sha256(z.read('DSA5208_Project2/'+i['path'])).hexdigest()==i['sha256']
    members=load(out/'members.json')['members'];assert 0<len(members)<=3 and all(r.get('name') and r.get('student_id') for r in members)
    status={'status':'PREPARED_CURRENT_INTEGRATED_DELIVERY','experiment_version':version,'selected_model':selection['selected_model'],
        'development_only_model_selection':True,'C_required_nonoptional_model_changes':'COMPLETE','full_fresh_machine_AB_reproduction':'NOT_PERFORMED','Windows_execution':'NOT_PERFORMED','original_2024_test_retained_not_rescored':True,'continuous_threshold_input':True,
        'threshold_performance_scope':'Nine development cuts 0..5 mm; empirical far tail not validated','model_reload':'PASS','compact_evidence_validation':'PASS','original_preservation':'PASS','Canvas_upload':'NOT_PERFORMED',
        'members':members,'language':'English integrated PDF and C revision sources; fetched B/history may be bilingual','source_payload_files':len(entries)}
    (out/'submission_status.json').write_text(json.dumps(status,indent=2)+'\n')
    names=['Final_Report.pdf','Final_Report_EN.md','Final_Report_Summary_EN.md','README.md','members.json','Source_Code.zip','Trained_Model.zip','submission_status.json','integration_sources.json','intro_requirements_check.md','requirements_and_review_acceptance.md','AI_USAGE_DECLARATION.md','remaining_improvement_ownership.md','C_REQUIRED_WORK_COMPLETION.md']
    manifest={'status':status['status'],'files':[{'path':n,'bytes':(out/n).stat().st_size,'sha256':sha(out/n)} for n in names]}
    (out/'submission_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    with zipfile.ZipFile(out/f'{version}_Submission.zip','w',zipfile.ZIP_DEFLATED) as z:
        for n in names+['submission_manifest.json']:z.write(out/n,n)
        for p in (out/'assets').glob('*.png'):z.write(p,'assets/'+p.name)
    print('Revision source/model/report payload hashes and ZIP CRC PASS:',out)
if __name__=='__main__':main()
