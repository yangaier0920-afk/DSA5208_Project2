"""Portable source/model/report submission payload with explicit requirements status."""
import hashlib,json,zipfile,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=ROOT/'submission'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
shutil.copy2(ROOT/'C_delivery/output/trained_model_C_v1.zip',OUT/'Trained_Model.zip')
files={}
for name in ['src','scripts','configs','manifests','docs','data/samples','B_delivery','C_delivery','outputs','archive/history_v1/docs','submission']:
 for p in (ROOT/name).rglob('*'):
  if not p.is_file():continue
  rel=p.relative_to(ROOT)
  if any(x in rel.parts for x in ['__pycache__','runtime','tmp','.ipynb_checkpoints']):continue
  if any('failed' in x for x in rel.parts):continue
  if rel.parts[0]=='B_delivery' and 'parquet' in rel.parts:continue
  if rel.parts[0]=='C_delivery' and any(x in rel.parts for x in ['anchors','predictions','models','preprocessor']):continue
  if rel.parts[0]=='submission' and (p.suffix=='.zip' or p.name in ['submission_manifest.json','submission_status.json']):continue
  if p.name=='.DS_Store':continue
  files[rel.as_posix()]=p
for name in ['intro.txt','requirements-spark.txt','.gitattributes','.gitignore']:
 files[name]=ROOT/name
files['README.md']=OUT/'README.md'
files['submission/reproduction/frozen_release_metadata.zip']=OUT/'reproduction/frozen_release_metadata.zip'
for item in load(ROOT/'manifests/release_manifest.json')['files']:
 if item['kind']!='full_data':
  p=ROOT/item['path'];assert p.is_file(),str(p)
  files.setdefault(item['path'],p)
entries=[{'path':n,'bytes':p.stat().st_size,'sha256':sha(p)} for n,p in sorted(files.items())]
archive=OUT/'Source_Code.zip'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for n,p in sorted(files.items()):z.write(p,'DSA5208_Project2/'+n)
 z.writestr('DSA5208_Project2/SOURCE_PAYLOAD_MANIFEST.json',json.dumps({'files':entries,'omitted':'full raw/processed datasets, B full feature Parquet, C fold anchors/predictions, caches and non-deployed fitted models; complete source/selected model/compact evidence shipped'},indent=2))
with zipfile.ZipFile(archive) as z:
 assert z.testzip() is None
 for i in entries:assert hashlib.sha256(z.read('DSA5208_Project2/'+i['path'])).hexdigest()==i['sha256']
 source_count=sum(1 for n in files if n.endswith('.py'))
 for n in ['src/01_audit_raw.py','src/02_clean_standardize.py','src/03_build_targets.py','B_delivery/src/B_task1_analysis.py','B_delivery/src/B_build_features.py','C_delivery/src/C_train.py','C_delivery/src/C_predict.py','submission/tools/prepare_frozen_runtime.py']:
  assert n in files,n
members=load(OUT/'members.json')['members'];member_ok=bool(members) and len(members)<=3 and all(m.get('name') and m.get('student_id') for m in members)
report=OUT/'Final_Report.pdf';assert report.is_file()
requirements={'2017_2024_Spark_processing':'COMPLETE','Task1_analysis_visualizations_discussion':'COMPLETE','Task2_Spark_MLlib_probabilities_and_evaluation':'COMPLETE',
 'cleaning_features_modelling_environment_limitations_AI_disclosure':'COMPLETE','complete_source_and_run_README':'COMPLETE',
 'trained_model_saved_reloaded_prediction_checked':'COMPLETE','integrated_PDF_report':'COMPLETE',
 'all_group_member_names_and_student_IDs':'COMPLETE' if member_ok else 'PENDING_USER_DETAILS',
 'Canvas_upload':'NOT_PERFORMED','arbitrary_continuous_threshold_support':'NOT_IMPLEMENTED_FIXED_FIVE_THRESHOLD_DOMAIN'}
status={'status':'READY_FOR_USER_SUBMISSION' if member_ok else 'CONTENT_COMPLETE_MEMBER_DETAILS_PENDING','requirements':requirements,
 'python_source_files':source_count,'source_payload_files':len(entries),'data_version':'v1','release_id':'rainfall_v1_r4_en',
 'selection_sha256':sha(ROOT/'C_delivery/results/C_v1_20261012/selection.json'),'no_model_retraining_or_test_tuning_during_integration':True,
 'threshold_domain':'0,0.5,1,2,5 mm only; see intro_requirements_check.md',
 'members':members,'deadline_from_intro':'2026-10-18','language':'English documents, notebooks and reports'}
(OUT/'submission_status.json').write_text(json.dumps(status,ensure_ascii=False,indent=2)+'\n')
outputs=['intro_requirements_check.md','english_translation_audit.json','Final_Report.pdf','Final_Report_EN.md','Final_Report_Summary_EN.md','README.md','members.json','Source_Code.zip','Trained_Model.zip','submission_status.json']
manifest={'status':status['status'],'files':[{'path':n,'bytes':(OUT/n).stat().st_size,'sha256':sha(OUT/n)} for n in outputs]}
(OUT/'submission_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
with zipfile.ZipFile(OUT/'DSA5208_Project2_Submission.zip','w',zipfile.ZIP_DEFLATED) as z:
 for n in outputs+['submission_manifest.json']:z.write(OUT/n,n)
 for p in (OUT/'assets').glob('*.png'):z.write(p,'assets/'+p.name)
print(status['status'],source_count,'Python source files;',len(entries),'source payload files')
print('Source/model/report ZIP payloads verified')
