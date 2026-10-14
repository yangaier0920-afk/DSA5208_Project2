"""Run the supported portable pipeline in a fresh directory, without creating ZIPs."""
import argparse,json,os,subprocess,sys
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from portable_runtime import configure_runtime

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S'));ap.add_argument('--output',type=Path);ap.add_argument('--preflight-only',action='store_true');ap.add_argument('--resume',action='store_true',help='Resume the same failed run, preserving its receipt and explicitly reusing completed stages');a=ap.parse_args()
    if not a.run_id or any(not (c.isalnum() or c in '_-') for c in a.run_id):ap.error('Invalid run ID')
    out=(a.output or ROOT/'outputs'/('reproduction_'+a.run_id)).resolve()
    prior=None
    if a.resume:
        prior=json.loads((out/'run_status.json').read_text(encoding='utf-8'))
        if prior['status'] not in ['FAIL','BLOCKED_INPUT_IDENTITY']:raise RuntimeError('Resume only an explicitly failed run')
        backup=out/('run_status_previous_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')+'.json')
        backup.write_text(json.dumps(prior,indent=2),encoding='utf-8')
    else:
        out.mkdir(parents=True,exist_ok=False);(out/'logs').mkdir()
    status={'status':'RUNNING','stages':[],'all_36_trained_from_scratch':False,'reference_model_fold_cells_reused':30,'new_model_fold_cells_planned':6,'new_2024_evaluation':False,'zip_created':False,'A_scope':'Representative frozen A sample replay; formal full A input is verified'}
    status['resumed_run']=bool(prior)
    if prior:status['prior_failure']=prior.get('error');status['completed_stages_reused']=[x['name'] for x in prior['stages'] if x['status']=='PASS']
    def save():
        (out/'run_status.json').write_text(json.dumps(status,indent=2),encoding='utf-8')
    def run(script,args,label):
        if prior and any(x['name']==label and x['status']=='PASS' for x in prior['stages']):
            status['stages'].append({'name':label,'status':'PASS','reused_from_previous_attempt':True});save();print('REUSED PASS: '+label,flush=True);return
        status['stages'].append({'name':label,'status':'RUNNING'});save()
        with (out/'logs'/(label+'.log')).open('w',encoding='utf-8') as f:
            result=subprocess.run([sys.executable,str(ROOT/script),*map(str,args)],cwd=ROOT,stdout=f,stderr=subprocess.STDOUT)
        status['stages'][-1]['status']='PASS' if result.returncode==0 else 'FAIL';save()
        if result.returncode:
            receipt=out/'C_model/C_pipeline_acceptance.json'
            status['status']='BLOCKED_INPUT_IDENTITY' if receipt.exists() and json.loads(receipt.read_text())['status']=='BLOCKED_INPUT_IDENTITY' else 'FAIL'
            raise RuntimeError(f'{label} failed; inspect {out / "logs" / (label+".log")}')
        print('PASS: '+label,flush=True)
    save()
    try:
        status['environment']=configure_runtime();save()
        if a.preflight_only:
            status['status']='PREFLIGHT_PASS_NOT_FULL_RUN';save();return
        data=ROOT/'.runtime/frozen_root';task=out/'B_task1';features=out/'B_features';anchors=out/'C_inputs';model=out/'C_model';source=ROOT/'C_delivery/results/C_v1_20261012'
        os.environ['B_SPARK_LOCAL_DIR']=str(out/'spark/B');os.environ['C_SPARK_LOCAL_DIR']=str(out/'spark/C')
        run('submission/tools/prepare_frozen_runtime.py',[],'frozen_inputs')
        run('scripts/read_data_release.py',['--root',data,'--member','C','--scope','full','--receipt',out/'full_read.json'],'formal_input_verification')
        run('scripts/reproduce_a_sample.py',['--root',data,'--output',out/'A_sample'],'A_sample')
        run('B_delivery/src/B_task1_analysis.py',['--root',data,'--output',task],'B_task1')
        run('B_delivery/src/B_coverage_sensitivity.py',['--input',task],'B_coverage_sensitivity')
        run('B_delivery/src/B_plot_results.py',['--input',task,'--root',data],'B_plots')
        run('B_delivery/src/B_build_features.py',['--root',data,'--output',features],'B_features')
        run('B_delivery/src/B_validate_feature_api.py',['--root',data,'--features',features],'B_feature_contract')
        run('B_delivery/src/B_join_audit.py',['--root',data,'--features',features,'--output',out/'B_join'],'B_join')
        run('C_delivery/src/C_revision_rebuild_anchors.py',['--root',data,'--features',features,'--reference-run',source,'--output',anchors],'C_inputs')
        run('C_delivery/src/C_run_leaf_study.py',['--base-run',ROOT/'C_delivery/results/C_revision_20261013','--source-run',source,'--input-anchors',anchors,'--root',data,'--features',features,'--config',ROOT/'C_delivery/configs/C_leaf_study.json','--output',model,'--expected-anchor-signatures',ROOT/'C_delivery/results/C_leaf_study_20261014/rebuilt_anchor_comparison.json'],'C_locked_protocol')
        run('C_delivery/src/C_revision_report.py',['--run',model,'--source-run',source],'C_report')
        run('C_delivery/src/C_finalize.py',['--run',model,'--no-archive'],'C_inventory')
        run('submission/tools/build_report.py',['--task1',task,'--c-run',source,'--revision-run',model,'--output',out/'report'],'integrated_report')
        run('submission/tools/build_english_summary.py',['--report-dir',out/'report'],'english_summary')
        status['status']='PASS_CURRENT_LOCKED_PROTOCOL';save()
    except Exception as error:
        if status['status']=='RUNNING':status['status']='FAIL'
        status['error']=str(error);save();raise
if __name__=='__main__':main()
