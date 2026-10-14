"""Portable supplementary leaf-size study using reconstructed deterministic inputs."""
import argparse,subprocess,sys,shutil
from pathlib import Path
from c_common import load,dump,sha256

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--base-run',type=Path,required=True);ap.add_argument('--source-run',type=Path,required=True);ap.add_argument('--input-anchors',type=Path,required=True);ap.add_argument('--root',type=Path,required=True);ap.add_argument('--features',type=Path,required=True);ap.add_argument('--config',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);ap.add_argument('--check-against-original-anchors',action='store_true');ap.add_argument('--expected-anchor-signatures',type=Path);a=ap.parse_args()
    if not (a.check_against_original_anchors or a.expected_anchor_signatures):ap.error('An original-anchor comparison or expected signature receipt is required')
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=True);logs=out/'logs';logs.mkdir(exist_ok=True);code=Path(__file__).resolve().parent
    config=load(a.config);config['experiment_version']=out.name
    config_path=out/'experiment_config.json'
    if config_path.exists():assert load(config_path)==config
    else:dump(config_path,config)
    baseline=load(a.base_run/'experiment_config.json');names=baseline['continuous_candidates']+list(baseline['diagnostic_classifiers'])
    for name in ['group_station_selection.json']:
        target=out/name
        if not target.exists():shutil.copyfile(a.base_run/name,target)
    # All inherited completed diagnostics remain visible and explicitly labelled reused.
    for fold in range(1,4):
        src=a.base_run/f'fold{fold}';dst=out/f'fold{fold}';dst.mkdir(exist_ok=True)
        for p in src.glob('*'):
            if p.is_file() and p.suffix in ['.json','.csv'] and p.name!='sample_identity.json' and not (dst/p.name).exists():
                shutil.copyfile(p,dst/p.name)
                if p.name.endswith('_complete.json'):
                    receipt=load(dst/p.name);receipt.update(evidence_reused_from=str(src.resolve()),not_retrained_in_leaf_study=True);dump(dst/p.name,receipt)
        for name in baseline['continuous_candidates']:
            source=src/'models'/name/'conditional_cdf.json';target=dst/'models'/name/'conditional_cdf.json'
            if not target.exists():
                if not source.is_file():raise FileNotFoundError('Missing compact reference CDF: '+str(source)+'; use the current source package or regenerate the baseline run first')
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,target)
    def execute(script,arguments,label):
        dump(out/'C_pipeline_acceptance.json',{'status':'RUNNING','stage':label,'new_model_fold_cells_planned':6,'reference_model_fold_cells_reused':30,'all_36_trained_from_scratch':False})
        try:
            with (logs/(label+'.log')).open('w',encoding='utf-8') as stream:subprocess.run([sys.executable,str(code/script),*map(str,arguments)],stdout=stream,stderr=subprocess.STDOUT,check=True)
        except subprocess.CalledProcessError:
            dump(out/'C_pipeline_acceptance.json',{'status':'BLOCKED_INPUT_IDENTITY' if label=='rebuilt_anchor_comparison' else 'FAIL','failed_stage':label,'downstream_completion_claimed':False,'all_36_trained_from_scratch':False})
            raise
        print('PASS: '+label,flush=True)
    execute('C_test_distribution.py',[],'distribution_contracts')
    if a.check_against_original_anchors:
        execute('C_check_rebuilt_anchors.py',['--original',a.source_run,'--rebuilt',a.input_anchors,'--receipt',out/'rebuilt_anchor_comparison.json'],'rebuilt_anchor_comparison')
    elif a.expected_anchor_signatures:
        execute('C_check_rebuilt_anchors.py',['--reference-receipt',a.expected_anchor_signatures,'--rebuilt',a.input_anchors,'--receipt',out/'rebuilt_anchor_comparison.json'],'rebuilt_anchor_comparison')
    execute('C_revision_experiments.py',['--source-run',a.source_run,'--input-anchors',a.input_anchors,'--root',a.root,'--features',a.features,'--output',out,'--config',config_path],'development')
    for stage in ['select','refit','reload','guard']:
        execute('C_revision_select_refit.py',['--run',out,'--source-run',a.source_run,'--input-anchors',a.input_anchors,'--root',a.root,'--stage',stage],stage)
    execute('C_revision_validate.py',['--run',out],'compact_validation')
    dump(out/'C_pipeline_acceptance.json',{'status':'PASS','new_model_fold_cells_trained':6,'reference_model_fold_cells_reused':30,'all_36_trained_from_scratch':False,'execution_python':sys.executable,'configuration_sha256':sha256(config_path),'input_anchors':str(a.input_anchors.resolve()),'new_training_used_rebuilt_anchors':True,'final_TRAIN_only_through_2023':True,'known_2024_not_scored':True,'stages':['development','select','refit','reload','guard','compact_validation'],'fresh_machine_full_AB_pipeline_claimed':False,'Windows_executed':sys.platform=='win32'})
if __name__=='__main__':main()
