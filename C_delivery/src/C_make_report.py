"""Create English C report and figures strictly from executed experiment evidence."""
import argparse, csv, html, json, statistics, subprocess, platform, os
from pathlib import Path
from c_common import PACKAGE,load,dump,csv_write,sha256,CONFIG

NAMES={'frequency_baseline':'Frequency baseline','logistic_regression':'Logistic regression','decision_tree':'Decision tree'}

def rows(path):
    with Path(path).open() as f:return list(csv.DictReader(f))

def table(headers, data):
    return '\n'.join(['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+
                     ['| '+' | '.join(str(v) for v in r)+' |' for r in data])

def fmt(x):return 'NA' if x is None else f'{x:.6f}'

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',type=Path,required=True);ap.add_argument('--output',type=Path);a=ap.parse_args();run=a.run.resolve()
    selection=load(run/'selection.json');final=load(run/'final_test.json');reload=load(run/'model_reload_validation.json')
    assert selection['status']=='LOCKED' and final['status']==reload['status']=='PASS'
    config=load(run/'experiment_config.json');winner=selection['selected_model'];thresholds=selection['thresholds']
    report=(a.output or run/'report').resolve();figs=report/'figures';figs.mkdir(parents=True,exist_ok=True)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'savefig.dpi':180})
    colors={'frequency_baseline':'#8a929c','logistic_regression':'#087f8c','decision_tree':'#dc7a20'}
    labels={'frequency_baseline':'Frequency baseline','logistic_regression':'Logistic regression','decision_tree':'Decision tree'}
    fig,ax=plt.subplots(figsize=(8,3.4))
    for s in selection['scores']:
        ys=s['year_scores'];ax.plot([2021,2022,2023],[ys[str(y)] for y in [2021,2022,2023]],'o-',color=colors[s['model']],label=labels[s['model']])
    ax.set(xlabel='Validation year',ylabel='Equal-threshold mean Brier (lower is better)',xticks=[2021,2022,2023]);ax.grid(alpha=.18);ax.legend();fig.tight_layout()
    fig.savefig(figs/'01_rolling_brier.png');fig.savefig(figs/'01_rolling_brier.pdf');plt.close(fig)
    base={m['threshold_mm']:m for m in final['metrics'] if m['model']=='frequency_baseline'}
    selected={m['threshold_mm']:m for m in final['metrics'] if m['model']==winner}
    fig,axes=plt.subplots(1,2,figsize=(9,3.3));x=list(range(len(thresholds)))
    axes[0].bar([i-.18 for i in x],[base[t]['brier'] for t in thresholds],width=.36,color=colors['frequency_baseline'],label='Baseline')
    axes[0].bar([i+.18 for i in x],[selected[t]['brier'] for t in thresholds],width=.36,color=colors[winner],label='Selected model')
    skill=[1-selected[t]['brier']/base[t]['brier'] for t in thresholds]
    axes[1].bar(x,skill,color=colors[winner]);axes[1].axhline(0,color='#333',lw=.8)
    for ax in axes:ax.set_xticks(x,[str(t) for t in thresholds]);ax.set_xlabel('Rainfall threshold (mm)');ax.grid(axis='y',alpha=.18)
    axes[0].set_ylabel('Brier score');axes[0].legend();axes[1].set_ylabel('Brier skill vs training frequency');fig.suptitle('Locked 2024 final evaluation');fig.tight_layout()
    fig.savefig(figs/'02_final_brier.png');fig.savefig(figs/'02_final_brier.pdf');plt.close(fig)
    calibration=rows(run/f'final/{winner}_calibration.csv')
    fig,axes=plt.subplots(1,len(thresholds),figsize=(12,2.8),sharex=True,sharey=True)
    for ax,t in zip(axes,thresholds):
        rr=[r for r in calibration if float(r['threshold_mm'])==t]
        ax.plot([0,1],[0,1],'--',color='#999',lw=1)
        ax.plot([float(r['mean_probability']) for r in rr],[float(r['observed_rate']) for r in rr],'o-',ms=3,color=colors[winner])
        ax.set(title=f'>{t:g} mm',xlabel='Mean predicted p',xlim=(-.02,1.02),ylim=(-.02,1.02));ax.grid(alpha=.15)
    axes[0].set_ylabel('Observed event rate');fig.suptitle('2024 reliability; fixed bins, unequal sample counts');fig.tight_layout()
    fig.savefig(figs/'03_final_reliability.png');fig.savefig(figs/'03_final_reliability.pdf');plt.close(fig)
    coverage={name:load(run/name/'coverage.json') for name in ['fold1','fold2','fold3','final']}
    covdata=[]
    for name,c in coverage.items():
        covdata.append([name,f"{c['sampled_train_anchors']:,}",f"{c['all_observed_anchors']:,}",f"{c['eligible_eval_before_station_filter']:,}",
            f"{c['unknown_station_eval_anchors']:,}",f"{c['scored_eval_anchors']:,}",f"{100*c['scored_fraction_of_observed']:.3f}%"])
    scoredata=[[NAMES[s['model']],fmt(s['year_scores']['2021']),fmt(s['year_scores']['2022']),fmt(s['year_scores']['2023']),fmt(s['mean_brier']),fmt(s['year_range'])] for s in selection['scores']]
    finaldata=[[f'{t:g}',f"{selected[t]['n']:,}",f"{100*selected[t]['positive_rate']:.3f}%",fmt(base[t]['brier']),fmt(selected[t]['brier']),f'{100*skill[i]:.2f}%',fmt(selected[t]['log_loss']),fmt(selected[t]['pr_auc']),fmt(selected[t]['roc_auc'])] for i,t in enumerate(thresholds)]
    dev=rows(run/'development_metrics.csv');devdata=[]
    for m in dev:devdata.append([m['year'],NAMES[m['model']],f"{float(m['threshold_mm']):g}",m['n'],fmt(float(m['positive_rate'])),fmt(float(m['brier'])),fmt(float(m['log_loss'])),fmt(float(m['pr_auc'])) if m['pr_auc'] else 'NA',fmt(float(m['roc_auc'])) if m['roc_auc'] else 'NA'])
    best=next(s for s in selection['scores'] if s['model']==winner);baseselection=next(s for s in selection['scores'] if s['model']=='frequency_baseline')
    final_basemean=sum(base[t]['brier'] for t in thresholds)/len(thresholds)
    finalskill=1-final['mean_brier']/final_basemean
    fit=load(run/f'final/{winner}_fit.json') if winner!='frequency_baseline' else {}
    diagnosis=load(run/f'final/{winner}_evaluation.json')
    times={name:load(run/name/'timing.json')['elapsed_seconds'] for name in ['fold1','fold2','fold3','final']}
    environment=load(run/'environment.json')
    if platform.system()=='Darwin':
        hardware={'chip_type':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip(),
            'physical_memory':str(int(subprocess.check_output(['sysctl','-n','hw.memsize'],text=True))//(1024**3))+' GB',
            'source':'post-run sysctl public CPU/memory inventory; peak not measured'}
    else:hardware={'chip_type':platform.processor() or platform.machine(),'physical_memory':'not measured',
                   'source':'post-run platform public inventory; peak not measured'}
    dump(run/'hardware_inventory.json',hardware)
    sections=[]
    def add(t):sections.append(t.strip())
    add('# Next-30-minute rainfall exceedance probability: C report\n\nExperiment '+config['experiment_version']+'; B_v2 features; data v1/rainfall_v1_r3. This is a C chapter; the integrated report and member details are in submission.')
    add(f"## Results\n\nPrespecified equal-threshold/equal-year Brier selected {NAMES[winner]}: development mean {best['mean_brier']:.6f}. After selection, refit on a fixed 10% 2017–2023 anchor sample and evaluate all supported 2024 anchors once: mean Brier {final['mean_brier']:.6f}, training-frequency baseline {final_basemean:.6f}, skill {100*finalskill:.2f}%. Positive skill means improvement, within the documented data/eligibility scope.")
    add('## Truth, time and supported inputs\n\nPredict P(next-30-minute total > threshold). A uses Decimal(38,18) for the six exact endpoints t+300,...,t+1800; equality is negative, unknown future is not negative. Supervision filters the correct role, full past/future windows and actual temporal boundary, including endpoint equality to the next block start. Internal training-year crossings remain usable. Past history requires 12 exact positions t−3300,...,t. Current interval (t−5min,t] is assumed available at t; official interval meaning and publication delay remain unverified.\n\nUse B 26 past-only numeric features, numeric threshold and training-only station one-hot coding. Future truth/completeness/eligibility/role are excluded. Inference checks only existing history, supported station, grid and threshold. Support is frozen from earliest full 2017–2020 training: at least 100 positives/negatives per candidate. Thresholds 0,0.5,1,2,5 mm all pass; arbitrary continuous-threshold generalization is not claimed.')
    add('## E2 and model selection\n\nExpanding folds train 2017–2020/validate 2021, 2017–2021/2022 and 2017–2022/2023; lock then refit 2017–2023/test 2024. A 1% pilot checks implementation; formal training samples 10%, evaluation is full eligible scope. pmod(xxhash64(station_id,prediction_epoch_s,seed),10000), seed=20261012, samples anchors before all threshold versions. Sampling is independent of outcomes with no class reweighting. Fit station StringIndexer/OneHotEncoder and LR standardization on each training fold only.\n\nCandidates: per-threshold training frequency; L2 LR regParam=0.01/maxIter=40/tol=1e-6; tree maxDepth=5/maxBins=32/minInstancesPerNode=100 expanded rows. Average Brier equally over thresholds, then years. Tie tolerance 1e-5 prioritizes baseline, LR, tree; choices were fixed before formal scores. This limited candidate set is not exhaustive tuning.\n\n'+table(['Model','2021','2022','2023','Mean','Year range'],scoredata)+'\n\n![Rolling Brier](figures/01_rolling_brier.png)\n\nselection.json was persisted before 2024 scoring; test metrics did not guide selection.')
    add('## Actual input joins and scoring coverage\n\nEach role/eligibility/station-filtered and training-sampled anchor set is audited before threshold expansion with B_join_audit.audit_anchor_join. Require unique/null-free keys, every left anchor matched and no row inflation; unused right-side features are allowed. All checks passed. Expanded station/epoch/threshold keys are unique, each anchor has five nonnull labels. Eligibility and role never enter model inputs.\n\n'+table(['Stage','Train anchors','Observed eval anchors','Eligible role anchors','Unknown-station exclusion','Scored anchors','Observed coverage'],covdata)+'\n\nCoverage denominator is observed evaluation anchors, not an ideal complete station calendar. History/future exclusions overlap. Separate coverage.json retains individual history/joint/boundary counts. The deployed model rejects new stations.')
    add('## Locked 2024 final evaluation\n\n'+table(['Threshold/mm','n','Positive rate','Baseline Brier','Selected Brier','Skill','Log loss','PR-AUC','ROC-AUC'],finaldata)+'\n\n![Final Brier](figures/02_final_brier.png)\n\nOverall Brier gives equal weight to five threshold scores; skill=1−model Brier/baseline Brier. Baseline is refit on the same final sampled training. Spark BinaryClassificationEvaluator uses numBins=1000 for approximate AUC; single-class groups are NA. Log loss clips only for numerical stability to [1e-15,1−1e-15]; saved raw probabilities are unchanged.')
    add(f"## Reliability and limitations\n\n![Final reliability](figures/03_final_reliability.png)\n\nBins were fixed in advance and have unequal support; counts are in final/{winner}_calibration.csv. No test calibration was applied. Adjacent-threshold probability increases in the test: {diagnosis['threshold_probability_increasing_pairs']:,} pairs. Raw predictions were not corrected for monotonicity; future inputs are not guaranteed monotone. Rare higher thresholds require support-aware interpretation rather than comparing raw Brier alone.\n\nStation/weather composition changes, missing data and overlapping windows limit generalization. No independent-sample confidence intervals were computed. Only 10% training anchors and few candidates were evaluated, without full-training comparisons, feature ablation or external weather/radar inputs. Reject incomplete history/unknown stations. Three development years and one test year do not establish climate trends or live-deployment performance.")
    add('## Model persistence and prediction\n\nThe complete PipelineModel and prediction_contract.json in trained_model/ preserve station encoding. A fresh Spark process compared 100 test anchors/500 probabilities: maximum absolute reload error '+fmt(reload['max_abs_probability_error'])+'. Past-only single-anchor predictions matched offline results at all five thresholds. Unknown station, unsupported threshold, missing history and off-grid time were correctly rejected. C_predict.RainfallPredictor.predict accepts observations, station, endpoint epoch and threshold; output includes those identifiers and probability. No future label is required to accept a request.')
    if winner=='decision_tree':
        weights=sorted(rows(run/'final/model_feature_weights.csv'),key=lambda r:float(r['weight']),reverse=True)
        add('Training impurity importance is diagnostic, not causal or independent ablation evidence. Full weights/tree rules are in final/.\n\n'+table(['Feature','Normalized impurity importance'],[[r['feature'],fmt(float(r['weight']))] for r in weights if float(r['weight'])>0]))
    add('## Computing environment and timing\n\n'+table(['Item','Measured/configured'],[['Machine',hardware['chip_type']],['Physical memory',hardware['physical_memory']],['OS',environment['os']],['Python/Spark/Java',environment['python']+' / '+environment['pyspark']+' / '+environment['java']],['Spark','local[2], 6g heap, 128 shuffle partitions, 32 MiB reads, AQE on/coalescing off'],['Storage','Stage anchor/prediction Parquet, DISK_ONLY projected training vectors, no full observation cache'],['Peak memory','Not measured']])+'\n\n'+table(['Stage','Seconds'],[[n,f'{t:.1f}'] for n,t in times.items()])+'\n\nStage elapsed includes actual joins, writes, expansion checks, preprocessing, fitting, full scoring and metrics/diagnostics. Excludes prior input hash verification, Spark startup, figures/report and later packaging/reload. Candidate fit_seconds measures classifier fit only, excluding shared preprocessing and frequency estimation. Single local measurements do not control OS file cache and are not stable benchmarks.')
    if winner=='logistic_regression':add(f"Final LR used {fit['iterations']} iterations (maximum 40); the objective history is saved. Successful execution or hitting the cap does not prove convergence. Parameters were not changed using 2024.")
    add('## AI use and submission scope\n\nOpenAI Codex assisted protocol implementation, code, execution, verification, figures and English drafting. Scores are actual saved Spark results; automated checks do not replace group understanding/review. The integrated submission contains A/B/C chapters and member identities; Canvas upload remains a user action. References: Spark 3.5.7 classification/regression and ML Pipelines documentation; archive/history_v1/docs/E2_ROLLING_VALIDATION.md.')
    for year in [2021,2022,2023]:
        add('<!-- PAGEBREAK -->')
        add(f'## Appendix: {year} complete development metrics\n\n'+table(['Model','Threshold','n','Positive rate','Brier','Log loss','PR-AUC','ROC-AUC'],[r[1:] for r in devdata if int(r[0])==year]))
    md=report/'C_report_zh.md';md.write_text('\n\n'.join(sections)+'\n',encoding='utf-8')
    from english_report_pdf import render
    pdf=report/'C_report.pdf';render(md,pdf)
    dump(report/'report_sources.json',{'status':'PASS','language':'en','run':str(run),'selection_sha256':sha256(run/'selection.json'),'final_test_sha256':sha256(run/'final_test.json'),'reload_validation_sha256':sha256(run/'model_reload_validation.json'),'config_sha256':sha256(run/'experiment_config.json'),'markdown':str(md),'pdf':str(pdf)})
    print('Created English C report:',pdf)

if __name__=='__main__':main()
