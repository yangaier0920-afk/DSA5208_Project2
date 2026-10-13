"""English revision report from completed development metrics, including feature ablation."""
import argparse,csv,json,statistics
from pathlib import Path
from c_common import load,dump,csv_write,sha256

def rows(path):
    with Path(path).open() as f:return list(csv.DictReader(f))
def table(headers,data):return '\n'.join(['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+['| '+' | '.join(str(v) for v in r)+' |' for r in data])
def mean_available(values):
    values=list(values);return statistics.mean(values) if values else None
def fmt(v):return 'NA' if v is None or v=='' else f'{float(v):.6f}'

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',required=True,type=Path);ap.add_argument('--source-run',required=True,type=Path);a=ap.parse_args();run=a.run.resolve();source=a.source_run.resolve()
    selection=load(run/'selection.json');config=load(run/'experiment_config.json');reload=load(run/'reload_validation.json')
    assert selection['status']=='LOCKED' and reload['status']=='PASS';assert load(run/'original_preservation_check.json')['status']=='PASS'
    report=run/'report';figs=report/'figures';figs.mkdir(parents=True,exist_ok=True);winner=selection['selected_model'];cuts=config['primary_thresholds_mm']
    metrics=rows(run/'development_metrics.csv');scores=selection['scores'];means={r['model']:r['mean_brier'] for r in scores}
    old=[load(source/f'fold{i}/decision_tree_evaluation.json')['metrics'] for i in range(1,4)];oldmean=statistics.mean(r['brier'] for rr in old for r in rr)
    candidate_diagnostics=[]
    for model_name in means:
        rr=[r for r in metrics if r['model']==model_name and float(r['threshold_mm']) in cuts]
        candidate_diagnostics.append({'model':model_name,'mean_brier':means[model_name],
            'mean_log_loss':statistics.mean(float(r['log_loss']) for r in rr),
            'mean_pr_auc':mean_available(float(r['pr_auc']) for r in rr if r['pr_auc']),
            'mean_roc_auc':mean_available(float(r['roc_auc']) for r in rr if r['roc_auc'])})
    csv_write(run/'candidate_probability_diagnostics.csv',candidate_diagnostics)
    ablation=[]
    sequence=['cdf_recent_d8','cdf_persistence_d8','cdf_calendar_d8','cdf_full_d8'];previous=None
    labels=['Recent amount features only (14)','Add wetness/persistence (22)','Add cyclic time (26)','Add training-only station coding (26 + station)']
    for name,label in zip(sequence,labels):
        ablation.append({'model':name,'feature_group':label,'mean_brier':means[name],'delta_brier_from_previous':means[name]-means[previous] if previous else None,
            'relative_brier_reduction_from_previous':1-means[name]/means[previous] if previous else None,'comparison':'Identical folds/anchors and d8/minLeaf500/prior50; predictive association, not causal contribution'})
        previous=name
    csv_write(run/'ablation_summary.csv',ablation)
    group=[];cal=[]
    compared=list(dict.fromkeys(['frequency_cdf','persistence_cdf','climatology_cdf',winner,'cdf_recent_d8','cdf_full_d8']))
    for fold in range(1,4):
        for name in compared:
            group+=rows(run/f'fold{fold}/{name}_group_metrics.csv');cal+=rows(run/f'fold{fold}/{name}_calibration_summary.csv')
    csv_write(run/'selected_group_metrics.csv',group);csv_write(run/'selected_calibration_summary.csv',cal)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size':9,'axes.spines.top':False,'axes.spines.right':False,'savefig.dpi':180})
    order=[s['model'] for s in sorted(scores,key=lambda r:r['mean_brier'])]+['original_v1_tree']
    values=[means.get(n,oldmean) for n in order]
    fig,ax=plt.subplots(figsize=(9,4.3));ax.barh(range(len(order)),values,color=['#087f8c' if n==winner else '#dc7a20' if n=='original_v1_tree' else '#8498a8' for n in order]);ax.set_yticks(range(len(order)),['Historical tree' if n=='original_v1_tree' else n for n in order]);ax.invert_yaxis();ax.set_xlabel('Equal-primary-threshold/year mean Brier (lower better)');ax.grid(axis='x',alpha=.2);fig.tight_layout();fig.savefig(figs/'01_revision_brier.png');fig.savefig(figs/'01_revision_brier.pdf');plt.close(fig)
    # Last development year only, explicitly not the known 2024 test. Size encodes sample support.
    fig,axes=plt.subplots(1,5,figsize=(12,3.3),sharex=True,sharey=True)
    chosen=rows(run/f'fold3/{winner}_calibration.csv');original=rows(source/'fold3/decision_tree_calibration.csv')
    ymax=max(.05,1.15*max(float(r['observed_rate']) for r in chosen+original if float(r['mean_probability'])<=.05))
    for ax,t in zip(axes,cuts):
        ax.plot([0,.05],[0,.05],'--',color='#555',lw=1)
        for rr,label,color in [(original,'Original tree','#dc7a20'),(chosen,'Selected model','#087f8c')]:
            data=[r for r in rr if float(r['threshold_mm'])==t and float(r['mean_probability'])<=.05]
            ax.scatter([float(r['mean_probability']) for r in data],[float(r['observed_rate']) for r in data],s=[15+75*(int(r['n'])/max(int(x['n']) for x in rr))**.5 for r in data],label=label,color=color,alpha=.75)
        ax.set(title=f'>{t:g} mm',xlabel='Mean predicted p',xlim=(-.001,.05),ylim=(-.001,ymax));ax.grid(alpha=.15)
    axes[0].set_ylabel('Observed event rate');axes[-1].legend(loc='upper left',fontsize=7);fig.suptitle('2023 development reliability, low-probability zoom; point size = support');fig.tight_layout();fig.savefig(figs/'02_low_probability_reliability.png');fig.savefig(figs/'02_low_probability_reliability.pdf');plt.close(fig)
    fig,axes=plt.subplots(1,3,figsize=(12,3.5))
    for ax,prefix,title in zip(axes,['month:','station:','history:'],['Month','Five training-selected stations','Complete-history state']):
        keys=sorted({r['group'] for r in group if r['group'].startswith(prefix)},key=lambda x:int(x.split(':')[1]) if prefix=='month:' else x)
        for name,color in [('persistence_cdf','#8498a8'),(winner,'#087f8c')]:
            yy=[statistics.mean(float(r['brier']) for r in group if r['model']==name and r['group']==k) for k in keys]
            ax.plot(range(len(keys)),yy,'o-',label=name,color=color)
        ax.set_xticks(range(len(keys)),[k.split(':')[1] for k in keys],rotation=45 if prefix=='station:' else 0);ax.set_title(title);ax.grid(alpha=.2)
    axes[0].set_ylabel('Equal-cut/year group Brier');axes[-1].legend(fontsize=7);fig.tight_layout();fig.savefig(figs/'03_group_brier.png');fig.savefig(figs/'03_group_brier.pdf');plt.close(fig)
    fig,axes=plt.subplots(1,3,figsize=(11,4.8))
    flow_records=[]
    for i,ax in enumerate(axes,1):
        count=load(source/f'fold{i}/coverage.json')
        steps=[('Observed anchors',count['all_observed_anchors']),('Complete past history',count['history_valid']),('Complete history + future',count['eligible_history_and_future']),('Safe role + known station',count['scored_eval_anchors'])]
        ax.set(xlim=(0,1),ylim=(-.7,3.8));ax.axis('off');ax.set_title(str(2020+i))
        for j,(label,n) in enumerate(steps):
            y=3-j;ax.text(.5,y,label+'\n'+f'{n:,} ({100*n/count["all_observed_anchors"]:.3f}%)',ha='center',va='center',bbox={'boxstyle':'round,pad=.45','facecolor':'#e5f1f2','edgecolor':'#087f8c'},fontsize=9)
            if j<3:ax.annotate('',xy=(.5,y-.65),xytext=(.5,y-.3),arrowprops={'arrowstyle':'->','color':'#697580'})
            flow_records.append({'year':2020+i,'step':label,'anchors':n,'fraction_of_observed':n/count['all_observed_anchors']})
    fig.suptitle('Scoring scope: observed anchors, not an ideal station calendar');fig.tight_layout();fig.savefig(figs/'04_sample_flow.png');fig.savefig(figs/'04_sample_flow.pdf');plt.close(fig)
    csv_write(run/'sample_flow.csv',flow_records)
    sections=[]
    def add(t):sections.append(t)
    add('# Rainfall probabilities, stronger baselines and feature ablation\n\nThe selected conditional-distribution model is evaluated on development folds. The historical classifier and its known 2024 test are retained as separate evidence.')
    best=means[winner];pers=means['persistence_cdf']
    add(f'## Findings\n\nSelected continuous candidate: **{winner}**, development equal-threshold/year Brier **{best:.6f}**. Strong persistence baseline: {pers:.6f}; climate baseline: {means["climatology_cdf"]:.6f}; global frequency: {means["frequency_cdf"]:.6f}; historical tree: {oldmean:.6f}. Relative Brier reduction is {100*(1-best/pers):.2f}% versus persistence and {100*(1-best/means["frequency_cdf"]):.2f}% versus frequency. These are probability-error reductions relative to named baselines, not accuracy improvements or superiority on every metric.')
    add('## Data, selection and test policy\n\nReuse exactly the original 10% fixed-hash training/full eligible validation anchors for each E2 fold. No new 2024 labels or scores are used. Original post-filter join and sample signatures are recorded; all candidates share keys. Selection uses five original cuts and equal year weights; only continuous-CDF candidates are deployment-eligible. Discrete tree/RF results are openly reported diagnostics. Held-out cuts 0.25/0.75/1.5/3 are not used to fit or select. A new 2017–2023 refit is saved after lock, but its evidence is development validation; there is no claim of a new untouched final test.')
    add(table(['Candidate','2021','2022','2023','Mean Brier','Continuous eligible'],[[s['model'],fmt(s['year_scores']['2021']),fmt(s['year_scores']['2022']),fmt(s['year_scores']['2023']),fmt(s['mean_brier']),s['eligible_for_continuous_deployment']] for s in scores])+'\n\n![Development comparison](figures/01_revision_brier.png)')
    add(table(['Candidate','Mean Brier','Mean log loss','Mean PR-AUC','Mean ROC-AUC'],[[r['model'],fmt(r['mean_brier']),fmt(r['mean_log_loss']),fmt(r['mean_pr_auc']),fmt(r['mean_roc_auc'])] for r in candidate_diagnostics])+'\n\nAdditional ranking/log-loss metrics are diagnostics, not replacement selection criteria. A lower Brier does not imply superiority for every cut/year/ranking measure. Complete per-cut/year metrics are retained.')
    add('## Arbitrary threshold design\n\nTraining-only conditional empirical distributions provide P(S>h)=1-F(h) for finite nonnegative Decimal(38,18) thresholds. Right-sided CDF lookup makes equality negative. For tree candidates Spark DecisionTreeRegressor fits log1p future amount using past features, then exact training amounts form leaf CDFs. Fixed prior weights are 50 anchors for leaves and 200 for baseline groups. No validation outcomes are written back into predictions. This naturally gives threshold-specific probabilities and nonincreasing stepwise survival curves without post-hoc isotonic correction; arbitrary input is not a claim of a smooth curve. Nine checked cuts lie in 0..5 mm; arbitrary input acceptance and monotonicity do not prove every threshold or extreme-tail reliability. Beyond maximum training amount, empirical probability is zero with no proven tail guarantee.')
    held=[]
    for t in config['held_out_thresholds_mm']:
        rr=[r for r in metrics if r['model']==winner and float(r['threshold_mm'])==t]
        held.append([t,fmt(statistics.mean(float(r['brier']) for r in rr)),fmt(statistics.mean(float(r['pr_auc']) for r in rr)),fmt(statistics.mean(float(r['log_loss']) for r in rr))])
    add(table(['Untrained/selection-excluded cut (mm)','Mean Brier','Mean PR-AUC','Mean log loss'],held))
    if config.get('candidate_overrides'):
        leaf_rows=[]
        for candidate,size in [('cdf_persistence_d8_leaf250',250),('cdf_persistence_d8',500),('cdf_persistence_d8_leaf1000',1000)]:
            selected_metrics=[r for r in metrics if r['model']==candidate and float(r['threshold_mm']) in cuts]
            calibration=[r for i in range(1,4) for r in rows(run/f'fold{i}/{candidate}_calibration_summary.csv') if float(r['threshold_mm']) in cuts]
            leaf_rows.append({'model':candidate,'minimum_leaf_anchors':size,'mean_brier':means[candidate],'mean_pr_auc':statistics.mean(float(r['pr_auc']) for r in selected_metrics),'mean_log_loss':statistics.mean(float(r['log_loss']) for r in selected_metrics),'mean_ECE':statistics.mean(float(r['ece_fixed_bins']) for r in calibration),'delta_Brier_vs_leaf500':means[candidate]-means['cdf_persistence_d8']})
        csv_write(run/'leaf_size_comparison.csv',leaf_rows)
        add('## Minimum leaf-size comparison\n\nThe supplementary development experiment freezes minInstancesPerNode at 250, 500 and 1000, holding 22 persistence features, depth 8, seed, folds, primary cuts, prior and row identities fixed. Thirty earlier candidate/fold cells are explicitly reused, and six new cells fit the two added sizes. The main rule keeps the original candidate within absolute 1e-5 of the best, then prefers 1000 over 250. This is an incremental search on already-observed development years, not an independent test. No 2024 score selects the result.\n\n'+table(['Minimum leaf anchors','Mean Brier','Delta vs 500','Mean PR-AUC','Mean log loss','Mean ECE'],[[r['minimum_leaf_anchors'],fmt(r['mean_brier']),fmt(r['delta_Brier_vs_leaf500']),fmt(r['mean_pr_auc']),fmt(r['mean_log_loss']),fmt(r['mean_ECE'])] for r in leaf_rows]))
        selected_leaf=next((r for r in leaf_rows if r['model']==winner),None)
        original_leaf=next(r for r in leaf_rows if r['minimum_leaf_anchors']==500)
        if selected_leaf:
            add(f"Selected leaf size is {selected_leaf['minimum_leaf_anchors']}. Mean PR-AUC is {selected_leaf['mean_pr_auc']:.6f}, versus {original_leaf['mean_pr_auc']:.6f} for leaf500; mean fixed-bin ECE is {selected_leaf['mean_ECE']:.6f}, versus {original_leaf['mean_ECE']:.6f}. The Brier-selected model need not improve every ranking metric, and the small differences are not claimed statistically significant.")
        add('## C input reconstruction acceptance\n\nThe C reconstruction script was executed from an extracted source directory using the existing formal A data and B feature Parquet. It regenerated the three development TRAIN/VALIDATION inputs and final 2017–2023 TRAIN only. Counts and two role-wise row-hash sums across all keys, targets and features match the historical anchors; these signatures are diagnostics, not cryptographic proof of row equality. Parquet bytes are expected to differ. New leaf candidates train on the rebuilt inputs; final model refit/reload and compact checks also complete. This accepts the C part of the missing-anchor workflow, not fresh-machine full A/B reproduction or Windows execution.')
    add('## Controlled feature contribution experiment\n\nAll depth-8 CDF ablations hold rows, seed, min leaf size 500, bins, prior and shared feature ordering fixed. This feature ablation precedes the separate leaf-size search. The threshold is a CDF query, not a tree input. Sequential differences test recent amount -> persistence -> periodic time -> station. Negative Brier delta means adding that group helps under this design; positive means it does not. These are model/data-conditional associations, not causal attribution. Impurity weights are separately saved but are not used as contribution evidence.\n\n'+table(['Feature group','Mean Brier','Delta from previous','Relative reduction'],[[r['feature_group'],fmt(r['mean_brier']),fmt(r['delta_brier_from_previous']),fmt(r['relative_brier_reduction_from_previous'])] for r in ablation]))
    add('## Calibration and low-risk discrimination\n\n![Low-probability development reliability](figures/02_low_probability_reliability.png)\n\nThe 2023 plot zooms to 0..5% and point size shows support. Complete bin counts are in each fold calibration CSV; fixed-bin ECE and low-region rates are in selected_calibration_summary.csv. The appendix gives the exact n for every point shown in the 2023 low-probability plot. Low-region membership depends on each model prediction, so its support can differ; it is not a common held-fixed subgroup comparison. Dry/wet history group tables provide that common covariate-defined comparison. No held-out calibrator is fitted.')
    caltable=[]
    for t in cuts:
        rr=[r for r in cal if r['model']==winner and float(r['threshold_mm'])==t]
        original_ece=[]
        for fold in range(1,4):
            prior_rows=[r for r in rows(source/f'fold{fold}/decision_tree_calibration.csv') if float(r['threshold_mm'])==t]
            denom=sum(int(r['n']) for r in prior_rows)
            original_ece.append(sum(int(r['n'])*abs(float(r['mean_probability'])-float(r['observed_rate'])) for r in prior_rows)/denom)
        caltable.append([t,fmt(statistics.mean(original_ece)),fmt(statistics.mean(float(r['ece_fixed_bins']) for r in rr)),fmt(statistics.mean(float(r['low_region_mean_p']) for r in rr)),fmt(statistics.mean(float(r['low_region_observed_rate']) for r in rr))])
    add(table(['Cut/mm','Original ECE','Selected ECE','Selected low mean p','Selected low observed rate'],caltable))
    add('## Group performance and coverage\n\n![Observed-anchor sample flow](figures/04_sample_flow.png)\n\n![Group Brier](figures/03_group_brier.png)\n\nGroups are 12 months, five stations chosen from first-fold training counts, and complete-history all-dry/any-wet states. Exact per-year/cut rows include n, positives, Brier, mean p, fixed-bin ECE, approximate PR/ROC-AUC, eligible count and observed denominator. Group AUC uses 1,000 equal-probability histogram bins and is explicitly approximate; overall AUC uses Spark MLlib numBins=1000. No subgroup is chosen using test outcomes. Coverage is of observed anchors, not the ideal station calendar; unknown history is retained in the coverage ledger but is unscored.')
    grouptable=[]
    for g in sorted({r['group'] for r in group}):
        rr=[r for r in group if r['model']==winner and r['group']==g]
        grouptable.append([g,fmt(statistics.mean(float(r['brier']) for r in rr)),fmt(mean_available(float(r['pr_auc_binned']) for r in rr if r['pr_auc_binned'])),fmt(statistics.mean(float(r['coverage_fraction']) for r in rr))])
    add(table(['Group','Equal-cut/year Brier','Approximate PR-AUC','Mean observed coverage'],grouptable))
    add('## Reproduction and immutable evidence\n\nFresh-process model reload reconstructs 100 anchors from only past observations and compares 1,000 probabilities at ten cuts, including 3.14159 mm. Maximum error: '+fmt(reload['maximum_error'])+'. Invalid station/time/history/threshold handling and monotonic examples pass. The model, report, ZIP, requirements lock and metadata stay inside this run. Finalization reads the executing Python environment via importlib.metadata, without private uv or OS-specific virtual-environment paths. All original source-run file hashes, including 2024 scores/model, pass preservation checks. Current evidence navigation identifies successful receipts without deleting historical failures.')
    env=load(run/'environment.json');resource=config['spark']
    candidate_times=[]
    for model_name in means:
        rr=[load(run/f'fold{i}/{model_name}_complete.json') for i in range(1,4)]
        candidate_times.append({'model':model_name,'mean_training_seconds':statistics.mean(r['training_seconds'] for r in rr),'mean_candidate_elapsed_seconds':statistics.mean(r['elapsed_seconds'] for r in rr)})
    csv_write(run/'candidate_timing_summary.csv',candidate_times)
    add('## Computing environment and timing\n\nPython '+env['python']+', Spark '+env['pyspark']+', Java '+env['java']+'; '+env['os']+'; '+str(env['logical_cpu_count'])+' logical CPUs. Each process: '+env['spark_master']+', '+resource['driver_memory']+' heap, 128 shuffle partitions, 32 MiB reads. At most two fold processes overlapped. Peaks were not measured; timings are not benchmarks. Candidate timing includes fit/CDF estimation, saves, scoring and metrics; excludes shared hashes/startup, reports and packaging. Training timing includes preprocessing. Exact fold JSON and candidate_timing_summary.csv define the measured scopes.')
    add('## Remaining limitations and AI use\n\nDevelopment results motivated by review are now known and should not be relabelled untouched. Fixed train sampling and small candidate set limit conclusions. Overlapping windows/weather across stations are correlated; no independent-row bootstrap or significance claims are used. Observed gaps, composition changes and training leaf support affect performance. Event-time availability and official interval meaning remain assumptions; live deployment latency is unverified. OpenAI Codex assisted implementation, execution, tests, figures and drafting. All scores come from actual saved Spark outputs; member review is still required.\n\nOfficial model references: [Spark DecisionTreeRegressor](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.regression.DecisionTreeRegressor.html) and [Spark RandomForestClassifier](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.classification.RandomForestClassifier.html).')
    for year in [2021,2022,2023]:
        add('<!-- PAGEBREAK -->');add(f'## Appendix: selected continuous model, {year}')
        rr=[r for r in metrics if r['model']==winner and int(r['year'])==year]
        add(table(['Cut/mm','n','Event rate','Brier','Log loss','PR-AUC','ROC-AUC'],[[r['threshold_mm'],r['n'],fmt(r['positive_rate']),fmt(r['brier']),fmt(r['log_loss']),fmt(r['pr_auc']),fmt(r['roc_auc'])] for r in rr]))
    add('<!-- PAGEBREAK -->');add('## Appendix: exact sample count for every plotted low-probability point (2023)')
    support=[]
    for t in cuts:
        for records,label in [(original,'Historical tree'),(chosen,'Selected model')]:
            for r in records:
                if float(r['threshold_mm'])==t and float(r['mean_probability'])<=.05:
                    support.append({'model':label,'threshold_mm':t,'bin':r['bin'],'n':int(r['n']),'mean_probability':float(r['mean_probability']),'observed_rate':float(r['observed_rate'])})
    csv_write(run/'reliability_plot_exact_counts_2023.csv',support)
    add(table(['Model','Cut/mm','Bin','n','Mean p','Observed rate'],[[r['model'],r['threshold_mm'],r['bin'],f"{r['n']:,}",fmt(r['mean_probability']),fmt(r['observed_rate'])] for r in support]))
    if config.get('candidate_overrides'):
        sections=[s.replace('Reuse exactly the original 10% fixed-hash training/full eligible validation anchors','Use freshly reconstructed inputs matching the original 10% fixed-hash training/full eligible validation anchors') for s in sections]
    md=report/'C_report.md';md.write_text('\n\n'.join(sections)+'\n')
    from english_report_pdf import render
    pdf=report/'C_report.pdf';render(md,pdf)
    dump(report/'report_sources.json',{'status':'PASS','run':str(run),'original_run':str(source),'selection_sha256':sha256(run/'selection.json'),'reload_sha256':sha256(run/'reload_validation.json'),'2024_new_evaluation':False,'language':'en','timing_scope':'Up to two independent local[2]/6g processes overlapped; elapsed times are not isolated benchmarks'})
    print('English revision report created:',pdf)
if __name__=='__main__':main()
