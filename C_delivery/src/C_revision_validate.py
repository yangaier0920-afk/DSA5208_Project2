"""Independent compact-evidence checks: calibration counts, CDFs and shared evaluation scope."""
import argparse,csv,math,json
from decimal import Decimal
from pathlib import Path
from c_common import load,dump

def rows(p):
    with p.open() as f:return list(csv.DictReader(f))
def validate_cdf(cdf):
    for d in [cdf['global'],*cdf['groups'].values()]:
        values=[Decimal(v) for v in d['values']];counts=d['cum_counts']
        assert len(values)==len(counts)>0 and values[0]>=0
        assert all(a<b for a,b in zip(values,values[1:]))
        assert counts[0]>0 and all(a<b for a,b in zip(counts,counts[1:])) and counts[-1]==d['n']
    assert sum(d['n'] for d in cdf['groups'].values())==cdf['global']['n']

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',type=Path,required=True);a=ap.parse_args();run=a.run.resolve();cfg=load(run/'experiment_config.json');checks=0;maximum_probability_roundoff=0.0
    for fold in range(1,4):
        root=run/f'fold{fold}';identity=load(root/'sample_identity.json');n=identity['validation_anchors'];train_n=identity['train_anchors']
        for name in cfg['continuous_candidates']+list(cfg['diagnostic_classifiers']):
            complete=load(root/f'{name}_complete.json');assert complete['status']=='PASS' and complete['source_2024_used'] is False
            evaluation=load(root/f'{name}_evaluation.json');metrics=evaluation['metrics'];bins=rows(root/f'{name}_calibration.csv')
            expected=9 if name in cfg['continuous_candidates'] else 5;assert len(metrics)==expected
            for m in metrics:
                assert m['n']==n and math.isfinite(m['brier']) and 0<=m['brier']<=1
                rr=[r for r in bins if float(r['threshold_mm'])==m['threshold_mm']]
                count=sum(int(r['n']) for r in rr);pos=sum(int(r['n'])*float(r['observed_rate']) for r in rr);prob=sum(int(r['n'])*float(r['mean_probability']) for r in rr)/count
                maximum_probability_roundoff=max(maximum_probability_roundoff,abs(prob-m['mean_probability']))
                assert count==n and abs(pos/n-m['positive_rate'])<1e-12 and abs(prob-m['mean_probability'])<1e-10
                checks+=1
            if name in cfg['continuous_candidates']:
                assert evaluation['threshold_probability_increasing_pairs']==0
                cdf=load(root/'models'/name/'conditional_cdf.json');validate_cdf(cdf);assert cdf['global']['n']==train_n
            for r in rows(root/f'{name}_group_metrics.csv'):
                assert int(r['n'])==int(r['eligible_anchors'])<=int(r['observed_anchors'])
                assert abs(float(r['coverage_fraction'])-int(r['n'])/int(r['observed_anchors']))<1e-12
    if (run/'trained_model/conditional_cdf.json').exists():validate_cdf(load(run/'trained_model/conditional_cdf.json'))
    dump(run/'compact_evidence_validation.json',{'status':'PASS','threshold_metric_cells_checked':checks,'maximum_mean_probability_reconciliation_roundoff':maximum_probability_roundoff,'probability_float_tolerance':1e-10,
        'checks':'Same candidate evaluation counts; calibration-bin counts/positives/mean p reconcile; exact sorted CDF/counts; all continuous threshold violations zero; group coverage denominators consistent','2024_used':False})
    print('PASS: independent compact-evidence checks:',checks)
if __name__=='__main__':main()
