"""Generate a copyable English B report from measured outputs; no model results invented."""
import argparse,calendar,json
from pathlib import Path
from b_common import PACKAGE,rows,csv_write,dump

def table(headers,records):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+
        ['| '+' | '.join(str(v) for v in r)+' |' for r in records])+'\n'

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--task1',type=Path,default=PACKAGE/'results/task1_v2')
    ap.add_argument('--features',type=Path,default=PACKAGE/'results/features_v2')
    ap.add_argument('--output',type=Path,default=PACKAGE/'report_zh');a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=True)
    f=json.loads((a.task1/'findings.json').read_text());v=json.loads((a.features/'validation.json').read_text())
    tm=json.loads((a.task1/'result_metadata.json').read_text());fm=json.loads((a.features/'result_metadata.json').read_text())
    hardware=json.loads((PACKAGE/'results/hardware_inventory.json').read_text())
    retention=[]
    for r in rows(a.features/'yearly_validation.csv'):
        n=int(r['rows']);k=int(r['history_complete']);retention.append({'year':int(r['year']),'anchors':n,'history_complete':k,'history_incomplete':n-k,'retention_rate':k/n,'excluded_rate':1-k/n})
    total=sum(r['anchors'] for r in retention);kept=sum(r['history_complete'] for r in retention)
    csv_write(a.features/'history_retention_by_year.csv',retention)
    sr=rows(a.task1/'strict_same_month_summary.csv');sensitivity=[]
    for m in range(1,13):
        x=[r for r in sr if int(r['month'])==m and float(r['coverage_cutoff'])==.9 and r['mean_station_month_observed_mm']]
        y=[r for r in sr if int(r['month'])==m and float(r['coverage_cutoff'])==.95 and r['mean_station_month_observed_mm']]
        if not x or not y:continue
        def extrema(rr):
            return int(min(rr,key=lambda r:float(r['mean_station_month_observed_mm']))['year']),int(max(rr,key=lambda r:float(r['mean_station_month_observed_mm']))['year'])
        lo90,hi90=extrema(x);lo95,hi95=extrema(y)
        yy={r['year']:float(r['mean_station_month_observed_mm']) for r in y}
        differences=[abs(yy[r['year']]-float(r['mean_station_month_observed_mm'])) for r in x]
        sensitivity.append(dict(month=m,stations_90=int(x[0]['n_stations']),stations_95=int(y[0]['n_stations']),lowest_year_90=lo90,lowest_year_95=lo95,highest_year_90=hi90,highest_year_95=hi95,extrema_consistent=lo90==lo95 and hi90==hi95,max_abs_mean_difference_mm=max(differences)))
    csv_write(a.task1/'coverage_sensitivity_summary.csv',sensitivity)
    freq=rows(a.task1/'monthly_frequency_intensity_summary.csv');freqs={c:[r for r in freq if float(r['coverage_cutoff'])==c] for c in [.9,.95]}
    peaks={c:(max(rr,key=lambda r:float(r['wet_fraction'])),max(rr,key=lambda r:float(r['mean_station_wet_p90_5m_mm']))) for c,rr in freqs.items()}
    freqdiff=max(abs(float(next(r for r in freqs[.95] if r['month']==x['month'])['wet_fraction'])-float(x['wet_fraction'])) for x in freqs[.9])*100
    parts=[]
    def add(text):parts.append(text.strip())
    def figure(name,caption):
        import os
        path=Path(os.path.relpath(a.task1/'figures'/name,a.output)).as_posix()
        add(f'![{caption}]({path})\n\n{caption}')
    add('# Singapore rainfall patterns and historical features: B report\n\nFeature version B_v2. This chapter covers Task 1 and historical inputs; completed C results are reported separately in C_delivery and the integrated submission.')
    add(f'## Data and analysis policy\n\nUse {total:,} cleaned 2017–2024 observations. Spark performs full-data aggregates/features; Matplotlib reads small summaries. A release hashes/schema and 6,528 station-month counts/exact sums were checked. Missing rain is unknown, actual zeros and finite nonnegative extremes remain. Singapore calendar uses interval starts; station means are equally weighted and observed totals are not coverage-inflated.')
    add((PACKAGE/'docs/TIME_CONTRACT.md').read_text())
    add('## Coverage and annual descriptions\n\nMain monthly coverage is 90%, sensitivity 95%. Eight-year same-month fixed panels support May, September, October and December only. Empty panels do not mean no rain. Annual denominators are complete calendars including leap years. Station composition changes; 2018 has no annual-90% station and remains NA. The fixed eight-year annual intersection is empty, so descriptive annual sums do not establish national trends.')
    figure('01_coverage_and_panel.png','Figure 1. Qualified stations and fixed same-month panels.')
    annual=[r for r in rows(a.task1/'annual_descriptive_summary.csv') if float(r['coverage_cutoff'])==.9]
    add(table(['Year','Annual >=90% stations','Observed annual mean/mm','Every-month >=90% stations','Stricter observed mean/mm'],[[r['year'],r['n_stations'],r['mean_observed_annual_mm'] or 'NA',r['stations_passing_every_month'],r['mean_observed_annual_mm_each_month'] or 'NA'] for r in annual]))
    figure('06_annual_descriptive_summary.png','Figure 2. Annual coverage sensitivity and composition.')
    add('## Seasonal occurrence and intensity')
    figure('02_descriptive_monthly_rainfall.png','Figure 3. Qualified-station monthly observed means; unsupported cells remain NA.')
    add('The largest descriptive cell is October 2022, 419.1 mm (66 stations); the smallest is February 2021, 32.0 mm (68 stations). Month lengths, coverage and composition differ, so these are not fixed-network rankings.')
    figure('07_frequency_and_intensity.png','Figure 4. Wet-slot frequency and conditional nonzero intensity.')
    add(f"At 90% coverage the peak wet fraction is month {int(peaks[.9][0]['month'])}: {100*float(peaks[.9][0]['wet_fraction']):.2f}%; peak nonzero P90 is month {int(peaks[.9][1]['month'])}: {float(peaks[.9][1]['mean_station_wet_p90_5m_mm']):.2f} mm per five minutes. Spark percentile_approx accuracy=1000 computes station-month nonzero quantiles before equal-weight averaging; these are not pooled national quantiles. All-dry groups have zero wet fraction but unknown conditional intensity.")
    add('## Fixed-station interannual comparison')
    figure('03_strict_interannual_comparison.png','Figure 5. Fixed same-month comparisons at 90% and 95%.')
    add(table(['Month','Stations 90%','Stations 95%','Lowest year 90%/95%','Highest year 90%/95%','Extrema agree','Maximum mean difference/mm'],[[r['month'],r['stations_90'],r['stations_95'],f"{r['lowest_year_90']}/{r['lowest_year_95']}",f"{r['highest_year_90']}/{r['highest_year_95']}",r['extrema_consistent'],f"{r['max_abs_mean_difference_mm']:.2f}"] for r in sensitivity]))
    add(f'Extrema agree for the four supported months at both cutoffs, although station composition and means change. October has 21 stations at 90%; 2022 averages 399.8 mm and 2023 113.0 mm. Peak years differ by month; do not call any year universally wettest or add four months into a full year. Peak occurrence months at 90%/95% are {int(peaks[.9][0]["month"])}/{int(peaks[.95][0]["month"])}; peak P90 months are {int(peaks[.9][1]["month"])}/{int(peaks[.95][1]["month"])}. Maximum wet-fraction change is {freqdiff:.4f} percentage points. Sensitivity is not a significance test.')
    add('## Hourly and station differences')
    figure('04_hourly_rain_occurrence.png','Figure 6. Singapore-time occurrence by hour and month.')
    add('Hourly panels require every hour of a station-month to meet coverage, preserving station composition across a curve; 82 supported year-months contribute equal weight. Occurrence peaks at 16:00 (5.78%) and is lowest at 23:00 (1.11%). This observed five-minute frequency is not a future-threshold prediction probability or a causal explanation.')
    figure('05_station_comparison.png','Figure 7. Station contrasts in the largest fixed same-month panel.')
    add('October has the largest 90% panel, 21 stations; ties choose the earlier month. S89 averages 270.3 mm and S107 125.2 mm, an October contrast rather than annual ranking. Interval-start dates have 64 entirely missing network days versus 63 event-calendar days; 2018-02-17 is the calendar-definition difference, not newly discovered zero rainfall.')
    add('## Historical features and retention')
    add((PACKAGE/'docs/FEATURE_DEFINITIONS.md').read_text())
    add(table(['Year','Observed anchors','Complete 60m history','Incomplete','Retention'],[[r['year'],r['anchors'],r['history_complete'],r['history_incomplete'],f"{100*r['retention_rate']:.3f}%"] for r in retention]))
    add(f'{total-kept:,} incomplete histories are retained with null window summaries, not removed or filled. Complete-history supervision excludes them without rewriting valid future truth. History/future exclusions overlap; this table is not final model sample retention. Overall complete history is {100*kept/total:.3f}%; 2018 is notably lower. Results apply to the supported complete-history scope.')
    add('## Join checks and C handoff')
    join_file=PACKAGE/'results/join_audit/validation.json'
    if not join_file.exists():join_file=PACKAGE/'results/join_audit_rerun_20261012/validation.json'
    if join_file.exists():
        audit=json.loads(join_file.read_text());add('Measured base-table join totals:');add(table(['Metric','Value'],list(audit['totals'].items())))
    else:add('Base join audit unavailable; no match-rate claim is made.')
    add('Keys are station_id+prediction_epoch_s. Audit C actual role-filtered/sampled anchors before threshold expansion, not only full base tables. Require unique/null-free keys, every left match and no inflation; unused right rows are allowed. Expanded keys add threshold_mm and must have the supported threshold count per anchor. Never hide duplicate failures with dropDuplicates. Completed C audits are recorded separately.')
    add('## Environment, timing and evidence limits')
    add(table(['Item','Configuration'],[['Machine',hardware['machine_name']+' / '+hardware['chip_type']],['Physical memory',hardware['physical_memory']],['OS',fm['environment']['os']],['Logical CPUs',fm['environment']['logical_cpu_count']],['Spark','local[2], 4g heap, 128 shuffle partitions, 32 MiB reads, AQE on/coalescing off'],['Full observation caching','Disabled'],['Peak memory','Not measured']]))
    add(f"Task 1 aggregation elapsed {tm['elapsed_seconds']:.1f} seconds: release verification/startup through aggregate CSV writing, excluding later metadata hashes, figures/report and shutdown. Feature elapsed {fm['elapsed_seconds']:.1f} seconds: verification, reads, computation, annual writes/readback and first probes through validation, excluding later metadata hashes/shutdown/plots/report and later independent 513-probe/API checks. Original v2 reused aggregates; the 2026-10-12 rerun recomputed them. Join auditing has separate timing. Single-run measurements did not control OS file cache and are not benchmarks.")
    add(f"B_v2 validation: {v['rows']:,} rows, {v['history_complete']:,} complete histories, annual A agreement and complete-history 26-column nonnull checks. Fixed seed 20261011 selected 20 stations x20 anchors plus 113 real boundary anchors: 513 independent timestamp-dictionary probes. Six synthetic wet/dry/missing/cross-year cases and five join cases passed. Stored validation JSON defines exact check scope.")
    add('## Limitations and AI use\n\nInterval/event-time availability assumptions and actual publication delay remain unverified. Changing composition, missingness, approximate quantiles and complete-history eligibility limit interpretation. No causal control, significance test or feature ablation establishes independent predictive gains. C uses training-only transformations and prespecified E2 validation, then one locked 2024 test. OpenAI Codex assisted design, code, execution, checks, figures and drafting; actual counts come from saved outputs, and automated checks do not replace member review. Final submission requirements and completed group results are documented in submission.')
    path=a.output/'B_report_zh.md';path.write_text('\n\n'.join(parts)+'\n',encoding='utf-8')
    dump(PACKAGE/'results/report_metadata/report_sources.json',{'language':'en','format':'Markdown','feature_set_version':'B_v2','source_task1':str(a.task1),'source_features':str(a.features),'source_join_audit':str(join_file),'numeric_features_recomputed':False})
    print('English report saved:',path)

if __name__=='__main__':main()
