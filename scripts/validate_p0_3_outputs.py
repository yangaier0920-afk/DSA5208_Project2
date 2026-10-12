"""Reconcile P0-3, recompute interval coverage, publish report and file hashes."""
import argparse
import calendar
import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal, localcontext
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text(encoding='utf-8'))


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path):
    with path.open(encoding='utf-8-sig',newline='') as stream:
        return list(csv.DictReader(stream))


def write_csv(path,rows):
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'data/processed/p0_3_v1')
    parser.add_argument('--evidence',type=Path,default=ROOT/'outputs/p0_3_clean/p0_3_v1')
    parser.add_argument('--self-test-dir',type=Path,default=ROOT/'outputs/p0_3_clean/self_test_v1')
    args=parser.parse_args();out=args.output.resolve();evidence=args.evidence.resolve()
    summary=load(evidence/'summary.json');state=load(evidence/'run_status.json')
    readback=load(evidence/'parquet_readback.json');test=load(args.self_test_dir/'self_test.json')
    test_state=load(args.self_test_dir/'run_status.json')
    signature=state['signature']
    checks={'run_pass':state['status']==summary['status']=='PASS' and state['shutdown']['gateway_exit_code']==0,
        'all_years_completed':state['completed_years']==list(range(2017,2025)),
        'all_annual_checks_pass':all(all(r['checks'].values()) for r in summary['annual_results']),
        'all_summary_checks_pass':all(summary['checks'].values()),
        'all_root_readback_checks_pass':readback['status']=='PASS' and all(readback['checks'].values()) and readback['shutdown']['gateway_exit_code']==0,
        'root_readback_matches_run':readback['cleaning_signature']==signature,
        'self_tests_pass':test['status']==test_state['status']=='PASS' and all(test['checks'].values()) and test_state['shutdown']['gateway_exit_code']==0,
        'tested_same_cleaning_code':test_state['signature']['code_sha256']==signature['code_sha256'],
        'cleaner_code_unchanged':digest(ROOT/'src/02_clean_standardize.py')==signature['code_sha256'],
        'root_verifier_code_unchanged':digest(ROOT/'src/02b_verify_parquet.py')==readback['verifier_sha256'],
        'config_unchanged':digest(ROOT/'configs/project.json')==signature['config_sha256'],
        'manifest_unchanged':digest(ROOT/'manifests/raw_manifest.json')==signature['manifest_sha256'],
        'spark_session_unchanged':digest(ROOT/'scripts/spark_session.py')==signature['session_helper_sha256'],
        'spark_runtime_unchanged':digest(ROOT/'scripts/spark_runtime.py')==signature['runtime_helper_sha256'],
        'source_files_unchanged':all((ROOT/r['path']).stat().st_size==r['bytes'] and (ROOT/r['path']).stat().st_mtime_ns==r['mtime_ns'] for r in summary['sources']),
        'output_matches_run':out.as_posix()==signature['output']}
    annual=[]; coverage=[]; stations_by_year={}; contributions=[]; interval=defaultdict(lambda:{'slots':0,'rain':Decimal(0)})
    with localcontext() as context:
        context.prec=50
        for result in summary['annual_results']:
            year=result['year'];m=result['metrics']
            annual.append({'year':year,'raw_rows':result['profile']['raw_rows'],'observations':m['observations'],
                'quarantine':m['quarantine_rows'],'merged_duplicates':m['merged_duplicate_rows'],
                'unique_keys':m['unique_keys'],'source_rows_shifted':result['profile']['shifted_source_rows'],
                'retained_rows_shifted':m['aligned_retained_rows'],'alignment_date_changed_rows':result['profile']['alignment_date_changed_rows'],
                'metadata_conflict_observations':m['metadata_conflict_observations'],'status':result['status']})
            part=read_csv(evidence/str(year)/'station_month_coverage.csv');coverage.extend(part)
            stations_by_year[year]={r['station_id'] for r in part}
            checks[f'year_{year}_coverage_reconciles']=sum(int(r['observed_slots']) for r in part)==m['observations'] and len(part)==len(stations_by_year[year])*12
            for row in read_csv(evidence/str(year)/'rain_period_month_contributions.csv'):
                contributions.append(row)
                key=(int(row['rain_year']),row['station_id'],int(row['rain_month']))
                interval[key]['slots']+=int(row['observed_slots']);interval[key]['rain']+=Decimal(row['observed_rain_mm'])
        checks['all_interval_contributions_reconcile']=sum(int(r['observed_slots']) for r in contributions)==summary['totals']['observations']
        interval_rows=[]
        interval_stations={y:set(stations_by_year[y]) for y in range(2017,2025)}
        for (rain_year,station,month) in interval:
            if rain_year in interval_stations:
                interval_stations[rain_year].add(station)
        for year in range(2017,2025):
            for station in sorted(interval_stations[year]):
                for month in range(1,13):
                    actual=interval[(year,station,month)];expected=calendar.monthrange(year,month)[1]*288
                    interval_rows.append({'year':year,'station_id':station,'month':month,'observed_slots':actual['slots'],
                        'calendar_slots':expected,'missing_calendar_slots':expected-actual['slots'],'coverage':actual['slots']/expected,
                        'observed_rain_mm':str(actual['rain'])})
    checks['interval_coverage_within_bounds']=all(0<=r['observed_slots']<=r['calendar_slots'] for r in interval_rows)
    checks['interval_calendar_rows_reconcile']=sum(r['observed_slots'] for r in interval_rows)==sum(int(r['observed_slots']) for r in contributions if 2017<=int(r['rain_year'])<=2024)
    common=set.intersection(*(stations_by_year[y] for y in range(2017,2025)))
    checks['common_38_candidates_preserved']=len(common)==38
    panel=[]
    lookup={(r['year'],r['station_id'],r['month']):r for r in interval_rows}
    for cutoff in [0.90,0.95]:
        for month in range(1,13):
            selected=sorted(s for s in common if all(lookup[(y,s,month)]['coverage']>=cutoff for y in range(2017,2025)))
            panel.append({'coverage_cutoff':cutoff,'month':month,'comparable_station_count':len(selected),'station_ids':'|'.join(selected),
                          'scope':'same month, station intersection across all eight years; rain-interval calendar'})
    write_csv(evidence/'annual_cleaning.csv',annual)
    write_csv(evidence/'station_month_observation_coverage.csv',coverage)
    write_csv(evidence/'station_month_rain_period_coverage.csv',interval_rows)
    write_csv(evidence/'task1_comparable_month_station_panel.csv',panel)
    checks['count_balance']=summary['raw_rows']==summary['totals']['observations']+summary['totals']['quarantine_rows']+summary['totals']['merged_duplicate_rows']
    checks['primary_key_unique_in_disjoint_years']=all(r['observations']==r['unique_keys'] for r in annual)
    collision=read_csv(evidence/'2017/collision_lineage.csv')
    checks['collision_ledger_closed']=len(collision)==172 and sum(r['disposition']=='MERGED_DUPLICATE' for r in collision)==86 and sum(r['disposition']=='RETAINED' for r in collision)==86
    checks['eight_year_success_markers_and_96_month_partitions']=all((out/'observations'/f'year={y}'/'_SUCCESS').is_file() for y in range(2017,2025)) and all((out/'observations'/f'year={y}'/f'month={m}').is_dir() for y in range(2017,2025) for m in range(1,13))
    # One run can have a wholly missing network month in other datasets; this v1
    # source has some observations in all 96 months, so each partition is expected.
    assert all(checks.values()),{k:v for k,v in checks.items() if not v}
    report=ROOT/'docs/p0_3_quality_report.md'
    lines=['# P0-3 cleaning and standardization acceptance report','',
        'Status: PASS. P0-2 passed 32 delivery checks; P0-3 completed eight-year Spark cleaning, annual Parquet readback and merged-root checks for B/C. E2 was confirmed; this stage did not create labels or train models.','',
        '## Count reconciliation','',
        '| Raw records | observations | quarantine | Merged duplicate removals |','|---:|---:|---:|---:|',
        f'| {summary["raw_rows"]:,} | {summary["totals"]["observations"]:,} | {summary["totals"]["quarantine_rows"]:,} | {summary["totals"]["merged_duplicate_rows"]:,} |','',
        'Raw = observations + quarantine + merged removals. Empty quarantine retains a readable schema. The 172 collision_lineage rows include 86 retained and 86 removed sides, not additional losses.','',
        '| Year | Raw | observations | Quarantine | Duplicate removals |','|---|---:|---:|---:|---:|']
    lines += [f'| {r["year"]} | {r["raw_rows"]:,} | {r["observations"]:,} | {r["quarantine"]} | {r["merged_duplicates"]} |' for r in annual]
    lines += ['', '## Implemented rules and verification','',
        '- T2: 1,775,327 raw offset rows in 2017 shifted forward one second, retaining original time; 6,191 raw records changed date. See annual ledgers for post-merge counts; do not mix pre/post processing scopes.',
        '- C1: 86 equal-rain collisions prefer the original standard-grid row without summing rainfall. S113 metadata differences in two groups are flagged, preserving both histories.',
        '- L1/D1: slot_end_ts is the interval endpoint; start is endpoint minus 300 seconds and supplies weather-analysis calendar. Interval meaning remains a project assumption, not official proof.',
        '- M1/R1: sparse storage without zero filling; actual zeros and valid finite nonnegative extremes remain.',
        f'- Exact rainfall: stored as {summary["decimal_storage"]}; maximum effective source decimal precision is {summary["max_source_effective_decimal_scale"]}; no silent rounding. Annual exact rainfall ledgers reconcile.',
        f'- {len(test["checks"])} synthetic anomaly/Parquet checks, annually {len(summary["annual_results"][0]["checks"])} readback checks, {len(readback["checks"])} merged-root checks, {len(checks)} delivery checks passed with normal Java exit.',
        '- All eight raw file hashes were rechecked before processing; size/mtime remained unchanged. Output hashes are separate. Raw CSV was not modified.','',
        '## Coverage and downstream use','',
        'Export endpoint- and interval-calendar coverage separately; combine all source-year contributions for interval coverage to handle month/year boundaries. Denominator is the full calendar; gaps are unknown. B1/K1 panels were recomputed with 38 common candidates.','',
        '| Month | Eight-year same-month stations (90%) | 95% sensitivity |','|---|---:|---:|']
    p={(r['coverage_cutoff'],r['month']):r['comparable_station_count'] for r in panel}
    lines += [f'| {m} | {p[(0.9,m)]} | {p[(0.95,m)]} |' for m in range(1,13)]
    lines += ['', 'Months without eligible stations cannot support matched eight-year comparisons. Supported-month sums are not annual rainfall. Missing 2025 context remains a data-edge limitation. Coverage panels do not filter global observations or Task 2.','',
        'See [p0_3_data_contract.md](p0_3_data_contract.md) for interfaces, fields, quarantine reasons and reproduction. Counts/coverage are in outputs/p0_3_clean/p0_3_v1; formal data in data/processed/p0_3_v1. P0-4 subsequently constructs exact past/future windows and E2 roles.','']
    report.write_text('\n'.join(lines),encoding='utf-8')
    validation={'status':'PASS','checks':checks,'validator_sha256':digest(Path(__file__)),'report_sha256':digest(report),
        'data_contract_sha256':digest(ROOT/'docs/p0_3_data_contract.md'),'note':'Full raw SHA256 verified before cleaning; final check uses unchanged sizes/mtimes.'}
    (evidence/'validation.json').write_text(json.dumps(validation,ensure_ascii=False,indent=2),encoding='utf-8')
    receipt={'P0-2':{'status':'PASS','validation':'outputs/p0_2_audit/p0_2_v1/validation.json'},
        'P0-3':{'status':'PASS','version':'p0_3_v1','validation':(evidence/'validation.json').relative_to(ROOT).as_posix(),
                'observations':(out/'observations').relative_to(ROOT).as_posix(),'totals':summary['totals']},
        'P0-4':{'status':'NOT_STARTED'},'E2':{'decision':'CONFIRMED','training_status':'NOT_STARTED'}}
    (ROOT/'configs/processing_status.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8')
    files=[]
    for folder in [out,evidence]:
        for path in sorted(folder.rglob('*')):
            if path.is_file() and path != evidence/'output_manifest.json':
                files.append({'path':path.relative_to(ROOT).as_posix(),'bytes':path.stat().st_size,'sha256':digest(path)})
    (evidence/'output_manifest.json').write_text(json.dumps({'status':'PASS','files':files},ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'PASS: {len(checks)} delivery checks; {len(files)} files hashed; P0-3 report and coverage published')


if __name__=='__main__':
    main()
