"""Reconcile P0-4 quality/roles/manual examples, report and hash deliverables."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text(encoding='utf-8'))


def digest(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''):
            value.update(block)
    return value.hexdigest()


def read_csv(path):
    with path.open(encoding='utf-8-sig',newline='') as stream:
        return list(csv.DictReader(stream))


def write_csv(path,rows):
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'data/processed/p0_4_v1')
    parser.add_argument('--evidence',type=Path,default=ROOT/'outputs/p0_4_targets/p0_4_v1')
    parser.add_argument('--self-test-dir',type=Path,default=ROOT/'outputs/p0_4_targets/self_test_v1')
    args=parser.parse_args();out=args.output.resolve();evidence=args.evidence.resolve()
    summary=load(evidence/'summary.json');state=load(evidence/'run_status.json');signature=state['signature']
    readback=load(evidence/'parquet_readback.json');test=load(args.self_test_dir/'self_test.json');teststate=load(args.self_test_dir/'run_status.json')
    prior=ROOT/'outputs/p0_3_clean/p0_3_v1';previous=load(prior/'summary.json')
    checks={'run_and_shutdown_pass':summary['status']==state['status']=='PASS' and state['shutdown']['gateway_exit_code']==0,
        'all_eight_years_completed':state['completed_years']==list(range(2017,2025)),
        'annual_checks_pass':all(all(r['checks'].values()) for r in summary['annual_results']),
        'summary_checks_pass':all(summary['checks'].values()),
        'root_checks_and_shutdown_pass':readback['status']=='PASS' and all(readback['checks'].values()) and readback['shutdown']['gateway_exit_code']==0,
        'root_signature_matches':readback['builder_signature']==signature,
        'self_tests_pass':test['status']==teststate['status']=='PASS' and all(test['checks'].values()) and teststate['shutdown']['gateway_exit_code']==0,
        'same_code_was_tested':teststate['signature']['code_sha256']==signature['code_sha256'],
        'builder_code_unchanged':digest(ROOT/'src/03_build_targets.py')==signature['code_sha256'],
        'root_verifier_code_unchanged':digest(ROOT/'src/03b_verify_targets.py')==readback['verifier_sha256'],
        'config_unchanged':digest(ROOT/'configs/project.json')==signature['config_sha256'],
        'session_helper_unchanged':digest(ROOT/'scripts/spark_session.py')==signature['session_helper_sha256'],
        'runtime_helper_unchanged':digest(ROOT/'scripts/spark_runtime.py')==signature['runtime_helper_sha256'],
        'source_manifest_unchanged':digest(prior/'output_manifest.json')==signature['source_output_manifest_sha256'],
        'source_validation_unchanged':digest(prior/'validation.json')==signature['source_validation_sha256'],
        'all_source_files_unchanged':all((ROOT/r['path']).stat().st_size==r['bytes'] and (ROOT/r['path']).stat().st_mtime_ns==r['mtime_ns'] for r in state['source_files']),
        'source_keys_exactly_match':readback['key_match']['target_without_observation']==readback['key_match']['observation_without_target']==0,
        'anchors_reconcile_to_observations':summary['totals']['anchors']==previous['totals']['observations'],
        'output_matches_signature':out.as_posix()==signature['output']}
    annual=[];quality=[];roles=[];examples=[];example_slots=[]
    for result in summary['annual_results']:
        y=result['year'];m=result['metrics']
        annual.append({'year':y,**{k:m[k] for k in ['anchors','future_valid','future_invalid','history_valid','history_invalid','base_model_eligible','future_valid_history_invalid','cross_calendar_year_anchors']},
            'future_valid_rate':m['future_valid']/m['anchors'],'base_model_eligible_rate':m['base_model_eligible']/m['anchors'],'manual_cases':result['manual_cases'],'status':result['status']})
        part=read_csv(evidence/str(y)/'station_month_target_quality.csv');quality.extend(part)
        checks[f'year_{y}_quality_table_reconciles']=all(sum(int(r[k]) for r in part)==m[k] for k in ['anchors','future_valid','history_valid','base_model_eligible'])
        part_roles=read_csv(evidence/str(y)/'e2_role_counts.csv');roles.extend(part_roles)
        checks[f'year_{y}_roles_reconcile']=all(sum(int(r['anchors']) for r in part_roles if r['protocol']==protocol)==m['anchors'] and
            sum(int(r['base_model_eligible']) for r in part_roles if r['protocol']==protocol)==m['base_model_eligible'] for protocol in ['fold1_role','fold2_role','fold3_role','final_role'])
        if y<=2020:
            e=read_csv(evidence/str(y)/'manual_recomputed_examples.csv');slots=read_csv(evidence/str(y)/'manual_example_slots.csv')
            checks[f'year_{y}_manual_examples_complete']=len(e)==result['manual_cases'] and len(slots)==18*len(e) and all(r['passed']=='True' for r in e)
            examples.extend({'year':y,**r} for r in e);example_slots.extend({'year':y,**r} for r in slots)
    checks['manual_examples_cover_required_cases']=set(r['case'] for r in examples)>={'valid_nonzero','valid_zero','internal_missing_future','cross_day','cross_month','cross_year','incomplete_history','alignment_transition'}
    checks['manual_examples_only_training_development_years']=all(int(r['year'])<=2020 for r in examples)
    checks['all_96_month_partitions_present']=all((out/'targets'/f'year={y}'/f'month={m}').is_dir() for y in range(2017,2025) for m in range(1,13))
    checks['all_year_success_markers_present']=all((out/'targets'/f'year={y}'/'_SUCCESS').is_file() for y in range(2017,2025))
    assert all(checks.values()),{k:v for k,v in checks.items() if not v}
    write_csv(evidence/'annual_target_quality.csv',annual)
    write_csv(evidence/'station_month_target_quality.csv',quality)
    write_csv(evidence/'e2_role_counts.csv',roles)
    write_csv(evidence/'manual_recomputed_examples.csv',examples)
    write_csv(evidence/'manual_example_slots.csv',example_slots)
    totals=summary['totals'];final_eligible=sum(int(r['base_model_eligible']) for r in roles if r['protocol']=='final_role' and r['role'] in ['TRAIN','TEST'])
    lines=['# P0-4 continuity and future-total acceptance report','',
        'Status: PASS. A generated complete targets under the confirmed protocol, without model training, threshold expansion or B feature computation. P0-3 input data/configuration remain unchanged.','',
        '## Full counts and validity','',
        '| Measure | Anchors |','|---|---:|',
        f'| All observation anchors | {totals["anchors"]:,} |',
        f'| Complete six-slot future, truth available | {totals["future_valid"]:,} |',
        f'| Missing future slots, total null | {totals["future_invalid"]:,} |',
        f'| Complete 12-slot past hour | {totals["history_valid"]:,} |',
        f'| Both past/future complete | {totals["base_model_eligible"]:,} |',
        f'| Final TRAIN/TEST joint window eligibility | {final_eligible:,} |','',
        'These window eligibility counts overlap; do not add history/future invalidity as disjoint losses. Every observation retains a target anchor, including invalid ones, with unknown totals null rather than zero. Base eligibility must intersect the selected E2 role and I1 station support; it is not final training sample size.','',
        '| Year | All anchors | Future valid | Future invalid | Both windows complete |','|---|---:|---:|---:|---:|']
    lines += [f'| {r["year"]} | {r["anchors"]:,} | {r["future_valid"]:,} | {r["future_invalid"]:,} | {r["base_model_eligible"]:,} |' for r in annual]
    lines += ['', '## Windows, boundaries and evidence','',
        '- Check six future endpoints t+300 through t+1800 and 12 history endpoints t−3300 through t. Use exact time windows, never next-six-row sums or zero filling.',
        '- Sum Decimal(38,18); strict exceedance is positive, equality negative, missing truth unknown. Specific thresholds were not expanded at this stage.',
        '- Load adjacent-year context before windows; date/month/year crossings are supported. Purge actual E2 right boundaries, not every calendar-year crossing.',
        '- fold1/2/3 and final_role identify use; 2024 is UNUSED in all development folds. EXCLUDED_BOUNDARY retains truth for audit but is excluded from that block training/evaluation.',
        f'- {len(test["checks"])} synthetic window/Decimal/E2/Parquet checks, annually {len(summary["annual_results"][0]["checks"])} acceptance checks, {len(readback["checks"])} root checks and {len(checks)} delivery checks passed with normal Java exit.',
        f'- {len(examples)} real cases independently reconstructed slot by slot, exporting {len(example_slots)} expected-slot rows covering dry/wet truth, missing past/future, date/month/year crossings and the 2017 phase transition. Real manual cases use 2017–2020 only.',
        '- Exact merged-root target/observation keys match without missing or extra keys. All 96 input observation Parquet SHA-256 values were rechecked; size/mtime were unchanged afterwards.',
        '- At this stage 2024 checks covered completeness, boundary and structure only, without threshold test distributions or score-driven model selection.','',
        '## B/C use','',
        'Data: data/processed/p0_4_v1/targets/. Counts, invalid reasons, station-month quality, E2 use and manual slots: outputs/p0_4_targets/p0_4_v1/. See [p0_4_data_contract.md](p0_4_data_contract.md) for reads/fields.','',
        'B constructs historical features from observations. C filters selected roles and joint eligibility, then freezes support/sampling and fits using training only. Future totals/validity/missing slots are label/audit information, never features. Inference requires existing history/support, not future six-slot availability.','']
    report=ROOT/'docs/p0_4_quality_report.md';report.write_text('\n'.join(lines),encoding='utf-8')
    validation={'status':'PASS','checks':checks,'validator_sha256':digest(Path(__file__)),'report_sha256':digest(report),
        'data_contract_sha256':digest(ROOT/'docs/p0_4_data_contract.md'),'manual_examples':len(examples),'manual_slots':len(example_slots)}
    (evidence/'validation.json').write_text(json.dumps(validation,ensure_ascii=False,indent=2),encoding='utf-8')
    receipt_path=ROOT/'configs/processing_status.json';receipt=load(receipt_path)
    receipt['P0-4']={'status':'PASS','owner':'A','version':'p0_4_v1','validation':(evidence/'validation.json').relative_to(ROOT).as_posix(),
                     'targets':(out/'targets').relative_to(ROOT).as_posix(),'totals':totals}
    receipt['E2']['training_status']='NOT_STARTED'
    receipt_path.write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8')
    files=[]
    for folder in [out,evidence]:
        for path in sorted(folder.rglob('*')):
            if path.is_file() and path!=evidence/'output_manifest.json':
                files.append({'path':path.relative_to(ROOT).as_posix(),'bytes':path.stat().st_size,'sha256':digest(path)})
    (evidence/'output_manifest.json').write_text(json.dumps({'status':'PASS','files':files},ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'PASS: {len(checks)} delivery checks; {len(examples)} independently recomputed examples; {len(files)} output files hashed')


if __name__=='__main__':
    main()
