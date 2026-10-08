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
    lines=['# P0-4 时间连续性与未来累计量验收报告','',
        '状态：PASS。A按确认的原计划生成完整targets，未进行模型训练、阈值展开或B的特征计算。P0-3输入数据及配置保持不变。','',
        '## 全量数量与有效性','',
        '| 指标 | 锚点数 |','|---|---:|',
        f'| observations对应的全部锚点 | {totals["anchors"]:,} |',
        f'| 未来六槽完整，真实累计量可用 | {totals["future_valid"]:,} |',
        f'| 未来缺槽，累计量为null | {totals["future_invalid"]:,} |',
        f'| 过去60分钟十二槽完整 | {totals["history_valid"]:,} |',
        f'| 同时满足未来和历史完整性 | {totals["base_model_eligible"]:,} |',
        f'| 最终阶段TRAIN/TEST内同时满足窗口资格 | {final_eligible:,} |','',
        '这些数值是不同窗口资格的统计，不能把未来失效与历史失效简单相加作为互斥损失。每条观测均保留一条targets；失效锚点未删掉，也没有把未知累计量写成0。基础模型资格还需与所选E2角色及I1站点支持范围相交，不能直接当作最终训练样本数。','',
        '| 年份 | 全部锚点 | 未来有效 | 未来无效 | 历史／未来均完整 |','|---|---:|---:|---:|---:|']
    lines += [f'| {r["year"]} | {r["anchors"]:,} | {r["future_valid"]:,} | {r["future_invalid"]:,} | {r["base_model_eligible"]:,} |' for r in annual]
    lines += ['', '## 窗口、边界与证据','',
        '- 未来逐一检查t+300至t+1800秒六槽；历史逐一检查t-3300至t十二槽。采用实际时间窗口，不按后六行累计，不补零。',
        '- 累计量使用Decimal(38,18)。严格超过阈值时才为正例，相等时为负例；缺测时标签应保持未知。当前未展开具体阈值。',
        '- 每年读取相邻年份上下文后构造窗口；跨日、月、年均可计算。只在真实E2分区右边界排除，而非删除所有跨日历年窗口。',
        '- fold1/2/3与final_role分别记录用途，2024在全部开发折均为UNUSED。role为EXCLUDED_BOUNDARY的累计量仍保留供复核，但不能用于对应段训练／评价。',
        f'- {len(test["checks"])}项合成窗口／Decimal／E2／Parquet检查、每年{len(summary["annual_results"][0]["checks"])}项验收、{len(readback["checks"])}项根目录检查及{len(checks)}项交付检查通过，Java正常退出。',
        f'- {len(examples)}个真实案例已独立逐槽复算，并导出{len(example_slots)}行预期槽细节；覆盖零／非零累计、缺未来槽、缺历史槽、跨日／月／年和2017相位切换。真实人工案例仅用2017—2020资料。',
        '- 合并根目录对全部targets与observations做精确主键匹配，双方无遗漏或新增键。输入96个观测Parquet在开始前重新核对完整SHA-256，运行后大小／修改时间未变。',
        '- 2024只做窗口完整性、边界及结构验收，没有按阈值报告测试标签分布，也没有依据测试分数选择模型。','',
        '## B/C使用','',
        '数据：`data/processed/p0_4_v1/targets/`；数量、失效原因、station-month质量、E2用途和人工槽细节：`outputs/p0_4_targets/p0_4_v1/`。读取方法和字段说明见 [p0_4_data_contract.md](p0_4_data_contract.md)。','',
        'B仍需从observations构造历史特征；C训练／评价时按目标轮次role和model_anchor_eligible过滤，再按训练期支持情况冻结阈值、采样及拟合。future_30m_mm、target_valid及未来缺槽位置是标签／筛选信息，不得进入预测特征。真正调用预测接口时只检查已有历史与支持范围，不能以未来六槽是否存在作为事前预测门槛。','']
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
