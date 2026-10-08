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
    lines=['# P0-3 清洗与标准化验收报告','',
        '状态：PASS。P0-2先通过32项交付复核，P0-3完成八年Spark清洗、逐年Parquet重读及B/C使用的根目录合并读取验证。E2已确认；本阶段没有生成标签或训练模型。','',
        '## 数量账目','',
        '| 原始记录 | observations | quarantine | 合并移除重复 |','|---:|---:|---:|---:|',
        f'| {summary["raw_rows"]:,} | {summary["totals"]["observations"]:,} | {summary["totals"]["quarantine_rows"]:,} | {summary["totals"]["merged_duplicate_rows"]:,} |','',
        '原始 = observations + quarantine + 合并移除重复。空quarantine保留可读schema；collision_lineage的172行包含86行保留侧和86行移除侧，不重复计入数量账目。','',
        '| 年份 | 原始 | observations | 隔离 | 移除重复 |','|---|---:|---:|---:|---:|']
    lines += [f'| {r["year"]} | {r["raw_rows"]:,} | {r["observations"]:,} | {r["quarantine"]} | {r["merged_duplicates"]} |' for r in annual]
    lines += ['', '## 已实施规则与验证','',
        '- T2：2017年1,775,327条原始偏移行派生时间加一秒；原始时间保留。加一秒使6,191条原始记录跨日。合并之后的偏移保留数见年度账目，不能混用处理前后口径。',
        '- C1：86组一致雨量碰撞优先保留原标准刻度记录；不相加雨量。S113的2组元数据差异已标志，双方历史均保留。',
        '- L1/D1：区间终点为slot_end_ts，起点为终点减300秒；天气分析按区间起点归属。此区间含义仍是项目假设，不能写成官方证明。',
        '- M1/R1：稀疏存储、不补零，合法零值和非负有限极端值保留。',
        f'- 精确雨量：存储为{summary["decimal_storage"]}，全量观测源有效小数位最高为{summary["max_source_effective_decimal_scale"]}；未静默舍入。逐年精确雨量账目闭合。',
        f'- {len(test["checks"])}项合成异常与Parquet检查、每年{len(summary["annual_results"][0]["checks"])}项重读检查、{len(readback["checks"])}项合并目录检查、{len(checks)}项交付检查均通过，Java进程正常退出。',
        '- 八个原始文件在处理前重新核对完整SHA-256，运行期间大小/修改时间未变。交付文件另列SHA-256；没有修改原始CSV。','',
        '## 覆盖与后续使用','',
        '分别导出观测终点日历覆盖及雨量区间日历覆盖，后者先汇总所有来源年份的贡献，正确处理跨月/年边界。完整日历为分母；缺口为未知。B1/K1面板已按清洗结果重算，候选共同站点仍为38个。','',
        '| 月份 | 八年同月共同可比站点（90%） | 95%敏感性 |','|---|---:|---:|']
    p={(r['coverage_cutoff'],r['month']):r['comparable_station_count'] for r in panel}
    lines += [f'| {m} | {p[(0.9,m)]} | {p[(0.95,m)]} |' for m in range(1,13)]
    lines += ['', '无合格站点的月份不能强行作八年同口径比较；也不能把有资格月份的总量称为全年雨量。2024年底没有2025相邻观测，资料边界缺口仍保留。覆盖表不筛掉全局observations或Task2。','',
        '接口、字段、隔离原因、复现命令见 [p0_3_data_contract.md](p0_3_data_contract.md)。数量及覆盖交付位于 `outputs/p0_3_clean/p0_3_v1/`；正式数据位于 `data/processed/p0_3_v1/`。下一阶段P0-4才构造完整历史/未来窗口并按E2划分。','']
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
