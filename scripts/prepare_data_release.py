"""Freeze a portable release manifest after checking the existing stage receipts."""
from datetime import datetime, timezone
from pathlib import Path
from data_release import ROOT, read_json, resolve_path, sha256, write_json

DESCRIPTIONS = {
    'raw_line': '原始CSV整行，原样追溯；不作为模型输入',
    'field_count': '原始CSV字段数量，应为12', 'odd_quotes': '原始整行引号数是否为奇数，解析审计标记',
    '_corrupt_record': 'Spark捕获的解析失败原文；有效观测应为空',
    'source_year': '原始年度文件年份', 'source_file': '原始文件名／路径标识',
    'source_sha256': '原始文件全文SHA-256', 'raw_record_sha256': '原始整行SHA-256，追溯键；不替代观测主键',
    'station_id': '保留原始站点ID（含S113）；用于主键及按站点分组',
    'station_name': '记录对应的站点名称，允许历史变化', 'station_device_id': '记录对应的设备ID',
    'event_ts': '解析的原始观测时间，尚未进行2017对齐',
    'update_ts': '解析的原始update_timestamp；审计用，不作为天气特征',
    'reading_update_ts': '解析的原始reading_update_timestamp；审计用，不作为天气特征',
    'rain_mm': '该五分钟区间雨量，double，单位mm；特征计算可用，精确阈值比较用Decimal',
    'rain_decimal_mm': '该五分钟区间精确雨量，Decimal(38,18)，mm；标签累计使用',
    'longitude': '原行经度，度；不回填未来最新位置', 'latitude': '原行纬度，度；不回填未来最新位置',
    'event_epoch_s': '原始观测时间的Unix秒', 'original_offset_s': '原始epoch对300取余，秒',
    'alignment_offset_s': '实际对齐增加秒数（0或1）', 'alignment_rule': '实际采用的对齐规则标识',
    'slot_end_epoch_s': '对齐后的五分钟雨量区间末端Unix秒，观测主键时间',
    'slot_end_ts': '对齐后的区间末端／预测锚点t，五分钟网格',
    'rain_period_start_ts': '区间起点slot_end_ts-300秒，Task 1日历归属依据',
    'observation_date': '末端时间所属新加坡日历日期', 'rain_period_date': '区间起点所属新加坡日历日期',
    'rain_period_hour': '区间起点所属新加坡小时，0至23',
    'update_delay_s': '更新时间减原始观测时间，秒；仅审计',
    'reading_update_delay_s': '读数更新时间减原始观测时间，秒；仅审计',
    'quarantine_reasons': '隔离原因数组；有效observations为空数组',
    'metadata_warning': '名称／设备缺失、坐标解析／范围异常或更新时间解析缺失的警示；保留有效雨量',
    'source_effective_decimal_scale': '原始数值有效小数位数，精确表示审计',
    'decimal_representation_error': '无法按既定Decimal精确表示的标记；有效观测为false',
    'rules_version': '清洗规则版本标识', 'aligned_source_count': '映射到相同标准键的源行数量',
    'metadata_conflict': '对齐碰撞中元数据存在差异；雨量相等仍可保留',
    'rain_variants': '碰撞组中不同精确雨量的数量', 'metadata_variants': '碰撞组中不同元数据组合数量',
    'collision_rank': '确定性碰撞排序序号，1为优先行',
    'retained_record_sha256': '碰撞组最终保留原行的SHA-256', 'disposition': '碰撞原行处置，如RETAINED／MERGED_DUPLICATE',
    'raw_rows': '该站点／元数据组合在此原始年份内的源行数量（含合并前行）',
    'first_event_ts': '该站点／元数据组合首次出现的原始时间；不是连续有效期起点',
    'last_event_ts': '该组合最后出现的原始时间；不是连续有效期终点',
    'prediction_epoch_s': '预测锚点t的Unix秒，与slot_end_epoch_s相同',
    'prediction_ts': '预测锚点t，显示使用Asia/Singapore',
    'label_end_epoch_s': 't+1800秒；监督标签窗口右端', 'label_end_ts': '未来30分钟标签窗口右端时间',
    'future_missing_offsets_s': '未来六个预期槽中缺失槽的相对t偏移秒数；仅标签审计',
    'historical_missing_offsets_s': '历史12个预期槽中缺失槽的相对t偏移秒数',
    'future_n_valid': '未来六槽实际有效观测数，0至6；仅标签审计',
    'historical_n_valid': '截至t的12个历史槽实际有效数，0至12',
    'target_valid': '未来六槽是否齐全；监督标签筛选，禁止用作预测输入',
    'historical_valid': 'H1历史12槽是否齐全，可检查事前输入条件',
    'future_30m_mm': '未来六槽精确累计mm；仅有效时有值，否则null；禁止进入特征',
    'model_anchor_eligible': 'target_valid且historical_valid；仅训练／评价基础资格，不是事前预测门槛',
    'year_boundary_safe': '标签末端与t是否同日历年，仅诊断；禁止据此全局删跨年锚点',
    'invalid_reason': '未来标签失效原因数组，可能多原因；未知不等于无雨',
    'historical_invalid_reason': '历史输入完整性失效原因数组',
    'fold1_role': 'E2第一轮TRAIN17—20、VALIDATION21、UNUSED／EXCLUDED_BOUNDARY',
    'fold2_role': 'E2第二轮TRAIN17—21、VALIDATION22、UNUSED／EXCLUDED_BOUNDARY',
    'fold3_role': 'E2第三轮TRAIN17—22、VALIDATION23、UNUSED／EXCLUDED_BOUNDARY',
    'final_role': '配置锁定后TRAIN17—23、TEST24或EXCLUDED_BOUNDARY',
    'targets_version': '标签表版本p0_4_v1', 'observations_version': '来源观测版本p0_3_v1',
    'month': '对齐末端／预测锚点所属新加坡月份，分区键1至12',
}
RAW_NAMES = {
    'date': '原始日期', 'timestamp': '原始观测时间文本', 'update_timestamp': '原始更新时间文本',
    'station_id': '原始站点ID', 'station_name': '原始站名', 'station_device_id': '原始设备ID',
    'location_longitude': '原始经度文本', 'location_latitude': '原始纬度文本',
    'reading_update_timestamp': '原始读数更新时间文本', 'reading_value': '原始雨量文本，源单位mm',
    'reading_type': '原始读数类型文本', 'reading_unit': '原始单位文本，期望mm',
}


def description(table, name):
    if name == 'year':
        return '来源原始文件年份，分区键' if table in {'station_metadata_history', 'quarantine', 'collision_lineage'} else '对齐末端／预测锚点所属新加坡年份，分区键'
    if name.startswith('raw_') and name[4:] in RAW_NAMES:
        return RAW_NAMES[name[4:]] + '，未经替换，供追溯'
    return DESCRIPTIONS[name]  # Fail if any interface field lacks a description.


def type_text(value):
    if isinstance(value, str):
        return value
    if value['type'] == 'array':
        return f'array<{type_text(value["elementType"])}>'
    raise ValueError(f'Undocumented type {value}')


def write_dictionary(schemas):
    lines = ['# 正式数据逐字段字典（rainfall_v1）', '',
             '所有timestamp按Asia/Singapore显示，epoch为Unix秒。Parquet的nullable是存储schema属性，并不表示每个字段实际允许缺失；观测有效性及标签空值约束见data_contract。原始与审计字段保留用于追溯，不能整表直接传入模型。', '']
    for table, schema in schemas.items():
        lines += [f'## {table}', '', '| 字段 | Spark类型 | schema nullable | 定义与用途 |', '|---|---|---|---|']
        for field in schema['fields']:
            lines.append(f'| {field["name"]} | {type_text(field["type"])} | {str(field["nullable"]).lower()} | {description(table, field["name"])} |')
        lines += ['']
    (ROOT / 'docs/field_dictionary.md').write_text('\n'.join(lines), encoding='utf-8')


def main():
    destination = ROOT / 'manifests/release_manifest.json'
    if destination.exists():
        raise RuntimeError('Frozen release exists; create a new release version instead of overwriting')
    evidence = {}
    checked = {}
    for stage in ['p0_3_clean/p0_3_v1', 'p0_4_targets/p0_4_v1']:
        base = ROOT / 'outputs' / stage
        validation = read_json(base / 'validation.json')
        if validation['status'] != 'PASS' or not all(validation['checks'].values()):
            raise RuntimeError(f'Previous stage did not pass: {stage}')
        for item in read_json(base / 'output_manifest.json')['files']:
            path = resolve_path(ROOT, item['path'])
            if item['path'] not in checked:
                if path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
                    raise RuntimeError(f'Previous stage output changed: {item["path"]}')
                checked[item['path']] = item
        for file, key in [('data_contract', 'data_contract_sha256'), ('quality_report', 'report_sha256')]:
            phase = 'p0_3' if 'p0_3' in stage else 'p0_4'
            if sha256(ROOT / f'docs/{phase}_{file}.md') != validation[key]:
                raise RuntimeError(f'Historical {phase} {file} changed')
        evidence[stage] = {'validation': f'outputs/{stage}/validation.json', 'output_manifest': f'outputs/{stage}/output_manifest.json'}
    config_hash = sha256(ROOT / 'configs/project.json')
    signature = read_json(ROOT / 'outputs/p0_4_targets/p0_4_v1/summary.json')['signature']
    if signature['config_sha256'] != config_hash:
        raise RuntimeError('Confirmed configuration changed')
    if sha256(ROOT / 'src/03_build_targets.py') != signature['code_sha256']:
        raise RuntimeError('Target builder changed')
    root_check = read_json(ROOT / 'outputs/p0_3_clean/p0_3_v1/parquet_readback.json')
    if sha256(ROOT / 'manifests/raw_manifest.json') != root_check['cleaning_signature']['manifest_sha256']:
        raise RuntimeError('Raw source manifest changed')
    if sha256(ROOT / 'outputs/p0_3_clean/p0_3_v1/output_manifest.json') != signature['source_output_manifest_sha256']:
        raise RuntimeError('Target source manifest changed')
    for path, key in [('src/02_clean_standardize.py', 'code_sha256'), ('scripts/spark_session.py', 'session_helper_sha256'), ('scripts/spark_runtime.py', 'runtime_helper_sha256')]:
        if sha256(ROOT / path) != root_check['cleaning_signature'][key]:
            raise RuntimeError(f'Frozen stage code changed: {path}')
    schemas = read_json(ROOT / 'outputs/p0_3_clean/p0_3_v1/schemas/root_interfaces.json')
    schemas['targets'] = read_json(ROOT / 'outputs/p0_4_targets/p0_4_v1/schemas/root_targets.json')
    write_dictionary(schemas)
    paths = set()
    for pattern in ['scripts/*.py', 'scripts/*.ps1', 'src/*.py', 'configs/*.json', 'docs/*.md', 'manifests/raw_manifest.json', 'data/samples/**/*', 'outputs/p0_2_audit/p0_2_v1/**/*', 'outputs/p0_3_clean/p0_3_v1/**/*', 'outputs/p0_4_targets/p0_4_v1/**/*']:
        paths.update(p for p in ROOT.glob(pattern) if p.is_file())
    paths.update(ROOT / p for p in ['README.md', 'project_requirement.md', '项目分工.md', 'A成员工作细化与数据底座方案.md', 'requirements-spark.txt', '.gitignore'])
    paths -= {ROOT / 'configs/processing_status.json', ROOT / 'docs/p1_5_status.md'}
    for prefix in ['data/processed/p0_3_v1', 'data/processed/p0_4_v1']:
        paths.update((ROOT / prefix).rglob('*.parquet'))
    paths = {p for p in paths if p.suffix != '.crc' and p.name != '_SUCCESS' and '__pycache__' not in p.parts}
    files = []
    for path in sorted(paths):
        relative = path.relative_to(ROOT).as_posix()
        kind = 'full_data' if relative.startswith('data/processed/') else 'sample_data' if relative.startswith('data/samples/') else 'metadata'
        item = checked.get(relative) or {'path': relative, 'bytes': path.stat().st_size, 'sha256': sha256(path)}
        files.append({**item, 'kind': kind})
    tables = {}
    for name, schema in schemas.items():
        relative = f'data/processed/p0_4_v1/{name}' if name == 'targets' else f'data/processed/p0_3_v1/{name}'
        payload = [i for i in files if i['path'].startswith(relative + '/')]
        tables[name] = {'version': 'p0_4_v1' if name == 'targets' else 'p0_3_v1', 'path': relative,
                        'schema': schema, 'parquet_files': len(payload), 'parquet_bytes': sum(i['bytes'] for i in payload)}
    tables['observations']['rows'] = root_check['metrics']['rows']
    tables['observations']['primary_key'] = ['station_id', 'slot_end_epoch_s']
    tables['targets']['rows'] = read_json(ROOT / 'configs/processing_status.json')['P0-4']['totals']['anchors']
    tables['targets']['primary_key'] = ['station_id', 'prediction_epoch_s']
    tables['quarantine']['rows'] = root_check['quarantine_rows']
    tables['collision_lineage']['rows'] = sum(root_check['ledger_dispositions'].values())
    tables['station_metadata_history']['row_count_note'] = 'Grouped metadata combinations; metadata_raw_rows sums to 48,538,713 (not table row count)'
    sample = read_json(ROOT / 'data/samples/p1_5_v1/sample_manifest.json')
    manifest = {'manifest_format_version': 1, 'status': 'FROZEN_LOCAL', 'release_id': 'rainfall_v1',
                'data_version': read_json(ROOT / 'configs/project.json')['data_version'],
                'config_hash': config_hash, 'config_hash_algorithm': 'sha256 of exact configs/project.json bytes',
                'created_at': datetime.now(timezone.utc).isoformat(), 'timezone': 'Asia/Singapore',
                'tables': tables, 'sample': {'version': sample['sample_version'], 'path': 'data/samples/p1_5_v1', 'rows_per_table': sample['rows_per_table']},
                'stage_evidence': evidence, 'raw_manifest': 'manifests/raw_manifest.json',
                'interface_contract': 'docs/data_contract.md', 'field_dictionary': 'docs/field_dictionary.md',
                'entrypoints': {'read_and_verify': 'scripts/read_data_release.py', 'downstream_provenance': 'scripts/record_result_metadata.py'},
                'publication': {'status': 'NOT_PUBLISHED', 'github_repository': None, 'download_urls': [], 'user_instruction': 'Publish only after final content confirmation'},
                'files': files}
    write_json(destination, manifest)
    write_json(ROOT / 'outputs/p1_5_release/freeze_receipt.json', {'status': 'PASS', 'prior_stage_files_checked': len(checked),
               'release_files': len(files), 'release_manifest_sha256': sha256(destination),
               'full_parquet_bytes': sum(i['bytes'] for i in files if i['kind'] == 'full_data'),
               'sample_parquet_bytes': sum(i['bytes'] for i in files if i['kind'] == 'sample_data' and i['path'].startswith('data/samples/p1_5_v1/') and i['path'].endswith('.parquet'))})
    print(f'Frozen {manifest["release_id"]}: {len(files)} files; config {config_hash}')


if __name__ == '__main__':
    main()
