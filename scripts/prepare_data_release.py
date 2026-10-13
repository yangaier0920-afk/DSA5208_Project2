"""Freeze a portable release manifest after checking the existing stage receipts."""
from datetime import datetime, timezone
from pathlib import Path
from data_release import ROOT, read_json, resolve_path, sha256, write_json

DESCRIPTIONS = {
    'raw_line': 'Original CSV record, preserved verbatim for lineage; not a model input',
    'field_count': 'Number of fields in the original CSV record; expected to be 12', 'odd_quotes': 'Whether the original record has an odd quote count; parsing audit flag',
    '_corrupt_record': 'Original text captured by Spark for a parsing failure; null for valid observations',
    'source_year': 'Year of the original annual source file', 'source_file': 'Original filename or source-path identifier',
    'source_sha256': 'SHA-256 of the complete original source file', 'raw_record_sha256': 'SHA-256 of the original record; lineage identifier, not a replacement observation key',
    'station_id': 'Preserved original station ID, including S113; used in the primary key and station grouping',
    'station_name': 'Station name associated with this record; historical changes are retained', 'station_device_id': 'Device ID associated with this record',
    'event_ts': 'Parsed original observation timestamp, before the 2017 alignment',
    'update_ts': 'Parsed original update_timestamp; audit only, not a weather predictor',
    'reading_update_ts': 'Parsed original reading_update_timestamp; audit only, not a weather predictor',
    'rain_mm': 'Five-minute interval rainfall as double, in mm; usable for features, while exact threshold comparisons use Decimal',
    'rain_decimal_mm': 'Exact five-minute rainfall as Decimal(38,18), in mm; used for label accumulation',
    'longitude': 'Longitude in the original row, in degrees; never backfilled with a future location', 'latitude': 'Latitude in the original row, in degrees; never backfilled with a future location',
    'event_epoch_s': 'Unix seconds of the original observation timestamp', 'original_offset_s': 'Original epoch modulo 300, in seconds',
    'alignment_offset_s': 'Seconds actually added during alignment (0 or 1)', 'alignment_rule': 'Identifier of the applied alignment rule',
    'slot_end_epoch_s': 'Aligned five-minute interval endpoint in Unix seconds; timestamp component of the observation key',
    'slot_end_ts': 'Aligned interval endpoint or prediction anchor t on the five-minute grid',
    'rain_period_start_ts': 'Interval start, slot_end_ts minus 300 seconds; calendar attribution for Task 1',
    'observation_date': 'Singapore calendar date of the interval endpoint', 'rain_period_date': 'Singapore calendar date of the interval start',
    'rain_period_hour': 'Singapore hour of the interval start, from 0 to 23',
    'update_delay_s': 'Update timestamp minus original observation time, in seconds; audit only',
    'reading_update_delay_s': 'Reading-update timestamp minus original observation time, in seconds; audit only',
    'quarantine_reasons': 'Quarantine-reason array; empty for valid observations',
    'metadata_warning': 'Warning for missing name/device, invalid coordinates or unparsed update timestamps; valid rainfall remains retained',
    'source_effective_decimal_scale': 'Effective decimal places in the source number; lossless-representation audit',
    'decimal_representation_error': 'Flag for failure to represent a value exactly with the selected Decimal; false for valid observations',
    'rules_version': 'Cleaning-rule version identifier', 'aligned_source_count': 'Number of source rows mapped to the same aligned key',
    'metadata_conflict': 'Metadata differs within an aligned-key collision; equal rainfall may still be retained',
    'rain_variants': 'Number of distinct exact rainfall values in a collision group', 'metadata_variants': 'Number of distinct metadata combinations in a collision group',
    'collision_rank': 'Deterministic collision rank; 1 identifies the preferred row',
    'retained_record_sha256': 'SHA-256 of the original record retained from the collision group', 'disposition': 'Disposition of a collision record, such as RETAINED or MERGED_DUPLICATE',
    'raw_rows': 'Source-row count for this station/metadata combination in the source year, before duplicate merging',
    'first_event_ts': 'First original appearance of this station/metadata combination; not the start of a continuous validity period',
    'last_event_ts': 'Last original appearance of this combination; not the end of a continuous validity period',
    'prediction_epoch_s': 'Prediction anchor t in Unix seconds; equal to slot_end_epoch_s',
    'prediction_ts': 'Prediction anchor t, displayed in Asia/Singapore',
    'label_end_epoch_s': 't+1800 seconds; right endpoint of the supervised label window', 'label_end_ts': 'Right endpoint of the future 30-minute label window',
    'future_missing_offsets_s': 'Missing expected future-slot offsets from t, in seconds; label audit only',
    'historical_missing_offsets_s': 'Missing expected historical-slot offsets from t, in seconds',
    'future_n_valid': 'Valid observations among the six future slots, from 0 to 6; label audit only',
    'historical_n_valid': 'Valid observations among the 12 historical slots through t, from 0 to 12',
    'target_valid': 'Whether all six future slots are present; supervision filter, prohibited as a predictor',
    'historical_valid': 'Whether all 12 H1 historical slots are present; may check an inference prerequisite',
    'future_30m_mm': 'Exact total over six future slots in mm; null if invalid; prohibited as a feature',
    'model_anchor_eligible': 'target_valid AND historical_valid; base supervision eligibility, not an inference prerequisite',
    'year_boundary_safe': 'Whether label end and t lie in the same calendar year; diagnostic only, never a global cross-year filter',
    'invalid_reason': 'Future-label invalidity reasons, potentially multiple; unknown is not dry',
    'historical_invalid_reason': 'Historical-input completeness failure reasons',
    'fold1_role': 'E2 fold 1: TRAIN 2017-2020, VALIDATION 2021, or UNUSED/EXCLUDED_BOUNDARY',
    'fold2_role': 'E2 fold 2: TRAIN 2017-2021, VALIDATION 2022, or UNUSED/EXCLUDED_BOUNDARY',
    'fold3_role': 'E2 fold 3: TRAIN 2017-2022, VALIDATION 2023, or UNUSED/EXCLUDED_BOUNDARY',
    'final_role': 'After configuration lock: TRAIN 2017-2023, TEST 2024, or EXCLUDED_BOUNDARY',
    'targets_version': 'Target-table version p0_4_v1', 'observations_version': 'Source-observation version p0_3_v1',
    'month': 'Singapore month of the aligned endpoint/prediction anchor; partition key from 1 to 12',
}
RAW_NAMES = {
    'date': 'Original date', 'timestamp': 'Original observation timestamp text', 'update_timestamp': 'Original update timestamp text',
    'station_id': 'Original station ID', 'station_name': 'Original station name', 'station_device_id': 'Original device ID',
    'location_longitude': 'Original longitude text', 'location_latitude': 'Original latitude text',
    'reading_update_timestamp': 'Original reading-update timestamp text', 'reading_value': 'Original rainfall text, in source unit mm',
    'reading_type': 'Original reading-type text', 'reading_unit': 'Original unit text; expected mm',
}


def description(table, name):
    if name == 'year':
        return 'Original source-file year; partition key' if table in {'station_metadata_history', 'quarantine', 'collision_lineage'} else 'Singapore year of the aligned endpoint/prediction anchor; partition key'
    if name.startswith('raw_') and name[4:] in RAW_NAMES:
        return RAW_NAMES[name[4:]] + ', unchanged for provenance'
    return DESCRIPTIONS[name]  # Fail if any interface field lacks a description.


def type_text(value):
    if isinstance(value, str):
        return value
    if value['type'] == 'array':
        return f'array<{type_text(value["elementType"])}>'
    raise ValueError(f'Undocumented type {value}')


def write_dictionary(schemas):
    lines = ['# Published data field dictionary (rainfall_v1)', '',
             'Timestamps display in Asia/Singapore and epochs are Unix seconds. Parquet nullable is a storage-schema property, not permission for every field to be missing. See data_contract for observation validity and target nullability. Raw and audit fields are retained for provenance; do not pass whole tables to a model.', '']
    for table, schema in schemas.items():
        lines += [f'## {table}', '', '| Field | Spark type | Schema nullable | Definition and purpose |', '|---|---|---|---|']
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
    paths.update(ROOT / p for p in ['README.md', 'project_requirement.md', 'TEAM_WORK_PLAN.md', 'A_DATA_FOUNDATION_PLAN.md', 'requirements-spark.txt', '.gitignore'])
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
