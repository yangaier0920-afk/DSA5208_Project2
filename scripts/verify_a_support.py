"""Freeze A support files, replay the extracted bundle, and record local scope."""
import csv
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from data_release import ROOT, load_release, provenance, read_json, resolve_path, sha256, verify_release, write_json


def main():
    output = ROOT / 'outputs/p1_6_support'
    archive = output / 'rainfall_v1_A_support.zip'
    support_path = ROOT / 'manifests/a_support_manifest.json'
    if archive.exists() or support_path.exists():
        raise RuntimeError('Frozen A support exists; create another support version')
    manifest = load_release()
    base_integrity = verify_release(ROOT, 'full')
    original_stats = {i['path']: (resolve_path(ROOT, i['path']).stat().st_size, resolve_path(ROOT, i['path']).stat().st_mtime_ns) for i in manifest['files'] if i['kind'] == 'full_data'}
    replay = read_json(output / 'local_replay_v1/verification.json')
    if replay['status'] != 'PASS' or not all(replay['checks'].values()) or not replay['shutdown']['passed']:
        raise RuntimeError('Initial representative replay is not valid')
    metrics = read_json(output / 'report_metrics.json')
    fixture = read_json(ROOT / 'data/samples/p1_6_v1/fixture_manifest.json')
    for value in [replay, metrics, fixture]:
        if any(value[k] != v for k, v in provenance().items()):
            raise RuntimeError('Support materials refer to different base versions')
    clean = read_json(ROOT / 'outputs/p0_3_clean/p0_3_v1/summary.json')
    target = read_json(ROOT / 'outputs/p0_4_targets/p0_4_v1/summary.json')
    with (output / 'annual_cleaning_and_targets.csv').open(encoding='utf-8-sig') as stream:
        annual = list(csv.DictReader(stream))
    if sum(int(r['observations']) for r in annual) != clean['totals']['observations'] or sum(int(r['future_valid']) for r in annual) != target['totals']['future_valid']:
        raise RuntimeError('Report annual totals differ from full-run evidence')
    for row, cleaning, targets in zip(annual, clean['annual_results'], target['annual_results']):
        if int(row['year']) != cleaning['year'] or int(row['raw_records']) != cleaning['profile']['raw_rows'] or int(row['observations']) != cleaning['metrics']['observations'] or int(row['future_invalid']) != targets['metrics']['future_invalid']:
            raise RuntimeError('Report annual source values differ')
    for timing in metrics['full_run_timings']:
        run = read_json(ROOT / timing['run_record'])
        expected = round((datetime.fromisoformat(run['finished_at_sgt']) - datetime.fromisoformat(run['started_at_sgt'])).total_seconds(), 3)
        if timing['wall_seconds'] != expected or timing['peak_rss_status'] != 'NOT_MEASURED':
            raise RuntimeError('Report runtime/memory claims differ')
    english = (ROOT / 'docs/a_report_section_en.md').read_text(encoding='utf-8')
    chinese = (ROOT / 'docs/a_cleaning_report_zh.md').read_text(encoding='utf-8')
    if re.search(r'[\u4e00-\u9fff]', english):
        raise RuntimeError('English report contains untranslated Chinese')
    for text in [english, chinese]:
        for value in [48_538_713, 48_538_627, 47_636_833, 901_794, 47_048_872]:
            if f'{value:,}' not in text:
                raise RuntimeError('Report misses a required verified count')
    paths = []
    for pattern in ['data/samples/p1_6_v1/**/*', 'outputs/p1_6_support/local_replay_v1/**/*']:
        paths += [p for p in ROOT.glob(pattern) if p.is_file() and p.suffix != '.crc' and p.name != '_SUCCESS']
    paths += [ROOT / p for p in [
        'README_A_P1_6.md', 'docs/a_cleaning_report_zh.md', 'docs/a_report_section_en.md', 'docs/p1_6_status.md',
        'scripts/prepare_a_reproduction_fixture.py', 'scripts/reproduce_a_sample.py', 'scripts/prepare_a_report_support.py', 'scripts/verify_a_support.py',
        'outputs/p1_6_support/fixture_preparation.json', 'outputs/p1_6_support/hardware_inventory.json',
        'outputs/p1_6_support/report_metrics.json', 'outputs/p1_6_support/full_run_resources.csv',
        'outputs/p1_6_support/annual_cleaning_and_targets.csv', 'outputs/p1_6_support/loss_ledger.csv']]
    files = [{'path': p.relative_to(ROOT).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in sorted(set(paths))]
    support = {'format_version': 1, 'support_version': 'a_support_v1', 'status': 'FROZEN_LOCAL_A_SUPPORT',
               **provenance(), 'created_at': datetime.now(timezone.utc).isoformat(),
               'base_release_manifest': 'manifests/release_manifest.json',
               'scope': 'A local reproduction and report materials; no downstream modelling, no remote publication',
               'entrypoint': 'README_A_P1_6.md', 'replay_entrypoint': 'scripts/reproduce_a_sample.py',
               'full_data_included': False, 'files': files}
    write_json(support_path, support)
    members = {i['path'] for i in manifest['files'] if i['kind'] != 'full_data'}
    members |= {i['path'] for i in files}
    members |= {'manifests/release_manifest.json', 'manifests/a_support_manifest.json', 'docs/p1_5_status.md', 'outputs/p1_5_receipts/A_sample.json'}
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        for relative in sorted(members):
            bundle.write(resolve_path(ROOT, relative), relative)
    with zipfile.ZipFile(archive) as bundle:
        if bundle.testzip() is not None or any(p.startswith(('Raw_Dataset/', 'data/processed/')) for p in bundle.namelist()):
            raise RuntimeError('Support archive content/CRC check failed')
    if os.name == 'nt' and (ROOT / '.runtime/java_manifest.json').exists():
        from spark_runtime import configure_runtime
        configure_runtime()
    staging = ROOT / '.runtime/p1_6_package_check'
    staging.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='relocated_', dir=staging) as temporary:
        root = Path(temporary) / 'A support reproduction'
        with zipfile.ZipFile(archive) as bundle:
            bundle.extractall(root)
        verify_release(root, 'sample')
        for item in files:
            path = resolve_path(root, item['path'])
            if path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
                raise RuntimeError('Extracted support file differs')
        run = root / 'outputs/p1_6_runs/relocated_run_001'
        subprocess.run([sys.executable, str(root / 'scripts/reproduce_a_sample.py'), '--output', str(run)], cwd=staging, check=True)
        relocated = read_json(run / 'verification.json')
        if relocated['status'] != 'PASS' or relocated['counts'] != replay['counts'] or relocated['release_manifest_sha256'] != replay['release_manifest_sha256']:
            raise RuntimeError('Extracted package replay differs')
        for path in run.rglob('*.json'):
            dest = output / 'relocated_replay_evidence' / path.relative_to(run)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(path.read_bytes())
        write_json(output / 'relocated_replay_verification.json', relocated)
    base_unchanged = all((resolve_path(ROOT, path).stat().st_size, resolve_path(ROOT, path).stat().st_mtime_ns) == value for path, value in original_stats.items())
    verify_release(ROOT, 'sample')
    checks = {'base_release_full_file_hashes_pass': base_integrity['status'] == 'PASS',
              'base_formal_parquet_unchanged_during_support_test': base_unchanged,
              'local_raw_to_target_replay_pass': True, 'independent_decimal_oracles_all_114_pass': len(read_json(output / 'local_replay_v1/independent_recomputation.json')) == 114,
              'report_annual_counts_match_full_evidence': True, 'run_times_match_original_receipts': True,
              'unmeasured_peak_memory_explicit': True, 'english_report_contains_no_chinese': True,
              'base_release_identity_preserved': True, 'support_files_match_after_extraction': True,
              'extracted_package_real_spark_replay_pass': True, 'archive_crc_and_no_full_data_pass': True}
    if not all(checks.values()):
        raise RuntimeError(f'Local A support checks failed: {checks}')
    write_json(output / 'validation.json', {'status': 'PASS', 'completion_scope': 'A_LOCAL_MATERIALS_AND_REPRODUCTION_ONLY',
        **provenance(), 'support_version': support['support_version'], 'support_manifest_sha256': sha256(support_path),
        'checks': checks, 'local_replay_seconds': replay['elapsed_seconds'], 'relocated_replay_seconds': relocated['elapsed_seconds'],
        'pending': {'B_C_other_machine_reproduction': 'PENDING', 'macos_actual_host_check': 'PENDING', 'final_model_reload_and_prediction': 'PENDING_C_MODEL',
                    'team_report_and_submission': 'PENDING_B_C_AND_TEAM_FINAL_REVIEW', 'github_publication': 'NOT_STARTED'},
        'relocation_scope': 'Same A Windows host and configured Java/Hadoop/Python; fresh extracted paths and a different working directory'})
    write_json(output / 'bundle_manifest.json', {'support_version': support['support_version'], **provenance(),
        'archive': archive.relative_to(ROOT).as_posix(), 'bytes': archive.stat().st_size, 'sha256': sha256(archive),
        'archive_members': len(members), 'support_manifest_sha256': sha256(support_path), 'contains_full_data': False, 'publication': 'LOCAL_ONLY'})
    state_path = ROOT / 'configs/processing_status.json'
    state = read_json(state_path)
    state['P1-6'] = {'status': 'LOCAL_A_COMPLETE', 'owner': 'A', 'data_version': manifest['data_version'],
                    'support_version': support['support_version'], 'manifest': 'manifests/a_support_manifest.json',
                    'readme': 'README_A_P1_6.md', 'validation': 'outputs/p1_6_support/validation.json',
                    'B_C_other_machine_reproduction': 'PENDING', 'final_model_validation': 'PENDING_C_MODEL', 'github_publication': 'NOT_STARTED'}
    write_json(state_path, state)
    print(f'PASS: {len(checks)} A support checks; ZIP {archive.stat().st_size:,} bytes')


if __name__ == '__main__':
    main()
