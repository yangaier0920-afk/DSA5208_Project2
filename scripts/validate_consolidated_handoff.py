"""Verify the consolidated entry point and package without changing data rules."""
import hashlib
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from data_release import ROOT, provenance, read_json, resolve_path, sha256, verify_release, write_json


def run_reader(root, receipt):
    subprocess.run([sys.executable, str(root / 'scripts/read_data_release.py'), '--member', 'A', '--receipt', str(receipt)], check=True, cwd=ROOT / 'outputs/team_handoff')
    if read_json(receipt)['status'] != 'PASS':
        raise RuntimeError('Reader did not pass')


def main():
    output = ROOT / 'outputs/team_handoff'
    output.mkdir(parents=True, exist_ok=True)
    manifest = read_json(ROOT / 'manifests/release_manifest.json')
    support = read_json(ROOT / 'manifests/a_support_manifest.json')
    old = read_json(ROOT / 'archive/history_v1/manifests/release_manifest.json')
    full_check = verify_release(ROOT, 'full')
    if manifest['tables'] != old['tables'] or manifest['config_hash'] != old['config_hash']:
        raise RuntimeError('Documentation revision changed data/configuration interfaces')
    if [p.name for p in ROOT.glob('*.md')] != ['README.md'] or [p.name for p in (ROOT / 'docs').glob('*.md')] != ['reference.md']:
        raise RuntimeError('Active documentation is not consolidated')
    for item in support['files']:
        path = resolve_path(ROOT, item['path'])
        if path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
            raise RuntimeError(f'Support archive differs: {item["path"]}')
    if os.name == 'nt':
        from spark_runtime import configure_runtime
        configure_runtime()
    run_reader(ROOT, output / 'A_sample_r2.json')
    members = {i['path'] for i in manifest['files'] if i['kind'] != 'full_data'} | {i['path'] for i in support['files']}
    members |= {'manifests/release_manifest.json', 'manifests/a_support_manifest.json'}
    archive = output / 'rainfall_v1_r2_handoff.zip'
    if archive.exists():
        raise RuntimeError('Handoff archive exists')
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        for relative in sorted(members):
            bundle.write(resolve_path(ROOT, relative), relative)
    staging = ROOT / '.runtime/consolidated_handoff'
    staging.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='check_', dir=staging) as temporary:
        root = Path(temporary) / 'team handoff'
        with zipfile.ZipFile(archive) as bundle:
            if bundle.testzip() is not None:
                raise RuntimeError('ZIP checksum failed')
            bundle.extractall(root)
        verify_release(root, 'sample')
        for item in support['files']:
            path = resolve_path(root, item['path'])
            if path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
                raise RuntimeError('Extracted support file differs')
        run_reader(root, root / 'outputs/A_sample_new_directory.json')
        write_json(output / 'A_relocated_sample_r2.json', read_json(root / 'outputs/A_sample_new_directory.json'))
        replay = root / 'outputs/reproduction/run_001'
        subprocess.run([sys.executable, str(root / 'scripts/reproduce_a_sample.py'), '--output', str(replay)], check=True, cwd=staging)
        receipt = read_json(replay / 'verification.json')
        if receipt['status'] != 'PASS' or not all(receipt['checks'].values()) or receipt['counts']['target_anchors'] != 114:
            raise RuntimeError('README reproduction entry failed')
        write_json(output / 'A_relocated_replay_r2.json', receipt)
    validation = {'status': 'PASS', **provenance(), 'scope': 'documentation_consolidation_and_A_local_read_replay',
        'checks': {'one_root_readme': True, 'one_active_reference_document': True, 'full_file_hashes_match': full_check['status'] == 'PASS',
                   'data_tables_and_configuration_unchanged': True, 'historical_support_files_preserved': True,
                   'current_reader_pass': True, 'extracted_reader_pass': True, 'extracted_raw_to_target_replay_pass': True},
        'pending': {'B_C_actual_hosts': 'PENDING', 'model_validation': 'PENDING_C_MODEL', 'github': 'NOT_PUBLISHED'}}
    write_json(output / 'validation.json', validation)
    status_path = ROOT / 'configs/processing_status.json'
    status = read_json(status_path)
    status['P1-5'].update({'release_id': manifest['release_id'], 'manifest': 'manifests/release_manifest.json',
                         'validation': 'outputs/team_handoff/validation.json'})
    status['P1-6'].update({'support_version': support['support_version'], 'readme': 'README.md',
                         'validation': 'outputs/team_handoff/validation.json'})
    status['documentation'] = {'entrypoint': 'README.md', 'reference': 'docs/reference.md', 'history': 'archive/history_v1', 'release_id': manifest['release_id']}
    write_json(status_path, status)
    # Final progress receipts do not enter the immutable input manifest.
    receipts = ['configs/processing_status.json', 'outputs/team_handoff/validation.json', 'outputs/team_handoff/A_sample_r2.json',
                'outputs/team_handoff/A_relocated_sample_r2.json', 'outputs/team_handoff/A_relocated_replay_r2.json']
    with zipfile.ZipFile(archive, 'a', zipfile.ZIP_DEFLATED) as bundle:
        for relative in receipts:
            bundle.write(ROOT / relative, relative)
    with zipfile.ZipFile(archive) as bundle:
        if bundle.testzip() is not None:
            raise RuntimeError('Final ZIP checksum failed')
        for item in manifest['files'] + support['files']:
            if item.get('kind') == 'full_data':
                continue
            payload = bundle.read(item['path'])
            if len(payload) != item['bytes'] or hashlib.sha256(payload).hexdigest() != item['sha256']:
                raise RuntimeError('Final ZIP payload differs')
        member_count = len(bundle.namelist())
    write_json(output / 'bundle_manifest.json', {'status': 'PASS', **provenance(), 'archive': archive.relative_to(ROOT).as_posix(),
        'bytes': archive.stat().st_size, 'sha256': sha256(archive), 'members': member_count,
        'entrypoint': 'README.md', 'reference': 'docs/reference.md', 'contains_full_data': False, 'publication': 'LOCAL_ONLY'})
    print(f'PASS: consolidated README, extracted reader/replay; package {archive.stat().st_size:,} bytes')


if __name__ == '__main__':
    main()
