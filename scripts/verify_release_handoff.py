"""Package the interface release and test its relocated copy on A's host."""
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from data_release import ROOT, load_release, read_json, resolve_path, sha256, verify_release, write_json


def main():
    manifest = load_release()
    receipt = read_json(ROOT / 'outputs/p1_5_receipts/A_sample.json')
    manifest_hash = sha256(ROOT / 'manifests/release_manifest.json')
    if receipt['status'] != 'PASS' or receipt['release_manifest_sha256'] != manifest_hash or not receipt['shutdown']['passed']:
        raise RuntimeError('A must pass the current release reader before packaging')
    verify_release(scope='sample')
    output = ROOT / 'outputs/p1_5_release'
    output.mkdir(parents=True, exist_ok=True)
    archive = output / 'rainfall_v1_interface.zip'
    if archive.exists():
        raise RuntimeError('Archive exists; do not overwrite a frozen handoff')
    members = [i['path'] for i in manifest['files'] if i['kind'] != 'full_data']
    members += ['manifests/release_manifest.json', 'docs/p1_5_status.md', 'outputs/p1_5_receipts/A_sample.json']
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
        for relative in sorted(members):
            bundle.write(resolve_path(ROOT, relative), relative)
    checks = {'A_sample_readback_pass': True, 'package_excludes_full_data_and_raw_csv': True}
    with zipfile.ZipFile(archive) as bundle:
        if bundle.testzip() is not None:
            raise RuntimeError('ZIP CRC failed')
        if any(p.startswith(('data/processed/', 'Raw_Dataset/')) for p in bundle.namelist()):
            raise RuntimeError('Interface archive includes full payload')
    if os.name == 'nt' and (ROOT / '.runtime/java_manifest.json').exists():
        from spark_runtime import configure_runtime
        configure_runtime()
    with tempfile.TemporaryDirectory(prefix='relocated_', dir=output) as temporary:
        root = Path(temporary) / 'rainfall handoff'
        with zipfile.ZipFile(archive) as bundle:
            bundle.extractall(root)
        checks['relocated_file_hashes_pass'] = verify_release(root, 'sample')['status'] == 'PASS'
        # Mutate only a disposable extracted copy, then restore it.
        config = root / 'configs/project.json'
        original = config.read_bytes()
        try:
            config.write_bytes(original + b'\n')
            try:
                load_release(root)
            except RuntimeError:
                checks['changed_config_rejected'] = True
            else:
                raise RuntimeError('Changed configuration was accepted')
        finally:
            config.write_bytes(original)
        extra = root / 'data/samples/p1_5_v1/observations/unlisted.parquet'
        try:
            extra.write_bytes(b'not a registered parquet file')
            try:
                verify_release(root, 'sample')
            except RuntimeError:
                checks['unlisted_parquet_rejected'] = True
            else:
                raise RuntimeError('Unlisted Parquet was accepted')
        finally:
            extra.unlink()
        try:
            resolve_path(root, '../outside.json')
        except ValueError:
            checks['path_traversal_rejected'] = True
        else:
            raise RuntimeError('Escaping release path accepted')
        reader = root / 'scripts/read_data_release.py'
        command = [sys.executable, str(reader), '--member', 'A', '--receipt', str(root / 'outputs/A_relocated.json')]
        subprocess.run(command, cwd=output, check=True)
        relocated = read_json(root / 'outputs/A_relocated.json')
        if relocated['status'] != 'PASS' or relocated['release_manifest_sha256'] != manifest_hash:
            raise RuntimeError('Relocated Spark reader did not pass')
        write_json(output / 'A_relocated_sample.json', relocated)
        checks['relocated_spark_read_write_pass'] = True
        results = root / 'outputs/A_provenance_check'
        results.mkdir()
        write_json(results / 'counts.json', {'rows_per_table': relocated['rows_per_table']})
        experiment = results / 'experiment.json'
        write_json(experiment, {'operation': 'interface_check', 'scope': 'sample', 'training': False})
        subprocess.run([sys.executable, str(root / 'scripts/record_result_metadata.py'), '--member', 'A', '--run-id', 'A_interface_provenance_check', '--input-scope', 'sample', '--result-dir', str(results), '--experiment-config', str(experiment), '--command', 'read_data_release.py --member A --scope sample'], cwd=output, check=True)
        metadata = read_json(results / 'result_metadata.json')
        if metadata['data_version'] != manifest['data_version'] or metadata['config_hash'] != manifest['config_hash'] or metadata['experiment_config_hash'] != sha256(experiment):
            raise RuntimeError('Downstream metadata identities differ')
        write_json(output / 'A_result_metadata_example.json', metadata)
        for path in results.iterdir():
            if path.is_file():
                destination = output / 'A_provenance_example' / path.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
        checks['downstream_metadata_entrypoint_pass'] = True
        checks['experiment_hash_separate_from_data_config_hash'] = metadata['experiment_config_hash'] != metadata['config_hash']
    validation = {'status': 'PASS', 'scope': 'A_local_handoff_only', 'release_id': manifest['release_id'],
                  'data_version': manifest['data_version'], 'config_hash': manifest['config_hash'],
                  'release_manifest_sha256': manifest_hash, 'checks': checks,
                  'B': 'PENDING_ACTUAL_RECEIPT', 'C': 'PENDING_ACTUAL_RECEIPT',
                  'macOS': 'PENDING_ACTUAL_HOST_CHECK', 'github': 'NOT_PUBLISHED',
                  'relocation_note': 'Relocated paths on the same Windows host, reusing its configured Java/Hadoop/Python runtime; this is not B/C machine verification.'}
    if not all(checks.values()):
        raise RuntimeError('Handoff checks failed')
    write_json(output / 'validation.json', validation)
    write_json(output / 'bundle_manifest.json', {'release_id': manifest['release_id'], 'archive': archive.relative_to(ROOT).as_posix(),
               'bytes': archive.stat().st_size, 'sha256': sha256(archive), 'archive_members': len(members),
               'release_manifest_sha256': manifest_hash, 'contains_full_data': False,
               'publication': 'LOCAL_ONLY'})
    status_path = ROOT / 'configs/processing_status.json'
    status = read_json(status_path)
    status['P1-5'] = {'status': 'LOCAL_HANDOFF_READY', 'owner': 'A', 'release_id': manifest['release_id'],
                     'data_version': manifest['data_version'], 'config_hash': manifest['config_hash'],
                     'manifest': 'manifests/release_manifest.json', 'validation': 'outputs/p1_5_release/validation.json',
                     'B_actual_receipt': 'PENDING', 'C_actual_receipt': 'PENDING', 'github_publication': 'NOT_STARTED'}
    write_json(status_path, status)
    print(f'PASS: {len(checks)} handoff checks; interface ZIP {archive.stat().st_size:,} bytes')


if __name__ == '__main__':
    main()
