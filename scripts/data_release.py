"""Portable release paths, integrity checks and downstream provenance."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def resolve_path(root, relative):
    root = Path(root).resolve()
    relative = Path(relative)
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError(f'Release path must be relative: {relative}')
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f'Release path leaves root: {relative}')
    return path


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def load_release(root=ROOT):
    root = Path(root).resolve()
    manifest = read_json(root / 'manifests/release_manifest.json')
    if manifest['status'] != 'FROZEN_LOCAL':
        raise RuntimeError('Release is not frozen')
    if sha256(root / 'configs/project.json') != manifest['config_hash']:
        raise RuntimeError('Project configuration differs from frozen release')
    return manifest


def verify_release(root=ROOT, scope='sample'):
    if scope not in {'sample', 'full'}:
        raise ValueError('scope must be sample or full')
    root = Path(root).resolve()
    manifest = load_release(root)
    selected = [i for i in manifest['files'] if scope == 'full' or i['kind'] != 'full_data']
    for item in selected:
        path = resolve_path(root, item['path'])
        if not path.is_file() or path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
            raise RuntimeError(f'Release file missing or changed: {item["path"]}')
    # Also reject additional Parquet files: Spark would read unlisted rows.
    prefixes = [manifest['sample']['path']]
    if scope == 'full':
        prefixes += [t['path'] for t in manifest['tables'].values()]
    for prefix in prefixes:
        actual = {p.relative_to(root).as_posix() for p in resolve_path(root, prefix).rglob('*.parquet')}
        expected = {i['path'] for i in selected if i['path'].startswith(prefix + '/') and i['path'].endswith('.parquet')}
        if actual != expected:
            raise RuntimeError(f'Parquet file set differs: {prefix}')
    return {'status': 'PASS', 'scope': scope, 'checked_files': len(selected),
            **provenance(root, manifest)}


def provenance(root=ROOT, manifest=None):
    manifest = manifest or load_release(root)
    return {k: manifest[k] for k in ['release_id', 'data_version', 'config_hash']} | {
        'release_manifest_sha256': sha256(Path(root) / 'manifests/release_manifest.json')}


def read_tables(spark, root=ROOT, scope='full', verify=True):
    """Verify once per run; returns observations and targets without model filtering."""
    manifest = load_release(root)
    if verify:
        verify_release(root, scope)
    spark.conf.set('spark.sql.session.timeZone', manifest['timezone'])
    base = manifest['sample']['path'] if scope == 'sample' else None
    frames = {}
    for name in ['observations', 'targets']:
        relative = f'{base}/{name}' if base else manifest['tables'][name]['path']
        frames[name] = spark.read.parquet(resolve_path(root, relative).as_posix())
        expected = manifest['tables'][name]['schema']['fields']
        actual = frames[name].schema.jsonValue()['fields']
        if {f['name']: f['type'] for f in actual} != {f['name']: f['type'] for f in expected}:
            raise RuntimeError(f'Schema differs: {name}')
    return frames
