"""Add published data downloads and refresh documentation-only frozen metadata."""
import copy
from datetime import datetime, timezone
from pathlib import Path

from data_release import ROOT, provenance, read_json, resolve_path, sha256, verify_release, write_json


def main():
    old = read_json(ROOT / 'manifests/release_manifest.json')
    support = read_json(ROOT / 'manifests/a_support_manifest.json')
    if old['release_id'] != 'rainfall_v1_r2':
        raise RuntimeError('This one-time documentation migration requires the r2 release')
    verify_release(ROOT, 'sample')
    published = read_json(ROOT / 'outputs/release_assets/rainfall_v1_r2/publication_receipt.json')
    packages = read_json(ROOT / 'outputs/release_assets/rainfall_v1_r2/download_manifest.json')
    if published['status'] != 'PUBLISHED_VERIFIED' or published['release_manifest_sha256'] != provenance()['release_manifest_sha256']:
        raise RuntimeError('The data release must be published and verified before adding downloads')
    assets = {i['name']: i for i in published['verified_assets']}
    archive = resolve_path(ROOT, 'archive/history_v1/releases/rainfall_v1_r2')
    if archive.exists():
        raise FileExistsError(archive)
    snapshots = ['README.md', 'docs/reference.md', 'manifests/release_manifest.json',
                 'manifests/a_support_manifest.json', 'data/samples/p1_6_v1/fixture_manifest.json']
    for relative in snapshots:
        destination = archive / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / relative).read_bytes())
    text = (ROOT / 'README.md').read_text(encoding='utf-8')
    # Shift existing sections to keep download -> paths -> environment -> work order.
    for n in range(5, 0, -1):
        text = text.replace(f'## {n}. ', f'## {n + 1}. ')
    rows = []
    for year in range(2017, 2025):
        item = assets[f'rainfall_v1_{year}.zip']
        rows.append(' | [Download ZIP](')
    download = '## 1. Download formal data\n\n[2017–2024 Parquet release](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2)\n\nDownload/extract the project code or clone it. Obtain all eight annual ZIPs (about 2.70 GB compressed, 4.68 GB extracted, 7.4 GB if retaining both). Extract from the project root preserving data/processed paths. Each year includes observations, targets, quarantine, lineage and metadata.\n\n| Year | Download | Approximate size |\n|---|---|---|\n' + '\n'.join(rows) + '\n\nWindows extraction (ZIPs in Downloads):\n\n```powershell\n2017..2024 | ForEach-Object { Expand-Archive -LiteralPath (Join-Path $HOME "Downloads\\rainfall_v1_$_.zip") -DestinationPath . -Force }\n```\n\nmacOS extraction:\n\n```bash\nfor year in {2017..2024}; do unzip -o "$HOME/Downloads/rainfall_v1_${year}.zip" -d .; done\n```\n\nAdjust Downloads if needed. Confirm data/processed/p0_3_v1 and p0_4_v1 exist, then install/check the environment. Code and small fixtures support interfaces; these annual archives support formal analysis/training.\n\n'
    text = text.replace('## 2. B/C delivery paths', download + '## 2. B/C delivery paths')
    old_version = 'Data v1: observations p0_3_v1, targets p0_4_v1; rainfall_v1_r2 revises documentation only. The small package omits about 4.68 GB of formal tables; obtain them separately at the documented paths. GitHub/download publication was pending at this historical stage.'
    new_version = 'Data v1: observations p0_3_v1, targets p0_4_v1; documentation revision rainfall_v1_r3. Download formal data from rainfall_v1_r2; Parquet content, fields, paths and rules are identical.'
    if old_version not in text:
        raise RuntimeError('Expected README version paragraph not found')
    text = text.replace(old_version, new_version)
    text = text.replace('## 3. Confirm local reads', '## 3. Environment setup and read checks')
    previous_check = 'With all formal Parquet, use --scope full for full checks, requiring more time/resources.'
    full_check = 'After extracting all eight annual ZIPs, perform a full read check (more time/resources):\n\n```powershell\n# Windows: B; C changes member and receipt name.\n.\\.venv\\Scripts\\python.exe scripts\\read_data_release.py --member B --scope full --receipt outputs/p1_5_receipts/B_full_r3.json\n```\n\n```bash\n# macOS: activate the virtual environment first.\npython scripts/read_data_release.py --member B --scope full --receipt outputs/p1_5_receipts/B_full_r3.json\n```\n\nRequire PASS: full with zero exit status. Choose a new receipt on every rerun.'
    if previous_check not in text:
        raise RuntimeError('Expected full-read paragraph not found')
    text = text.replace(previous_check, full_check)
    text = text.replace('Historical status: A local data/replay passed; B/C machine checks, analysis/features/models, model reload, group report integration and final publication were pending.',
                        'Historical status: A local data/replay and formal release completed; B/C machine checks, analysis/features/models, reload, group report integration and course submission remained pending.')
    (ROOT / 'README.md').write_text(text, encoding='utf-8')
    reference_path = ROOT / 'docs/reference.md'
    reference = reference_path.read_text(encoding='utf-8')
    reference = reference.replace('rainfall_v1_r2 is the documentation consolidation revision.', 'rainfall_v1_r3 updates download documentation; formal data archives remain rainfall_v1_r2.')
    reference = reference.replace('GitHub was unpublished; final downloads and the course package required confirmation.',
        'Formal Parquet was published in [GitHub Releases](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2). README provides download/extraction; the course package required group integration.')
    reference_path.write_text(reference, encoding='utf-8')
    current = copy.deepcopy(old)
    current.update({'release_id': 'rainfall_v1_r3',
        'previous_release_manifest': 'archive/history_v1/releases/rainfall_v1_r2/manifests/release_manifest.json',
        'revision_scope': 'README published downloads and documentation metadata only; data/configuration/table paths unchanged',
        'documentation_updated_at': datetime.now(timezone.utc).isoformat(),
        'published_data_release': {'release_id': 'rainfall_v1_r2', 'url': published['url'],
            'release_manifest_sha256': published['release_manifest_sha256'],
            'download_manifest_sha256': sha256(ROOT / 'outputs/release_assets/rainfall_v1_r2/download_manifest.json'),
            'data_version': packages['data_version'], 'same_full_data_file_hashes': True}})
    for item in current['files']:
        if item['path'] in {'README.md', 'docs/reference.md'}:
            path = ROOT / item['path']
            item.update({'bytes': path.stat().st_size, 'sha256': sha256(path)})
    for relative in [f'archive/history_v1/releases/rainfall_v1_r2/{r}' for r in snapshots] + ['scripts/update_download_readme.py']:
        path = ROOT / relative
        current['files'].append({'path': relative, 'bytes': path.stat().st_size, 'sha256': sha256(path), 'kind': 'metadata'})
    if current['tables'] != old['tables'] or current['config_hash'] != old['config_hash'] or current['data_version'] != old['data_version']:
        raise RuntimeError('Documentation edit altered the data interface')
    if [i for i in current['files'] if i['kind'] == 'full_data'] != [i for i in old['files'] if i['kind'] == 'full_data']:
        raise RuntimeError('Documentation edit altered full-data hashes')
    write_json(ROOT / 'manifests/release_manifest.json', current)
    fixture_path = ROOT / 'data/samples/p1_6_v1/fixture_manifest.json'
    fixture = read_json(fixture_path)
    fixture.update(provenance())
    write_json(fixture_path, fixture)
    support.update({'support_version': 'a_support_v1_r3', 'release_id': current['release_id'],
        'release_manifest_sha256': provenance()['release_manifest_sha256'],
        'previous_support_manifest': 'archive/history_v1/releases/rainfall_v1_r2/manifests/a_support_manifest.json'})
    for item in support['files']:
        if item['path'] in {'README.md', 'docs/reference.md', 'data/samples/p1_6_v1/fixture_manifest.json'}:
            path = ROOT / item['path']
            item.update({'bytes': path.stat().st_size, 'sha256': sha256(path)})
    write_json(ROOT / 'manifests/a_support_manifest.json', support)
    print('Updated README downloads and r3 documentation identity; all data hashes and configuration preserved')


if __name__ == '__main__':
    main()
