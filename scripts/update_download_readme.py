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
        rows.append(f'| {year} | [下载ZIP]({item["url"]}) | {item["bytes"] / 1e6:.0f} MB |')
    download = '''## 1. 下载正式数据（B/C先从这里开始）

**[打开正式数据下载页：2017—2024八年 Parquet](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2)**

1. 先[下载最新版项目代码ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/archive/refs/heads/main.zip)并解压，也可以clone仓库。
2. 下载下表**全部8个年度包**。合计约2.70GB，解压后的正式数据约4.68GB；保留下载包和解压数据时，数据部分需约7.4GB空间。
3. 在代码中`README.md`所在目录打开终端，把8个包解压到该目录，保留`data/processed/`下的路径。每包包含对应年份的观测、标签、异常隔离、重复追溯和站点元数据。

| 年份 | 下载链接 | 压缩包大小（约） |
|---|---|---|
''' + '\n'.join(rows) + '''

**Windows解压**（假设8个ZIP保存在默认Downloads目录）：

```powershell
2017..2024 | ForEach-Object { Expand-Archive -LiteralPath (Join-Path $HOME "Downloads\\rainfall_v1_$_.zip") -DestinationPath . -Force }
```

**macOS解压**（同样假设ZIP保存在Downloads目录）：

```bash
for year in {2017..2024}; do unzip -o "$HOME/Downloads/rainfall_v1_${year}.zip" -d .; done
```

若ZIP存在其他目录，替换命令中的Downloads路径。解压后应看到`data/processed/p0_3_v1/`和`data/processed/p0_4_v1/`，然后按第3节安装环境并检查读取。仓库代码与交接小样本包用于代码／接口准备，正式分析和训练使用这8个年度包。

'''
    text = text.replace('## 2. 交付给B/C的是什么', download + '## 2. 交付给B/C的是什么')
    old_version = '数据版本为`v1`，观测表版本`p0_3_v1`，标签表版本`p0_4_v1`；当前交接发布标识`rainfall_v1_r2`仅更新说明组织。样本包没有约4.68GB的正式大表，正式分析／训练须另外取得大表并保持上表路径。GitHub及下载链接等待最终确认，目前尚未发布。'
    new_version = '数据版本为`v1`，观测表版本`p0_3_v1`，标签表版本`p0_4_v1`；当前交接说明修订为`rainfall_v1_r3`。正式数据包使用已发布的`rainfall_v1_r2`下载，Parquet内容、字段、表路径与规则完全一致。'
    if old_version not in text:
        raise RuntimeError('Expected README version paragraph not found')
    text = text.replace(old_version, new_version)
    text = text.replace('## 3. 先确认本机能读取', '## 3. 安装环境与读取检查')
    previous_check = '取得全部正式Parquet后可加`--scope full`校验全表，耗时及资源需求更高。'
    full_check = '''下载并解压8个年度包后，再执行**全量读取检查**，耗时及资源需求更高：

```powershell
# Windows，B执行；C把成员和收据文件名中的B改成C
.\\.venv\\Scripts\\python.exe scripts\\read_data_release.py --member B --scope full --receipt outputs/p1_5_receipts/B_full_r3.json
```

```bash
# macOS，先激活上面的虚拟环境
python scripts/read_data_release.py --member B --scope full --receipt outputs/p1_5_receipts/B_full_r3.json
```

看到`PASS: full`且退出码0即通过。每次复跑选择新的收据文件名。'''
    if previous_check not in text:
        raise RuntimeError('Expected full-read paragraph not found')
    text = text.replace(previous_check, full_check)
    text = text.replace('当前A本地数据工作和复现通过；B/C实机验收、分析／特征／模型、最终模型重载、全组报告合并及最终发布仍待完成。',
                        'A本地数据工作、复现和正式数据发布已完成；B/C实机验收、分析／特征／模型、最终模型重载、全组报告合并及课程最终提交仍待完成。')
    (ROOT / 'README.md').write_text(text, encoding='utf-8')
    reference_path = ROOT / 'docs/reference.md'
    reference = reference_path.read_text(encoding='utf-8')
    reference = reference.replace('rainfall_v1_r2是说明整理后的交接修订。', 'rainfall_v1_r3是下载说明更新后的交接修订；正式数据包沿用rainfall_v1_r2。')
    reference = reference.replace('GitHub未发布，最终下载渠道和课程提交包还需确认。',
        '正式Parquet已发布到[GitHub Releases](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2)，下载和解压步骤集中在README；课程提交包仍需全组合并确认。')
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
