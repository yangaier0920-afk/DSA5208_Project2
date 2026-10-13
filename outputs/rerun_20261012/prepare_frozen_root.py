import pathlib,os,json
root=pathlib.Path.cwd(); dest=root/'.runtime/frozen_root';dest.mkdir(exist_ok=True)
m=json.loads((root/'manifests/release_manifest.json').read_text())
paths={i['path'] for i in m['files'] if (root/i['path']).is_file()}
for folder in ['scripts','src','configs','manifests','data/samples','outputs/p0_3_clean','outputs/p0_4_targets']:
 paths.update(p.relative_to(root).as_posix() for p in (root/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts)
for name in paths:
 source=root/name;replacement=root/'.runtime/frozen_files'/name
 if replacement.is_file():source=replacement
 target=dest/name;target.parent.mkdir(parents=True,exist_ok=True)
 if not target.exists():os.link(source,target)
print(dest)
