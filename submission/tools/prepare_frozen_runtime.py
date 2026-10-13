"""Restore immutable original checksum bytes into an isolated portable runtime."""
import argparse,hashlib,json,os,shutil,zipfile
from pathlib import Path

def digest(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--project-root',type=Path,default=Path(__file__).resolve().parents[2]);a=ap.parse_args()
    root=a.project_root.resolve();dest=root/'.runtime/frozen_root';dest.mkdir(parents=True,exist_ok=True)
    snapshot=root/'submission/reproduction/frozen_release_metadata.zip'
    with zipfile.ZipFile(snapshot) as z:
        assert z.testzip() is None
        manifest=json.loads(z.read('manifests/release_manifest.json'))
        for name in z.namelist():
            target=(dest/name).resolve()
            if not target.is_relative_to(dest):raise ValueError('Unsafe snapshot path')
            content=z.read(name);target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists():
                if digest(target)!=hashlib.sha256(content).hexdigest():raise RuntimeError('Existing original runtime differs: '+name)
            else:target.write_bytes(content)
    for item in manifest['files']:
        target=dest/item['path']
        if item['kind']=='full_data' and not target.exists():
            source=root/item['path']
            if not source.exists():continue
            target.parent.mkdir(parents=True,exist_ok=True)
            try:os.link(source,target)
            except OSError:shutil.copy2(source,target)
        if item['kind']=='full_data' and not target.exists():continue
        assert target.is_file() and target.stat().st_size==item['bytes'] and digest(target)==item['sha256'],item['path']
    print('PASS: original-byte frozen release restored and verified:',dest)
if __name__=='__main__':main()
