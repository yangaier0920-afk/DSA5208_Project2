"""Fetch pinned Windows Hadoop local-filesystem components into this project.

These are community builds from cdarlint/winutils, not ASF binary releases.
Verify Git blob identities and record SHA-256 for reproducibility.
"""
import hashlib
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMMIT = "7386986d5d8a079b5cd4464f4599766dd27e7d13"
VERSION = "3.3.5"
BLOBS = {"hadoop.dll": "60fb8b1336de7ec890e594c6910678e7be35786f",
         "winutils.exe": "4fd286dd74c2d37d73ccaf3c83007afa99f5586b"}


def main():
    target = ROOT / ".runtime" / f"hadoop-{VERSION}" / "bin"
    target.mkdir(parents=True, exist_ok=True)
    files = []
    for name, expected in BLOBS.items():
        url = f"https://raw.githubusercontent.com/cdarlint/winutils/{COMMIT}/hadoop-{VERSION}/bin/{name}"
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read()
        blob = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
        if blob != expected:
            raise RuntimeError(f"Pinned Git blob verification failed for {name}")
        (target / name).write_bytes(data)
        files.append({"name": name, "url": url, "bytes": len(data), "git_blob_sha1": blob,
                      "sha256": hashlib.sha256(data).hexdigest()})
        print(f"Verified and saved {name}: {len(data)} bytes", flush=True)
    manifest = {"community_source": "https://github.com/cdarlint/winutils", "commit": COMMIT,
                "native_component_version": VERSION, "spark_bundled_hadoop_java_version": "3.3.4",
                "hadoop_home_relative": target.parent.relative_to(ROOT).as_posix(), "files": files,
                "scope": "Windows local-filesystem components; compatibility assessed by actual Spark read/write test."}
    (ROOT / ".runtime" / "hadoop_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
