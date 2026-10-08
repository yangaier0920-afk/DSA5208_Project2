"""Install the pinned Microsoft OpenJDK ZIP inside this project only."""
import hashlib
import json
import re
import subprocess
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "17.0.20.1"
BASE = f"https://aka.ms/download-jdk/microsoft-jdk-{VERSION}-windows-x64.zip"
DOWNLOADS = ROOT / ".runtime" / "downloads"
TARGET = ROOT / ".runtime" / "java"


def main():
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(BASE + ".sha256sum.txt", timeout=60) as response:
        checksum_text = response.read().decode("utf-8")
        checksum_url = response.url
    match = re.search(r"\b[0-9a-fA-F]{64}\b", checksum_text)
    if not match:
        raise RuntimeError("The official SHA-256 response did not contain a checksum")
    expected = match.group().lower()
    archive = DOWNLOADS / f"microsoft-jdk-{VERSION}-windows-x64.zip"
    source_url = BASE
    if not archive.exists():
        partial = archive.with_suffix(".zip.partial")
        with urllib.request.urlopen(BASE, timeout=120) as response, partial.open("wb") as output:
            source_url = response.url
            count = 0
            next_log = 32 * 1024 * 1024
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                count += len(chunk)
                if count >= next_log:
                    print(f"Java ZIP downloaded: {count // 1024 // 1024} MiB", flush=True)
                    next_log += 32 * 1024 * 1024
        partial.replace(archive)
    with archive.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != expected:
        raise RuntimeError(f"Java archive checksum mismatch: {actual} != {expected}")
    TARGET.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as z:
        for item in z.infolist():
            destination = (TARGET / item.filename).resolve()
            if not destination.is_relative_to(TARGET.resolve()):
                raise RuntimeError("Unsafe archive path")
        z.extractall(TARGET)
    candidates = list(TARGET.glob("*/bin/java.exe"))
    if len(candidates) != 1:
        raise RuntimeError("Expected one portable Java installation")
    java_home = candidates[0].parent.parent
    version = subprocess.run([str(candidates[0]), "-version"], capture_output=True, text=True, check=True)
    manifest = {"version": VERSION, "java_home_relative": java_home.relative_to(ROOT).as_posix(),
                "source_url": source_url, "checksum_url": checksum_url, "sha256": actual,
                "java_version_output": version.stderr.strip()}
    (ROOT / ".runtime" / "java_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    print(version.stderr.strip(), flush=True)
    print("Portable Java SHA-256 verified; manifest saved.", flush=True)


if __name__ == "__main__":
    main()
