"""Publish raw-file manifest and a bounded shared sample from the passed run."""
import csv
import hashlib
import json
import shutil
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IDS = {
    2017: "d_1990a5a1aeaf3dd243cf4dae294a61c4",
    2018: "d_024fb501ce7092b71bb713eaf54fa7eb",
    2019: "d_61995f092320e7155b7528050880b502",
    2020: "d_9e7de44094f876f6804b8b5bcee45c81",
    2021: "d_3b41598f74f1f11fc3430348fea51af5",
    2022: "d_42d64cc6c176ace1c52fbb40b9ede302",
    2023: "d_f864cc30d58b467db83659ad17c737bf",
    2024: "d_a0b69d3e02576a1fd0ab673e71f83507",
}


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    recorded = json.loads((ROOT / "outputs/raw_reconnaissance.json").read_text(encoding="utf-8"))
    pointer = json.loads((ROOT / "outputs/spark_smoke/latest_pass.json").read_text())
    evidence = json.loads((ROOT / pointer["report_relative_path"]).read_text(encoding="utf-8"))
    if evidence["status"] != "PASS" or not all(c["passed"] for c in evidence["checks"].values()):
        raise RuntimeError("The source Spark verification did not pass")
    now = datetime.now(timezone(timedelta(hours=8))).isoformat()
    sources = []
    for entry in recorded["files"]:
        path = ROOT / "Raw_Dataset" / entry["file"]
        actual = digest(path)
        if actual != entry["sha256"] or path.stat().st_size != entry["bytes"]:
            raise RuntimeError(f"Raw data changed since reconnaissance: {path.name}")
        year = int(path.stem[-4:])
        sources.append({"year": year, "path": path.relative_to(ROOT).as_posix(),
                        "bytes": path.stat().st_size, "sha256": actual,
                        "physical_data_lines": entry["physical_data_lines"],
                        "columns": entry["header"], "dataset_id": IDS[year],
                        "source_page": f"https://data.gov.sg/datasets/{IDS[year]}/view",
                        "source_title": f"Historical Rainfall across Singapore ({year})",
                        "downloaded_at": None, "original_download_url": None})
        print(f"Raw SHA-256 rechecked: {year}", flush=True)
    raw_manifest = {"manifest_version": "1.0", "checked_at_sgt": now,
                    "collection_source": "https://data.gov.sg/collections/2279/view",
                    "source_attribution": "Annual dataset titles verified against data.gov.sg; original download log and remote file hash were not captured.",
                    "row_count_definition": "Physical data lines excluding header, not validated parsed record counts.",
                    "total_bytes": sum(x["bytes"] for x in sources), "files": sources}
    write_json(ROOT / "manifests/raw_manifest.json", raw_manifest)
    sample_dir = ROOT / "data/samples/p0_1_v1"
    if sample_dir.exists():
        raise RuntimeError("Shared sample v1 already exists; do not overwrite a published data version")
    sample_dir.mkdir(parents=True)
    passed_dir = (ROOT / pointer["report_relative_path"]).parent
    shutil.copy2(passed_dir / "sample.csv", sample_dir / "sample.csv")
    shutil.copytree(passed_dir / "parquet", sample_dir / "parquet")
    # Explicit list keeps the handoff independent of the 8 GB raw directory/runtime.
    for source, name in [(ROOT / "requirements-spark.txt", "requirements-spark.txt"),
                         (ROOT / "configs/environment.lock.json", "environment.lock.json"),
                         (ROOT / "manifests/raw_manifest.json", "raw_manifest.json"),
                         (ROOT / "scripts/read_shared_sample.py", "read_shared_sample.py"),
                         (ROOT / "docs/shared_sample_readme.md", "README.md")]:
        shutil.copy2(source, sample_dir / name)
    groups = defaultdict(lambda: [0, 0.0])
    with (sample_dir / "sample.csv").open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            year = datetime.fromisoformat(row["timestamp"]).year
            groups[(year, row["station_id"])][0] += 1
            groups[(year, row["station_id"])][1] += float(row["reading_value"])
    payload = [{"path": p.relative_to(sample_dir).as_posix(), "bytes": p.stat().st_size,
                "sha256": digest(p)} for p in sorted(sample_dir.rglob("*")) if p.is_file()]
    sample_manifest = {"sample_version": "p0_1_v1", "created_at_sgt": now,
                       "source_run_id": evidence["run_id"], "purpose": "Shared environment-read verification only; not cleaned or representative weather data.",
                       "sampling": "First 2000 records of each 2017-2024 CSV",
                       "rows": sum(v[0] for v in groups.values()), "schema": evidence["schema"],
                       "expected_aggregates": [{"year": y, "station_id": s, "n": v[0], "rain_sum_mm": v[1]}
                                               for (y, s), v in sorted(groups.items())],
                       "payload_files": payload}
    write_json(sample_dir / "sample_manifest.json", sample_manifest)
    archive = ROOT / "outputs/p0_1_shared_sample_v1.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in sorted(sample_dir.rglob("*")):
            if p.is_file():
                z.write(p, Path("p0_1_v1") / p.relative_to(sample_dir))
    with zipfile.ZipFile(archive) as z:
        if z.testzip() is not None:
            raise RuntimeError("ZIP CRC check failed")
        for entry in payload:
            content = z.read("p0_1_v1/" + entry["path"])
            if hashlib.sha256(content).hexdigest() != entry["sha256"]:
                raise RuntimeError("ZIP payload differs from published sample")
    write_json(ROOT / "outputs/p0_1_handoff_checks.json", {
        "checked_at_sgt": now, "raw_file_hashes_rechecked": len(sources), "raw_manifest": "manifests/raw_manifest.json",
        "archive": archive.relative_to(ROOT).as_posix(), "archive_bytes": archive.stat().st_size,
        "archive_sha256": digest(archive), "zip_crc_and_payload_hashes_passed": True,
        "team_receipts": {"A": "pending_shared_sample_reader", "B": "pending", "C": "pending"}})
    print(f"Published shared sample: {sample_dir}; ZIP {archive.stat().st_size} bytes", flush=True)


if __name__ == "__main__":
    main()
