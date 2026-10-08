"""Read-only reconnaissance: byte-spaced CSV blocks plus full byte hashes.

This is not the Spark cleaning pipeline and does not measure full-data quality.
Physical line counts are not asserted to be parsed CSV record counts.
"""
import argparse
import csv
import hashlib
import io
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd


def sample_blocks(path, blocks):
    size = path.stat().st_size
    frames, spans = [], []
    with path.open("rb") as f:
        header = next(csv.reader([f.readline().decode("utf-8-sig")]))
        header_end = f.tell()
        positions = [(header_end, 1024 * 1024)]
        positions += [(int(size * k / (blocks + 1)), 128 * 1024)
                      for k in range(1, blocks + 1)]
        positions += [(max(header_end, size - 1024 * 1024), 1024 * 1024)]
        previous_end = header_end
        for offset, width in positions:
            if offset < previous_end and offset != header_end:
                continue
            f.seek(offset)
            if offset != header_end:
                f.readline()  # Discard a potentially incomplete physical line.
            start = f.tell()
            payload = f.read(width)
            if f.tell() < size:
                payload = payload[:payload.rfind(b"\n") + 1]
            previous_end = start + len(payload)
            rows = list(csv.reader(io.StringIO(payload.decode("utf-8"))))
            malformed = sum(len(row) != len(header) for row in rows)
            rows = [row for row in rows if len(row) == len(header)]
            frame = pd.DataFrame(rows, columns=header)
            frame["_segment"] = len(spans)
            frames.append(frame)
            spans.append({"start_byte": start, "end_byte": previous_end,
                          "parsed_rows": len(rows), "wrong_field_count_rows": malformed})
    return header, pd.concat(frames, ignore_index=True), spans


def hash_and_lines(path):
    digest, line_breaks, last = hashlib.sha256(), 0, b""
    with path.open("rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            digest.update(chunk)
            line_breaks += chunk.count(b"\n")
            last = chunk[-1:]
    return digest.hexdigest(), line_breaks + int(last != b"\n") - 1


def profile(path, blocks, full_hash):
    header, d, spans = sample_blocks(path, blocks)
    rain = pd.to_numeric(d.reading_value, errors="coerce")
    ts = pd.to_datetime(d.timestamp, errors="coerce", utc=True)
    update = pd.to_datetime(d.update_timestamp, errors="coerce", utc=True)
    reading_update = pd.to_datetime(d.reading_update_timestamp, errors="coerce", utc=True)
    seconds = ts.dt.second
    offset = (ts.dt.minute % 5) * 60 + seconds
    d["_time"] = ts
    gap = d.sort_values(["_segment", "station_id", "_time"]).groupby(
        ["_segment", "station_id"])["_time"].diff().dt.total_seconds().dropna()
    metadata_cols = ["station_id", "station_name", "station_device_id",
                     "location_longitude", "location_latitude"]
    metadata = d[metadata_cols].drop_duplicates()
    delays = (reading_update - ts).dt.total_seconds()
    examples = d.loc[offset.ne(0), header].head(3).to_dict("records")
    delayed_examples = d.loc[delays.gt(3600), header].head(3).to_dict("records")
    result = {
        "file": path.name, "bytes": path.stat().st_size,
        "header": header, "sample_rows": len(d), "sample_spans": spans,
        "first_sample_record": d[header].iloc[0].to_dict(),
        "last_sample_record": d[header].iloc[-1].to_dict(),
        "sample_timestamp_min": str(ts.min()), "sample_timestamp_max": str(ts.max()),
        "sample_station_count": int(d.station_id.nunique()),
        "sample_station_ids": sorted(d.station_id.unique().tolist()),
        "sample_blank_counts": d[header].eq("").sum().astype(int).to_dict(),
        "sample_invalid_timestamp": int(ts.isna().sum()),
        "sample_rain": {"invalid_or_nonfinite": int((rain.isna() | ~rain.abs().lt(float("inf"))).sum()),
                        "negative": int(rain.lt(0).sum()), "zero": int(rain.eq(0).sum()),
                        "positive": int(rain.gt(0).sum()), "min": float(rain.min()),
                        "max": float(rain.max())},
        "sample_reading_types": d.reading_type.value_counts().to_dict(),
        "sample_units": d.reading_unit.value_counts().to_dict(),
        "sample_seconds": seconds.value_counts().astype(int).to_dict(),
        "sample_offset_in_300_seconds": offset.value_counts().astype(int).to_dict(),
        "sample_off_grid_rows": int(offset.ne(0).sum()),
        "sample_duplicate_station_timestamp_extra_rows": int(d.duplicated(["station_id", "timestamp"]).sum()),
        "sample_within_segment_station_gaps_seconds": gap.value_counts().head(15).astype(int).to_dict(),
        "sample_reading_update_delay_seconds": {
            "min": float(delays.min()), "median": float(delays.median()),
            "p95": float(delays.quantile(.95)), "max": float(delays.max()),
            "over_one_hour_rows": int(delays.gt(3600).sum()),
            "negative_rows": int(delays.lt(0).sum())},
        "sample_update_vs_reading_update_differ_rows": int((update != reading_update).sum()),
        "sample_station_metadata_variants": metadata.to_dict("records"),
        "off_grid_examples": examples, "delayed_update_examples": delayed_examples,
    }
    if full_hash:
        result["sha256"], result["physical_data_lines"] = hash_and_lines(path)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, default=Path("Raw_Dataset"))
    parser.add_argument("--out", type=Path, default=Path("outputs/raw_reconnaissance.json"))
    parser.add_argument("--blocks", type=int, default=32)
    parser.add_argument("--full-hash", action="store_true")
    args = parser.parse_args()
    if args.blocks < 1:
        parser.error("--blocks must be positive")
    files = sorted(args.raw.glob("HistoricalRainfallacrossSingapore*.csv"))
    if not files:
        raise SystemExit("No annual CSV files found")
    results = []
    for path in files:
        results.append(profile(path, args.blocks, args.full_hash))
        r = results[-1]
        print(json.dumps({k: r[k] for k in ["file", "bytes", "sample_rows", "sample_station_count",
                                           "sample_off_grid_rows", "sample_rain"]}, ensure_ascii=False), flush=True)
    report = {"generated_at": datetime.now().astimezone().isoformat(),
              "scope": "Full byte hashes/physical line counts if requested; all quality metrics are block samples.",
              "limitations": ["Byte-spaced samples are not uniform random samples or full-data quality statistics.",
                              "CSV parsing assumes no quoted multiline fields in sampled physical blocks.",
                              "Station counts are sample lower bounds; gaps are within sampled blocks only.",
                              "First/last sampled rows do not establish full-file time ordering."],
              "total_bytes": sum(r["bytes"] for r in results), "files": results}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(f"Saved {args.out}", flush=True)


if __name__ == "__main__":
    main()
