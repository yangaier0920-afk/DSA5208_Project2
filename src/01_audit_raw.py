"""P0-2: full Spark audit, without changing or cleaning the source CSV files."""
import argparse
import calendar
import csv
import hashlib
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from spark_session import start_spark, stop_spark

COLUMNS = ["date", "timestamp", "update_timestamp", "station_id", "station_name",
           "station_device_id", "location_longitude", "location_latitude",
           "reading_update_timestamp", "reading_value", "reading_type", "reading_unit"]
PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX"
EXPECTED_TYPE = "TB1 Rainfall 5 Minute Total F"
VERSION = "p0_2_v1"


def now():
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False,
                                    default=str), encoding="utf-8")
    temporary.replace(path)


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def collect(df, order=None):
    if order:
        df = df.orderBy(*order)
    return [row.asDict(recursive=True) for row in df.collect()]


def numeric_coordinate_key(value):
    # Preserve invalid metadata variants in the audit rather than crashing while
    # building its small cross-year summary.
    import math
    try:
        numeric = float(value)
        return str(numeric) if math.isfinite(numeric) else "nonfinite:" + str(value)
    except (ValueError, TypeError):
        return "invalid:" + str(value)


def audit_year(spark, source, year, out, expected_physical=None):
    from pyspark.sql import functions as F, types as T, Window
    from pyspark import StorageLevel
    started = time.monotonic()
    print(f"{now()} YEAR {year}: full CSV structure and typed-field scan", flush=True)
    with source.open(encoding="utf-8-sig", newline="") as stream:
        header_line = stream.readline().rstrip("\r\n")
    if next(csv.reader([header_line])) != COLUMNS:
        raise RuntimeError(f"Unexpected header: {source}")
    lines = spark.read.text(source.as_posix()).withColumnRenamed("value", "raw_line")
    line_stats = lines.agg(F.count("*").alias("physical_lines"),
                          F.sum((F.col("raw_line") == header_line).cast("long")).alias("header_occurrences"),
                          F.sum((F.length("raw_line") == 0).cast("long")).alias("blank_lines")).first().asDict()
    # Parse every non-header physical record, including blank records. Odd quote
    # counts reveal possible multiline data: never silently count these as valid.
    schema = T.StructType([T.StructField(c, T.StringType()) for c in COLUMNS] +
                          [T.StructField("_corrupt_record", T.StringType())])
    options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record",
               "escape": '"', "unescapedQuoteHandling": "RAISE_ERROR"}
    data = (lines.filter(F.col("raw_line") != header_line)
            .withColumn("field_count", F.size(F.split("raw_line", ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1)))
            .withColumn("odd_quotes", F.pmod(F.length("raw_line") - F.length(F.regexp_replace("raw_line", '"', "")), F.lit(2)) != 0)
            .withColumn("parsed", F.from_csv("raw_line", schema.simpleString(), options))
            .select("raw_line", "field_count", "odd_quotes", "parsed.*"))
    for raw, typed in [("timestamp", "event_ts"), ("update_timestamp", "update_ts"),
                       ("reading_update_timestamp", "reading_update_ts")]:
        data = data.withColumn(typed, F.to_timestamp(raw, PATTERN))
    data = (data.withColumn("rain_mm", F.col("reading_value").cast("double"))
            .withColumn("longitude", F.col("location_longitude").cast("double"))
            .withColumn("latitude", F.col("location_latitude").cast("double"))
            .withColumn("epoch_s", F.col("event_ts").cast("long"))
            .withColumn("event_year", F.year("event_ts"))
            .withColumn("month", F.month("event_ts"))
            .withColumn("event_date", F.date_format("event_ts", "yyyy-MM-dd"))
            .withColumn("offset_s", F.pmod("epoch_s", F.lit(300)))
            .withColumn("update_delay_s", F.col("update_ts").cast("long") - F.col("epoch_s"))
            .withColumn("reading_update_delay_s", F.col("reading_update_ts").cast("long") - F.col("epoch_s")))
    def missing(column):
        return F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0)
    def finite(column):
        return F.col(column).isNotNull() & ~F.isnan(column) & (F.abs(F.col(column)) != float("inf"))
    conditions = {
        "csv_corrupt": F.col("_corrupt_record").isNotNull(),
        "csv_field_count_mismatch": F.col("field_count") != 12,
        "csv_odd_quotes": F.col("odd_quotes"),
        "missing_station_id": missing("station_id"),
        "missing_station_name": missing("station_name"),
        "missing_station_device_id": missing("station_device_id"),
        "missing_rainfall": missing("reading_value"),
        "invalid_rainfall_number": ~missing("reading_value") & F.col("rain_mm").isNull(),
        "nonfinite_rainfall": F.isnan("rain_mm") | (F.abs(F.col("rain_mm")) == float("inf")),
        "negative_rainfall": finite("rain_mm") & (F.col("rain_mm") < 0),
        "unexpected_unit": ~F.col("reading_unit").eqNullSafe("mm"),
        "unexpected_reading_type": ~F.col("reading_type").eqNullSafe(EXPECTED_TYPE),
        "invalid_coordinates": (~finite("longitude") | ~finite("latitude") |
                                ~F.col("longitude").between(-180, 180) | ~F.col("latitude").between(-90, 90)),
        "event_year_mismatch": F.col("event_ts").isNotNull() & (F.col("event_year") != year),
        "date_mismatch_or_missing": ~F.col("date").eqNullSafe(F.col("event_date")) | missing("date"),
        "off_five_minute_grid": F.col("offset_s") != 0,
        "timestamp_offset_not_plus08": ~F.col("timestamp").rlike(r"\+08:00$"),
        "negative_update_delay": F.col("update_delay_s") < 0,
        "negative_reading_update_delay": F.col("reading_update_delay_s") < 0,
        "update_delay_gt_day": F.col("update_delay_s") > 86400,
        "reading_update_delay_gt_day": F.col("reading_update_delay_s") > 86400,
    }
    for raw, typed in [("timestamp", "event_ts"), ("update_timestamp", "update_ts"),
                       ("reading_update_timestamp", "reading_update_ts")]:
        conditions[f"missing_{raw}"] = missing(raw)
        conditions[f"invalid_{raw}"] = ~missing(raw) & F.col(typed).isNull()
    structural = conditions["csv_corrupt"] | conditions["csv_field_count_mismatch"] | conditions["csv_odd_quotes"]
    key_eligible = ~structural & ~missing("station_id") & F.col("event_ts").isNotNull()
    candidate = (key_eligible & finite("rain_mm") & (F.col("rain_mm") >= 0) &
                 ~conditions["unexpected_unit"] & ~conditions["unexpected_reading_type"] &
                 ~conditions["event_year_mismatch"] & ~conditions["date_mismatch_or_missing"])
    data = (data.withColumn("structurally_valid", ~structural)
            .withColumn("key_eligible", key_eligible).withColumn("candidate_valid", candidate)
            .persist(StorageLevel.DISK_ONLY))
    sums = [F.sum(F.coalesce(condition, F.lit(False)).cast("long")).alias(name + "_rows")
            for name, condition in conditions.items()]
    expressions = [F.count("*").alias("spark_record_rows"), *sums,
                   F.sum(F.col("structurally_valid").cast("long")).alias("structurally_valid_rows"),
                   F.sum(F.col("key_eligible").cast("long")).alias("key_eligible_rows"),
                   F.sum(F.col("candidate_valid").cast("long")).alias("candidate_valid_rows"),
                   F.countDistinct("station_id").alias("station_ids_including_invalid_rows"),
                   F.min("event_ts").cast("string").alias("first_event_sgt"),
                   F.max("event_ts").cast("string").alias("last_event_sgt"),
                   F.min(F.when(finite("rain_mm"), F.col("rain_mm"))).alias("rain_min_finite_mm"),
                   F.max(F.when(finite("rain_mm"), F.col("rain_mm"))).alias("rain_max_finite_mm"),
                   F.sum((F.col("candidate_valid") & (F.col("rain_mm") > 0)).cast("long")).alias("positive_candidate_rows")]
    for delay in ["update_delay_s", "reading_update_delay_s"]:
        expressions += [F.count(delay).alias(delay + "_parsed_rows"), F.min(delay).alias(delay + "_min"),
                        F.max(delay).alias(delay + "_max"),
                        F.percentile_approx(delay, [0.5, 0.95, 0.99], 10000).alias(delay + "_p50_p95_p99_approx")]
    metrics = {"year": year, **line_stats, **data.agg(*expressions).first().asDict()}
    print(f"{now()} YEAR {year}: {metrics['spark_record_rows']:,} records parsed; duplicate keys and metadata", flush=True)
    tables = {}
    tables["units_types"] = collect(data.groupBy("reading_unit", "reading_type").count(), ["reading_unit", "reading_type"])
    tables["timestamp_offsets"] = collect(data.groupBy("offset_s").count(), ["offset_s"])
    tables["daily_timestamp_offsets"] = collect(data.groupBy("event_date", "offset_s").count(), ["event_date", "offset_s"])
    delay_bucket = lambda c: (F.when(F.col(c).isNull(), "unparsed").when(F.col(c) < 0, "negative")
                             .when(F.col(c) == 0, "zero").when(F.col(c) <= 300, "0_to_5min")
                             .when(F.col(c) <= 3600, "5min_to_1hour").when(F.col(c) <= 86400, "1hour_to_1day")
                             .when(F.col(c) <= 604800, "1day_to_7days").otherwise("over_7days"))
    tables["update_delay_buckets"] = collect(data.groupBy(delay_bucket("update_delay_s").alias("update_delay_bucket"),
                                                              delay_bucket("reading_update_delay_s").alias("reading_update_delay_bucket")).count(),
                                             ["update_delay_bucket", "reading_update_delay_bucket"])
    # Distinct structs are exact comparisons, not hash-based duplicate estimates.
    eligible = data.filter("key_eligible")
    key = (eligible.groupBy("station_id", "epoch_s").agg(
        F.count("*").alias("n"), F.countDistinct(F.struct(*COLUMNS)).alias("distinct_full_rows"),
        F.countDistinct(F.struct("reading_value", "reading_type", "reading_unit")).alias("distinct_readings"),
        F.sum(F.col("candidate_valid").cast("long")).alias("candidate_rows"))
           .persist(StorageLevel.MEMORY_AND_DISK))
    key_metrics = key.agg(F.count("*").alias("unique_station_time_keys"),
                          F.sum((F.col("n") > 1).cast("long")).alias("duplicate_key_groups"),
                          F.sum(F.when(F.col("n") > 1, F.col("n")).otherwise(0)).alias("duplicate_key_rows"),
                          F.sum(F.col("n") - 1).alias("duplicate_key_excess_rows"),
                          F.sum(F.col("n") - F.col("distinct_full_rows")).alias("exact_duplicate_excess_key_eligible_rows"),
                          F.sum((F.col("distinct_readings") > 1).cast("long")).alias("conflicting_reading_key_groups"),
                          F.sum((F.col("candidate_rows") > 0).cast("long")).alias("candidate_unique_keys")).first().asDict()
    metrics.update(key_metrics)
    tables["duplicate_key_examples"] = collect(key.filter("n > 1").orderBy(F.desc("distinct_readings"), "station_id", "epoch_s").limit(30))
    meta = eligible.groupBy("station_id", "station_name", "station_device_id", "location_longitude", "location_latitude").agg(
        F.count("*").alias("rows"), F.min("epoch_s").alias("first_epoch_s"), F.max("epoch_s").alias("last_epoch_s"))
    tables["station_metadata_history"] = collect(meta, ["station_id", "first_epoch_s", "station_name", "location_longitude"])
    meta_stats = eligible.groupBy("station_id").agg(
        F.countDistinct(F.struct("station_name")).alias("name_variants"),
        F.countDistinct(F.struct("station_device_id")).alias("device_variants"),
        F.countDistinct(F.struct("longitude", "latitude")).alias("numeric_coordinate_variants"),
        F.count("*").alias("rows"), F.min("epoch_s").alias("first_epoch_s"), F.max("epoch_s").alias("last_epoch_s"))
    tables["station_summary"] = collect(meta_stats, ["station_id"])
    metrics["stations"] = len(tables["station_summary"])
    for field in ["name_variants", "device_variants", "numeric_coordinate_variants"]:
        metrics["stations_with_" + field] = sum(row[field] > 1 for row in tables["station_summary"])
    print(f"{now()} YEAR {year}: full station/month coverage and ordered cadence", flush=True)
    # Floor-to-slot is a diagnostic only, never a cleaned timestamp. Full coverage
    # counts occupied nominal slots to avoid ratios >1 from mixed time phases.
    event = key.select("station_id", "epoch_s", "candidate_rows").withColumn("event_ts", F.col("epoch_s").cast("timestamp"))
    event = (event.withColumn("event_year", F.year("event_ts")).withColumn("month", F.month("event_ts"))
             .withColumn("offset_s", F.pmod("epoch_s", F.lit(300)))
             .withColumn("slot", F.floor(F.col("epoch_s") / 300).cast("long")))
    monthly = event.filter(F.col("event_year") == year).groupBy("station_id", "month").agg(
        F.count("*").alias("unique_timestamps"), F.countDistinct("slot").alias("occupied_nominal_slots"),
        F.countDistinct("offset_s").alias("time_phase_variants"),
        F.sum((F.col("offset_s") != 0).cast("long")).alias("off_grid_unique_timestamps"),
        F.sum((F.col("candidate_rows") > 0).cast("long")).alias("candidate_unique_timestamps"),
        F.countDistinct(F.when(F.col("candidate_rows") > 0, F.col("slot"))).alias("candidate_occupied_nominal_slots"),
        F.min("epoch_s").alias("first_epoch_s"), F.max("epoch_s").alias("last_epoch_s"))
    observed = {(row["station_id"], row["month"]): row for row in collect(monthly)}
    coverage = []
    for station in tables["station_summary"]:
        for month in range(1, 13):
            row = observed.get((station["station_id"], month), {})
            expected = calendar.monthrange(year, month)[1] * 288
            occupied = row.get("occupied_nominal_slots", 0)
            span = ((row["last_epoch_s"] // 300 - row["first_epoch_s"] // 300 + 1) if row else 0)
            coverage.append({"station_id": station["station_id"], "month": month,
                             "unique_timestamps": row.get("unique_timestamps", 0),
                             "occupied_nominal_slots": occupied, "expected_calendar_slots": expected,
                             "calendar_coverage_ratio": round(occupied / expected, 8),
                             "calendar_unobserved_slots": expected - occupied,
                             "observed_span_slots": span, "observed_span_unobserved_slots": span - occupied,
                             "observed_span_coverage_ratio": round(occupied / span, 8) if span else None,
                             "time_phase_variants": row.get("time_phase_variants", 0),
                             "off_grid_unique_timestamps": row.get("off_grid_unique_timestamps", 0),
                             "slot_collision_excess": row.get("unique_timestamps", 0) - occupied,
                             "candidate_unique_timestamps": row.get("candidate_unique_timestamps", 0),
                             "candidate_calendar_coverage_ratio": round(row.get("candidate_occupied_nominal_slots", 0) / expected, 8),
                             "first_epoch_s": row.get("first_epoch_s"), "last_epoch_s": row.get("last_epoch_s")})
    tables["station_month_coverage"] = coverage
    window = Window.partitionBy("station_id").orderBy("epoch_s")
    gaps = (event.withColumn("previous_epoch_s", F.lag("epoch_s").over(window))
            .withColumn("gap_s", F.col("epoch_s") - F.col("previous_epoch_s"))
            .filter(F.col("gap_s").isNotNull()).persist(StorageLevel.DISK_ONLY))
    tables["cadence_gap_counts"] = collect(gaps.groupBy("gap_s").count(), ["gap_s"])
    metrics["adjacent_unique_intervals"] = sum(r["count"] for r in tables["cadence_gap_counts"])
    metrics["gap_300s_intervals"] = sum(r["count"] for r in tables["cadence_gap_counts"] if r["gap_s"] == 300)
    metrics["gap_gt_300s_intervals"] = sum(r["count"] for r in tables["cadence_gap_counts"] if r["gap_s"] > 300)
    metrics["gap_lt_300s_intervals"] = sum(r["count"] for r in tables["cadence_gap_counts"] if r["gap_s"] < 300)
    metrics["gap_not_multiple_300s_intervals"] = sum(r["count"] for r in tables["cadence_gap_counts"] if r["gap_s"] % 300 != 0)
    tables["cadence_gap_examples"] = collect(gaps.filter("gap_s != 300").orderBy(F.desc("gap_s"), "station_id", "epoch_s").limit(30))
    gaps.unpersist()
    # Each anomaly type gets a bounded example sample from the complete audit.
    # Only example selection is bounded; all counts above are full-data counts.
    # Delayed updates and off-grid times can be millions of rows: avoid sorting
    # all examples. Per-reason predicate push-down with limit suffices for samples.
    examples = []
    selected_columns = [*COLUMNS, "field_count", "_corrupt_record", "raw_line"]
    for reason, condition in conditions.items():
        if metrics[reason + "_rows"]:
            for row in collect(data.filter(condition).select(*selected_columns).limit(5)):
                examples.append({"reason": reason, **row})
    tables["anomaly_examples"] = examples
    tables["largest_rainfall_examples"] = collect(data.filter("candidate_valid").orderBy(F.desc("rain_mm"), "epoch_s", "station_id")
                                                    .select(*COLUMNS).limit(15))
    # Executable reconciliation checks, not assumptions from physical lines.
    checks = {
        "one_header": metrics["header_occurrences"] == 1,
        "spark_vs_text_records": metrics["spark_record_rows"] == metrics["physical_lines"] - metrics["header_occurrences"],
        "spark_vs_manifest_physical_lines": expected_physical is None or metrics["spark_record_rows"] == expected_physical,
        "key_reconciliation": metrics["key_eligible_rows"] == metrics["unique_station_time_keys"] + metrics["duplicate_key_excess_rows"],
        "unit_type_reconciliation": sum(r["count"] for r in tables["units_types"]) == metrics["spark_record_rows"],
        "offset_reconciliation": sum(r["count"] for r in tables["timestamp_offsets"]) == metrics["spark_record_rows"],
        "metadata_reconciliation": sum(r["rows"] for r in tables["station_metadata_history"]) == metrics["key_eligible_rows"],
        "cadence_reconciliation": metrics["adjacent_unique_intervals"] == metrics["unique_station_time_keys"] - metrics["stations"],
        "monthly_key_reconciliation": sum(r["unique_timestamps"] for r in coverage) == metrics["unique_station_time_keys"] - eligible.filter(F.col("event_year") != year).select("station_id", "epoch_s").distinct().count(),
        "coverage_in_range": all(0 <= r["occupied_nominal_slots"] <= r["expected_calendar_slots"] and r["observed_span_unobserved_slots"] >= 0 for r in coverage),
    }
    for name in tables:
        tables[name] = [{"year": year, **row} for row in tables[name]]
    metrics["elapsed_seconds"] = round(time.monotonic() - started, 3)
    result = {"metrics": metrics, "tables": tables, "checks": checks, "completed_at_sgt": now()}
    key.unpersist()
    data.unpersist()
    write_json(out / str(year) / "audit.json", result)
    for name, rows in tables.items():
        write_csv(out / str(year) / (name + ".csv"), rows)
    print(f"{now()} YEAR {year}: completed in {metrics['elapsed_seconds']}s; checks={checks}", flush=True)
    if not all(checks.values()):
        raise RuntimeError(f"Reconciliation failed for {year}: {checks}")
    return result


def self_test(spark, out):
    # Deliberately broken CSVs and changing station time phases test failure
    # detection, as well as the distinction between records, keys and slots.
    base = ["2020-01-01", "2020-01-01T00:00:00+08:00", "2020-01-01T00:05:00+08:00", "TEST_A",
            "Test, station", "device1", "103.8", "1.3", "2020-01-01T00:05:00+08:00", "0.2", EXPECTED_TYPE, "mm"]
    rows = [base.copy(), base.copy()]
    conflict = base.copy(); conflict[9] = "0.4"; rows.append(conflict)
    for i, value in enumerate(["NaN", "Infinity", "-1", "", "broken"], 1):
        row = base.copy(); row[1] = f"2020-01-01T00:{i * 5:02d}:00+08:00"; row[9] = value; rows.append(row)
    bad_time = base.copy(); bad_time[1] = "invalid"; rows.append(bad_time)
    no_station = base.copy(); no_station[3] = ""; rows.append(no_station)
    odd_phase = base.copy(); odd_phase[1] = "2020-01-01T00:04:59+08:00"; odd_phase[4] = "Changed"; rows.append(odd_phase)
    bad_unit = base.copy(); bad_unit[1] = "2020-01-01T01:00:00+08:00"; bad_unit[11] = "inch"; rows.append(bad_unit)
    rows += [base + ["extra"], base[:-1]]
    path = out / "fixture.csv"; path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream); writer.writerow(COLUMNS); writer.writerows(rows)
    result = audit_year(spark, path, 2020, out, len(rows))
    m = result["metrics"]
    expected = {"spark_record_rows": 14, "csv_field_count_mismatch_rows": 2,
                "nonfinite_rainfall_rows": 2, "negative_rainfall_rows": 1, "missing_rainfall_rows": 1,
                "invalid_rainfall_number_rows": 1, "invalid_timestamp_rows": 1,
                "missing_station_id_rows": 1, "unexpected_unit_rows": 2,
                "off_five_minute_grid_rows": 1, "duplicate_key_excess_rows": 2,
                "exact_duplicate_excess_key_eligible_rows": 1, "conflicting_reading_key_groups": 1,
                "unique_station_time_keys": 8, "candidate_valid_rows": 4, "candidate_unique_keys": 2}
    checks = {key: {"expected": value, "actual": m[key], "passed": m[key] == value} for key, value in expected.items()}
    january = next(r for r in result["tables"]["station_month_coverage"] if r["month"] == 1)
    checks["slot_collision"] = {"passed": january["slot_collision_excess"] == 1}
    checks["leap_year_february"] = {"passed": result["tables"]["station_month_coverage"][1]["expected_calendar_slots"] == 29 * 288}
    checks["quoted_comma_preserved"] = {"passed": any(r["station_name"] == "Test, station" for r in result["tables"]["station_metadata_history"])}
    checks["phase_change_cadence"] = {"passed": any(r["gap_s"] == 299 for r in result["tables"]["cadence_gap_counts"])}
    write_json(out / "self_test.json", {"status": "PASS" if all(r["passed"] for r in checks.values()) else "FAIL", "checks": checks})
    assert all(r["passed"] for r in checks.values()), checks
    print("Synthetic audit self-test PASS", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "outputs" / "p0_2_audit" / VERSION)
    args = parser.parse_args()
    out = args.output.resolve(); out.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((ROOT / "manifests" / "raw_manifest.json").read_text(encoding="utf-8"))
    sources = [{**entry, "mtime_ns": (ROOT / entry["path"]).stat().st_mtime_ns} for entry in manifest["files"]]
    for source in sources:
        assert (ROOT / source["path"]).stat().st_size == source["bytes"]
    signature = {"version": VERSION, "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                 "session_helper_sha256": hashlib.sha256((ROOT / "scripts" / "spark_session.py").read_bytes()).hexdigest(),
                 "manifest_sha256": hashlib.sha256((ROOT / "manifests" / "raw_manifest.json").read_bytes()).hexdigest(),
                 "source_sizes_mtimes": [(s["path"], s["bytes"], s["mtime_ns"]) for s in sources]}
    signature = json.loads(json.dumps(signature))
    state_path = out / "run_status.json"
    if state_path.exists() and not args.self_test:
        previous = json.loads(state_path.read_text(encoding="utf-8"))
        if not args.resume:
            raise RuntimeError("Output exists; use --resume with unchanged inputs/code, or a fresh --output")
        if previous["signature"] != signature:
            raise RuntimeError("Resume input/code signature changed; choose a fresh --output")
    state = {"status": "RUNNING", "started_at_sgt": now(), "signature": signature, "completed_years": []}
    write_json(state_path, state)
    spark = None
    try:
        spark, hooks = start_spark("DSA5208-P0-2-full-quality-audit")
        state["environment"] = {"python": sys.version, "spark": spark.version,
                                "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                                "master": spark.sparkContext.master, "driver_memory": "4g", "shuffle_partitions": 48,
                                "timezone": spark.conf.get("spark.sql.session.timeZone")}
        if args.self_test:
            self_test(spark, out)
        else:
            results = []
            for source in sources:
                year = source["year"]; annual = out / str(year) / "audit.json"
                if args.resume and annual.exists():
                    result = json.loads(annual.read_text(encoding="utf-8"))
                    assert all(result["checks"].values())
                    print(f"Verified checkpoint: {year}", flush=True)
                else:
                    state["current_year"] = year; write_json(state_path, state)
                    result = audit_year(spark, ROOT / source["path"], year, out, source["physical_data_lines"])
                results.append(result); state["completed_years"].append(year); write_json(state_path, state)
            metrics = [r["metrics"] for r in results]
            write_csv(out / "annual_quality.csv", metrics)
            for name in results[0]["tables"]:
                write_csv(out / (name + ".csv"), [row for result in results for row in result["tables"][name]])
            # Full-period station metadata variants and year-boundary gaps.
            history = [row for r in results for row in r["tables"]["station_metadata_history"]]
            stations = sorted({r["station_id"] for r in history})
            station_rows, boundary_rows = [], []
            summaries = [row for r in results for row in r["tables"]["station_summary"]]
            for station in stations:
                h = [r for r in history if r["station_id"] == station]
                s = sorted([r for r in summaries if r["station_id"] == station], key=lambda r: r["year"])
                station_rows.append({"station_id": station, "years_observed": ";".join(str(r["year"]) for r in s),
                                     "year_count": len(s), "rows": sum(r["rows"] for r in s),
                                     "name_variants": len({r["station_name"] for r in h}),
                                     "device_variants": len({r["station_device_id"] for r in h}),
                                     "numeric_coordinate_variants": len({(numeric_coordinate_key(r["location_longitude"]), numeric_coordinate_key(r["location_latitude"])) for r in h}),
                                     "first_epoch_s": min(r["first_epoch_s"] for r in s), "last_epoch_s": max(r["last_epoch_s"] for r in s)})
                for left, right in zip(s, s[1:]):
                    boundary_rows.append({"station_id": station, "from_year": left["year"], "to_year": right["year"],
                                          "previous_epoch_s": left["last_epoch_s"], "next_epoch_s": right["first_epoch_s"],
                                          "gap_s": right["first_epoch_s"] - left["last_epoch_s"]})
            write_csv(out / "station_full_period_summary.csv", station_rows)
            write_csv(out / "year_boundary_gaps.csv", boundary_rows)
            summary = {"status": "PASS", "scope": "Full raw-data audit; candidate-valid records are not approved cleaned data.",
                       "sources": sources, "annual_metrics": metrics, "station_count_full_period": len(stations),
                       "total_spark_record_rows": sum(m["spark_record_rows"] for m in metrics),
                       "total_structurally_valid_rows": sum(m["structurally_valid_rows"] for m in metrics),
                       "total_candidate_valid_rows": sum(m["candidate_valid_rows"] for m in metrics),
                       "total_unique_station_time_keys_within_files": sum(m["unique_station_time_keys"] for m in metrics),
                       "all_reconciliation_checks_passed": all(all(r["checks"].values()) for r in results),
                       "source_sizes_mtimes_unchanged": all((ROOT / s["path"]).stat().st_size == s["bytes"] and (ROOT / s["path"]).stat().st_mtime_ns == s["mtime_ns"] for s in sources),
                       "completed_at_sgt": now()}
            assert summary["source_sizes_mtimes_unchanged"] and summary["all_reconciliation_checks_passed"]
            write_json(out / "summary.json", summary)
            state["summary"] = {k: v for k, v in summary.items() if k not in {"sources", "annual_metrics"}}
        state["status"] = "PASS"
    except Exception as error:
        state["status"] = "FAIL"; state["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if spark is not None:
            try:
                state["shutdown"] = stop_spark(spark, hooks)
            except Exception as error:
                state["status"] = "FAIL"; state["shutdown_error"] = str(error)
                write_json(state_path, state)
                raise
        state["finished_at_sgt"] = now(); write_json(state_path, state)
        print(f"AUDIT {state['status']}: {out}", flush=True)


if __name__ == "__main__":
    main()
