"""Diagnostic only: inspect 2017 time-phase boundaries and a possible +1s rule."""
import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).parent))
from spark_session import start_spark, stop_spark
from importlib import import_module
audit = import_module("01_audit_raw")


def main():
    from pyspark.sql import functions as F, types as T, Window
    from pyspark import StorageLevel
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=ROOT / "outputs" / "p0_2_audit" / "p0_2_v1")
    args = parser.parse_args()
    out = args.audit_dir.resolve() / "time_alignment_diagnostic"
    full = json.loads((out.parent / "summary.json").read_text(encoding="utf-8"))
    state = json.loads((out.parent / "run_status.json").read_text(encoding="utf-8"))
    assert full["status"] == state["status"] == "PASS"
    source = next(s for s in full["sources"] if s["year"] == 2017)
    path = ROOT / source["path"]
    assert path.stat().st_size == source["bytes"] and path.stat().st_mtime_ns == source["mtime_ns"]
    year = next(m for m in full["annual_metrics"] if m["year"] == 2017)
    assert year["csv_corrupt_rows"] == year["csv_field_count_mismatch_rows"] == year["invalid_timestamp_rows"] == year["duplicate_key_excess_rows"] == 0
    report = {"status": "RUNNING", "scope": "Diagnostic comparison only; source data and cleaned timestamps are not changed.",
              "proposed_rule": "only offset=299 seconds: add 1 second; all other offsets unchanged",
              "started_at_sgt": audit.now(), "source": source,
              "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    spark = None
    try:
        spark, hooks = start_spark("DSA5208-P0-2-time-phase-diagnostic")
        schema = T.StructType([T.StructField(c, T.StringType()) for c in audit.COLUMNS])
        data = (spark.read.options(header="true", enforceSchema="false", mode="FAILFAST", escape='"')
                .schema(schema).csv(path.as_posix())
                .withColumn("epoch_s", F.to_timestamp("timestamp", audit.PATTERN).cast("long"))
                .withColumn("rain_mm", F.col("reading_value").cast("double"))
                .withColumn("offset_s", F.pmod("epoch_s", F.lit(300)))
                .withColumn("proposed_epoch_s", F.col("epoch_s") + F.when(F.col("offset_s") == 299, 1).otherwise(0))
                .persist(StorageLevel.DISK_ONLY))
        report["rows"] = data.count(); assert report["rows"] == year["spark_record_rows"]
        window = Window.partitionBy("station_id").orderBy("epoch_s")
        pairs = (data.withColumn("previous_epoch_s", F.lag("epoch_s").over(window))
                 .withColumn("previous_offset_s", F.lag("offset_s").over(window))
                 .withColumn("previous_timestamp", F.lag("timestamp").over(window))
                 .withColumn("previous_rain_mm", F.lag("rain_mm").over(window))
                 .withColumn("gap_s", F.col("epoch_s") - F.col("previous_epoch_s")))
        transitions = audit.collect(pairs.filter(F.col("offset_s") != F.col("previous_offset_s"))
                                    .select("station_id", "previous_timestamp", "timestamp", "previous_epoch_s", "epoch_s",
                                            "previous_offset_s", "offset_s", "gap_s", "previous_rain_mm", "rain_mm")
                                    .orderBy("epoch_s", "station_id"))
        audit.write_csv(out / "phase_transition_events.csv", transitions)
        short_gaps = audit.collect(pairs.filter("gap_s < 300").select("station_id", "previous_timestamp", "timestamp",
                                            "previous_epoch_s", "epoch_s", "previous_offset_s", "offset_s", "gap_s", "previous_rain_mm", "rain_mm")
                                  .orderBy("epoch_s", "station_id"))
        audit.write_csv(out / "all_sub_300s_pairs.csv", short_gaps)
        diagnostic_key = (data.groupBy("station_id", "proposed_epoch_s").agg(
            F.count("*").alias("n"), F.countDistinct("rain_mm").alias("distinct_numeric_rainfall"),
            F.countDistinct(F.struct("reading_value", "reading_type", "reading_unit")).alias("distinct_reading_text"),
            F.countDistinct(F.struct("station_name", "station_device_id", "location_longitude", "location_latitude")).alias("distinct_metadata"))
            .persist(StorageLevel.MEMORY_AND_DISK))
        report.update(diagnostic_key.agg(
            F.count("*").alias("proposed_unique_keys"),
            F.sum((F.col("n") > 1).cast("long")).alias("proposed_collision_groups"),
            F.sum(F.col("n") - 1).alias("proposed_collision_excess_rows"),
            F.sum((F.col("distinct_numeric_rainfall") > 1).cast("long")).alias("proposed_numeric_rainfall_conflicts"),
            F.sum((F.col("distinct_metadata") > 1).cast("long")).alias("proposed_metadata_conflicts")).first().asDict())
        collisions = diagnostic_key.filter("n > 1")
        audit.write_csv(out / "proposed_alignment_collision_groups.csv", audit.collect(collisions, ["station_id", "proposed_epoch_s"]))
        collision_rows = audit.collect(data.join(collisions.select("station_id", "proposed_epoch_s"), ["station_id", "proposed_epoch_s"], "inner")
                                      .select(*audit.COLUMNS, "epoch_s", "offset_s", "proposed_epoch_s", "rain_mm")
                                      .orderBy("proposed_epoch_s", "station_id", "epoch_s"))
        audit.write_csv(out / "proposed_alignment_collision_records.csv", collision_rows)
        report["proposed_date_changes"] = data.filter(F.to_date(F.col("epoch_s").cast("timestamp")) != F.to_date(F.col("proposed_epoch_s").cast("timestamp"))).count()
        report["phase_transition_events"] = len(transitions)
        report["sub_300s_pairs"] = len(short_gaps)
        report["checks"] = {"row_count_matches_full_audit": report["rows"] == year["spark_record_rows"],
                            "short_gaps_match_full_audit": len(short_gaps) == year["gap_lt_300s_intervals"],
                            "proposed_key_reconciliation": report["proposed_unique_keys"] + report["proposed_collision_excess_rows"] == report["rows"],
                            "source_unchanged": path.stat().st_size == source["bytes"] and path.stat().st_mtime_ns == source["mtime_ns"]}
        diagnostic_key.unpersist(); data.unpersist()
        assert all(report["checks"].values())
        report["status"] = "PASS"
    except Exception as error:
        report["status"] = "FAIL"; report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if spark is not None:
            report["shutdown"] = stop_spark(spark, hooks)
        report["finished_at_sgt"] = audit.now()
        audit.write_json(out / "diagnostic.json", report)
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
