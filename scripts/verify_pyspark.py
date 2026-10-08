"""Verify local Spark using bounded real CSV samples and a Parquet round trip."""
import argparse
import atexit
import csv
import hashlib
import json
import math
import platform
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
from spark_runtime import configure_runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-per-year", type=int, default=2000)
    args = parser.parse_args()
    if args.rows_per_year < 1:
        parser.error("--rows-per-year must be positive")
    java_manifest = configure_runtime()
    from pyspark.sql import SparkSession, functions as F, types as T
    import pyspark

    run_id = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    run_dir = ROOT / "outputs" / "spark_smoke" / run_id
    run_dir.mkdir(parents=True)
    sample = run_dir / "sample.csv"
    files = sorted((ROOT / "Raw_Dataset").glob("HistoricalRainfallacrossSingapore*.csv"))
    if len(files) != 8:
        raise RuntimeError("Expected eight annual CSV files")
    expected_groups = defaultdict(lambda: [0, 0.0])
    counts, source_metadata, header = {}, [], None
    first_timestamp = None
    with sample.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        for path in files:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                if header is None:
                    header = reader.fieldnames
                    writer.writerow(header)
                elif header != reader.fieldnames:
                    raise RuntimeError("Annual CSV schemas differ")
                count = 0
                for row in reader:
                    if count >= args.rows_per_year:
                        break
                    if None in row or any(v is None for v in row.values()):
                        raise RuntimeError("Malformed CSV sample record")
                    writer.writerow([row[c] for c in header])
                    ts = datetime.fromisoformat(row["timestamp"])
                    key = (ts.year, row["station_id"])
                    expected_groups[key][0] += 1
                    expected_groups[key][1] += float(row["reading_value"])
                    first_timestamp = first_timestamp or row["timestamp"]
                    count += 1
            counts[path.name] = count
            source_metadata.append({"file": path.name, "bytes": path.stat().st_size,
                                    "mtime_ns": path.stat().st_mtime_ns})
    expected_rows = sum(counts.values())
    local_dir = ROOT / ".runtime" / "spark-local"
    local_dir.mkdir(parents=True, exist_ok=True)
    report = {"status": "RUNNING", "run_id": run_id, "scope": "Environment verification only; first rows of each year, not representative EDA or full cleaning.",
              "python_executable": sys.executable, "python_version": platform.python_version(),
              "pyspark_version": pyspark.__version__, "platform": platform.platform(),
              "java": java_manifest, "sample_rows_per_file": counts, "source_files": source_metadata,
              "sample_sha256": hashlib.sha256(sample.read_bytes()).hexdigest(), "checks": {}}
    spark = None
    gateway_cleanup_hooks = []
    started = datetime.now(timezone.utc)
    try:
        # Capture only this pinned PySpark version's Windows fallback hook.
        # Unregister it only after our gateway is confirmed to have exited.
        original_register = atexit.register
        def capture_register(function, *positional, **keywords):
            if function.__module__ == "pyspark.java_gateway" and function.__name__ == "killChild":
                gateway_cleanup_hooks.append(function)
            return original_register(function, *positional, **keywords)
        atexit.register = capture_register
        try:
            spark = (SparkSession.builder.appName("DSA5208-local-read-write-verification")
                 .master("local[2]")
                 .config("spark.driver.memory", "2g")
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .config("spark.sql.session.timeZone", "Asia/Singapore")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.ui.enabled", "false")
                 .config("spark.ui.showConsoleProgress", "false")
                 .config("spark.local.dir", str(local_dir))
                 .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
                 .getOrCreate())
        finally:
            atexit.register = original_register
        spark.sparkContext.setLogLevel("ERROR")
        report.update({"spark_version": spark.version, "master": spark.sparkContext.master,
                       "java_runtime_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                       "timezone": spark.conf.get("spark.sql.session.timeZone"),
                       "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")})
        report["hadoop_java_version"] = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        report["hadoop_native_loaded"] = bool(spark.sparkContext._jvm.org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
        if (ROOT / ".runtime" / "hadoop_manifest.json").exists():
            report["hadoop_windows"] = json.loads((ROOT / ".runtime" / "hadoop_manifest.json").read_text(encoding="utf-8"))
        print("Spark started; reading bounded samples from all eight years.", flush=True)
        schema = T.StructType([T.StructField(c, T.StringType(), True) for c in header])
        raw = spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(sample.as_posix())
        data = (raw.withColumn("event_ts", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssXXX"))
                .withColumn("rain_mm", F.col("reading_value").cast("double"))
                .withColumn("year", F.year("event_ts"))
                .withColumn("month", F.month("event_ts"))
                .withColumn("epoch_s", F.col("event_ts").cast("long")))
        actual_rows = data.count()
        assert actual_rows == expected_rows, (actual_rows, expected_rows)
        invalid = data.filter(F.col("event_ts").isNull() | F.col("rain_mm").isNull()
                              | F.isnan("rain_mm") | (F.abs(F.col("rain_mm")) == float("inf"))).count()
        assert invalid == 0, invalid
        assert data.filter(F.col("date") != F.date_format("event_ts", "yyyy-MM-dd")).count() == 0
        expected_epoch = int(datetime.fromisoformat(first_timestamp).timestamp())
        assert data.filter(F.col("timestamp") == first_timestamp).select("epoch_s").first()[0] == expected_epoch
        report["checks"]["csv_read_and_time_parse"] = {"passed": True, "rows": actual_rows,
                                                         "invalid_rows": invalid, "first_timestamp": first_timestamp,
                                                         "first_epoch_s": expected_epoch}
        aggregated = data.groupBy("year", "station_id").agg(F.count("*").alias("n"), F.sum("rain_mm").alias("rain_sum_mm"))
        actual_groups = {(r.year, r.station_id): (r.n, r.rain_sum_mm) for r in aggregated.collect()}
        assert set(actual_groups) == set(expected_groups)
        for key, (expected_n, expected_sum) in expected_groups.items():
            n, total = actual_groups[key]
            assert n == expected_n and math.isclose(total, expected_sum, rel_tol=1e-10, abs_tol=1e-8), key
        report["checks"]["spark_aggregation_vs_independent_python"] = {"passed": True, "groups": len(actual_groups)}
        print("CSV types, local dates, epoch seconds and group aggregates matched.", flush=True)
        parquet_path = run_dir / "parquet"
        data.write.mode("errorifexists").partitionBy("year", "month").parquet(parquet_path.as_posix())
        restored = spark.read.parquet(parquet_path.as_posix()).select(*data.columns)
        restored_rows = restored.count()
        assert restored_rows == expected_rows
        assert data.exceptAll(restored).count() == 0
        assert restored.exceptAll(data).count() == 0
        parquet_files = list(parquet_path.rglob("*.parquet"))
        assert parquet_files
        report["checks"]["partitioned_parquet_round_trip"] = {"passed": True, "rows": restored_rows,
                                                              "parquet_files": len(parquet_files),
                                                              "bytes": sum(p.stat().st_size for p in parquet_files),
                                                              "differences_both_directions": 0}
        print("Partitioned Parquet write/read passed with identical rows.", flush=True)
        worker_result = spark.sparkContext.parallelize(range(20), 2).map(lambda x: x + 1).sum()
        assert worker_result == 210
        report["checks"]["python_worker"] = {"passed": True, "actual_sum": worker_result}
        report["schema"] = data.schema.jsonValue()
        report["status"] = "PASS"
    except Exception as error:
        report["status"] = "FAIL"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if spark is not None:
            gateway = spark.sparkContext._gateway
            spark.stop()
            gateway.shutdown()
            if gateway.proc is not None:
                if gateway.proc.stdin is not None:
                    gateway.proc.stdin.close()
                gateway.proc.wait(timeout=15)
                report["checks"]["spark_process_shutdown"] = {"passed": True, "exit_code": gateway.proc.returncode}
                for hook in gateway_cleanup_hooks:
                    atexit.unregister(hook)
        report["elapsed_seconds"] = round((datetime.now(timezone.utc) - started).total_seconds(), 3)
        report["completed_at_sgt"] = datetime.now(timezone(timedelta(hours=8))).isoformat()
        (run_dir / "verification.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        if report["status"] == "PASS":
            (ROOT / "outputs" / "spark_smoke" / "latest_pass.json").write_text(
                json.dumps({"run_id": run_id, "report_relative_path": (run_dir / "verification.json").relative_to(ROOT).as_posix()}, indent=2),
                encoding="utf-8")
        print(f"{report['status']}: {run_dir / 'verification.json'}", flush=True)


if __name__ == "__main__":
    main()
