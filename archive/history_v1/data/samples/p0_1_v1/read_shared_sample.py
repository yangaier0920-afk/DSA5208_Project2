"""Read the fixed shared sample on Windows/macOS and save a member receipt."""
import argparse
import atexit
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--member", choices=["A", "B", "C"], required=True)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    base = args.sample_dir.resolve()
    manifest = json.loads((base / "sample_manifest.json").read_text(encoding="utf-8"))
    lock = json.loads((base / "environment.lock.json").read_text(encoding="utf-8"))
    if f"{sys.version_info.major}.{sys.version_info.minor}" != lock["required_python_major_minor"]:
        raise RuntimeError("Shared environment contract requires Python 3.11")
    for item in manifest["payload_files"]:
        path = (base / item["path"]).resolve()
        if not path.is_relative_to(base):
            raise RuntimeError("Manifest path leaves sample directory")
        with path.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if path.stat().st_size != item["bytes"] or actual != item["sha256"]:
            raise RuntimeError(f"Shared file differs: {item['path']}")
    # When run from this Windows project, reuse its already verified runtime.
    if os.name == "nt" and (Path.cwd() / ".runtime/java_manifest.json").exists():
        sys.path.insert(0, str(Path.cwd() / "scripts"))
        from spark_runtime import configure_runtime
        configure_runtime()
    import pyspark
    import py4j
    from pyspark.sql import SparkSession, functions as F
    if pyspark.__version__ != lock["pyspark"] or py4j.__version__ != lock["py4j"]:
        raise RuntimeError("PySpark/Py4J do not match environment.lock.json")
    os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).resolve().parent)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    hooks = []
    original_register = atexit.register
    def capture_register(function, *positional, **keywords):
        if function.__module__ == "pyspark.java_gateway" and function.__name__ == "killChild":
            hooks.append(function)
        return original_register(function, *positional, **keywords)
    spark = None
    receipt = {"status": "RUNNING", "member": args.member, "sample_version": manifest["sample_version"],
               "sample_manifest_sha256": hashlib.sha256((base / "sample_manifest.json").read_bytes()).hexdigest(),
               "platform": platform.platform(), "architecture": platform.machine(),
               "python": platform.python_version(), "pyspark": pyspark.__version__, "py4j": py4j.__version__}
    try:
        atexit.register = capture_register
        try:
            spark = (SparkSession.builder.appName("DSA5208-shared-sample-receipt")
                     .master("local[2]").config("spark.driver.memory", "2g")
                     .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.sql.session.timeZone", lock["spark_timezone"])
                     .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
                     .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        finally:
            atexit.register = original_register
        spark.sparkContext.setLogLevel("ERROR")
        java = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
        if java.split(".")[0] != str(lock["required_java_major"]):
            raise RuntimeError(f"Java 17 required; actual version is {java}")
        receipt["java"] = java
        data = spark.read.parquet((base / "parquet").as_posix())
        if data.count() != manifest["rows"]:
            raise RuntimeError("Row count mismatch")
        expected_types = {f["name"]: f["type"] for f in manifest["schema"]["fields"]}
        actual_types = {f["name"]: f["type"] for f in data.schema.jsonValue()["fields"]}
        if expected_types != actual_types:
            raise RuntimeError("Schema/type mismatch")
        grouped = data.groupBy("year", "station_id").agg(F.count("*").alias("n"), F.sum("rain_mm").alias("rain_sum_mm"))
        actual = {(r.year, r.station_id): r for r in grouped.collect()}
        expected = manifest["expected_aggregates"]
        if set(actual) != {(r["year"], r["station_id"]) for r in expected}:
            raise RuntimeError("Group keys mismatch")
        for r in expected:
            a = actual[(r["year"], r["station_id"])]
            if a.n != r["n"] or not math.isclose(a.rain_sum_mm, r["rain_sum_mm"], rel_tol=1e-10, abs_tol=1e-8):
                raise RuntimeError("Group aggregates mismatch")
        if spark.sparkContext.parallelize(range(20), 2).map(lambda x: x + 1).sum() != 210:
            raise RuntimeError("Python worker check failed")
        receipt.update({"status": "PASS", "rows": manifest["rows"], "groups": len(actual),
                        "payload_hashes_passed": True, "schema_passed": True,
                        "aggregate_comparison_passed": True, "python_worker_passed": True,
                        "timezone": spark.conf.get("spark.sql.session.timeZone")})
    except Exception as error:
        receipt.update({"status": "FAIL", "error": f"{type(error).__name__}: {error}"})
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
                if gateway.proc.returncode != 0:
                    raise RuntimeError("Spark gateway exited with an error")
                for hook in hooks:
                    atexit.unregister(hook)
        now = datetime.now(timezone(timedelta(hours=8)))
        receipt["checked_at_sgt"] = now.isoformat()
        destination = args.receipt or Path.cwd() / "outputs/p0_1_receipts" / f"{args.member}_{now.strftime('%Y%m%d_%H%M%S')}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"{receipt['status']}: {destination}", flush=True)


if __name__ == "__main__":
    main()
