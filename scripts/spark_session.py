"""Pinned local Spark lifecycle; confirm Java exits before removing its fallback."""
import atexit
from pathlib import Path


def start_spark(name, threads=4, memory="4g", partitions=48):
    from pyspark.sql import SparkSession
    root = Path(__file__).resolve().parents[1]
    local = root / ".runtime" / "spark-local"
    local.mkdir(parents=True, exist_ok=True)
    hooks = []
    original = atexit.register
    def capture(function, *args, **kwargs):
        if function.__module__ == "pyspark.java_gateway" and function.__name__ == "killChild":
            hooks.append(function)
        return original(function, *args, **kwargs)
    atexit.register = capture
    try:
        spark = (SparkSession.builder.appName(name).master(f"local[{threads}]")
                 .config("spark.driver.memory", memory)
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .config("spark.sql.session.timeZone", "Asia/Singapore")
                 .config("spark.sql.shuffle.partitions", str(partitions))
                 .config("spark.sql.adaptive.enabled", "true")
                 .config("spark.sql.ansi.enabled", "false")
                 .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
                 .config("spark.sql.csv.parser.columnPruning.enabled", "false")
                 .config("spark.ui.enabled", "false")
                 .config("spark.ui.showConsoleProgress", "false")
                 .config("spark.local.dir", str(local))
                 .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
                 .getOrCreate())
    finally:
        atexit.register = original
    spark.sparkContext.setLogLevel("ERROR")
    return spark, hooks


def stop_spark(spark, hooks):
    gateway = spark.sparkContext._gateway
    spark.stop()
    gateway.shutdown()
    if gateway.proc is not None:
        if gateway.proc.stdin is not None:
            gateway.proc.stdin.close()
        gateway.proc.wait(timeout=30)
        if gateway.proc.returncode != 0:
            raise RuntimeError(f"Java gateway exit code {gateway.proc.returncode}")
        for hook in hooks:
            atexit.unregister(hook)
    return {"passed": True, "gateway_exit_code": getattr(gateway.proc, "returncode", None)}
