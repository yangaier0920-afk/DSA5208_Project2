"""Project-local Java, Hadoop and Python settings for native Windows Spark."""
import atexit
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def configure_runtime():
    manifest_path = ROOT / ".runtime" / "java_manifest.json"
    if not manifest_path.exists():
        raise RuntimeError("First run scripts/setup_java17.py")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    java_home = ROOT / manifest["java_home_relative"]
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYTHONIOENCODING"] = "utf-8"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_HOME"] = str(ROOT / ".venv" / "Lib" / "site-packages" / "pyspark")
    hadoop_manifest_path = ROOT / ".runtime" / "hadoop_manifest.json"
    if not hadoop_manifest_path.exists():
        raise RuntimeError("First run scripts/setup_hadoop_windows.py")
    hadoop_manifest = json.loads(hadoop_manifest_path.read_text(encoding="utf-8"))
    hadoop_home = ROOT / hadoop_manifest["hadoop_home_relative"]
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ["PATH"]
    if os.name == "nt":
        import ctypes
        kernel = ctypes.windll.kernel32
        previous_input_cp, previous_output_cp = kernel.GetConsoleCP(), kernel.GetConsoleOutputCP()
        kernel.SetConsoleCP(65001)
        kernel.SetConsoleOutputCP(65001)
        atexit.register(lambda: (kernel.SetConsoleCP(previous_input_cp), kernel.SetConsoleOutputCP(previous_output_cp)))
        os.environ["JAVA_TOOL_OPTIONS"] = (os.environ.get("JAVA_TOOL_OPTIONS", "") + " -Dfile.encoding=UTF-8").strip()
    return manifest
