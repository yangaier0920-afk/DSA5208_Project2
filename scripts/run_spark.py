"""Run a project Spark script with the same environment as the verification."""
import runpy
import sys
from pathlib import Path
from spark_runtime import configure_runtime

if len(sys.argv) < 2:
    raise SystemExit("Usage: .venv/Scripts/python.exe scripts/run_spark.py <script.py> [arguments]")
target = Path(sys.argv[1]).resolve()
if not target.is_file():
    raise SystemExit(f"Script does not exist: {target}")
configure_runtime()
sys.argv = [str(target)] + sys.argv[2:]
sys.path.insert(0, str(target.parent))
runpy.run_path(str(target), run_name="__main__")
