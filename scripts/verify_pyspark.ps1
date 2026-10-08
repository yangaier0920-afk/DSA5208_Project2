$ErrorActionPreference = 'Stop'
$env:PYTHONIOENCODING = 'utf-8'
$taskRoot = Split-Path $PSScriptRoot -Parent
$taskPython = Join-Path $taskRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $taskPython)) {
    throw 'Project Python environment is missing. Create .venv and install requirements-spark.txt first.'
}
& $taskPython (Join-Path $PSScriptRoot 'verify_pyspark.py') @args
if ($LASTEXITCODE -ne 0) { throw "Spark verification failed with exit code $LASTEXITCODE" }
