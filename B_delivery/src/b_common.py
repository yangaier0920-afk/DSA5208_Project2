"""Portable B workflow helpers. The frozen A release is read-only."""
import csv, hashlib, json, os, platform, sys, time
from datetime import datetime, timezone
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
CONFIG = PACKAGE / 'configs/B_experiment.json'

def dump(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str)+'\n')

def rows(path):
    with Path(path).open(encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))

def csv_write(path, records, fields=None):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    records=iter(records)
    first=next(records,None)
    if fields is None:
        if first is None: raise ValueError('Empty CSV needs explicit fields')
        fields=list(first)
    with path.open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
        if first is not None:w.writerow(first)
        w.writerows(records)

def log(message):
    print(datetime.now().strftime('%H:%M:%S'),message,flush=True)

def release(root):
    sys.path.insert(0,str(Path(root)/'scripts'))
    import data_release
    return data_release

def start(root, name):
    import pyspark
    os.environ['SPARK_HOME']=str(Path(pyspark.__file__).resolve().parent)
    os.environ['PYSPARK_PYTHON']=sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
    os.environ['SPARK_LOCAL_IP']='127.0.0.1'
    tmp=Path(os.environ.get('B_SPARK_LOCAL_DIR', PACKAGE/'runtime/spark-local')).resolve()
    tmp.mkdir(parents=True,exist_ok=True)
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.appName(name).master('local[2]')
        .config('spark.driver.memory','4g')
        .config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
        .config('spark.sql.session.timeZone','Asia/Singapore')
        .config('spark.sql.shuffle.partitions','128')
        .config('spark.sql.files.maxPartitionBytes','33554432')
        .config('spark.sql.adaptive.enabled','true')
        .config('spark.sql.adaptive.coalescePartitions.enabled','false')
        .config('spark.local.dir',str(tmp))
        .config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false')
        .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark

def metadata(root, out, command, elapsed, extra=None):
    d=release(root)
    files=[p for p in sorted(Path(out).rglob('*')) if p.is_file() and p.name!='result_metadata.json' and not p.name.startswith('.')]
    obj={**d.provenance(root),'member':'B','input_scope':'full',
        'created_at':datetime.now(timezone.utc).isoformat(),
        'experiment_config_hash':d.sha256(CONFIG),'experiment_config_file':'configs/B_experiment.json',
        'command':command,'elapsed_seconds':elapsed,
        'environment':{'os':platform.platform(),'architecture':platform.machine(),'python':platform.python_version(),
            'logical_cpu_count':os.cpu_count(),'spark':'3.5.7','java':'17',
            'spark_settings':json.loads(CONFIG.read_text())['spark'],
            'physical_memory':'not measured','peak_rss':'not measured'},
        'outputs':[{'path':p.relative_to(out).as_posix(),'bytes':p.stat().st_size,'sha256':d.sha256(p)} for p in files]}
    if extra:obj.update(extra)
    dump(Path(out)/'result_metadata.json',obj)
