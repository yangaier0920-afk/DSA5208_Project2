"""C utilities; reuse B's exact feature and anchor-join contracts."""
import csv, hashlib, json, os, platform, sys
from datetime import datetime, timezone
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
ROOT = PACKAGE.parent
sys.path.insert(0, str(ROOT / 'B_delivery/src'))
sys.path.insert(0, str(ROOT / 'scripts'))
from B_feature_api import NUMERIC_FEATURES, features_at
from B_join_audit import audit_anchor_join
from data_release import verify_release, provenance, sha256

KEYS = ['station_id', 'prediction_epoch_s']
CONFIG = PACKAGE / 'configs/C_experiment.json'

def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def dump(path, data):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str)+'\n',encoding='utf-8',newline='\n')

def csv_write(path, records):
    records = list(records)
    if not records: return
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader(); writer.writerows(records)

def log(message):
    print(datetime.now().isoformat(timespec='seconds'), message, flush=True)

def spark_start(config):
    from portable_runtime import configure_runtime
    configure_runtime()
    import pyspark
    os.environ.update(SPARK_HOME=str(Path(pyspark.__file__).parent), PYSPARK_PYTHON=sys.executable,
                      PYSPARK_DRIVER_PYTHON=sys.executable, SPARK_LOCAL_IP='127.0.0.1')
    from pyspark.sql import SparkSession
    c=config['spark']; tmp=Path(os.environ.get('C_SPARK_LOCAL_DIR',PACKAGE/'runtime/spark-local')); tmp.mkdir(parents=True,exist_ok=True)
    spark=(SparkSession.builder.master(f"local[{c['threads']}]").appName('C E2 rainfall probabilities')
           .config('spark.driver.memory',c['driver_memory']).config('spark.driver.host','127.0.0.1')
           .config('spark.driver.bindAddress','127.0.0.1').config('spark.sql.session.timeZone','Asia/Singapore')
           .config('spark.sql.shuffle.partitions',str(c['shuffle_partitions']))
           .config('spark.sql.files.maxPartitionBytes',str(c['read_partition_bytes']))
           .config('spark.sql.adaptive.enabled','true').config('spark.sql.adaptive.coalescePartitions.enabled','false')
           .config('spark.local.dir',str(tmp)).config('spark.ui.enabled','false')
           .config('spark.ui.showConsoleProgress','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    assert spark.sparkContext._jvm.java.lang.System.getProperty('java.version').split('.')[0]=='17'
    return spark

def sampled(frame, fraction, seed):
    from pyspark.sql import functions as F
    return frame.filter(F.pmod(F.xxhash64(*KEYS,F.lit(seed)),F.lit(10000)) < int(fraction*10000))

def expand(frame, thresholds):
    from pyspark.sql import functions as F
    # Threshold literals are represented exactly as Decimal before comparison.
    data=frame.withColumn('threshold_mm',F.explode(F.array(*[F.lit(float(t)) for t in thresholds])))
    return data.withColumn('label',(F.col('future_30m_mm') > F.col('threshold_mm').cast('decimal(38,18)')).cast('double'))

def check_expansion(frame, anchors, thresholds, output):
    from pyspark.sql import functions as F
    n=frame.count()
    unique=frame.select(*KEYS,'threshold_mm').distinct().count()
    bad=frame.groupBy(*KEYS).agg(F.count('*').alias('n'),F.countDistinct('threshold_mm').alias('k')).filter(
        (F.col('n')!=len(thresholds))|(F.col('k')!=len(thresholds))).limit(1).count()
    nulls=frame.filter(F.col('label').isNull()).limit(1).count()
    assert n==unique==anchors*len(thresholds) and bad==nulls==0
    dump(output,{'status':'PASS','anchor_rows':anchors,'thresholds':thresholds,'expanded_rows':n,
                 'unique_three_column_keys':unique,'bad_threshold_multiplicity':bad,'null_labels':nulls})

def environment(spark):
    return {'python':platform.python_version(),'os':platform.platform(),'architecture':platform.machine(),
            'logical_cpu_count':os.cpu_count(),'pyspark':spark.version,
            'java':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
            'spark_master':spark.sparkContext.master,'peak_memory':'not measured'}
