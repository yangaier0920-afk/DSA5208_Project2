"""Validate the current release and execute a paired Spark read/write check."""
import argparse
import os
import platform
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from data_release import ROOT, load_release, read_json, read_tables, verify_release, write_json
from spark_session import start_spark, stop_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--member', choices=['A', 'B', 'C'], required=True)
    parser.add_argument('--scope', choices=['sample', 'full'], default='sample')
    parser.add_argument('--receipt', type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    receipt_path = args.receipt or root / f'outputs/p1_5_receipts/{args.member}_{args.scope}.json'
    if receipt_path.exists():
        raise RuntimeError('Receipt exists; supply a new --receipt path')
    receipt = {'status': 'RUNNING', 'member': args.member, 'scope': args.scope,
               'started_at': datetime.now(timezone.utc).isoformat(), 'os': platform.platform(),
               'architecture': platform.machine(), 'python': platform.python_version()}
    spark = None
    try:
        receipt.update(verify_release(root, args.scope))
        receipt['status'] = 'RUNNING'
        lock = read_json(root / 'configs/environment.lock.json')
        if f'{sys.version_info.major}.{sys.version_info.minor}' != lock['required_python_major_minor']:
            raise RuntimeError('Python minor must match environment.lock.json')
        if os.name == 'nt' and (ROOT / '.runtime/java_manifest.json').exists():
            from spark_runtime import configure_runtime
            configure_runtime()
        import pyspark
        import py4j
        from pyspark.sql import functions as F
        if pyspark.__version__ != lock['pyspark'] or py4j.__version__ != lock['py4j']:
            raise RuntimeError('PySpark/Py4J version mismatch')
        os.environ['SPARK_HOME'] = str(Path(pyspark.__file__).resolve().parent)
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        full = args.scope == 'full'
        spark, hooks = start_spark(
            'P1-5 release reader',
            threads=2,
            memory='4g' if full else '2g',
            partitions=256 if full else 8
        )
        if full:
            spark.conf.set(
                'spark.sql.adaptive.coalescePartitions.enabled', 'false'
            )
            spark.conf.set('spark.sql.files.maxPartitionBytes', '33554432')
        java = spark.sparkContext._jvm.java.lang.System.getProperty('java.version')
        if java.split('.')[0] != str(lock['required_java_major']):
            raise RuntimeError('Java major mismatch')
        frames = read_tables(spark, root, args.scope, verify=False)
        obs, tar = frames['observations'].cache(), frames['targets'].cache()
        manifest = load_release(root)
        expected_rows = (manifest['sample']['rows_per_table'] if args.scope == 'sample' else manifest['tables']['observations']['rows'])
        if obs.count() != expected_rows or tar.count() != expected_rows:
            raise RuntimeError('Table row counts differ')
        a = obs.select('station_id', F.col('slot_end_epoch_s').alias('prediction_epoch_s'))
        b = tar.select('station_id', 'prediction_epoch_s')
        if a.distinct().count() != expected_rows or b.distinct().count() != expected_rows or a.exceptAll(b).limit(1).count() or b.exceptAll(a).limit(1).count():
            raise RuntimeError('Primary keys or one-to-one pairing differ')
        if tar.filter((~F.col('target_valid') & F.col('future_30m_mm').isNotNull()) | (F.col('target_valid') & F.col('future_30m_mm').isNull())).limit(1).count():
            raise RuntimeError('Unknown future must remain null')
        if tar.filter((F.col('targets_version') != 'p0_4_v1') | (F.col('observations_version') != 'p0_3_v1')).limit(1).count():
            raise RuntimeError('Embedded table versions differ')
        if args.scope == 'sample':
            aggregates = [r.asDict() for r in tar.groupBy('year').agg(F.count('*').alias('rows'), F.sum(F.col('target_valid').cast('long')).alias('future_valid'), F.sum(F.col('historical_valid').cast('long')).alias('history_valid')).orderBy('year').collect()]
            sample = read_json(root / manifest['sample']['path'] / 'sample_manifest.json')
            if aggregates != sample['expected_by_year']:
                raise RuntimeError('Year aggregates differ')
        if spark.sparkContext.parallelize(range(20), 2).map(lambda x: x + 1).sum() != 210:
            raise RuntimeError('Python worker failed')
        # A small write/read also checks native filesystem support on each member host.
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='p1_5_', dir=receipt_path.parent) as temporary:
            path = Path(temporary) / 'roundtrip'
            fixture = tar.orderBy('station_id', 'prediction_epoch_s').limit(32)
            fixture.coalesce(1).write.parquet(path.as_posix())
            back = spark.read.parquet(path.as_posix())
            if fixture.exceptAll(back).count() or back.exceptAll(fixture).count():
                raise RuntimeError('Parquet roundtrip differs')
        receipt.update({'status': 'PASS', 'rows_per_table': expected_rows, 'java': java,
                        'pyspark': pyspark.__version__, 'py4j': py4j.__version__,
                        'timezone': spark.conf.get('spark.sql.session.timeZone'),
                        'checks': {'file_hashes': True, 'schemas': True, 'counts': True, 'unique_paired_keys': True, 'null_semantics': True, 'table_versions': True, 'python_worker': True, 'parquet_roundtrip': True}})
    except Exception as error:
        receipt.update({'status': 'FAIL', 'error': f'{type(error).__name__}: {error}'})
        raise
    finally:
        try:
            if spark is not None:
                receipt['shutdown'] = stop_spark(spark, hooks)
        except Exception as error:
            receipt.update({'status': 'FAIL', 'shutdown_error': str(error)})
            raise
        finally:
            receipt['finished_at'] = datetime.now(timezone.utc).isoformat()
            write_json(receipt_path, receipt)
    print(f'PASS: {args.scope}, {expected_rows:,} paired rows; receipt {receipt_path}')


if __name__ == '__main__':
    main()
