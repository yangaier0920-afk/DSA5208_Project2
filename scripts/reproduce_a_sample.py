"""Replay frozen production functions on genuine raw fragments in a new run."""
import argparse
import importlib.util
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
from data_release import ROOT, load_release, provenance, read_json, resolve_path, sha256, write_json
from spark_session import start_spark, stop_spark


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def same_rows(left, right):
    columns = right.columns
    return left.select(*columns).exceptAll(right).limit(1).count() == 0 and right.exceptAll(left.select(*columns)).limit(1).count() == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root, output = args.root.resolve(), args.output.resolve()
    if output.exists():
        raise RuntimeError('Output exists; select a new run directory')
    if output == root or root.is_relative_to(output):
        raise RuntimeError('Output must be a dedicated run directory')
    base = root / 'data/samples/p1_6_v1'
    fixture = read_json(base / 'fixture_manifest.json')
    manifest = load_release(root)
    for key, value in provenance(root, manifest).items():
        if fixture[key] != value:
            raise RuntimeError(f'Fixture belongs to another release: {key}')
    for item in fixture['payload_files']:
        path = resolve_path(base, item['path'])
        if path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
            raise RuntimeError(f'Fixture changed: {item["path"]}')
    frozen = {i['path']: i['sha256'] for i in manifest['files']}
    for name in ['src/02_clean_standardize.py', 'src/03_build_targets.py', 'scripts/spark_session.py', 'scripts/spark_runtime.py', 'configs/environment.lock.json']:
        if sha256(root / name) != frozen[name]:
            raise RuntimeError(f'Frozen production code/config changed: {name}')
    cleaner = load_module('production_cleaner', root / 'src/02_clean_standardize.py')
    target_builder = load_module('production_targets', root / 'src/03_build_targets.py')
    lock = read_json(root / 'configs/environment.lock.json')
    if f'{sys.version_info.major}.{sys.version_info.minor}' != lock['required_python_major_minor']:
        raise RuntimeError('Python must match the locked minor version')
    if os.name == 'nt' and (ROOT / '.runtime/java_manifest.json').exists():
        from spark_runtime import configure_runtime
        configure_runtime()
    import pyspark
    import py4j
    from pyspark.sql import functions as F
    if pyspark.__version__ != lock['pyspark'] or py4j.__version__ != lock['py4j']:
        raise RuntimeError('PySpark/Py4J must match the lock')
    os.environ['SPARK_HOME'] = str(Path(pyspark.__file__).resolve().parent)
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    output.mkdir(parents=True)
    started = time.monotonic()
    receipt = {'status': 'RUNNING', **provenance(root), 'scope': 'A_local_real_raw_fragment_replay_no_training',
               'started_at': datetime.now(timezone.utc).isoformat(), 'fixture_sha256': sha256(base / 'fixture_manifest.json'),
               'output': str(output), 'checks': {}, 'environment': {'os': platform.platform(), 'architecture': platform.machine(),
               'python': platform.python_version(), 'pyspark': pyspark.__version__, 'py4j': py4j.__version__},
               'memory_note': 'Driver heap limit is a configuration; full-run peak RSS was not measured.'}
    spark = None
    try:
        spark, hooks = start_spark('P1-6 representative raw-to-target replay', threads=2, memory='2g', partitions=8)
        java = spark.sparkContext._jvm.java.lang.System.getProperty('java.version')
        if java.split('.')[0] != str(lock['required_java_major']):
            raise RuntimeError('Java major must match lock')
        receipt['environment'].update({'java': java, 'master': spark.sparkContext.master,
            'driver_memory': spark.sparkContext.getConf().get('spark.driver.memory'),
            'shuffle_partitions': int(spark.conf.get('spark.sql.shuffle.partitions')),
            'timezone': spark.conf.get('spark.sql.session.timeZone'),
            'jvm_max_heap_bytes': spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()})
        observations, lineages = [], []
        raw_count = 0
        quarantined = 0
        merged = 0
        for entry in fixture['raw_sources']:
            path = base / entry['path']
            header = path.read_text(encoding='utf-8').splitlines()[0]
            lines = spark.read.text(path.as_posix()).withColumnRenamed('value', 'raw_line').filter(F.col('raw_line') != header)
            if lines.count() != entry['records']:
                raise RuntimeError('Raw fragment record count differs')
            parsed = cleaner.parsed_lines(lines, entry['year'], entry['source_file'], entry['source_sha256']).cache()
            if parsed.filter('decimal_representation_error').limit(1).count():
                raise RuntimeError('Lossless Decimal representation failed')
            obs, quarantine, ledger, duplicate_groups = cleaner.resolve_collisions(parsed)
            check = cleaner.validate_observations(obs, quarantine, ledger, entry['records'])
            raw_count += entry['records']
            quarantined += check['metrics']['quarantine_rows']
            merged += check['metrics']['merged_duplicate_rows']
            observations.append(obs)
            lineages.append(ledger)
            write_json(output / f'cleaning_{entry["year"]}.json', json.loads(json.dumps(check, default=str)))
        observations_all = observations[0]
        lineage_all = lineages[0]
        for frame in observations[1:]:
            observations_all = observations_all.unionByName(frame)
        for frame in lineages[1:]:
            lineage_all = lineage_all.unionByName(frame)
        observations_all = observations_all.cache()
        obs_columns = [f['name'] for f in manifest['tables']['observations']['schema']['fields']]
        observations_all.select(*obs_columns).coalesce(1).write.parquet((output / 'observations').as_posix())
        saved_obs = spark.read.parquet((output / 'observations').as_posix()).cache()
        expected_obs = spark.read.parquet((base / 'expected/observations').as_posix())
        if not same_rows(saved_obs, expected_obs):
            raise RuntimeError('Replayed observations differ from the full-data reference')
        expected_lineage = spark.read.parquet((base / 'expected/collision_lineage').as_posix())
        if not same_rows(lineage_all, expected_lineage):
            raise RuntimeError('Replayed collision lineage differs')
        keys = spark.createDataFrame([(r['station_id'], r['prediction_epoch_s']) for r in fixture['anchor_keys']], 'station_id string, prediction_epoch_s long')
        # Compute on context first, then keep selected anchors.
        targets = target_builder.build_targets(saved_obs).join(F.broadcast(keys), ['station_id', 'prediction_epoch_s'], 'left_semi')
        target_columns = [f['name'] for f in manifest['tables']['targets']['schema']['fields']]
        targets.select(*target_columns).coalesce(1).write.parquet((output / 'targets').as_posix())
        saved_targets = spark.read.parquet((output / 'targets').as_posix())
        expected_targets = spark.read.parquet((base / 'expected/targets').as_posix())
        if not same_rows(saved_targets, expected_targets):
            raise RuntimeError('Replayed targets differ from full-context targets')
        target_metrics, target_checks = target_builder.metrics_and_checks(saved_targets, fixture['target_rows'])
        # Independent timestamp-key Decimal oracle for every retained replay anchor.
        lookup = {(r.station_id, r.slot_end_epoch_s): r.rain_decimal_mm for r in saved_obs.select('station_id', 'slot_end_epoch_s', 'rain_decimal_mm').collect()}
        oracle = []
        with localcontext() as context:
            context.prec = 50
            for row in saved_targets.collect():
                future_missing = [off for off in target_builder.FUTURE if (row.station_id, row.prediction_epoch_s + off) not in lookup]
                history_missing = [off for off in target_builder.HISTORY if (row.station_id, row.prediction_epoch_s + off) not in lookup]
                total = None if future_missing else sum((lookup[(row.station_id, row.prediction_epoch_s + off)] for off in target_builder.FUTURE), Decimal(0))
                passed = total == row.future_30m_mm and future_missing == row.future_missing_offsets_s and history_missing == row.historical_missing_offsets_s
                if not passed:
                    raise RuntimeError('Independent Decimal oracle differs')
                oracle.append({'station_id': row.station_id, 'prediction_epoch_s': row.prediction_epoch_s,
                               'future_30m_mm': str(total) if total is not None else None, 'passed': passed})
        write_json(output / 'independent_recomputation.json', oracle)
        obs_count = saved_obs.count()
        checks = {'fixture_and_production_hashes_match': True, 'real_raw_record_count_matches': raw_count == fixture['raw_records'],
                  'cleaning_count_ledger_closes': raw_count == obs_count + quarantined + merged,
                  'observations_match_full_reference_all_columns': True, 'collision_lineage_matches_full_reference_all_columns': True,
                  'targets_match_full_reference_all_columns': True, 'all_target_structural_checks': all(target_checks.values()),
                  'all_anchor_decimal_oracles_pass': len(oracle) == fixture['target_rows'],
                  'all_86_collision_merges_reproduced': merged == 86,
                  'two_metadata_conflicts_preserved': saved_obs.filter('metadata_conflict').count() == 2,
                  'python_worker_pass': spark.sparkContext.parallelize(range(20), 2).map(lambda x: x + 1).sum() == 210}
        if not all(checks.values()):
            raise RuntimeError(f'Replay check failed: {checks}')
        receipt.update({'status': 'PASS', 'checks': checks,
                        'counts': {'raw_records': raw_count, 'observations': obs_count, 'quarantine': quarantined, 'merged_duplicate_rows': merged, 'target_anchors': len(oracle)},
                        'target_metrics': target_metrics, 'target_checks': target_checks})
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
            receipt['elapsed_seconds'] = round(time.monotonic() - started, 3)
            receipt['finished_at'] = datetime.now(timezone.utc).isoformat()
            write_json(output / 'verification.json', receipt)
    print(f'PASS: raw-to-target replay; {raw_count:,} raw rows, {len(oracle)} anchors; {receipt["elapsed_seconds"]} seconds')


if __name__ == '__main__':
    main()
