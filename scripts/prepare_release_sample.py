"""Extract paired interface fixtures from frozen full tables, without cleaning again."""
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from data_release import ROOT, read_json, sha256, write_json
from spark_session import start_spark, stop_spark


def main():
    from pyspark.sql import functions as F
    output = ROOT / 'data/samples/p1_5_v1'
    if output.exists():
        raise RuntimeError('Sample version already exists; use a new version')
    spark, hooks = start_spark('P1-5 paired interface sample', partitions=8)
    try:
        source = ROOT / 'data/processed/p0_3_v1/observations'
        frames = []
        # Fixed station and chronological prefix; not selected using rainfall/labels.
        for year in range(2017, 2025):
            january = spark.read.option('basePath', source.as_posix()).parquet((source / f'year={year}/month=1').as_posix())
            frames.append(january.filter(F.col('station_id') == 'S08').orderBy('slot_end_epoch_s').limit(96))
        examples = list(csv.DictReader((ROOT / 'outputs/p0_4_targets/p0_4_v1/manual_recomputed_examples.csv').open(encoding='utf-8-sig')))
        conditions = F.lit(False)
        for row in examples:
            epoch = int(row['prediction_epoch_s'])
            conditions = conditions | ((F.col('station_id') == row['station_id']) & F.col('slot_end_epoch_s').between(epoch - 3600, epoch + 3600))
        frames.append(spark.read.parquet(source.as_posix()).filter((F.col('year') <= 2020) & conditions))
        observations = frames[0]
        for frame in frames[1:]:
            observations = observations.unionByName(frame)
        observations = observations.dropDuplicates(['station_id', 'slot_end_epoch_s']).cache()
        keys = observations.select('station_id', F.col('slot_end_epoch_s').alias('prediction_epoch_s'))
        targets = spark.read.parquet((ROOT / 'data/processed/p0_4_v1/targets').as_posix()).join(F.broadcast(keys), ['station_id', 'prediction_epoch_s'], 'left_semi')
        # Align column order with the frozen root schema (joins may reorder key columns).
        schemas = read_json(ROOT / 'outputs/p0_3_clean/p0_3_v1/schemas/root_interfaces.json')
        schemas['targets'] = read_json(ROOT / 'outputs/p0_4_targets/p0_4_v1/schemas/root_targets.json')
        for name, frame in [('observations', observations), ('targets', targets)]:
            frame.select(*[f['name'] for f in schemas[name]['fields']]).coalesce(1).write.mode('error').parquet((output / name).as_posix())
        obs = spark.read.parquet((output / 'observations').as_posix())
        tar = spark.read.parquet((output / 'targets').as_posix())
        rows = obs.count()
        if tar.count() != rows or rows == 0:
            raise RuntimeError('Sample pairing failed')
        aggregates = [r.asDict() for r in tar.groupBy('year').agg(F.count('*').alias('rows'), F.sum(F.col('target_valid').cast('long')).alias('future_valid'), F.sum(F.col('historical_valid').cast('long')).alias('history_valid')).orderBy('year').collect()]
        if [r['year'] for r in aggregates] != list(range(2017, 2025)):
            raise RuntimeError('Sample must include all eight years')
        write_json(output / 'sample_manifest.json', {
            'sample_version': 'p1_5_v1', 'rows_per_table': rows,
            'selection': 'S08 first 96 January observations/year plus +/-60min around the 29 pre-existing 2017-2020 manual cases; deduplicated keys',
            'purpose': 'Interface/read-write fixtures only; not a representative training or evaluation dataset',
            'context_warning': 'Targets were calculated from full observations; sample is sparse and does not guarantee complete context for every sample anchor. Do not rebuild production labels from it.',
            'expected_by_year': aggregates,
            'tables': {name: {'version': 'p0_3_v1' if name == 'observations' else 'p0_4_v1', 'schema': schemas[name]} for name in ['observations', 'targets']},
            'payload_files': [{'path': p.relative_to(output).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in sorted(output.rglob('*.parquet'))]})
        print(f'Paired sample: {rows:,} rows per table', flush=True)
    finally:
        shutdown = stop_spark(spark, hooks)
        if (output / 'sample_manifest.json').exists():
            write_json(ROOT / 'outputs/p1_5_release/sample_preparation.json', {'status': 'PASS', 'shutdown': shutdown, 'sample': 'data/samples/p1_5_v1'})


if __name__ == '__main__':
    main()
