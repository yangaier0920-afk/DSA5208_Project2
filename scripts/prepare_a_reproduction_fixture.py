"""Freeze real source-record fragments and full-output references for A replay."""
import csv
from pathlib import Path
from data_release import ROOT, load_release, provenance, sha256, verify_release, write_json
from spark_session import start_spark, stop_spark


def main():
    from pyspark.sql import functions as F
    output = ROOT / 'data/samples/p1_6_v1'
    if output.exists():
        raise RuntimeError('Fixture exists; do not overwrite')
    verification = verify_release(ROOT, 'full')
    manifest = load_release()
    spark, hooks = start_spark('P1-6 real raw-record fixture', partitions=8)
    status = {'status': 'RUNNING', **provenance(), 'input_verification': verification}
    try:
        cases = list(csv.DictReader((ROOT / 'outputs/p0_4_targets/p0_4_v1/manual_recomputed_examples.csv').open(encoding='utf-8-sig')))
        lineage = spark.read.parquet((ROOT / manifest['tables']['collision_lineage']['path']).as_posix())
        collision_keys = [(r.station_id, r.slot_end_epoch_s) for r in lineage.select('station_id', 'slot_end_epoch_s').distinct().collect()]
        anchor_keys = sorted(set([(r['station_id'], int(r['prediction_epoch_s'])) for r in cases] + collision_keys))
        offsets = list(range(-3300, 1, 300)) + list(range(300, 1801, 300))
        context_keys = sorted({(station, epoch + off) for station, epoch in anchor_keys for off in offsets})
        key_frame = spark.createDataFrame(context_keys, 'station_id string, slot_end_epoch_s long')
        source = spark.read.parquet((ROOT / manifest['tables']['observations']['path']).as_posix())
        observations = source.join(F.broadcast(key_frame), ['station_id', 'slot_end_epoch_s'], 'left_semi').cache()
        anchors = spark.createDataFrame(anchor_keys, 'station_id string, prediction_epoch_s long')
        targets = spark.read.parquet((ROOT / manifest['tables']['targets']['path']).as_posix()).join(F.broadcast(anchors), ['station_id', 'prediction_epoch_s'], 'left_semi')
        if targets.count() != len(anchor_keys):
            raise RuntimeError('Fixture anchors must exist in the full reference')
        raw_columns = ['source_year', 'source_file', 'source_sha256', 'raw_record_sha256', 'raw_line']
        raw = observations.select(*raw_columns).unionByName(lineage.select(*raw_columns)).dropDuplicates(raw_columns)
        records = [r.asDict() for r in raw.orderBy('source_year', 'raw_line').collect()]
        annual = []
        header = 'date,timestamp,update_timestamp,station_id,station_name,station_device_id,location_longitude,location_latitude,reading_update_timestamp,reading_value,reading_type,reading_unit'
        for year in sorted({r['source_year'] for r in records}):
            selected = [r for r in records if r['source_year'] == year]
            identities = {(r['source_file'], r['source_sha256']) for r in selected}
            if len(identities) != 1:
                raise RuntimeError('Expected a single official source per year')
            source_file, source_hash = next(iter(identities))
            path = output / 'raw' / f'{year}.csv'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(header + '\n' + '\n'.join(r['raw_line'] for r in selected) + '\n', encoding='utf-8')
            annual.append({'year': year, 'path': path.relative_to(output).as_posix(), 'records': len(selected),
                           'source_file': source_file, 'source_sha256': source_hash,
                           'fixture_sha256': sha256(path)})
        for name, frame in [('observations', observations), ('targets', targets), ('collision_lineage', lineage)]:
            columns = [f['name'] for f in manifest['tables'][name]['schema']['fields']]
            frame.select(*columns).coalesce(1).write.mode('error').parquet((output / 'expected' / name).as_posix())
        write_json(output / 'fixture_manifest.json', {
            'fixture_version': 'p1_6_v1', **provenance(), 'timezone': 'Asia/Singapore',
            'scope': 'Real raw_line fragments preserved by full cleaning/lineage, not a fresh full CSV export or representative modelling sample',
            'raw_fragment_note': 'CSV headers/newlines reconstructed; each data record is the unchanged original raw_line. source_sha256 identifies the original annual file, fixture_sha256 identifies the fragment file.',
            'portable_payload_note': 'Only CSV and Parquet payloads; .crc and _SUCCESS are runtime sidecars, not portable data identity.',
            'context_rule': 'All actually present observations at the 12 historical and 6 future positions for every selected anchor; absent positions remain absent',
            'selection': 'The 29 existing 2017-2020 manual cases plus all 86 approved 2017 collision keys, using development data only',
            'cases': [{'case': r['case'], 'station_id': r['station_id'], 'prediction_epoch_s': int(r['prediction_epoch_s'])} for r in cases],
            'anchor_keys': [{'station_id': s, 'prediction_epoch_s': t} for s, t in anchor_keys],
            'raw_sources': annual, 'raw_records': len(records), 'observation_rows': observations.count(),
            'target_rows': len(anchor_keys), 'lineage_rows': lineage.count(),
            'payload_files': [{'path': p.relative_to(output).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in sorted(output.rglob('*')) if p.is_file() and p.suffix != '.crc' and p.name != '_SUCCESS']})
        status.update({'status': 'PASS', 'raw_records': len(records), 'anchors': len(anchor_keys)})
        print(f'Fixture: {len(records):,} raw records, {len(anchor_keys)} anchors', flush=True)
    except Exception as error:
        status.update({'status': 'FAIL', 'error': f'{type(error).__name__}: {error}'})
        raise
    finally:
        try:
            status['shutdown'] = stop_spark(spark, hooks)
        except Exception as error:
            status.update({'status': 'FAIL', 'shutdown_error': str(error)})
            raise
        finally:
            write_json(ROOT / 'outputs/p1_6_support/fixture_preparation.json', status)


if __name__ == '__main__':
    main()
