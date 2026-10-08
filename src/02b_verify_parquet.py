"""Verify the combined Parquet interfaces consumed by B/C, after all years pass."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from spark_session import start_spark,stop_spark


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'data/processed/p0_3_v1')
    parser.add_argument('--evidence',type=Path,default=ROOT/'outputs/p0_3_clean/p0_3_v1')
    args=parser.parse_args();out=args.output.resolve();evidence=args.evidence.resolve()
    summary=json.loads((evidence/'summary.json').read_text(encoding='utf-8'))
    state=json.loads((evidence/'run_status.json').read_text(encoding='utf-8'))
    assert summary['status']==state['status']=='PASS' and state['shutdown']['gateway_exit_code']==0
    assert state['signature']['output']==out.as_posix()
    from pyspark.sql import functions as F,types as T
    spark,hooks=start_spark('P0-3-combined-Parquet-verification')
    result={'status':'RUNNING','verifier_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'cleaning_signature':state['signature']}
    try:
        obs=spark.read.parquet((out/'observations').as_posix())
        q=spark.read.parquet((out/'quarantine').as_posix())
        ledger=spark.read.parquet((out/'collision_lineage').as_posix())
        metadata=spark.read.parquet((out/'station_metadata_history').as_posix())
        (evidence/'schemas'/'root_interfaces.json').write_text(json.dumps({
            'observations':obs.schema.jsonValue(),'quarantine':q.schema.jsonValue(),
            'collision_lineage':ledger.schema.jsonValue(),'station_metadata_history':metadata.schema.jsonValue()},
            ensure_ascii=False,indent=2),encoding='utf-8')
        by_year={r['year']:r['count'] for r in obs.groupBy('year').count().collect()}
        metrics=obs.agg(F.count('*').alias('rows'),F.countDistinct('station_id').alias('stations'),
            F.sum(((F.col('year')!=F.year('slot_end_ts'))|(F.col('month')!=F.month('slot_end_ts'))).cast('long')).alias('bad_partition'),
            F.sum((~F.col('station_id').eqNullSafe(F.col('raw_station_id'))).cast('long')).alias('changed_station_id'),
            F.sum((F.to_timestamp('raw_timestamp',"yyyy-MM-dd'T'HH:mm:ssXXX")!=F.col('event_ts')).cast('long')).alias('changed_original_time'),
            F.sum((F.pmod('slot_end_epoch_s',F.lit(300))!=0).cast('long')).alias('bad_grid')).first().asDict()
        qcount=q.count()
        ledger_counts={r['disposition']:r['count'] for r in ledger.groupBy('disposition').count().collect()}
        meta_rows=metadata.agg(F.sum('raw_rows').alias('n')).first()['n']
        checks={'all_year_counts_match':by_year=={r['year']:r['metrics']['observations'] for r in summary['annual_results']},
            'total_count_matches':metrics['rows']==summary['totals']['observations'],
            'all_91_stations_retained':metrics['stations']==91,
            'root_partitions_consistent':not metrics['bad_partition'],
            'original_station_ids_preserved':not metrics['changed_station_id'],
            'original_timestamps_preserved':not metrics['changed_original_time'],
            'standard_time_grid':not metrics['bad_grid'],
            'decimal_schema':obs.schema['rain_decimal_mm'].dataType==T.DecimalType(38,18),
            'empty_quarantine_readable':qcount==summary['totals']['quarantine_rows']==0 and 'quarantine_reasons' in q.columns,
            'two_sided_collision_ledger':ledger_counts=={'RETAINED':86,'MERGED_DUPLICATE':86},
            'metadata_history_includes_premerge_rows':meta_rows==summary['raw_rows']}
        assert all(checks.values()),checks
        result.update(status='PASS',checks=checks,metrics=metrics,by_year=by_year,
                      quarantine_rows=qcount,ledger_dispositions=ledger_counts,metadata_raw_rows=meta_rows)
        print(f'PASS: {len(checks)} combined-Parquet checks; {metrics["rows"]:,} observations',flush=True)
    except BaseException as exc:
        result.update(status='FAILED',error=repr(exc));raise
    finally:
        try:
            result['shutdown']=stop_spark(spark,hooks)
        except BaseException as exc:
            result.update(status='FAILED',shutdown_error=repr(exc));raise
        finally:
            (evidence/'parquet_readback.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':
    main()
