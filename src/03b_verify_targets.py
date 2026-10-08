"""Verify combined P0-4 targets and exact full-data observation-key matching."""
import argparse
import importlib.util
import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from spark_session import start_spark,stop_spark
spec=importlib.util.spec_from_file_location('target_builder',ROOT/'src/03_build_targets.py')
builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'data/processed/p0_4_v1')
    parser.add_argument('--evidence',type=Path,default=ROOT/'outputs/p0_4_targets/p0_4_v1')
    args=parser.parse_args();out=args.output.resolve();evidence=args.evidence.resolve()
    summary=json.loads((evidence/'summary.json').read_text(encoding='utf-8'))
    state=json.loads((evidence/'run_status.json').read_text(encoding='utf-8'))
    assert summary['status']==state['status']=='PASS' and state['shutdown']['gateway_exit_code']==0
    assert state['signature']['output']==out.as_posix() and state['signature']['code_sha256']==builder.digest(ROOT/'src/03_build_targets.py')
    from pyspark.sql import functions as F,types as T
    spark,hooks=start_spark('P0-4-combined-target-verification')
    result={'status':'RUNNING','verifier_sha256':builder.digest(Path(__file__)),'builder_signature':state['signature']}
    try:
        target=spark.read.parquet((out/'targets').as_posix())
        m,checks=builder.metrics_and_checks(target,summary['totals']['anchors'])
        by_year={r['year']:r['count'] for r in target.groupBy('year').count().collect()}
        checks['annual_counts_match']=by_year=={r['year']:r['metrics']['anchors'] for r in summary['annual_results']}
        checks['decimal_truth_schema']=target.schema['future_30m_mm'].dataType==T.DecimalType(38,18)
        checks['all_91_stations']=target.select('station_id').distinct().count()==91
        obs=spark.read.parquet(state['signature']['source']).select('station_id',F.col('slot_end_epoch_s').alias('epoch'),F.lit(1).alias('has_obs'))
        keys=target.select('station_id',F.col('prediction_epoch_s').alias('epoch'),F.lit(1).alias('has_target'))
        match=keys.join(obs,['station_id','epoch'],'full_outer').agg(F.count('*').alias('joined_rows'),
            F.sum(F.col('has_obs').isNull().cast('long')).alias('target_without_observation'),
            F.sum(F.col('has_target').isNull().cast('long')).alias('observation_without_target')).first().asDict()
        checks['exact_observation_keys_match']=match['joined_rows']==m['anchors'] and match['target_without_observation']==match['observation_without_target']==0
        # Test the exact Spark expression C will use, without choosing thresholds
        # or inspecting any validation/test label distribution.
        demo=spark.createDataFrame([(Decimal('0.3'),),(None,),(Decimal('0.300000000000000001'),)],
            T.StructType([T.StructField('truth',T.DecimalType(38,18))]))
        labels=[r['label'] for r in demo.select((F.col('truth')>F.lit(Decimal('0.3')).cast('decimal(38,18)')).cast('double').alias('label')).collect()]
        checks['spark_strict_threshold_and_unknown_label']=labels==[0.0,None,1.0]
        assert all(checks.values()),checks
        builder.write_json(evidence/'schemas'/'root_targets.json',target.schema.jsonValue())
        result.update(status='PASS',checks=checks,metrics=m,by_year=by_year,key_match=match)
        print(f'PASS: {len(checks)} combined target checks; exact keys match all {m["anchors"]:,} observations',flush=True)
    except BaseException as exc:
        result.update(status='FAILED',error=repr(exc));raise
    finally:
        try:
            result['shutdown']=stop_spark(spark,hooks)
        except BaseException as exc:
            result.update(status='FAILED',shutdown_error=repr(exc));raise
        finally:
            builder.write_json(evidence/'parquet_readback.json',result)


if __name__=='__main__':
    main()
