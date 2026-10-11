"""Causal Spark features at every observed anchor, with exact time windows."""
import argparse, json, math, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from b_common import PACKAGE, csv_write, dump, log, metadata, release, rows, start

SGT=timezone(timedelta(hours=8))

def epoch(year,month=1,day=1,hour=0,minute=0):
    return int(datetime(year,month,day,hour,minute,tzinfo=SGT).timestamp())

def make_features(obs, year):
    """Causal features. Run length and recency are bounded by a complete 60m history."""
    from pyspark.sql import functions as F, Window
    begin, end = epoch(year), epoch(year + 1)
    x = (obs.filter(F.col('year').isin(year - 1, year))
        .filter(F.col('slot_end_epoch_s').between(begin - 3300, end - 1))
        .select('station_id', F.col('slot_end_epoch_s').alias('prediction_epoch_s'),
            F.col('slot_end_ts').alias('prediction_ts'), F.col('rain_mm').alias('current_5m_mm')))
    base = Window.partitionBy('station_id').orderBy('prediction_epoch_s')
    for label, left, right, count in [
        ('15m', -600, 0, 3), ('30m', -1500, 0, 6),
        ('60m', -3300, 0, 12), ('previous_15m', -1500, -900, 3)
    ]:
        w = base.rangeBetween(left, right)
        x = x.withColumn('n_valid_' + label, F.count('current_5m_mm').over(w))
        x = x.withColumn('rain_sum_' + label + '_mm',
            F.when(F.col('n_valid_' + label) == count, F.sum('current_5m_mm').over(w)))
    w60 = base.rangeBetween(-3300, 0)
    complete = F.col('n_valid_60m') == 12
    x = (x.withColumn('history_complete_60m', complete)
        .withColumn('rain_max_60m_mm', F.when(complete, F.max('current_5m_mm').over(w60)))
        .withColumn('rain_std_60m_mm', F.when(complete, F.stddev_pop('current_5m_mm').over(w60)))
        .withColumn('wet_slots_60m', F.when(complete,
            F.sum((F.col('current_5m_mm') > 0).cast('int')).over(w60)))
        .withColumn('rain_change_15m_mm', F.col('rain_sum_15m_mm') - F.col('rain_sum_previous_15m_mm')))
    # Exact offsets, not preceding-row positions: gaps remain unknown.
    for minutes in [5, 10, 15, 30]:
        w = base.rangeBetween(-60 * minutes, -60 * minutes)
        x = x.withColumn(f'lag_{minutes}m_mm',
            F.when(F.count('current_5m_mm').over(w) == 1, F.sum('current_5m_mm').over(w)))
    x = x.withColumn('wet_current', (F.col('current_5m_mm') > 0).cast('int'))
    for minutes in [5, 10]:
        x = x.withColumn(f'wet_lag_{minutes}m', (F.col(f'lag_{minutes}m_mm') > 0).cast('int'))
    x = (x.withColumn('rain_change_5m_mm', F.col('current_5m_mm') - F.col('lag_5m_mm'))
        .withColumn('rain_ratio_15m_30m',
            F.when(F.col('n_valid_30m') == 6,
                F.when(F.col('rain_sum_30m_mm') > 0,
                    F.col('rain_sum_15m_mm') / F.col('rain_sum_30m_mm')).otherwise(F.lit(0.0)))))
    x = (x.withColumn('_last_dry_epoch', F.max(F.when(F.col('current_5m_mm') == 0,
            F.col('prediction_epoch_s'))).over(w60))
        .withColumn('_last_wet_epoch', F.max(F.when(F.col('current_5m_mm') > 0,
            F.col('prediction_epoch_s'))).over(w60)))
    x = (x.withColumn('consecutive_wet_slots_60m', F.when(complete,
            F.when(F.col('_last_dry_epoch').isNull(), F.lit(12))
             .otherwise(((F.col('prediction_epoch_s') - F.col('_last_dry_epoch')) / 300).cast('int'))))
        .withColumn('wet_run_left_censored_60m', F.when(complete,
            F.col('_last_dry_epoch').isNull().cast('int')))
        .withColumn('no_rain_observed_60m', F.when(complete,
            F.col('_last_wet_epoch').isNull().cast('int')))
        # Raw recency remains null when no wet slot is observed; the exact age is unknown.
        .withColumn('minutes_since_last_rain_60m', F.when(complete & F.col('_last_wet_epoch').isNotNull(),
            (F.col('prediction_epoch_s') - F.col('_last_wet_epoch')) / 60.0))
        # 60 is a documented censoring code, accompanied by no_rain_observed_60m.
        # It does not assert that the last rainfall ended exactly 60 minutes ago.
        .withColumn('minutes_since_last_rain_60m_capped', F.when(complete,
            F.coalesce(F.col('minutes_since_last_rain_60m'), F.lit(60.0))))
        .drop('_last_dry_epoch', '_last_wet_epoch'))
    clock = F.hour('prediction_ts') + F.minute('prediction_ts') / F.lit(60.0)
    for name, angle in [('hour', 2 * math.pi * clock / 24.0),
                        ('month', 2 * math.pi * (F.month('prediction_ts') - 1) / 12.0)]:
        x = x.withColumn(name + '_sin', F.sin(angle)).withColumn(name + '_cos', F.cos(angle))
    return x.filter(F.col('prediction_epoch_s').between(begin, end - 1))

def validate_probes(spark, obs, feature_root, out):
    """Independent Python lookup for boundary cases + seeded 20-station/400-anchor probes."""
    import hashlib, random
    from collections import defaultdict
    from pyspark.sql import functions as F
    seed = 20261011
    boundary_stations = ['S24', 'S77', 'S113']
    anchors = [epoch(y, m, d, h, mi) for y in range(2017, 2025)
        for m, d, h, mi in [(1,1,0,0),(1,1,0,30),(5,15,14,30),(9,30,23,55),(12,31,23,55)]]
    boundary_keys = {(s, t) for s in boundary_stations for t in anchors}
    station_ids = sorted(r['station_id'] for r in obs.groupBy('station_id').count().filter(F.col('count') >= 20).select('station_id').collect())
    stations = random.Random(seed).sample(station_ids, min(20, len(station_ids)))
    sampled = (obs.filter(F.col('station_id').isin(stations))
        .select('station_id', 'slot_end_epoch_s')
        .filter(F.pmod(F.xxhash64('station_id', 'slot_end_epoch_s', F.lit(seed)), F.lit(1000)) == 0)
        .collect())
    per_station = defaultdict(list)
    for r in sampled:
        per_station[r['station_id']].append(r['slot_end_epoch_s'])
    random_keys = set()
    for s in stations:
        times = sorted(per_station[s], key=lambda t: hashlib.sha256(f'{seed}|{s}|{t}'.encode()).digest())
        # Sparse stations may need an exact hash-ordered fallback to supply 20 anchors.
        if len(times) < 20:
            times = [r['slot_end_epoch_s'] for r in obs.filter(F.col('station_id') == s)
                .select('slot_end_epoch_s').orderBy(F.xxhash64('slot_end_epoch_s', F.lit(seed))).limit(20).collect()]
        random_keys.update((s, t) for t in times[:20])
    keys = sorted(boundary_keys | random_keys)
    key_frame = spark.createDataFrame(keys, 'station_id string, prediction_epoch_s long')
    needed = sorted({(s, t + offset) for s, t in keys for offset in range(-3300, 1, 300)})
    need_frame = spark.createDataFrame(needed, 'station_id string, slot_end_epoch_s long')
    source = obs.select('station_id', 'slot_end_epoch_s', 'rain_mm').join(
        F.broadcast(need_frame), ['station_id', 'slot_end_epoch_s'], 'inner')
    lookup = {(r['station_id'], r['slot_end_epoch_s']): r['rain_mm'] for r in source.collect()}
    features = spark.read.parquet(str(feature_root))
    probes = features.join(F.broadcast(key_frame), ['station_id', 'prediction_epoch_s'], 'inner').collect()
    assert len(random_keys) == 400, 'Expected 20 eligible stations with 20 distinct anchors each'
    assert len(probes) > 400, 'Expected boundary probes plus 400 random probes'
    def equal(actual, expected, label):
        assert (actual is None) == (expected is None), (label, actual, expected)
        if expected is not None:
            assert math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-8), (label, actual, expected)
    checked = []
    for row in probes:
        r = row.asDict(); s, t = r['station_id'], r['prediction_epoch_s']
        for label, offsets in [('15m', range(-600,1,300)), ('30m', range(-1500,1,300)),
                               ('60m', range(-3300,1,300)), ('previous_15m', range(-1500,-899,300))]:
            values = [lookup.get((s, t + offset)) for offset in offsets]
            known = [v for v in values if v is not None]
            assert r['n_valid_' + label] == len(known), (s, t, label)
            equal(r['rain_sum_' + label + '_mm'], sum(known) if len(known) == len(values) else None, label)
        values = [lookup.get((s, t + offset)) for offset in range(-3300,1,300)]
        complete = all(v is not None for v in values)
        assert r['history_complete_60m'] == complete
        for minutes in [5,10,15,30]:
            equal(r[f'lag_{minutes}m_mm'], lookup.get((s,t-minutes*60)), f'lag{minutes}')
        equal(r['wet_current'], int(lookup[(s,t)] > 0), 'wet_current')
        for minutes in [5,10]:
            v = lookup.get((s,t-minutes*60))
            equal(r[f'wet_lag_{minutes}m'], int(v > 0) if v is not None else None, 'wet_lag')
        lag = lookup.get((s,t-300))
        equal(r['rain_change_5m_mm'], lookup[(s,t)] - lag if lag is not None else None, 'change5')
        last30 = [lookup.get((s,t+o)) for o in range(-1500,1,300)]
        ratio = None
        if all(v is not None for v in last30):
            ratio = sum(last30[-3:])/sum(last30) if sum(last30) > 0 else 0.0
        equal(r['rain_ratio_15m_30m'], ratio, 'ratio')
        if complete:
            mean = sum(values)/12
            equal(r['rain_max_60m_mm'], max(values), 'max')
            equal(r['rain_std_60m_mm'], math.sqrt(sum((v-mean)**2 for v in values)/12), 'std')
            equal(r['wet_slots_60m'], sum(v > 0 for v in values), 'wet count')
            run = 0
            for v in reversed(values):
                if v <= 0: break
                run += 1
            ages = [i*5 for i,v in enumerate(reversed(values)) if v > 0]
            age = min(ages) if ages else None
            equal(r['consecutive_wet_slots_60m'], run, 'run')
            equal(r['wet_run_left_censored_60m'], int(run == 12), 'left censor')
            equal(r['no_rain_observed_60m'], int(not ages), 'no rain')
            equal(r['minutes_since_last_rain_60m'], age, 'raw age')
            equal(r['minutes_since_last_rain_60m_capped'], age if age is not None else 60, 'capped age')
        else:
            for column in ['rain_max_60m_mm','rain_std_60m_mm','wet_slots_60m','consecutive_wet_slots_60m',
                           'wet_run_left_censored_60m','no_rain_observed_60m',
                           'minutes_since_last_rain_60m','minutes_since_last_rain_60m_capped']:
                equal(r[column], None, column)
        checked.append({'station_id':s, 'prediction_epoch_s':t,
            'history_complete_60m':complete,
            'probe_type':'boundary' if (s,t) in boundary_keys else 'seeded_random', 'status':'PASS'})
    csv_write(out/'independent_timestamp_probes.csv', sorted(checked,key=lambda r:(r['station_id'],r['prediction_epoch_s'])))
    dump(out/'probe_design.json', {'seed':seed, 'random_stations':stations,
        'random_anchor_count':len(random_keys), 'boundary_keys_requested':len(boundary_keys),
        'actual_checked':len(probes), 'selection':'fixed seeded stations; stable hash sample and per-station hash order'})
    return len(probes)

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path)
    p.add_argument('--output',type=Path,default=PACKAGE/'results/features_v2')
    a=p.parse_args();root=a.root.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'result_metadata.json').exists():raise RuntimeError('Use a new output directory for a new run')
    begin=time.monotonic();d=release(root);log('Verifying full release before feature generation')
    verification=d.verify_release(root,'full');spark=start(root,'B causal rainfall features')
    try:
        from pyspark.sql import functions as F
        tables=d.read_tables(spark,root,'full',verify=False);obs=tables['observations']
        log('Reading reference yearly row counts and history-completeness counts')
        references={r['year']:r.asDict() for r in tables['targets'].groupBy('year').agg(
            F.count('*').alias('rows'),F.sum(F.col('historical_valid').cast('long')).alias('history_complete')).collect()}
        feature_root=out/'parquet';summaries=[]
        for year in range(2017,2025):
            log(f'{year}: computing time windows with previous-year context')
            f=make_features(obs,year)
            target=feature_root/f'year={year}'
            f.write.mode('errorifexists').parquet(str(target))
            log(f'{year}: checking saved feature counts and null semantics')
            saved=spark.read.parquet(str(target))
            expressions=[F.count('*').alias('rows'),F.sum(F.col('history_complete_60m').cast('long')).alias('history_complete')]
            for label,n in [('15m',3),('30m',6),('60m',12),('previous_15m',3)]:
                invalid=((F.col('n_valid_'+label)==n)!=F.col('rain_sum_'+label+'_mm').isNotNull())
                expressions.append(F.sum(invalid.cast('long')).alias('invalid_'+label))
            from B_feature_api import NUMERIC_FEATURES
            from functools import reduce
            any_model_null = reduce(lambda left,right:left|right,
                (F.col(c).isNull() for c in NUMERIC_FEATURES))
            expressions.append(F.sum((F.col('history_complete_60m') & any_model_null).cast('long')).alias('invalid_model_nulls_complete'))
            invalid_age = F.col('history_complete_60m') & (
                F.col('minutes_since_last_rain_60m').isNull() != (F.col('no_rain_observed_60m') == 1))
            expressions.append(F.sum(invalid_age.cast('long')).alias('invalid_recency_semantics'))
            stats=saved.agg(*expressions).first().asDict()
            assert stats['invalid_model_nulls_complete']==0 and stats['invalid_recency_semantics']==0, stats
            assert stats['rows']==references[year]['rows'],(year,stats,references[year])
            assert stats['history_complete']==references[year]['history_complete'],(year,stats,references[year])
            assert all(stats['invalid_'+label]==0 for label in ['15m','30m','60m','previous_15m']),stats
            summaries.append({'year':year,**stats,'status':'PASS'})
            csv_write(out/'yearly_validation.csv',summaries)
            dump(out/'feature_schema.json',saved.schema.jsonValue())
            log(f'{year}: PASS, {stats["rows"]:,} anchors')
        log('Validating independent timestamp probes, including year boundaries')
        n_probes=validate_probes(spark,obs,feature_root,out)
        columns=spark.read.parquet(str(feature_root)).columns
        prohibited=['future_30m_mm','target_valid','model_anchor_eligible','fold1_role','fold2_role','fold3_role','final_role','year_boundary_safe']
        assert not set(columns).intersection(prohibited)
        dump(out/'validation.json',{'status':'PASS','rows':sum(r['rows'] for r in summaries),
            'history_complete':sum(r['history_complete'] for r in summaries),'independent_probes':n_probes,
            'yearly_counts_match_A_targets':True,'window_null_semantics':True,'no_future_columns':True,
            'key_uniqueness':'preserved from verified observations; windows neither duplicate nor group anchors',
            'input_integrity':verification,'feature_set_version':'B_v2',
            'complete_history_model_inputs_nonnull':True,'recency_censor_semantics':True})
        metadata(root,out,'python src/B_build_features.py --root PROJECT_ROOT',time.monotonic()-begin, {'feature_set_version':'B_v2'})
        log('All historical features PASS')
    finally:spark.stop()

if __name__=='__main__':main()
