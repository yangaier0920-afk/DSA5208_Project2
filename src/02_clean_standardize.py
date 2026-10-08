"""P0-3: reproducible Spark cleaning, traceable collisions and verified Parquet."""
import argparse
import calendar
import csv
import hashlib
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from spark_session import start_spark, stop_spark

COLUMNS = ['date','timestamp','update_timestamp','station_id','station_name','station_device_id',
           'location_longitude','location_latitude','reading_update_timestamp','reading_value','reading_type','reading_unit']
PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX"
EXPECTED_TYPE = 'TB1 Rainfall 5 Minute Total F'
VERSION = 'p0_3_v1'
DECIMAL_SCALE = 18
KEY = ['station_id', 'slot_end_epoch_s']


def now():
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(8*1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False, default=str), encoding='utf-8')
    temp.replace(path)


def write_csv(path, rows, columns=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns or list(rows[0]) if rows else columns or [])
        writer.writeheader()
        writer.writerows(rows)


def collect(df):
    return [r.asDict(recursive=True) for r in df.collect()]


def parsed_lines(lines, year, source_file, source_sha256):
    """Keep original text, apply only approved time/rainfall transformations."""
    from pyspark.sql import functions as F, types as T
    schema = T.StructType([T.StructField(c,T.StringType()) for c in COLUMNS] +
                          [T.StructField('_corrupt_record',T.StringType())])
    options = {'mode':'PERMISSIVE', 'columnNameOfCorruptRecord':'_corrupt_record',
               'escape':'"', 'unescapedQuoteHandling':'RAISE_ERROR'}
    data = (lines.withColumn('field_count', F.size(F.split('raw_line', ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1)))
            .withColumn('odd_quotes', F.pmod(F.length('raw_line')-F.length(F.regexp_replace('raw_line','"','')),F.lit(2))!=0)
            .withColumn('parsed', F.from_csv('raw_line',schema.simpleString(),options))
            .select('raw_line','field_count','odd_quotes','parsed.*'))
    for name in COLUMNS:
        data = data.withColumnRenamed(name,'raw_'+name)
    data = (data.withColumn('source_year',F.lit(year)).withColumn('source_file',F.lit(source_file))
            .withColumn('source_sha256',F.lit(source_sha256)).withColumn('raw_record_sha256',F.sha2('raw_line',256))
            .withColumn('station_id',F.col('raw_station_id'))
            .withColumn('station_name',F.col('raw_station_name')).withColumn('station_device_id',F.col('raw_station_device_id'))
            .withColumn('event_ts',F.to_timestamp('raw_timestamp',PATTERN))
            .withColumn('update_ts',F.to_timestamp('raw_update_timestamp',PATTERN))
            .withColumn('reading_update_ts',F.to_timestamp('raw_reading_update_timestamp',PATTERN))
            .withColumn('rain_mm',F.col('raw_reading_value').cast('double'))
            .withColumn('rain_decimal_mm',F.col('raw_reading_value').cast(T.DecimalType(38,DECIMAL_SCALE)))
            .withColumn('longitude',F.col('raw_location_longitude').cast('double'))
            .withColumn('latitude',F.col('raw_location_latitude').cast('double'))
            .withColumn('event_epoch_s',F.col('event_ts').cast('long'))
            .withColumn('original_offset_s',F.pmod('event_epoch_s',F.lit(300))))
    shifted = (F.col('source_year')==2017)&(F.col('original_offset_s')==299)
    data = (data.withColumn('alignment_offset_s',F.when(shifted,1).otherwise(0))
            .withColumn('alignment_rule',F.when(shifted,'T2_PLUS_1_SECOND').otherwise('IDENTITY'))
            .withColumn('slot_end_epoch_s',F.col('event_epoch_s')+F.col('alignment_offset_s'))
            .withColumn('slot_end_ts',F.col('slot_end_epoch_s').cast('timestamp'))
            .withColumn('rain_period_start_ts',(F.col('slot_end_epoch_s')-300).cast('timestamp'))
            .withColumn('observation_date',F.to_date('slot_end_ts'))
            .withColumn('rain_period_date',F.to_date('rain_period_start_ts'))
            .withColumn('rain_period_hour',F.hour('rain_period_start_ts'))
            .withColumn('year',F.year('slot_end_ts')).withColumn('month',F.month('slot_end_ts'))
            .withColumn('update_delay_s',F.col('update_ts').cast('long')-F.col('event_epoch_s'))
            .withColumn('reading_update_delay_s',F.col('reading_update_ts').cast('long')-F.col('event_epoch_s')))
    def missing(c):
        return F.col(c).isNull() | (F.length(F.trim(F.col(c)))==0)
    def finite(c):
        return F.col(c).isNotNull() & ~F.isnan(c) & (F.abs(F.col(c)) != float('inf'))
    conditions = {
        'CSV_CORRUPT':F.col('_corrupt_record').isNotNull(),
        'CSV_FIELD_COUNT':F.col('field_count')!=12,
        'CSV_ODD_QUOTES':F.col('odd_quotes'),
        'MISSING_STATION_ID':missing('station_id'),
        'INVALID_EVENT_TIMESTAMP':F.col('event_ts').isNull(),
        'UNEXPECTED_TIMEZONE':~F.col('raw_timestamp').rlike(r'\+08:00$'),
        'SOURCE_YEAR_MISMATCH':F.year('event_ts')!=year,
        'RAW_DATE_MISMATCH':~F.col('raw_date').eqNullSafe(F.date_format('event_ts','yyyy-MM-dd')),
        'INVALID_RAINFALL':~finite('rain_mm'),
        'NEGATIVE_RAINFALL':finite('rain_mm')&(F.col('rain_mm')<0),
        'UNEXPECTED_UNIT':~F.col('raw_reading_unit').eqNullSafe('mm'),
        'UNEXPECTED_READING_TYPE':~F.col('raw_reading_type').eqNullSafe(EXPECTED_TYPE),
        'UNSUPPORTED_TIME_PHASE':F.pmod('slot_end_epoch_s',F.lit(300))!=0,
    }
    data = data.withColumn('quarantine_reasons',F.filter(F.array(*[
        F.when(F.coalesce(condition,F.lit(False)),F.lit(name)) for name,condition in conditions.items()]),lambda x:x.isNotNull()))
    data = data.withColumn('metadata_warning',~finite('longitude')|~finite('latitude')|
                           ~F.col('longitude').between(-180,180)|~F.col('latitude').between(-90,90)|
                           missing('station_name')|missing('station_device_id')|
                           F.col('update_ts').isNull()|F.col('reading_update_ts').isNull())
    # Validate decimal capacity from source text, including scientific notation;
    # valid rainfall must never be silently rounded or removed for this reason.
    text = F.trim(F.col('raw_reading_value'))
    frac = F.regexp_extract(text,r'^[+-]?(?:\d*)\.(\d*)',1)
    fraction_digits = F.length(F.regexp_replace(frac,'0+$',''))
    exponent_text = F.regexp_extract(text,r'[eE]([+-]?\d+)$',1)
    exponent = F.when(F.length(exponent_text)>0,exponent_text.cast('int')).otherwise(0)
    integer_digits = F.length(F.regexp_replace(F.regexp_extract(text,r'^[+-]?(\d*)',1),'^0+',''))
    numeric_syntax = text.rlike(r'^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$')
    data = (data.withColumn('source_effective_decimal_scale',F.greatest(fraction_digits-exponent,F.lit(0)))
            .withColumn('decimal_representation_error',finite('rain_mm')&(F.col('rain_mm')>=0)&(
                ~numeric_syntax|exponent.isNull()|(F.col('source_effective_decimal_scale')>DECIMAL_SCALE)|
                (integer_digits+exponent>38-DECIMAL_SCALE)|F.col('rain_decimal_mm').isNull())))
    return data.withColumn('rules_version',F.lit(VERSION))


def resolve_collisions(data):
    """Return observations, quarantine, two-sided collision ledger and key stats."""
    from pyspark.sql import functions as F, Window
    from pyspark import StorageLevel
    base = data.filter(F.size('quarantine_reasons')==0)
    stats = base.groupBy(*KEY).agg(F.count('*').alias('aligned_source_count'),
        F.countDistinct('rain_decimal_mm').alias('rain_variants'),
        F.countDistinct(F.struct('station_name','station_device_id','longitude','latitude')).alias('metadata_variants'))
    duplicates = stats.filter('aligned_source_count > 1').persist(StorageLevel.MEMORY_AND_DISK)
    duplicates.count()
    collision_rows = base.join(duplicates,KEY,'inner')
    order = Window.partitionBy(*KEY).orderBy(F.col('alignment_offset_s').asc(),F.col('raw_record_sha256').asc(),F.col('raw_line').asc())
    collision_rows = (collision_rows.withColumn('collision_rank',F.row_number().over(order))
        .withColumn('retained_record_sha256',F.first('raw_record_sha256').over(order.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))))
    equal = collision_rows.filter('rain_variants = 1')
    kept = equal.filter('collision_rank = 1')
    unique = (base.join(duplicates.select(*KEY),KEY,'left_anti')
        .withColumn('aligned_source_count',F.lit(1).cast('long')).withColumn('metadata_conflict',F.lit(False)))
    kept = kept.withColumn('metadata_conflict',F.col('metadata_variants')>1)
    obscols = data.columns+['aligned_source_count','metadata_conflict']
    observations = unique.select(*obscols).unionByName(kept.select(*obscols))
    invalid = (data.filter(F.size('quarantine_reasons')>0)
        .withColumn('aligned_source_count',F.lit(None).cast('long')).withColumn('metadata_conflict',F.lit(False)))
    conflicts = (collision_rows.filter('rain_variants > 1')
        .withColumn('quarantine_reasons',F.array(F.lit('ALIGNED_RAINFALL_CONFLICT')))
        .withColumn('metadata_conflict',F.col('metadata_variants')>1))
    quarantine = invalid.select(*obscols).unionByName(conflicts.select(*obscols))
    ledger = collision_rows.withColumn('disposition',
        F.when(F.col('rain_variants')>1,'QUARANTINED_CONFLICT').when(F.col('collision_rank')==1,'RETAINED').otherwise('MERGED_DUPLICATE'))
    return observations,quarantine,ledger,duplicates


def validate_observations(observations, quarantine, ledger, expected_rows):
    from pyspark.sql import functions as F
    metrics = observations.agg(F.count('*').alias('observations'),F.countDistinct(F.struct(*KEY)).alias('unique_keys'),
        F.sum((F.pmod('slot_end_epoch_s',F.lit(300))!=0).cast('long')).alias('off_grid'),
        F.sum((F.size('quarantine_reasons')>0).cast('long')).alias('invalid_retained'),
        F.sum((F.col('rain_decimal_mm').isNull()|(F.col('rain_decimal_mm')<0)).cast('long')).alias('invalid_decimal'),
        F.sum((F.col('slot_end_epoch_s')!=F.col('event_epoch_s')+F.col('alignment_offset_s')).cast('long')).alias('bad_alignment'),
        F.sum((F.col('rain_period_start_ts').cast('long')!=F.col('slot_end_epoch_s')-300).cast('long')).alias('bad_interval'),
        F.sum((F.col('year')!=F.year('slot_end_ts')).cast('long')).alias('bad_year'),
        F.sum((F.col('month')!=F.month('slot_end_ts')).cast('long')).alias('bad_month'),
        F.sum((F.col('rain_mm')==0).cast('long')).alias('zero_rainfall_rows'),
        F.sum((F.col('alignment_offset_s')==1).cast('long')).alias('aligned_retained_rows'),
        F.sum(F.col('metadata_conflict').cast('long')).alias('metadata_conflict_observations'),
        F.sum('rain_decimal_mm').alias('rain_sum_mm')).first().asDict()
    qcount=quarantine.count()
    dispositions={r['disposition']:r['count'] for r in collect(ledger.groupBy('disposition').count())}
    removed=dispositions.get('MERGED_DUPLICATE',0)
    checks={'count_balance':expected_rows==metrics['observations']+qcount+removed,
        'primary_key_unique':metrics['observations']==metrics['unique_keys'],
        'time_grid_valid':not metrics['off_grid'], 'no_invalid_measurements':not metrics['invalid_retained'],
        'decimal_valid':not metrics['invalid_decimal'], 'alignment_valid':not metrics['bad_alignment'],
        'interval_valid':not metrics['bad_interval'], 'partitions_valid':not metrics['bad_year'] and not metrics['bad_month']}
    assert all(checks.values()),checks
    return {'metrics':{**metrics,'quarantine_rows':qcount,'merged_duplicate_rows':removed,'ledger_dispositions':dispositions},'checks':checks}


def clean_year(spark, entry, out, evidence):
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    year=entry['year']; source=ROOT/entry['path']; started=time.monotonic()
    print(f'{now()} YEAR {year}: parse and standardize',flush=True)
    with source.open(encoding='utf-8-sig') as stream:
        header=stream.readline().rstrip('\r\n')
    assert next(csv.reader([header]))==COLUMNS
    raw=spark.read.text(source.as_posix()).withColumnRenamed('value','raw_line')
    physical=raw.agg(F.count('*').alias('lines'),F.sum((F.col('raw_line')==header).cast('long')).alias('headers')).first().asDict()
    assert physical['headers']==1 and physical['lines']-1==entry['physical_data_lines'],physical
    data=parsed_lines(raw.filter(F.col('raw_line')!=header),year,entry['path'],entry['sha256']).persist(StorageLevel.DISK_ONLY)
    profile=data.agg(F.count('*').alias('raw_rows'),F.sum(F.col('decimal_representation_error').cast('long')).alias('decimal_errors'),
        F.max(F.when(F.size('quarantine_reasons')==0,F.col('source_effective_decimal_scale'))).alias('max_source_decimal_scale'),
        F.sum((F.col('alignment_offset_s')==1).cast('long')).alias('shifted_source_rows'),
        F.sum((F.to_date('event_ts')!=F.to_date('slot_end_ts')).cast('long')).alias('alignment_date_changed_rows')).first().asDict()
    assert profile['raw_rows']==entry['physical_data_lines']
    if profile['decimal_errors']:
        raise RuntimeError('Source decimals exceed lossless storage capacity; stop without rounding. '+str(profile))
    print(f'{now()} YEAR {year}: {profile["raw_rows"]:,} rows; resolve aligned keys and save Parquet',flush=True)
    observations,quarantine,ledger,duplicates=resolve_collisions(data)
    duplicate_summary=collect(duplicates.orderBy(*KEY))
    obs_path=out/'observations'/f'year={year}'
    quarantine_path=out/'quarantine'/f'year={year}'
    ledger_path=out/'collision_lineage'/f'year={year}'
    # Completed-year checkpoints prevent accidental overwrites of published data.
    # An interrupted year is rebuilt in place only under an unchanged signature.
    observations.drop('year').repartition(24,'month').write.mode('overwrite').partitionBy('month').option('compression','snappy').parquet(obs_path.as_posix())
    quarantine.drop('year','month').coalesce(1).write.mode('overwrite').parquet(quarantine_path.as_posix())
    ledger.drop('year','month').coalesce(1).write.mode('overwrite').parquet(ledger_path.as_posix())
    metadata=(data.filter(F.size('quarantine_reasons')==0).groupBy('station_id','station_name','station_device_id','raw_location_longitude','raw_location_latitude')
        .agg(F.count('*').alias('raw_rows'),F.min('event_ts').alias('first_event_ts'),F.max('event_ts').alias('last_event_ts'))
        .withColumn('source_file',F.lit(entry['path'])))
    metadata.coalesce(1).write.mode('overwrite').parquet((out/'station_metadata_history'/f'year={year}').as_posix())
    print(f'{now()} YEAR {year}: re-read Parquet, unique keys and count balance',flush=True)
    reread=spark.read.parquet(obs_path.as_posix()).withColumn('year',F.lit(year))
    qread=spark.read.parquet(quarantine_path.as_posix()); lread=spark.read.parquet(ledger_path.as_posix())
    validation=validate_observations(reread,qread,lread,profile['raw_rows'])
    # Compare readback to pre-merge source rainfall accounting at full Decimal precision.
    raw_sum=data.agg(F.sum('rain_decimal_mm').alias('s')).first()['s']
    q_sum=qread.agg(F.sum('rain_decimal_mm').alias('s')).first()['s'] or 0
    removed_sum=lread.filter("disposition='MERGED_DUPLICATE'").agg(F.sum('rain_decimal_mm').alias('s')).first()['s'] or 0
    validation['checks']['rainfall_amount_balance']=raw_sum==validation['metrics']['rain_sum_mm']+q_sum+removed_sum
    assert validation['checks']['rainfall_amount_balance']
    monthly=collect(reread.groupBy('station_id','month').agg(F.count('*').alias('observed_slots'),
        F.sum('rain_decimal_mm').alias('observed_rain_mm'),F.min('slot_end_ts').alias('first_slot_end'),F.max('slot_end_ts').alias('last_slot_end')).orderBy('station_id','month'))
    stations=sorted({r['station_id'] for r in monthly}); lookup={(r['station_id'],r['month']):r for r in monthly}
    coverage=[]
    for station in stations:
        for month in range(1,13):
            r=lookup.get((station,month),{}); expected=calendar.monthrange(year,month)[1]*288; n=r.get('observed_slots',0)
            coverage.append({'year':year,'station_id':station,'month':month,'observed_slots':n,'calendar_slots':expected,
                'missing_calendar_slots':expected-n,'coverage':n/expected,'observed_rain_mm':r.get('observed_rain_mm',0),
                'first_slot_end':r.get('first_slot_end'),'last_slot_end':r.get('last_slot_end')})
    write_csv(evidence/str(year)/'station_month_coverage.csv',coverage)
    # Interval-calendar views can include 2016-12-31 or require next-year context;
    # retain these rows, and aggregate across all source years for Task1 later.
    periods=collect(reread.groupBy('station_id',F.year('rain_period_start_ts').alias('rain_year'),F.month('rain_period_start_ts').alias('rain_month'))
        .agg(F.count('*').alias('observed_slots'),F.sum('rain_decimal_mm').alias('observed_rain_mm')).orderBy('station_id','rain_year','rain_month'))
    write_csv(evidence/str(year)/'rain_period_month_contributions.csv',[{'source_year':year,**r} for r in periods])
    reasons=collect(qread.select(F.explode('quarantine_reasons').alias('reason')).groupBy('reason').count().orderBy('reason'))
    write_csv(evidence/str(year)/'quarantine_reason_counts.csv',reasons,['reason','count'])
    write_json(evidence/str(year)/'collision_groups.json',duplicate_summary)
    if duplicate_summary:
        export=lread.orderBy(*KEY,'collision_rank')
        write_csv(evidence/str(year)/'collision_lineage.csv',collect(export))
    write_json(evidence/'schemas'/'observations.json',reread.schema.jsonValue())
    write_json(evidence/'schemas'/'quarantine.json',qread.schema.jsonValue())
    write_json(evidence/'schemas'/'collision_lineage.json',lread.schema.jsonValue())
    result={'status':'PASS','year':year,'source':entry,'profile':profile,**validation,
            'duplicate_groups':len(duplicate_summary),'elapsed_seconds':round(time.monotonic()-started,2)}
    write_json(evidence/str(year)/'cleaning.json',result)
    duplicates.unpersist(); data.unpersist()
    print(f'{now()} YEAR {year}: PASS {validation["metrics"]["observations"]:,} observations',flush=True)
    return result


def self_test(spark,out,evidence):
    """Exercise actual Spark transforms and Parquet for consequential rules."""
    from pyspark.sql import functions as F
    rows=[]
    def row(station,time_,rain='0.0',name='Test',lon='103.8',unit='mm'):
        return ','.join(['2017-04-25','2017-04-25T'+time_+'+08:00','2017-04-25T12:00:00+08:00',station,name,station,lon,'1.3','2017-04-25T12:00:00+08:00',rain,EXPECTED_TYPE,unit])
    rows += [row('S1','11:19:59','1.4',name='Old',lon='103.7'),row('S1','11:20:00','1.400',name='New')]
    rows += [row('S2','11:19:59','0.2'),row('S2','11:20:00','0.4')]
    rows += [row('S0','11:25:00','0.0'),row('SGAP','11:20:00'),row('SGAP','11:30:00')]
    rows += [row('NEG','11:25:00','-0.2'),row('NAN','11:25:00','NaN'),row('INF','11:25:00','Infinity'),
             row('UNIT','11:25:00',unit='cm'),row('BADTIME','bad'),row('PHASE','11:25:17')]
    rows += [row('DEC','11:25:00','0.123456789123456789'),row('BIG','11:25:00','999.9'),row('EXP','11:25:00','2e-1')]
    midnight=row('MID','23:59:59','0.2')
    rows.append(midnight)
    rows.append(row('CSV','11:25:00')+',extra')
    data=parsed_lines(spark.createDataFrame([(r,) for r in rows],['raw_line']),2017,'synthetic.csv','synthetic').cache()
    assert data.filter('decimal_representation_error').count()==0
    obs,q,ledger,dups=resolve_collisions(data)
    obs.drop('year').write.mode('overwrite').partitionBy('month').parquet((out/'observations'/'year=2017').as_posix())
    q.write.mode('overwrite').parquet((out/'quarantine').as_posix())
    ledger.write.mode('overwrite').parquet((out/'collision_lineage').as_posix())
    saved=spark.read.parquet((out/'observations'/'year=2017').as_posix()).withColumn('year',F.lit(2017))
    checks=validate_observations(saved,spark.read.parquet((out/'quarantine').as_posix()),spark.read.parquet((out/'collision_lineage').as_posix()),len(rows))['checks']
    kept=saved.filter("station_id='S1'").first()
    checks.update({'standard_grid_row_preferred':kept['station_name']=='New' and kept['raw_reading_value']=='1.400',
        'metadata_conflict_flagged':kept['metadata_conflict'] and kept['aligned_source_count']==2,
        'both_collision_records_traceable':ledger.filter("station_id='S1'").count()==2,
        'unequal_rainfall_whole_group_quarantined':q.filter("station_id='S2'").count()==2 and obs.filter("station_id='S2'").count()==0,
        'zero_retained':saved.filter("station_id='S0' AND rain_mm=0").count()==1,
        'gap_not_filled':saved.filter("station_id='SGAP'").count()==2,
        'extreme_retained':saved.filter("station_id='BIG' AND rain_mm=999.9").count()==1,
        'decimal_lossless':str(saved.filter("station_id='DEC'").first()['rain_decimal_mm'])=='0.123456789123456789',
        'scientific_decimal_supported':str(saved.filter("station_id='EXP'").first()['rain_decimal_mm'])=='0.200000000000000000',
        'midnight_interval_previous_day':str(saved.filter("station_id='MID'").first()['observation_date'])=='2017-04-26' and str(saved.filter("station_id='MID'").first()['rain_period_date'])=='2017-04-25',
        'raw_time_preserved':kept['raw_timestamp']=='2017-04-25T11:20:00+08:00',
        'bad_values_quarantined':q.filter(F.col('station_id').isin('NEG','NAN','INF','UNIT','BADTIME','PHASE','CSV')).count()==7})
    extra=parsed_lines(spark.createDataFrame([(row('PREC','11:25:00','0.1234567891234567891'),)],['raw_line']),2017,'test','test')
    checks['precision_overflow_stops_before_rounding']=extra.filter('decimal_representation_error').count()==1
    assert all(checks.values()),checks
    write_json(evidence/'self_test.json',{'status':'PASS','checks':checks})
    dups.unpersist();data.unpersist()
    print(f'PASS: {len(checks)} synthetic cleaning/Parquet checks',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'data/processed'/VERSION)
    parser.add_argument('--evidence',type=Path,default=ROOT/'outputs/p0_3_clean'/VERSION)
    parser.add_argument('--self-test',action='store_true')
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args();out=args.output.resolve();evidence=args.evidence.resolve()
    # Only workspace-owned outputs may be overwritten by an interrupted-year resume.
    for target in [out,evidence]:
        if not target.is_relative_to(ROOT) or target in (ROOT,ROOT/'Raw_Dataset') or target.is_relative_to(ROOT/'Raw_Dataset'):
            raise RuntimeError('Unsafe output location: '+str(target))
    cfg=ROOT/'configs/project.json';manifest_path=ROOT/'manifests/raw_manifest.json'
    config=json.loads(cfg.read_text(encoding='utf-8'))
    expected={'T':'T2','C':'C1','L':'L1','D':'D1','M':'M1','H':'H1','R':'R1','S':'S1','B':'B1','K':'K1','A':'A1','E':'E2','P':'P1','V':'V1','I':'I1'}
    assert config['selected']==expected
    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
    validation=json.loads((ROOT/'outputs/p0_2_audit/p0_2_v1/validation.json').read_text(encoding='utf-8'))
    assert validation['status']=='PASS' and all(validation['checks'].values())
    signature={'version':VERSION,'code_sha256':digest(Path(__file__)),'config_sha256':digest(cfg),
        'manifest_sha256':digest(manifest_path),'session_helper_sha256':digest(ROOT/'scripts/spark_session.py'),
        'runtime_helper_sha256':digest(ROOT/'scripts/spark_runtime.py'),'output':out.as_posix(),
        'self_test':args.self_test}
    state_path=evidence/'run_status.json'
    if state_path.exists():
        previous=json.loads(state_path.read_text(encoding='utf-8'))
        if not args.resume or previous['signature']!=signature:
            raise RuntimeError('Output exists; use --resume with unchanged code/config, or a fresh output AND evidence folder')
    elif out.exists() and any(out.iterdir()):
        raise RuntimeError('Output exists without matching run state; choose a new version')
    sources=[]
    if not args.self_test:
        for entry in manifest['files']:
            path=ROOT/entry['path']
            print(f'{now()} Verify original SHA256: {entry["year"]}',flush=True)
            assert path.stat().st_size==entry['bytes'] and digest(path)==entry['sha256'],'Source changed: '+str(path)
            sources.append({**entry,'mtime_ns':path.stat().st_mtime_ns})
    state={'status':'RUNNING','started_at_sgt':now(),'signature':signature,'completed_years':[],'sources':sources}
    write_json(state_path,state)
    spark=None;hooks=[]
    try:
        spark,hooks=start_spark('P0-3-cleaning')
        if args.self_test:
            self_test(spark,out,evidence)
        else:
            annual=[]
            for entry in sources:
                checkpoint=evidence/str(entry['year'])/'cleaning.json'
                if args.resume and checkpoint.exists():
                    result=json.loads(checkpoint.read_text(encoding='utf-8'))
                    assert result['status']=='PASS' and result['source']==entry
                    print(f'{now()} YEAR {entry["year"]}: reuse verified checkpoint',flush=True)
                else:
                    result=clean_year(spark,entry,out,evidence)
                annual.append(result);state['completed_years'].append(entry['year']);write_json(state_path,state)
            total=lambda key:sum(r['metrics'][key] for r in annual)
            checks={'all_eight_years_pass':len(annual)==8 and all(all(r['checks'].values()) for r in annual),
                'global_count_balance':sum(r['profile']['raw_rows'] for r in annual)==total('observations')+total('quarantine_rows')+total('merged_duplicate_rows'),
                'year_partitions_disjoint':all(r['checks']['partitions_valid'] for r in annual),
                'expected_v1_count':total('observations')==48538627 and total('merged_duplicate_rows')==86 and total('quarantine_rows')==0,
                'sources_unchanged_during_run':all((ROOT/s['path']).stat().st_size==s['bytes'] and (ROOT/s['path']).stat().st_mtime_ns==s['mtime_ns'] for s in sources)}
            assert all(checks.values()),checks
            summary={'status':'PASS','version':VERSION,'annual_results':annual,'sources':sources,'signature':signature,
                'totals':{k:total(k) for k in ['observations','quarantine_rows','merged_duplicate_rows','zero_rainfall_rows','aligned_retained_rows','metadata_conflict_observations']},
                'raw_rows':sum(r['profile']['raw_rows'] for r in annual),'checks':checks,
                'decimal_storage':f'decimal(38,{DECIMAL_SCALE})','max_source_effective_decimal_scale':max(r['profile']['max_source_decimal_scale'] for r in annual),
                'scope':'P0-3 only; no labels, model training or Task1 analytical panels created'}
            write_json(evidence/'summary.json',summary)
        state['status']='PASS'
    except BaseException as exc:
        state.update(status='FAILED',error=repr(exc))
        raise
    finally:
        if spark is not None:
            try:
                state['shutdown']=stop_spark(spark,hooks)
            except BaseException as exc:
                state.update(status='FAILED',shutdown_error=repr(exc));write_json(state_path,state);raise
        state['finished_at_sgt']=now();write_json(state_path,state)


if __name__=='__main__':
    main()
