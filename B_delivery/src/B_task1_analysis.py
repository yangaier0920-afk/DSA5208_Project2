"""Task 1 Spark aggregations; no raw data collected into Python."""
import argparse, calendar, json, sys, time
from decimal import Decimal
from pathlib import Path
from b_common import PACKAGE, csv_write, dump, log, metadata, release, rows, start

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True,type=Path)
    p.add_argument('--output',type=Path,default=PACKAGE/'results/task1_v2')
    a=p.parse_args(); root=a.root.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'result_metadata.json').exists():raise RuntimeError('Use a new output directory for a new run')
    begin=time.monotonic(); d=release(root)
    log('Verifying frozen full release (all hashes)'); verification=d.verify_release(root,'full')
    spark=start(root,'B Task 1 rainfall patterns')
    try:
        from pyspark.sql import functions as F
        obs=d.read_tables(spark,root,'full',verify=False)['observations'].select(
            'station_id','rain_period_start_ts','rain_mm','rain_decimal_mm')
        x=(obs.withColumn('rain_year',F.year('rain_period_start_ts'))
            .withColumn('rain_month',F.month('rain_period_start_ts'))
            .withColumn('rain_hour',F.hour('rain_period_start_ts'))
            .filter(F.col('rain_year').between(2017,2024)))
        log('Aggregating station-month rainfall and nonzero intensity percentiles')
        monthly=(x.groupBy('rain_year','rain_month','station_id').agg(
            F.count('*').alias('observed_slots'),F.sum('rain_decimal_mm').alias('observed_rain_mm'),
            F.sum((F.col('rain_mm')>0).cast('long')).alias('wet_slots'),F.max('rain_mm').alias('max_5m_mm'),
            F.expr('percentile_approx(CASE WHEN rain_mm > 0 THEN rain_mm END, array(0.5,0.9,0.99), 1000)').alias('wet_quantiles')))
        monthly_rows=[]
        for r in monthly.toLocalIterator():
            q=r['wet_quantiles'] or [None,None,None]
            monthly_rows.append({'year':r['rain_year'],'month':r['rain_month'],'station_id':r['station_id'],
                'observed_slots':r['observed_slots'],'observed_rain_mm':str(r['observed_rain_mm']),
                'wet_slots':r['wet_slots'],'max_5m_mm':r['max_5m_mm'],
                'wet_p50_5m_mm':q[0],'wet_p90_5m_mm':q[1],'wet_p99_5m_mm':q[2]})
        reference=root/'outputs/p0_3_clean/p0_3_v1'
        coverage=rows(reference/'station_month_rain_period_coverage.csv')
        lookup={(r['year'],r['station_id'],r['month']):r for r in monthly_rows}
        checked=0
        for r in coverage:
            key=(int(r['year']),r['station_id'],int(r['month'])); actual=lookup.get(key)
            assert (actual['observed_slots'] if actual else 0)==int(r['observed_slots']),key
            if actual:
                assert abs(Decimal(actual['observed_rain_mm'])-Decimal(r['observed_rain_mm']))<Decimal('0.0000001'),key
                actual['calendar_slots']=int(r['calendar_slots']);actual['coverage']=float(r['coverage'])
                actual['wet_fraction_observed']=actual['wet_slots']/actual['observed_slots']
                actual['mean_5m_mm_observed']=float(actual['observed_rain_mm'])/actual['observed_slots']
            checked+=1
        assert len(lookup)==sum(int(r['observed_slots'])>0 for r in coverage)
        monthly_rows.sort(key=lambda r:(r['year'],r['month'],r['station_id']))
        csv_write(out/'station_month_metrics.csv',monthly_rows)
        csv_write(out/'coverage_calendar.csv',coverage)
        panel=rows(reference/'task1_comparable_month_station_panel.csv')
        csv_write(out/'strict_panel_membership.csv',panel)
        log('Aggregating station-month-hour patterns; collecting summaries only')
        hourly=x.groupBy('rain_year','rain_month','rain_hour','station_id').agg(
            F.count('*').alias('observed_slots'),F.sum('rain_decimal_mm').alias('observed_rain_mm'),
            F.sum((F.col('rain_mm')>0).cast('long')).alias('wet_slots'))
        def hourly_records():
            for r in hourly.toLocalIterator():
                yield {'year':r['rain_year'],'month':r['rain_month'],'hour':r['rain_hour'],'station_id':r['station_id'],
                    'observed_slots':r['observed_slots'],'observed_rain_mm':str(r['observed_rain_mm']),'wet_slots':r['wet_slots']}
        csv_write(out/'station_month_hour_metrics.csv',hourly_records(),
            ['year','month','hour','station_id','observed_slots','observed_rain_mm','wet_slots'])
        log('Measuring network dates and individual missing-day coverage')
        daily=x.groupBy(F.to_date('rain_period_start_ts').alias('rain_date')).agg(
            F.count('*').alias('observed_slots'),F.countDistinct('station_id').alias('observed_stations'))
        daily_rows=[r.asDict() for r in daily.collect()]
        csv_write(out/'network_daily_coverage.csv',sorted(daily_rows,key=lambda r:r['rain_date']))
        elapsed=time.monotonic()-begin
        dump(out/'validation.json',{'status':'PASS','coverage_reference_rows_checked':checked,
            'station_months_with_observations':len(lookup),'calendar_observation_rows':sum(r['observed_slots'] for r in monthly_rows),
            'rainfall_sums_match_A_interval_calendar':True,'count_match_A_interval_calendar':True,
            'input_integrity':verification})
        metadata(root,out,'python src/B_task1_analysis.py --root PROJECT_ROOT',elapsed)
        log(f'Task 1 aggregates PASS; {elapsed:.1f}s')
    finally:spark.stop()

if __name__=='__main__':main()
