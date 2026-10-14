"""Derive the report's measured coverage sensitivity from this run's B aggregates."""
import argparse
from pathlib import Path
from b_common import rows,csv_write

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',type=Path,required=True);a=ap.parse_args()
    source=rows(a.input/'strict_same_month_summary.csv');result=[]
    for month in range(1,13):
        panels={cut:[r for r in source if int(r['month'])==month and float(r['coverage_cutoff'])==cut and r['mean_station_month_observed_mm']] for cut in [.9,.95]}
        low,high=panels[.9],panels[.95]
        if not low or not high:continue
        assert {r['year'] for r in low}=={r['year'] for r in high},'Sensitivity panels must compare the same years'
        def extrema(records):
            return int(min(records,key=lambda r:float(r['mean_station_month_observed_mm']))['year']),int(max(records,key=lambda r:float(r['mean_station_month_observed_mm']))['year'])
        lo90,hi90=extrema(low);lo95,hi95=extrema(high)
        values={r['year']:float(r['mean_station_month_observed_mm']) for r in high}
        result.append(dict(month=month,stations_90=int(low[0]['n_stations']),stations_95=int(high[0]['n_stations']),lowest_year_90=lo90,lowest_year_95=lo95,highest_year_90=hi90,highest_year_95=hi95,extrema_consistent=lo90==lo95 and hi90==hi95,max_abs_mean_difference_mm=max(abs(values[r['year']]-float(r['mean_station_month_observed_mm'])) for r in low)))
    csv_write(a.input/'coverage_sensitivity_summary.csv',result)
    print('Coverage sensitivity summary PASS: '+str(len(result))+' supported months')
if __name__=='__main__':main()
