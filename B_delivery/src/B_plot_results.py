"""Plot only Spark-produced aggregates and export every figure's source table."""
import argparse, calendar, collections, math
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from b_common import PACKAGE,csv_write,dump,rows

YEARS=list(range(2017,2025)); MONTHS=list(range(1,13))

def avg(values):
    values=list(values);return sum(values)/len(values) if values else None

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,default=PACKAGE/'results/task1_v2')
    p.add_argument('--root',type=Path,help='A project root for coordinate audit and station map')
    a=p.parse_args();out=a.input;fig=out/'figures';fig.mkdir(exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,
        'axes.titleweight':'bold','figure.dpi':130,'savefig.dpi':180,'axes.labelcolor':'#243746','text.color':'#243746'})
    def save(name):
        plt.savefig(fig/(name+'.png'),bbox_inches='tight',facecolor='white')
        plt.savefig(fig/(name+'.pdf'),bbox_inches='tight',facecolor='white');plt.close()
    monthly=rows(out/'station_month_metrics.csv')
    for r in monthly:
        for k in ['year','month','observed_slots','wet_slots']:r[k]=int(r[k])
        for k in ['coverage','observed_rain_mm','wet_fraction_observed','mean_5m_mm_observed','max_5m_mm']:
            r[k]=float(r[k])
    lookup={(r['year'],r['month'],r['station_id']):r for r in monthly}
    cover=rows(out/'coverage_calendar.csv')
    panel=rows(out/'strict_panel_membership.csv')
    strict=[]
    for p in panel:
        cutoff=float(p['coverage_cutoff']);m=int(p['month']);ids=p['station_ids'].split('|') if p['station_ids'] else []
        for y in YEARS:
            selected=[lookup[(y,m,s)] for s in ids]
            strict.append({'coverage_cutoff':cutoff,'year':y,'month':m,'n_stations':len(selected),
                'mean_station_month_observed_mm':avg(r['observed_rain_mm'] for r in selected),
                'mean_station_wet_fraction':avg(r['wet_fraction_observed'] for r in selected)})
    csv_write(out/'strict_same_month_summary.csv',strict)
    descriptive=[]
    for cutoff in [.9,.95]:
        for y in YEARS:
            for m in MONTHS:
                selected=[r for r in monthly if r['year']==y and r['month']==m and r['coverage']>=cutoff]
                descriptive.append({'coverage_cutoff':cutoff,'year':y,'month':m,'n_stations':len(selected),
                    'mean_station_month_observed_mm':avg(r['observed_rain_mm'] for r in selected),
                    'mean_station_wet_fraction':avg(r['wet_fraction_observed'] for r in selected)})
    csv_write(out/'descriptive_year_month_summary.csv',descriptive)
    # Exposure and station-panel availability are displayed before rainfall comparisons.
    grid=np.full((8,12),np.nan)
    coverage_summary=[]
    for y in YEARS:
        for m in MONTHS:
            rr=[r for r in cover if int(r['year'])==y and int(r['month'])==m]
            n=sum(float(r['coverage'])>=.9 for r in rr)
            grid[y-2017,m-1]=n
            coverage_summary.append({'year':y,'month':m,'available_station_ids':len(rr),'qualified_90pct':n,
                'network_observed_slots':sum(int(r['observed_slots']) for r in rr)})
    csv_write(out/'coverage_figure_source.csv',coverage_summary)
    f,ax=plt.subplots(2,1,figsize=(10.8,6.7),gridspec_kw={'height_ratios':[3,1]},layout='constrained')
    im=ax[0].imshow(grid,aspect='auto',cmap='Blues',vmin=0)
    ax[0].set(xticks=range(12),xticklabels=[calendar.month_abbr[m] for m in MONTHS],yticks=range(8),yticklabels=YEARS,
        title='Coverage determines which comparisons are supported')
    for iy in range(8):
        for ix in range(12):ax[0].text(ix,iy,str(int(grid[iy,ix])),ha='center',va='center',fontsize=8,color='white' if grid[iy,ix]>35 else '#243746')
    f.colorbar(im,ax=ax[0],label='Stations with at least 90% monthly coverage')
    for cutoff,color,offset in [(.9,'#187f86',-.17),(.95,'#e39b38',.17)]:
        counts=[next(int(p['comparable_station_count']) for p in panel if float(p['coverage_cutoff'])==cutoff and int(p['month'])==m) for m in MONTHS]
        ax[1].bar(np.arange(12)+offset,counts,width=.32,label=f'{cutoff:.0%} cutoff',color=color)
    ax[1].set(xticks=range(12),xticklabels=[calendar.month_abbr[m] for m in MONTHS],ylabel='Common stations',title='Same-month intersection across all eight years')
    ax[1].legend(frameon=False,ncol=2,fontsize=8);save('01_coverage_and_panel')
    # Descriptive eight-year seasonal picture: no unsupported fixed-panel inference.
    g=np.full((8,12),np.nan)
    for r in descriptive:
        if r['coverage_cutoff']==.9 and r['mean_station_month_observed_mm'] is not None:g[r['year']-2017,r['month']-1]=r['mean_station_month_observed_mm']
    f,ax=plt.subplots(figsize=(11.2,5),layout='constrained')
    cm=plt.get_cmap('YlGnBu').copy();cm.set_bad('#e9ecef')
    im=ax.imshow(g,aspect='auto',cmap=cm)
    ax.set(xticks=range(12),xticklabels=[calendar.month_abbr[m] for m in MONTHS],yticks=range(8),yticklabels=YEARS,
        title='Monthly rainfall observed at qualified stations',xlabel='Within-year description; station composition varies between cells')
    for iy in range(8):
        for ix in range(12):
            v=g[iy,ix]
            ax.text(ix,iy,'NA' if np.isnan(v) else f'{v:.0f}',ha='center',va='center',fontsize=8,
                color='white' if np.isfinite(v) and v>np.nanmax(g)*.65 else '#243746')
    f.colorbar(im,ax=ax,label='Mean observed station-month total (mm)');save('02_descriptive_monthly_rainfall')
    # Strict supported comparisons with sensitivity threshold.
    supported=[m for m in MONTHS if any(r['month']==m and r['n_stations']>0 and r['coverage_cutoff']==.9 for r in strict)]
    f,axes=plt.subplots(2,2,figsize=(10.8,6.7),layout='constrained')
    strict_findings=[]
    for ax,m in zip(axes.flat,supported):
        for cutoff,style,color in [(.9,'-','#187f86'),(.95,'--','#de8b2f')]:
            rr=[r for r in strict if r['month']==m and r['coverage_cutoff']==cutoff]
            ax.plot([r['year'] for r in rr],[r['mean_station_month_observed_mm'] for r in rr],style,marker='o',ms=4,
                color=color,label=f'{cutoff:.0%} coverage, n={rr[0]["n_stations"]}')
        ax.set(title=calendar.month_name[m],xticks=YEARS[::2],ylabel='Mean observed monthly rain (mm)');ax.grid(alpha=.18);ax.legend(frameon=False,fontsize=8)
        rr=[r for r in strict if r['month']==m and r['coverage_cutoff']==.9]
        lo=min(rr,key=lambda r:r['mean_station_month_observed_mm']);hi=max(rr,key=lambda r:r['mean_station_month_observed_mm'])
        strict_findings.append({'month':m,'n_stations':rr[0]['n_stations'],'lowest_year':lo['year'],'lowest_mm':lo['mean_station_month_observed_mm'],
            'highest_year':hi['year'],'highest_mm':hi['mean_station_month_observed_mm']})
    f.suptitle('Interannual comparisons on fixed same-month station panels',fontsize=14,fontweight='bold');save('03_strict_interannual_comparison')
    # Hourly rates: require >=90% coverage in ALL 24 hours for each station-month.
    hrows=rows(out/'station_month_hour_metrics.csv');hour_groups=collections.defaultdict(list)
    hour_coverage=collections.defaultdict(list)
    for r in hrows:
        y,m=int(r['year']),int(r['month'])
        hour_coverage[(y,m,r['station_id'])].append(int(r['observed_slots'])/(calendar.monthrange(y,m)[1]*12))
    balanced_station_months={key for key,v in hour_coverage.items()
        if len(v)==24 and min(v)>=.9 and lookup[key]['coverage']>=.9}
    for r in hrows:
        y,m,h=int(r['year']),int(r['month']),int(r['hour']);s=r['station_id'];n=int(r['observed_slots'])
        if (y,m,s) in balanced_station_months:
            hour_groups[(y,m,h)].append((int(r['wet_slots'])/n,float(r['observed_rain_mm'])/n))
    hourly=[]
    for (y,m,h),v in sorted(hour_groups.items()):hourly.append({'year':y,'month':m,'hour':h,'n_stations':len(v),
        'wet_fraction':avg(x[0] for x in v),'mean_observed_5m_mm':avg(x[1] for x in v)})
    csv_write(out/'hourly_qualified_year_month.csv',hourly)
    monthhour=[];hg=np.full((12,24),np.nan)
    for m in MONTHS:
        for h in range(24):
            rr=[r for r in hourly if r['month']==m and r['hour']==h]
            value=avg(r['wet_fraction'] for r in rr)
            monthhour.append({'month':m,'hour':h,'n_years':len(rr),'wet_fraction':value})
            if value is not None:hg[m-1,h]=value*100
    csv_write(out/'month_hour_figure_source.csv',monthhour)
    hourline=[{'hour':h,'n_year_months':len([r for r in hourly if r['hour']==h]),
        'wet_fraction':avg(r['wet_fraction'] for r in hourly if r['hour']==h)} for h in range(24)]
    csv_write(out/'hourly_profile_figure_source.csv',hourline)
    f,ax=plt.subplots(2,1,figsize=(10.8,7),layout='constrained',gridspec_kw={'height_ratios':[2,1]})
    im=ax[0].imshow(hg,aspect='auto',cmap='YlGnBu');ax[0].set(yticks=range(12),yticklabels=[calendar.month_abbr[m] for m in MONTHS],xticks=range(0,24,3),xticklabels=range(0,24,3),title='Rain occurrence by month and hour (Singapore time)')
    f.colorbar(im,ax=ax[0],label='Observed five-minute slots with rain (%)')
    ax[1].plot(range(24),[r['wet_fraction']*100 for r in hourline],color='#187f86',marker='o',ms=4)
    ax[1].set(xlabel='Hour at interval start (SGT)',ylabel='Rain occurrence (%)',xticks=range(0,24,3));ax[1].grid(alpha=.2)
    save('04_hourly_rain_occurrence')
    # Spatial differences use one fixed, sufficiently supported calendar month.
    spatial_choice = min((p for p in panel if float(p['coverage_cutoff'])==.9 and int(p['comparable_station_count'])>0),
        key=lambda p:(-int(p['comparable_station_count']),int(p['month'])))
    spatial_month = int(spatial_choice['month'])
    ids=next(p['station_ids'].split('|') for p in panel if float(p['coverage_cutoff'])==.9 and int(p['month'])==spatial_month)
    spatial=[]
    for s in ids:
        v=[lookup[(y,spatial_month,s)]['observed_rain_mm'] for y in YEARS]
        spatial.append({'station_id':s,'month':spatial_month,'years':8,'mean_observed_month_mm':avg(v),'minimum_mm':min(v),'maximum_mm':max(v)})
    spatial.sort(key=lambda r:r['mean_observed_month_mm'])
    csv_write(out/'station_selected_month_figure_source.csv',spatial)
    f,ax=plt.subplots(figsize=(9.5,6.2),layout='constrained')
    ax.barh([r['station_id'] for r in spatial],[r['mean_observed_month_mm'] for r in spatial],color='#187f86',height=.7)
    ax.set(xlabel=f'Mean observed {calendar.month_name[spatial_month]} rainfall, 2017-2024 (mm)',
        title=f'Station differences: {calendar.month_name[spatial_month]} has the largest fixed panel (n={len(ids)})')
    ax.grid(axis='x',alpha=.2);save('05_station_comparison')
    # Annual descriptive summary; no interpolation for unsupported years.
    annual_by_station = collections.defaultdict(list)
    for r in cover:
        annual_by_station[(int(r['year']),r['station_id'])].append(r)
    annual_station = []
    for (y,s),rr in sorted(annual_by_station.items()):
        slots = sum(int(r['observed_slots']) for r in rr)
        expected = sum(int(r['calendar_slots']) for r in rr)
        annual_station.append({'year':y,'station_id':s,'observed_slots':slots,
            'calendar_slots':expected,'annual_coverage':slots/expected,
            'minimum_month_coverage':min(float(r['coverage']) for r in rr),
            'months_ge_90pct':sum(float(r['coverage'])>=.9 for r in rr),
            'observed_annual_rain_mm':sum(float(r['observed_rain_mm']) for r in rr)})
    csv_write(out/'annual_station_coverage.csv',annual_station)
    annual_summary=[]; annual_fixed=[]
    for cutoff in [.9,.95]:
        selected_by_year={y:{r['station_id'] for r in annual_station if r['year']==y and r['annual_coverage']>=cutoff} for y in YEARS}
        fixed_ids=set.intersection(*(selected_by_year[y] for y in YEARS))
        for y in YEARS:
            selected=[r for r in annual_station if r['year']==y and r['station_id'] in selected_by_year[y]]
            fixed=[r for r in annual_station if r['year']==y and r['station_id'] in fixed_ids]
            annual_summary.append({'coverage_cutoff':cutoff,'year':y,'n_stations':len(selected),
                'mean_observed_annual_mm':avg(r['observed_annual_rain_mm'] for r in selected),
                'mean_annual_coverage':avg(r['annual_coverage'] for r in selected),
                'lowest_month_coverage':min((r['minimum_month_coverage'] for r in selected),default=None),
                'stations_passing_every_month':sum(r['minimum_month_coverage']>=cutoff for r in selected),
                'mean_observed_annual_mm_each_month':avg(r['observed_annual_rain_mm'] for r in selected if r['minimum_month_coverage']>=cutoff),
                'scope':'descriptive within-year annual coverage; station composition varies'})
            annual_fixed.append({'coverage_cutoff':cutoff,'year':y,'n_stations':len(fixed),
                'mean_observed_annual_mm':avg(r['observed_annual_rain_mm'] for r in fixed),
                'scope':'fixed annual station intersection across all eight years'})
    csv_write(out/'annual_descriptive_summary.csv',annual_summary)
    csv_write(out/'annual_fixed_panel_summary.csv',annual_fixed)
    f,ax=plt.subplots(2,1,figsize=(10.8,6.5),layout='constrained',gridspec_kw={'height_ratios':[2,1]})
    for cutoff,color,style in [(.9,'#187f86','-'),(.95,'#de8b2f','--')]:
        rr=[r for r in annual_summary if r['coverage_cutoff']==cutoff]
        values=[r['mean_observed_annual_mm'] if r['mean_observed_annual_mm'] is not None else np.nan for r in rr]
        ax[0].plot(YEARS,values,style,marker='o',color=color,label=f'Annual coverage >= {cutoff:.0%}')
        ax[1].plot(YEARS,[r['n_stations'] for r in rr],style,marker='o',color=color)
    strict_annual=[r for r in annual_summary if r['coverage_cutoff']==.9]
    ax[0].plot(YEARS,[r['mean_observed_annual_mm_each_month'] if r['mean_observed_annual_mm_each_month'] is not None else np.nan for r in strict_annual],
        ':',marker='s',color='#405366',label='Every month >= 90% (stronger gate)')
    for r in annual_summary:
        if r['coverage_cutoff']==.9 and r['n_stations']==0:
            ax[0].text(r['year'],.08,'NA: no qualified stations',rotation=90,ha='center',va='bottom',fontsize=8,transform=ax[0].get_xaxis_transform())
    ax[0].set(ylabel='Mean observed station-year rainfall (mm)',xticks=YEARS,
        title='Annual observed rainfall: descriptive, changing station sets')
    ax[0].legend(frameon=False);ax[0].grid(alpha=.18)
    ax[1].set(xticks=YEARS,ylabel='Qualified stations',xlabel='Full-year fixed eight-year panel is empty; missing years remain NA')
    ax[1].grid(alpha=.18);save('06_annual_descriptive_summary')

    # Rain occurrence versus nonzero intensity; averages of station quantiles, not pooled quantiles.
    frequency_year_month=[]
    for cutoff in [.9,.95]:
        for y in YEARS:
            for m in MONTHS:
                selected=[r for r in monthly if r['year']==y and r['month']==m and r['coverage']>=cutoff]
                metric={'coverage_cutoff':cutoff,'year':y,'month':m,'n_stations':len(selected),
                    'wet_fraction':avg(r['wet_fraction_observed'] for r in selected)}
                for q in ['p50','p90','p99']:
                    vv=[float(r[f'wet_{q}_5m_mm']) for r in selected if r[f'wet_{q}_5m_mm'] not in ['',None]]
                    metric[f'mean_station_wet_{q}_5m_mm']=avg(vv)
                    metric[f'n_stations_wet_{q}']=len(vv)
                frequency_year_month.append(metric)
    csv_write(out/'frequency_intensity_year_month.csv',frequency_year_month)
    frequency_summary=[]
    for cutoff in [.9,.95]:
        for m in MONTHS:
            rr=[r for r in frequency_year_month if r['coverage_cutoff']==cutoff and r['month']==m and r['n_stations']>0]
            metric={'coverage_cutoff':cutoff,'month':m,'available_years':len(rr),
                'wet_fraction':avg(r['wet_fraction'] for r in rr)}
            for q in ['p50','p90','p99']:
                values=[r[f'mean_station_wet_{q}_5m_mm'] for r in rr if r[f'mean_station_wet_{q}_5m_mm'] is not None]
                metric[f'mean_station_wet_{q}_5m_mm']=avg(values)
                metric[f'available_years_wet_{q}']=len(values)
            frequency_summary.append(metric)
    csv_write(out/'monthly_frequency_intensity_summary.csv',frequency_summary)
    f,ax=plt.subplots(2,1,figsize=(10.8,6.7),layout='constrained')
    for cutoff,color,style in [(.9,'#187f86','-'),(.95,'#de8b2f','--')]:
        rr=[r for r in frequency_summary if r['coverage_cutoff']==cutoff]
        ax[0].plot(MONTHS,[r['wet_fraction']*100 if r['wet_fraction'] is not None else np.nan for r in rr],
            style,marker='o',color=color,label=f'{cutoff:.0%} coverage')
    rr=[r for r in frequency_summary if r['coverage_cutoff']==.9]
    for q,color in [('p50','#8799a1'),('p90','#187f86'),('p99','#9b5a76')]:
        ax[1].plot(MONTHS,[r[f'mean_station_wet_{q}_5m_mm'] if r[f'mean_station_wet_{q}_5m_mm'] is not None else np.nan for r in rr],
            marker='o',color=color,label=f'Mean station-month wet {q.upper()}')
    for axis in ax:
        axis.set(xticks=MONTHS,xticklabels=[calendar.month_abbr[m] for m in MONTHS]);axis.grid(alpha=.18);axis.legend(frameon=False,fontsize=8)
    ax[0].set(title='Rainfall frequency and conditional intensity describe different patterns',ylabel='Observed wet five-minute slots (%)')
    ax[1].set(ylabel='Nonzero five-minute rainfall (mm)',xlabel='Stations equal within year-month, then available years equal; composition varies')
    save('07_frequency_and_intensity')

    # Full network gaps are coverage evidence, not dry weather.
    import datetime
    network=rows(out/'network_daily_coverage.csv');seen={r['rain_date'] for r in network};missing=[]
    day=datetime.date(2017,1,1)
    while day<datetime.date(2025,1,1):
        if day.isoformat() not in seen:missing.append({'rain_date':day.isoformat(),'interpretation':'no interval-calendar observations; rainfall unknown'})
        day+=datetime.timedelta(days=1)
    csv_write(out/'network_dates_without_observations.csv',missing,['rain_date','interpretation'])
    peak=max(hourline,key=lambda r:r['wet_fraction']);low=min(hourline,key=lambda r:r['wet_fraction'])
    summaries={'strict_supported_months':supported,'strict_month_findings':strict_findings,
        'hourly_peak':peak,'hourly_minimum':low,'missing_interval_calendar_network_dates':len(missing),
        'descriptive_max_cell':max((r for r in descriptive if r['coverage_cutoff']==.9 and r['mean_station_month_observed_mm'] is not None),key=lambda r:r['mean_station_month_observed_mm']),
        'descriptive_min_cell':min((r for r in descriptive if r['coverage_cutoff']==.9 and r['mean_station_month_observed_mm'] is not None),key=lambda r:r['mean_station_month_observed_mm']),
        'october_station_low':spatial[0],'october_station_high':spatial[-1],
        'spatial_month':spatial_month,'spatial_panel_station_count':len(ids),
        'spatial_selection':'largest strict 90% same-month eight-year panel, earliest month breaks ties',
        'annual_unsupported_years':[r['year'] for r in annual_summary if r['coverage_cutoff']==.9 and r['n_stations']==0],
        'annual_fixed_panel_stations':annual_fixed[0]['n_stations'],
        'limits':['No fixed all-month eight-year rainfall panel.','Observed rainfall totals are not coverage-corrected.','Descriptive panels vary in station composition.','No causal or long-term climate attribution.']}
    dump(out/'findings.json',summaries)
    if a.root is not None:
        from B_station_map import build_station_map
        build_station_map(a.root,out)
    from B_adjacent_years import build_adjacent_years
    build_adjacent_years(out)
    print('Plots and source tables saved:',fig)

if __name__=='__main__':main()
