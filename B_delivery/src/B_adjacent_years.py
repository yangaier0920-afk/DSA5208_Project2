"""Local adjacent-year same-month comparisons from verified Spark aggregates."""
import argparse, calendar, hashlib
from pathlib import Path
from decimal import Decimal
from b_common import PACKAGE, rows, csv_write, dump

def build_adjacent_years(out):
    out=Path(out)
    source=out/'station_month_metrics.csv'
    lookup={}
    for r in rows(source):
        key=(int(r['year']),int(r['month']),r['station_id'])
        assert key not in lookup, 'Duplicate station-month aggregate'
        lookup[key]=r
    comparisons=[]
    for y in range(2017,2024):
        for m in range(1,13):
            left={s for (yy,mm,s),r in lookup.items() if yy==y and mm==m and Decimal(r['coverage'])>=Decimal('0.90')}
            right={s for (yy,mm,s),r in lookup.items() if yy==y+1 and mm==m and Decimal(r['coverage'])>=Decimal('0.90')}
            ids=sorted(left & right); n=len(ids)
            first=second=delta=None
            if n:
                first=sum(Decimal(lookup[(y,m,s)]['observed_rain_mm']) for s in ids)/n
                second=sum(Decimal(lookup[(y+1,m,s)]['observed_rain_mm']) for s in ids)/n
                delta=second-first
                # Same stations, equal weights: paired change equals difference of means.
                paired=sum(Decimal(lookup[(y+1,m,s)]['observed_rain_mm'])-Decimal(lookup[(y,m,s)]['observed_rain_mm']) for s in ids)/n
                assert abs(paired-delta)<Decimal('1e-20')
            comparisons.append({'year_from':y,'year_to':y+1,'month':m,'coverage_cutoff':0.9,
                'n_common_stations':n,'station_ids':'|'.join(ids),
                'mean_from_observed_mm':first,'mean_to_observed_mm':second,'mean_paired_change_mm':delta,
                'status':'SUPPORTED' if n else 'NO_COMMON_STATIONS'})
    csv_write(out/'adjacent_year_same_month_summary.csv',comparisons)
    supported=[r for r in comparisons if r['n_common_stations']]
    dump(out/'adjacent_year_validation.json',{'status':'PASS','comparisons_total':84,
        'supported_comparisons':len(supported),'unsupported_comparisons':84-len(supported),
        'source_aggregate_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'rule':'2017-2018 through 2023-2024; same month; station coverage >=90% in both years; all common IDs equally weighted',
        'paired_change_checked':True,'scope':'local paired year-month comparisons; panels vary between cells',
        'limits':['Observed totals, not missingness-corrected totals.',
            'No common stations is NA, not zero rainfall/change.',
            'Station IDs do not establish stable measurement conditions.',
            'Leap-year February has unequal calendar duration; monthly totals are not rates.',
            'No significance test, causal attribution, chaining of local changes, or full-year total reconstruction.']})
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    grid=np.full((7,12),np.nan)
    for r in supported:grid[r['year_from']-2017,r['month']-1]=float(r['mean_paired_change_mm'])
    limit=max(abs(float(r['mean_paired_change_mm'])) for r in supported) if supported else 1
    from matplotlib.colors import LinearSegmentedColormap
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,
        'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold',
        'axes.labelcolor':'#243746','text.color':'#243746'})
    fig,ax=plt.subplots(figsize=(12.8,6.2),layout='constrained')
    cm=LinearSegmentedColormap.from_list('rainfall_change',['#de8b2f','#f4f6f5','#187f86']);cm.set_bad('#e9ecef')
    im=ax.imshow(grid,aspect='auto',cmap=cm,vmin=-limit,vmax=limit)
    for r in comparisons:
        v=r['mean_paired_change_mm']; label=f'{float(v):+.0f}\nn={r["n_common_stations"]}' if v is not None else 'NA\nn=0'
        ax.text(r['month']-1,r['year_from']-2017,label,ha='center',va='center',fontsize=8,
            color='white' if v is not None and abs(float(v))>limit*.65 else '#243746')
    ax.set(xticks=range(12),xticklabels=[calendar.month_abbr[m] for m in range(1,13)],
        yticks=range(7),yticklabels=[f'{y} to {y+1}' for y in range(2017,2024)],
        title='Local interannual changes: paired stations within each year pair and month',
        xlabel='Later year minus earlier year (mm); n = common station IDs with >=90% coverage in BOTH years')
    fig.colorbar(im,ax=ax,label='Change in mean observed same-month rainfall (mm)')
    fig.suptitle('Orange: drier in the later year | Teal: wetter in the later year',fontsize=12)
    (out/'figures').mkdir(exist_ok=True)
    for ext in ['png','pdf']:fig.savefig(out/'figures'/('09_adjacent_year_comparison.'+ext),dpi=180,bbox_inches='tight',facecolor='white')
    plt.close(fig)
    print('Adjacent-year analysis PASS:',len(supported),'of 84 cells supported')
    return comparisons

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',type=Path,default=PACKAGE/'results/task1_v2');a=ap.parse_args()
    build_adjacent_years(a.input)

if __name__=='__main__':main()
