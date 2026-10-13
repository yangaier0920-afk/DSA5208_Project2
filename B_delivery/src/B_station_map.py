"""Audit metadata coordinates and map exactly the existing Task 1 spatial panel.
No geocoding, interpolation, inferred relocation, or meteorological re-estimation.
"""
import argparse, math, hashlib
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from b_common import PACKAGE, rows, csv_write, dump

def distance_m(a,b):
    lon1,lat1=map(math.radians,a);lon2,lat2=map(math.radians,b)
    h=math.sin((lat2-lat1)/2)**2+math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return 6371008.8*2*math.asin(min(1,math.sqrt(h)))

def build_station_map(root, out):
    root,out=Path(root),Path(out)
    source=root/'outputs/p0_2_audit/p0_2_v1/station_metadata_history.csv'
    history=rows(source)
    spatial=rows(out/'station_selected_month_figure_source.csv')
    assert spatial and len({r['station_id'] for r in spatial})==len(spatial)
    month=int(spatial[0]['month']); ids={r['station_id'] for r in spatial}
    panel=next(r for r in rows(out/'strict_panel_membership.csv') if float(r['coverage_cutoff'])==.9 and int(r['month'])==month)
    assert ids==set(panel['station_ids'].split('|')), 'Map must use the existing strict 90% panel'
    grouped={}; relevant=[]
    for r in history:
        if int(r['year']) not in range(2017,2025): continue
        # Rows are audit metadata groups, not station relocation events.
        if r['location_longitude'] and r['location_latitude']:
            xy=(float(r['location_longitude']),float(r['location_latitude']))
            assert all(math.isfinite(x) for x in xy)
            assert 103<xy[0]<105 and 1<xy[1]<2
            grouped.setdefault(r['station_id'],[]).append((xy,r))
        if r['station_id'] in ids: relevant.append(r)
    csv_write(out/'spatial_panel_metadata_history.csv',relevant)
    audits=[]; mapped=[]
    values={r['station_id']:float(r['mean_observed_month_mm']) for r in spatial}
    for s, records in sorted(grouped.items()):
        coords=sorted({xy for xy,r in records})
        # Display coordinate = record with latest observed epoch, then first epoch and year.
        xy,latest=max(records,key=lambda v:(int(v[1]['last_epoch_s']),int(v[1]['first_epoch_s']),int(v[1]['year']),v[0]))
        sep=max(distance_m(a,b) for a in coords for b in coords)
        audits.append({'station_id':s,'in_spatial_panel':s in ids,'coordinate_pair_count':len(coords),
            'station_name_count':len({r['station_name'] for _,r in records}),
            'device_id_count':len({r['station_device_id'] for _,r in records}),
            'max_coordinate_separation_m':round(sep,3),'longitude_display':xy[0],'latitude_display':xy[1],
            'display_coordinate_rule':'metadata group with latest observed epoch; not reconstructed station trajectory',
            'interpretation':'coordinate variation; relocation vs metadata correction undetermined' if len(coords)>1 else 'one recorded coordinate; measurement conditions not independently verified'})
        if s in ids:
            mapped.append({'station_id':s,'station_name':latest['station_name'],'month':month,'coverage_cutoff':.9,
                'years':'2017-2024','longitude':xy[0],'latitude':xy[1],
                'mean_observed_month_mm':values[s],'coordinate_pair_count':len(coords),
                'max_coordinate_separation_m':round(sep,3)})
    assert {r['station_id'] for r in mapped}==ids, 'Missing coordinates in spatial panel'
    csv_write(out/'station_coordinate_audit.csv',audits)
    csv_write(out/'station_map_figure_source.csv',mapped)
    changed=[r for r in audits if r['in_spatial_panel'] and r['coordinate_pair_count']>1]
    dump(out/'station_map_validation.json',{'status':'PASS','station_count':len(ids),'month':month,
        'panel':'same strict 90% eight-year same-month panel as station comparison',
        'coordinate_changed_panel_stations':changed,'metadata_source':str(source),
        'metadata_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'coordinate_system':'longitude/latitude as supplied by A; approximate geodesic distance assumes spherical Earth',
        'limits':['No meteorological homogenization or independent device/location verification.',
            'First/last epochs are metadata-group observation ranges and may overlap; no exact move date inferred.',
            'Latest recorded coordinate is a display convention; eight-year mean is not attributed to a proven stable site.',
            'Station location diagram without basemap; no interpolation or whole-island spatial trend inferred.']})
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import calendar
    from matplotlib.colors import LinearSegmentedColormap
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,
        'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold',
        'axes.labelcolor':'#243746','text.color':'#243746'})
    fig,ax=plt.subplots(figsize=(10.8,5.3),layout='constrained')
    teal=LinearSegmentedColormap.from_list('rainfall_teal',['#dceeed','#7bbdb6','#187f86','#0c5660'])
    points=ax.scatter([r['longitude'] for r in mapped],[r['latitude'] for r in mapped],
        c=[r['mean_observed_month_mm'] for r in mapped],s=100,cmap=teal,edgecolor='white',linewidth=.8,zorder=3)
    offsets={'S89':(7,-13),'S107':(7,-13),'S113':(7,7)}
    for r in mapped:
        label=r['station_id']+('*' if r['coordinate_pair_count']>1 else '')
        ax.annotate(label,(r['longitude'],r['latitude']),xytext=offsets.get(r['station_id'],(6,6)),textcoords='offset points',fontsize=8)
    ax.set(xlabel='Longitude (degrees east)',ylabel='Latitude (degrees north)',
        title=f'{calendar.month_name[month]} rainfall across the fixed station panel (n={len(ids)})')
    ax.set_aspect(1/math.cos(math.radians(1.35)));ax.margins(.12);ax.grid(alpha=.18)
    fig.colorbar(points,ax=ax,shrink=.82,pad=.025,label=f'Mean observed {calendar.month_name[month]} rainfall, 2017-2024 (mm)')
    (out/'figures').mkdir(exist_ok=True)
    for ext in ['png','pdf']:fig.savefig(out/'figures'/('08_station_map.'+ext),dpi=180,bbox_inches='tight',facecolor='white')
    plt.close(fig)
    print('Station map and coordinate audit PASS:',len(ids),'stations;',len(changed),'with coordinate variation')
    return mapped, changed

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',required=True,type=Path)
    ap.add_argument('--input',type=Path,default=PACKAGE/'results/task1_v2');a=ap.parse_args()
    build_station_map(a.root,a.input)

if __name__=='__main__':main()
