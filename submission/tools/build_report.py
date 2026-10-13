"""Integrated, evidence-driven course report; member details remain user supplied."""
import csv,json,shutil,html,os,hashlib,argparse
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
ap=argparse.ArgumentParser(description='Integrate explicitly selected evidence without mixing experiment runs')
ap.add_argument('--task1',type=Path,default=ROOT/'B_delivery/results/task1_v2')
ap.add_argument('--c-run',type=Path,default=ROOT/'C_delivery/results/C_v1_20261012')
ap.add_argument('--c-report',type=Path)
ap.add_argument('--revision-run',type=Path,default=ROOT/'C_delivery/results/C_leaf_study_20261014')
ap.add_argument('--output',type=Path,default=ROOT/'submission')
a_args=ap.parse_args();OUT=a_args.output.resolve();OUT.mkdir(parents=True,exist_ok=True)
B=a_args.task1.resolve();C=a_args.c_run.resolve()
C_REPORT=a_args.c_report.resolve() if a_args.c_report else (C/'report' if (C/'report/figures').exists() else ROOT/'C_delivery/report_zh')
if C!=ROOT/'C_delivery/results/C_v1_20261012' and a_args.c_report is None and not (C/'report/figures').exists():
 raise ValueError('Specify --c-report matching the selected C run; shared legacy figures cannot be mixed with a new run')
shutil.copyfile(ROOT/'submission/members.json',OUT/'members.json') if OUT!=ROOT/'submission' else None
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def rows(p):
 with p.open(encoding='utf-8-sig') as f:return list(csv.DictReader(f))
def fmt(x,n=6):return 'NA' if x is None or x=='' else f'{float(x):.{n}f}'
def num(x):return f'{int(float(x)):,}'
assets=OUT/'assets';assets.mkdir(exist_ok=True)
for src,prefix in [(B/'figures','task1'),(C_REPORT/'figures','model')]:
 for p in src.glob('*.png'):shutil.copy2(p,assets/f'{prefix}_{p.name}')
a=rows(ROOT/'outputs/p1_6_support/annual_cleaning_and_targets.csv');find=load(B/'findings.json')
selection=load(C/'selection.json');test=load(C/'final_test.json');reload=load(C/'model_reload_validation.json')
winner=selection['selected_model'];assert winner=='decision_tree' and test['status']==reload['status']=='PASS'
base={m['threshold_mm']:m for m in test['metrics'] if m['model']=='frequency_baseline'}
model={m['threshold_mm']:m for m in test['metrics'] if m['model']==winner}
thresholds=selection['thresholds'];baseline=sum(m['brier'] for m in base.values())/len(thresholds);skill=1-test['mean_brier']/baseline
members=load(OUT/'members.json')['members'];assert len(members)<=3
complete=bool(members) and all(m.get('name') and m.get('student_id') for m in members)
blocks=[]
def h(t):blocks.append({'type':'heading','text':t})
def p(t):blocks.append({'type':'paragraph','text':t})
def t(headers,data):blocks.append({'type':'table','headers':headers,'rows':data})
def f(name,caption,height=220):blocks.append({'type':'figure','path':'assets/'+name,'caption':caption,'height':height})
def page():blocks.append({'type':'pagebreak'})
page();h('1. Introduction and main findings')
p('This project uses the eight annual Singapore rainfall datasets from 2017 to 2024 to address two tasks: describe within-year and interannual rainfall patterns, and estimate the probability that a station will accumulate more than an input threshold during the next 30 minutes. Apache Spark performs structured processing and feature computation; Spark MLlib trains and evaluates the probability models. All times use Asia/Singapore.')
p('The cleaning ledger retains 48,538,627 of 48,538,713 parsed records, merging 86 equal-rainfall aligned-key duplicates and quarantining none. Missing coverage is consequential: a fixed eight-year same-month comparison is supported only in May, September, October and December at the primary 90% coverage rule. The largest descriptive monthly cell is October 2022, but changing station composition prevents it from being interpreted as a nationwide annual trend.')
h('2. Data provenance and cleaning')
p('The source is NEA\'s Historical Rainfall across Singapore collection on data.gov.sg [1]. It provides station measurements and timestamps in Singapore Time; its notice states that records may be missing and do not have the quality-control status of official climate data. The project uses 2017-2024, not the collection\'s additional 2016 file. Raw exports occupy 8,047,044,701 bytes. Annual titles, dataset URLs and locally computed whole-file SHA-256 hashes are recorded in manifests/raw_manifest.json. Download completeness was confirmed in the original project; the local hashes are not server-issued checksums.')
p('We retain the original 12 text columns, original record, source-file identity, hashes and event timestamp. Structural, rainfall-number, unit/type and original-key checks found no violations in this release. Only 2017 records with epoch remainder 299 modulo 300 are shifted forward by one second to an aligned five-minute endpoint. This affects 1,775,327 source rows. Original timestamps and alignment flags remain available for traceability.')
p('Alignment creates 86 equal-rainfall key-collision pairs. We keep the original exact-grid row, neither summing nor averaging rainfall, and preserve both records in 172 lineage rows. Two retained observations have metadata-conflict flags. Unequal-rainfall collisions would be quarantined as entire groups; none occur. Finite nonnegative measurements, genuine zero values and extremes are retained without arbitrary clipping or rounding to 0.2 mm. Missing timestamps remain unknown; no zero rows are synthesized.')
p('The count reconciliation is 48,538,713 = 48,538,627 observations + 86 merged removals + 0 quarantined. There are 47,121,647 genuine zero-rainfall observations. Zero quarantined rows establish the implemented ledger, not meteorological quality or temporal completeness. The raw event-date audit identifies 63 days with no network observations, including 4-31 August 2018. Their physical cause cannot be inferred from these files.')
t(['Year','Raw rows','Retained','Future complete','History complete','Both complete'],[[r['year'],num(r['raw_records']),num(r['observations']),num(r['future_valid']),num(r['history_valid']),num(r['base_model_eligible'])] for r in a])
p('Table 1. Annual cleaning and window counts. Retained rows sum to 48,538,627. All anchors are preserved in the targets table, including invalid-window anchors; window exclusion is a downstream supervision rule, not cleaning deletion.')
page();h('3. Time contract, truth and missingness')
p('Let R(t) denote rainfall in the five-minute interval (t-5 min, t], with t the aligned endpoint. This interval meaning and the availability of R(t) at t are explicit project assumptions; they are not independently established official semantics or verified real-time publication behaviour. Calendar summaries use rain_period_start_ts = t-300 s, so a midnight-ending interval belongs to the preceding date.')
p('The future truth is S(t) = R(t+300) + R(t+600) + ... + R(t+1800), covering (t,t+30 min] and excluding R(t). All six exact endpoints must exist. The most recent hour uses the 12 exact endpoints t-3300,...,t, covering (t-60 min,t]. Adjacent-year context is loaded before anchor selection. Row offsets cannot replace exact timestamps when records are missing.')
t(['Component','Exact endpoint offsets from t','Policy'],[['History','-3300,-3000,...,-300,0 seconds','All 12 positions required'],['Future','300,600,900,1200,1500,1800 seconds','All 6 positions required'],['Label','S(t) > input threshold (mm)','Equality is negative; unknown is not negative']])
p('Future totals and threshold comparisons use Decimal(38,18). Source precision required at most five effective decimal places; storage scale does not imply measurement precision. There are 47,636,833 complete future windows and 901,794 unknown future totals; 47,347,833 complete histories and 1,190,794 incomplete histories. Both are complete at 47,048,872 anchors. These exclusions overlap and must not be added as disjoint losses.')
p('Training/validation/test role fields purge the actual temporal boundary, including label endpoints equal to the next block\'s start. Internal year boundaries within the training block may be used when context is complete. Inference checks only station support and past-history completeness; future truth, model_anchor_eligible and role never determine whether a real-time request is accepted.')
h('4. Task 1: coverage determines valid comparisons')
p('Station-month observed rainfall sums and wet-slot fractions are computed by Spark and checked against A\'s interval-calendar coverage reference for 6,528 station-month entries. A month qualifies at >=90% observed calendar slots; >=95% is a prespecified sensitivity rule. Within-year descriptive summaries weight qualified stations equally, then available year-months equally where appropriate. Sums are not scaled to fill missing rainfall.')
f('task1_01_coverage_and_panel.png','Figure 1. Qualified station counts and fixed eight-year same-month panels.',340)
page();h('4.1 Annual summaries and the limits of coverage')
annual=[r for r in rows(B/'annual_descriptive_summary.csv') if float(r['coverage_cutoff'])==.9]
t(['Year','Annual >=90% stations','Observed mean (mm)','Every month >=90% stations'],[[r['year'],r['n_stations'],fmt(r['mean_observed_annual_mm'],1),r['stations_passing_every_month']] for r in annual])
f('task1_06_annual_descriptive_summary.png','Figure 2. Annual descriptions at 90%/95% annual coverage and the stricter every-month rule.',245)
p('2018 has no station meeting 90% annual coverage and is kept as NA. The eight-year fixed annual station intersection is empty. Descriptive means range from approximately 1,667.9 mm in 2019 to 2,898.9 mm in 2022 among each year\'s qualifying stations, but these are observed totals from changing station sets. They are not comparable estimates of national annual totals, nor evidence of a long-term climate trend. Requiring every month to pass coverage eliminates the entire annual panel in several years, demonstrating why an annual average coverage rule can hide concentrated missingness.')
page();h('4.2 Within-year seasonality: frequency differs from intensity')
f('task1_02_descriptive_monthly_rainfall.png','Figure 3. Descriptive station-mean monthly observed rainfall; NA means unsupported coverage.',240)
p('The largest descriptive year-month cell is October 2022: 419.1 mm across 66 qualifying stations. The smallest is February 2021: 32.0 mm across 68 stations. Month lengths, actual weather, coverage and station composition all contribute to these differences; they are not fixed-network cross-year rankings.')
f('task1_07_frequency_and_intensity.png','Figure 4. Observed wet-slot frequency and conditional nonzero rainfall intensity.',240)
p('At the 90% rule, November has the highest descriptive wet-slot fraction (4.30%), while March has the highest mean station-month nonzero five-minute P90 (2.84 mm). Thus rain occurrence and conditional intensity need not peak in the same month. Spark percentile_approx uses accuracy=1000; station-month nonzero quantiles are averaged across qualified stations and available years. They are not quantiles of all pooled national observations. All-dry groups have wet frequency zero but unknown conditional intensity, not a zero-filled intensity.')
page();h('4.3 Interannual comparisons on fixed same-month stations')
f('task1_03_strict_interannual_comparison.png','Figure 5. Fixed eight-year same-month panels; 90% versus 95% coverage.',245)
sens=rows(B/'coverage_sensitivity_summary.csv')
t(['Month','Stations 90%/95%','Lowest year','Highest year','Max |90%-95% mean| across years (mm)'],[[r['month'],r['stations_90']+'/'+r['stations_95'],r['lowest_year_90']+'/'+r['lowest_year_95'],r['highest_year_90']+'/'+r['highest_year_95'],fmt(r['max_abs_mean_difference_mm'],2)] for r in sens])
p('Sensitivity difference for month m is max over years y of |mean_90%(y,m) - mean_95%(y,m)|. It is not the difference between the wettest and driest years.')
p('Only May, September, October and December have supported eight-year panels. The lowest/highest years agree at both coverage cutoffs for each supported month. For October\'s 21-station 90% panel, the 2022 mean is 399.8 mm and the 2023 mean is 113.0 mm. May peaks in 2024, September in 2020, October in 2022 and December in 2023; no single year is wettest everywhere. Adding these four months would not create an annual comparison. The 95% cutoff changes composition and means, so stable extrema are sensitivity evidence rather than a significance test.')
p('Across 12 descriptive months, changing the rule to 95% leaves the peak occurrence month November and peak P90 month March unchanged; the maximum difference in mean wet-slot fraction is 0.0528 percentage points. This quantifies stability of the descriptive definition, not independence or causality.')
page();h('4.4 Diurnal patterns and station heterogeneity')
f('task1_04_hourly_rain_occurrence.png','Figure 6. Singapore-time wet-slot occurrence by month/hour and daily profile.',225)
p('The hourly panel requires every hour of a station-month to meet 90% coverage, ensuring the same station composition throughout its daily curve. Station means are then averaged across 82 supported year-months. Wet-slot occurrence peaks at 16:00 (5.78%) and is lowest at 23:00 (1.11%). This is the frequency of observed five-minute rain slots, not a forecast probability of exceeding a future 30-minute threshold or evidence of a physical mechanism.')
f('task1_05_station_comparison.png','Figure 7. Station contrasts in October, the largest supported fixed same-month panel.',210)
p('October is selected by the largest 90% eight-year panel (21 stations), breaking ties toward the earliest month. S89 averages about 270.3 mm and S107 about 125.2 mm. This is an October contrast, not an annual station ranking or a location-adjusted causal result. The interval-start calendar has 64 entirely missing network dates versus 63 in the event-date audit; the additional 2018-02-17 reflects calendar definition. Missing dates are not dry dates.')
page();h('4.5 Adjacent-year comparisons and spatial context')
f('task1_09_adjacent_year_comparison.png','Figure 8. Later minus earlier observed monthly rainfall on the same stations within each adjacent-year/month cell; labels show station counts.',210)
p('All seven adjacent-year pairs and 12 months were fixed in advance. Both years use the identical equal-weight station set meeting 90% monthly coverage. Of 84 cells, 63 are supported, including 35 outside the four-month eight-year panel. The largest increase is October 2021 to 2022: +255.27 mm across 63 stations; the largest decrease is October 2022 to 2023: -301.58 mm across 59 stations. These extrema are descriptive, without significance or multiple-comparison testing. Changing local panels cannot be chained into a national trend or annual total; February also differs in length across leap years.')
f('task1_08_station_map.png','Figure 9. Locations of the same 21-station October panel; colour shows mean observed October rainfall.',210)
p('Coordinates use the latest recorded metadata. S113 has two recorded positions approximately 39 metres apart; the cause is unknown. Fixed station IDs do not prove stable instruments or measurement conditions. The map provides context without spatial interpolation or location-adjusted causal interpretation.')
p('The November occurrence peak and afternoon maximum are compatible with the seasonal and diurnal background described by Meteorological Service Singapore, Climate of Singapore (https://www.weather.gov.sg/climate-climate-of-singapore/; B source accessed 13 October 2026). This is contextual interpretation, not storm classification or physical attribution. Five-minute wet-slot frequency is not the official rain-day measure, and the differing periods, stations and coverage rules prevent direct numeric comparisons.')
page();h('5. Task 2: current model and past-only features')
p('B preserves 48,538,627 anchors and publishes 26 numeric features; 47,347,833 (97.547%) have complete 60-minute history. The current selected model uses 22 recent-amount and wetness/persistence features. The four cyclic calendar columns and training-only station coding were tested in controlled ablations but did not improve mean Brier. Station chooses its historical series and prediction time chooses the exact anchor; neither must be a split feature to be a required API input.')
p('Recent features cover current rainfall, exact lags, 15/30/60-minute sums, preceding-window changes, maximum, population standard deviation and wet counts. Persistence adds wet indicators, trailing wet run with left-censor flag and capped recency with no-observed-rain flag. Complete dry windows use defined zeros; missing slots remain null. Feature definitions are in B_delivery/docs/FEATURE_DEFINITIONS.md. No future rainfall, target validity, role or eligibility enters inference features. Threshold is queried against the fitted distribution, not included in the current tree vector.')
# Integrate the current C chapter as the main modelling evidence, rather than an addendum.
revision=a_args.revision_run.resolve();rs=load(revision/'selection.json')
assert rs['status']=='LOCKED' and load(revision/'reload_validation.json')['status']=='PASS'
selected_brier=next(x['mean_brier'] for x in rs['scores'] if x['model']==rs['selected_model'])
persistence_brier=next(x['mean_brier'] for x in rs['scores'] if x['model']=='persistence_cdf')
selected_skill=100*(1-selected_brier/persistence_brier)
text=(revision/'report/C_report.md').read_text()
lines=text.splitlines();i=0
while i<len(lines):
 line=lines[i].strip()
 if line.startswith('## '):
  if not line.startswith(('## Appendix','## Findings','## Computing environment','## Remaining limitations','## C input reconstruction')):page()
  h(line[3:]);i+=1
 elif line.startswith('|'):
  table=[]
  while i<len(lines) and lines[i].strip().startswith('|'):
   table.append([x.strip() for x in lines[i].strip().strip('|').split('|')]);i+=1
  if table[0]==['Candidate','Mean Brier','Mean log loss','Mean PR-AUC','Mean ROC-AUC']:page();h('Development ranking and probability-loss diagnostics')
  if table[0][0]=='Group':page();h('Complete group summary')
  t(table[0],table[2:])
 elif line.startswith('!['):
  import re
  match=re.match(r'!\[(.*?)\]\((.*?)\)',line);src=revision/'report'/match.group(2);target=assets/('revision_'+src.name);shutil.copyfile(src,target);f(target.name,match.group(1),245);i+=1
 elif line=='<!-- PAGEBREAK -->':page();i+=1
 elif line and not line.startswith('# ') and not line.startswith('C_revision_'):
  paragraph=[line];i+=1
  while i<len(lines) and lines[i].strip() and not lines[i].startswith(('#','|','![','<!--')):paragraph.append(lines[i].strip());i+=1
  p(' '.join(paragraph).replace('**',''))
 else:i+=1
page();h('Historical benchmark: original 2024 test')
p(f'The original depth-5 threshold classifier, selected before its 2024 test, achieved mean Brier {test["mean_brier"]:.8f} against frequency {baseline:.8f}, a {100*skill:.2f}% reduction on 6,338,426 eligible anchors per cut. Its development mean was 0.01758544. These are preserved historical classifier results and do not evaluate the current conditional-CDF model. The selected model used development folds only and refit through 2023; 2024 has not been rescored or used to select the new model. An independent later test would be needed to establish its final generalization performance.')
h('Computing environments across A/B/C')
p('A full raw processing was recorded on Windows with Intel i7-12700H; current Mac sample replay verifies a narrower scope and is not a new full raw run. The B rerun records Apple M5, 24 GB memory, 10 logical CPUs, macOS 26.6.2, local[2], 4g heap and 128 shuffle partitions. The C modelling run records Python 3.11.17, Spark 3.5.7, Java 17.0.20.1, macOS 14.1 arm64, 14 logical CPUs, local[2], 6g and 128 shuffle partitions. These are separate saved execution records; no common-machine speed comparison or measured peak-memory claim is made.')
h('Conclusions and submission')
p(f'Coverage-aware Task 1 supports seasonal occurrence/intensity differences, four fixed eight-year same-month comparisons and additional local adjacent-year contrasts. It does not establish national annual trends. Task 2 selects a continuous-threshold conditional distribution with development mean Brier {selected_brier:.8f}, {selected_skill:.2f}% lower than the stronger persistence baseline. Persistence features give a small model-conditional gain; calendar/station additions do not. Month and history-state diagnostics show heterogeneous error, and overlapping windows are correlated. Arbitrary input support does not establish every-threshold or live-deployment reliability.')
p('intro.txt requires complete source with run README, a trained model and a PDF naming all members by 18 October 2026. These materials are prepared; Canvas upload is not performed. Remaining improvement ownership is documented separately in remaining_improvement_ownership.md.')
page();h('AI usage declaration')
for paragraph in (ROOT/'submission/AI_USAGE_DECLARATION.md').read_text().split('\n\n'):
 if paragraph.strip() and not paragraph.startswith('#'):p(paragraph.strip())
h('Data and source references')
p('NEA Historical Rainfall across Singapore: https://data.gov.sg/collections/2279/view. Spark 3.5.7 MLlib classification/regression and pipelines: https://spark.apache.org/docs/3.5.7/ml-classification-regression.html and https://spark.apache.org/docs/3.5.7/ml-pipeline.html. Evidence directories and identities are listed in integration_sources.json; CURRENT_EVIDENCE.md indexes model checks and the historical test. Signed provenance remains in submission/reproduction/frozen_release_metadata.zip.')
(OUT/'integration_sources.json').write_text(json.dumps({'B_task1':str(B),'original_C_run':str(C),'revision_C_run':str(revision),'revision_selection_sha256':hashlib.sha256((revision/'selection.json').read_bytes()).hexdigest(),'2024_policy':'Historical original model only; current revision not rescored'},indent=2)+'\n')
# Place appendices after the main conclusions.
first=next(i for i,b in enumerate(blocks) if b.get('text','').startswith('Appendix:'))
last=next(i for i,b in enumerate(blocks) if b.get('text','').startswith('Historical benchmark:'))
blocks=blocks[:first-1]+blocks[last-1:]+blocks[first-1:last-1]
# Keep the main report concise and avoid isolated overflow paragraphs.
filtered=[]
intro=False
for block in blocks:
 if block['type']=='heading':
  intro=block['text']=='1. Introduction and main findings'
  if block['text'].startswith('4. Task 1:') or block['text'].startswith('9. Limitations'):
   if filtered and filtered[-1]['type']!='pagebreak':filtered.append({'type':'pagebreak'})
  if block['text'].startswith('10. AI usage') and filtered and filtered[-1]['type']=='pagebreak':filtered.pop()
 if intro and block['type']=='paragraph' and not block['text'].startswith('This project uses'):continue
 filtered.append(block)
blocks=filtered
# Editable English version.
md=['# Singapore rainfall patterns and 30-minute exceedance prediction','DSA5208 Project 2 | 2017-2024']
md+=['## Group members',*([f"- {m['name']} - Student ID: {m['student_id']}" for m in members] if complete else ['Member names and student IDs pending. This required cover detail has not been invented.'])]
for b in blocks:
 if b['type']=='heading':md.append('## '+b['text'])
 elif b['type']=='paragraph':md.append(b['text'])
 elif b['type']=='figure':md.append(f"![{b['caption']}]({b['path']})\n\n{b['caption']}")
 elif b['type']=='table':md.append('\n'.join(['| '+' | '.join(x.replace('|',r'\|') for x in b['headers'])+' |','|'+'|'.join(['---']*len(b['headers']))+'|']+['| '+' | '.join(str(x).replace('|',r'\|') for x in row)+' |' for row in b['rows']]))
(OUT/'Final_Report_EN.md').write_text('\n\n'.join(md)+'\n')
(OUT/'report_blocks.json').write_text(json.dumps(blocks,ensure_ascii=False,indent=2))
# PDF embeds a Chinese-capable TrueType font so names remain renderable.
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,Image,PageBreak,KeepTogether
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
font=os.environ.get('C_FONT_PATH','/System/Library/Fonts/STHeiti Light.ttc')
if Path(font).is_file():pdfmetrics.registerFont(TTFont('ReportBody',font,subfontIndex=0 if 'C_FONT_PATH' in os.environ else 1))
else:
 from matplotlib import font_manager
 pdfmetrics.registerFont(TTFont('ReportBody',font_manager.findfont('DejaVu Sans')))
body=ParagraphStyle('body',fontName='ReportBody',fontSize=9.5,leading=13,spaceAfter=7)
heading=ParagraphStyle('heading',parent=body,fontSize=15,leading=20,spaceAfter=12,keepWithNext=True,textColor=colors.HexColor('#087f8c'))
cell=ParagraphStyle('cell',parent=body,fontSize=8,leading=11,spaceAfter=0)
caption=ParagraphStyle('caption',parent=body,fontSize=8.5,leading=12,textColor=colors.HexColor('#53616d'),spaceAfter=10)
story=[]
def para(s,style=body):return Paragraph(html.escape(str(s)),style)
story +=[Spacer(1,55),para('Singapore rainfall patterns and 30-minute exceedance prediction',ParagraphStyle('title',parent=heading,fontSize=25,leading=33)),Spacer(1,16),para('DSA5208 Project 2 | 2017-2024'),Spacer(1,20),para('Group members',heading)]
if complete:
 for m in members:story.append(para(m['name']+' | Student ID: '+m['student_id']))
else:story +=[para('MEMBER DETAILS PENDING',heading),para('The required names and student IDs must be supplied before this report is submitted to Canvas.')]
story +=[Spacer(1,25),para('Abstract',heading),para(f'Spark cleaning retains 48.54 million rainfall observations with exact-time windows and explicit missingness. Coverage-aware exploration includes seasonal, diurnal, fixed-panel, adjacent-year and spatial comparisons. Three development folds compare twelve model candidates, leaf-size comparisons and controlled feature groups. The selected conditional-distribution model uses 22 past-only features and achieves mean Brier {selected_brier:.8f}, {selected_skill:.2f}% lower than a stronger persistence baseline. It accepts continuous nonnegative thresholds with strict exceedance and monotone empirical probabilities. Calibration and group diagnostics bound the conclusions. The original 2024 test is retained only as a historical benchmark; the new model has no new untouched final test.')]
for b in blocks:
 kind=b['type']
 if kind=='pagebreak':story.append(PageBreak())
 elif kind=='heading':story.append(para(b['text'],heading))
 elif kind=='paragraph':story.append(para(b['text']))
 elif kind=='figure':
  story.append(Image(str(OUT/b['path']),width=495,height=b['height'],kind='proportional'));story.append(Spacer(1,4));story.append(para(b['caption'],caption))
 else:
  width=495/len(b['headers']);content=[[para(x,cell) for x in row] for row in [b['headers']]+b['rows']]
  tab=Table(content,colWidths=[width]*len(b['headers']),repeatRows=1,hAlign='LEFT');tab.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e5f1f2')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f7f9fa')]),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),('LINEBELOW',(0,0),(-1,0),.5,colors.HexColor('#087f8c'))]));story.extend([KeepTogether([tab,Spacer(1,8)])] if len(b["rows"])<=6 else [tab,Spacer(1,8)])
def footer(canvas,doc):
 canvas.setFont('ReportBody',8);canvas.setFillColor(colors.HexColor('#697580'));canvas.drawString(40,22,'DSA5208 Project 2 | Singapore rainfall | 2017-2024');canvas.drawRightString(A4[0]-40,22,str(doc.page))
SimpleDocTemplate(str(OUT/'Final_Report.pdf'),pagesize=A4,leftMargin=40,rightMargin=40,topMargin=35,bottomMargin=42,title='DSA5208 Project 2: Singapore Rainfall',author='; '.join(m['name'] for m in members) if complete else 'Group member details pending').build(story,onFirstPage=footer,onLaterPages=footer)
print('Created integrated English report; member cover complete:',complete)
