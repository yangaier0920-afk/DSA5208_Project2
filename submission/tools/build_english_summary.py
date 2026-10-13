"""Write an actual short English summary from explicitly selected report evidence."""
import argparse,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
ap=argparse.ArgumentParser();ap.add_argument('--report-dir',type=Path,default=ROOT/'submission');a=ap.parse_args();out=a.report_dir.resolve()
source=out/'report_blocks.json';blocks=json.loads(source.read_text());paragraphs=[]
for b in blocks:
 if b['type']=='paragraph' and any(b['text'].startswith(prefix) for prefix in [
    'This project uses','2018 has no station','Only May, September','At the 90% rule','The final refit uses',
    'The five-threshold mean Brier','The selected model','OpenAI Codex','Selected continuous candidate:',
    'All seven adjacent-year','All depth-8 CDF','The original depth-5 threshold','Coverage-aware Task 1','Training-only conditional empirical']):paragraphs.append(b['text'])
# Bound length and preserve scientific caveats rather than copying all tables/figures.
text=['# DSA5208 Project 2: concise results summary',
      'This summary is shorter than the full report. See Final_Report_EN.md / Final_Report.pdf for figures, tables, methods and limitations.']
text+=paragraphs[:14]
text+=['Inference assumes recorded historical rainfall is available at the event timestamp; actual publication latency is unverified. Missing rainfall is unknown, and comparisons/predictions apply to their documented coverage and input scope. Overlapping windows are not independent observations.',
       'All group member names and IDs are in members.json and the PDF cover. Source, model and README are supplied separately. Canvas upload is not performed by this script.']
path=out/'Final_Report_Summary_EN.md';path.write_text('\n\n'.join(text)+'\n')
assert path.read_bytes()!=(out/'Final_Report_EN.md').read_bytes()
print('Saved genuine concise English summary:',path)
