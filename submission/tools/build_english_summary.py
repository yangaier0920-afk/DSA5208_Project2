"""Generate an English editable summary from the integrated report."""
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
source=ROOT/'submission/Final_Report_EN.md'
text=source.read_text(encoding='utf-8')
(ROOT/'submission/Final_Report_Summary_EN.md').write_text(text,encoding='utf-8')
print('Saved English editable report companion')
