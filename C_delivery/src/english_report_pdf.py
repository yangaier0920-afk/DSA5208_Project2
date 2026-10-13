"""Render English evidence reports with embedded tables and figures."""
import re,html
from pathlib import Path
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,Image,PageBreak,KeepTogether
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

def render(markdown,pdf):
    markdown=Path(markdown);pdf=Path(pdf);pdf.parent.mkdir(parents=True,exist_ok=True)
    styles=getSampleStyleSheet();styles['BodyText'].fontSize=9;styles['BodyText'].leading=13
    story=[];lines=markdown.read_text().splitlines();i=0
    def para(s,style='BodyText'):
        s=html.escape(s);s=re.sub(r'\[([^]]+)\]\((https?://[^)]+)\)',r'<link href="\2" color="#087f8c">\1</link>',s);s=re.sub(r'`([^`]+)`',r'<font name="Courier">\1</font>',s);s=re.sub(r'\*\*([^*]+)\*\*',r'<b>\1</b>',s)
        story.append(Paragraph(s,styles[style]))
        if not style.startswith('Heading'):story.append(Spacer(1,6))
    while i<len(lines):
        line=lines[i].strip();i+=1
        if not line:continue
        if line=='<!-- PAGEBREAK -->':story.append(PageBreak());continue
        if line.startswith('|'):
            rows=[]
            while True:
                vals=[v.strip() for v in line.strip('|').split('|')]
                if not all(re.fullmatch(r'[: -]+',v) for v in vals):rows.append(vals)
                if i>=len(lines) or not lines[i].strip().startswith('|'):break
                line=lines[i].strip();i+=1
            n=max(map(len,rows));rows=[r+['']*(n-len(r)) for r in rows]
            content=[[Paragraph(html.escape(v),styles['BodyText']) for v in r] for r in rows]
            t=Table(content,colWidths=[515/n]*n,repeatRows=1);t.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e5f1f2')),('GRID',(0,0),(-1,-1),.2,colors.lightgrey)]));story.append(KeepTogether([t,Spacer(1,9)]) if len(rows)<=6 else t)
            if len(rows)>6:story.append(Spacer(1,9))
            continue
        m=re.match(r'!\[([^]]*)\]\(([^)]+)\)',line)
        if m:
            p=markdown.parent/m[2]
            if p.is_file():
                from PIL import Image as PI
                with PI.open(p) as im:w,h=im.size
                scale=min(515/w,300/h);story.append(Image(str(p),width=w*scale,height=h*scale));para(m[1])
            continue
        if line.startswith('#'):para(line.lstrip('# '),'Heading1' if line.startswith('# ') else 'Heading2')
        else:para(line)
    SimpleDocTemplate(str(pdf),pagesize=A4,leftMargin=40,rightMargin=40,topMargin=35,bottomMargin=35).build(story)
