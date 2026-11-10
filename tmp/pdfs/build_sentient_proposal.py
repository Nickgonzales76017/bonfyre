import json
import sys
from pathlib import Path
from html import escape

sys.path.insert(0, '/private/tmp/aurekai-pdf-deps')
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[2]
d = json.loads((ROOT / '.bonfyre/revenue/funding-followthrough-20261008/sentient-application-draft.json').read_text())
out = ROOT / 'output/pdf/sentient-grant-proposal.pdf'
ink = colors.HexColor('#1b1c1a')
green = colors.HexColor('#1b3b2b')
styles = {
 'title': ParagraphStyle('title',fontName='Times-Roman',fontSize=25,leading=29,textColor=ink,spaceAfter=13),
 'sub': ParagraphStyle('sub',fontName='Helvetica-Bold',fontSize=12,leading=16,textColor=green,spaceBefore=13,spaceAfter=6),
 'body': ParagraphStyle('body',fontName='Helvetica',fontSize=10,leading=14,textColor=ink,spaceAfter=8),
 'small': ParagraphStyle('small',fontName='Helvetica',fontSize=8.5,leading=12,textColor=ink,spaceAfter=6),
}
story=[]
def p(text,style='body'):
 story.append(Paragraph(escape(text),styles[style]))
def link(label,url):
 story.append(Paragraph(f'<a href="{escape(url,quote=True)}" color="#1b3b2b">{escape(label)}</a>',styles['small']))
def footer(c,doc):
 c.setFillColor(green); c.setFont('Helvetica-Bold',9);c.drawString(48,755,'AUREKAI / BONFYRE')
 c.setFont('Helvetica',8);c.drawRightString(564,755,'Draft for Nick review - not submitted')
 c.setFillColor(ink);c.setFont('Helvetica',8);c.drawString(48,29,'Nick Gonzales | nickgonzales76017@gmail.com | October 8, 2026')
 c.drawRightString(564,29,str(doc.page))

p(d['title'],'title')
p('Sentient public-goods grant proposal | Proposed request: $10,000')
p(d['one_line'])
p('Problem and intended users','sub')
p(d['problem'])
p(d['beneficiaries'])
p('Existing work','sub')
p(d['team'])
p('The verifier checks declared traces across separate task, authority, effect, evidence and recovery dimensions. A synthetic Helpdesk environment supplies deterministic reset, branching and replay. The current PR head passed 29 local tests on October 8. These checks do not establish independent adoption or prove that a trace emitter is truthful.')
p('The receipt-isolation fix is already implemented in draft PR 9. It is not part of the proposed grant budget and remains unmerged and unreleased.')
link('Public verifier, code and examples',d['demo_url'])
link('Draft PR 9: receipt-isolation repair','https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier/pull/9')
p('What stays open','sub')
p(d['openness'])
story.append(PageBreak())
p('Proposed work and budget','title')
p('The first integration would connect the verifier to ROMA tool boundaries. ROMA documents local file and MCP toolkits and execution-scoped storage. The exact adapter interface and supported versions would be established in the first milestone; no completed ROMA integration is claimed.')
for m in d['milestones']:
 p(m['window'],'sub');p(m['deliverable'])
p('Acceptance','sub')
for a in d['acceptance'][:3]:p(a)
p('Signatures would identify the emitter and expose altered evidence. They would not prove that the emitter is honest, that a model is safe, or that every possible action was captured. Unsupported observations would remain explicit.')
p('Proposed allocation','sub')
rows=[[Paragraph('Work',styles['small']),Paragraph('USD',styles['small'])]]
for b in d['budget']:
 rows.append([Paragraph(escape(b['scope']),styles['small']),Paragraph(f"${b['usd']:,}",styles['small'])])
rows.append([Paragraph('Total proposed request',styles['small']),Paragraph('$10,000',styles['small'])])
t=Table(rows,colWidths=[444,72],hAlign='LEFT')
t.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e8eee8')),('LINEBELOW',(0,-1),(-1,-1),.5,green),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
story.append(t);story.append(Spacer(1,10))
p('The schedule and allocation are estimates for review. No reviewer has been contracted. An August FUTO application covers related trace interoperability work and remains pending; the scope and cost allocation would be reconciled before accepting funds, with no cost claimed twice. No grant has been awarded or application submitted.','small')
doc=SimpleDocTemplate(str(out),pagesize=(612,792),leftMargin=48,rightMargin=48,topMargin=59,bottomMargin=49,title=d['title'],author='Nick Gonzales')
doc.build(story,onFirstPage=footer,onLaterPages=footer)
r=PdfReader(out)
assert len(r.pages)==2,len(r.pages)
text=' '.join(' '.join(x.extract_text().split()) for x in r.pages)
assert '$10,000' in text and 'FUTO' in text and 'unmerged and unreleased' in text
print({'pdf':str(out),'pages':len(r.pages),'bytes':out.stat().st_size})

fields=[('Email',d['applicant']['email']),('LinkedIn',d['applicant']['linkedin']),('Primary role',d['applicant']['role']),('Location',d['applicant']['location']),('What problem are you solving, and why now?',d['problem']),('Who does this help?',d['beneficiaries']),('In one line, what are you building?',d['one_line']),('Who is building this, and why is your team the right one to do it?',d['team']),('What is open about it?',d['openness']),('Demo or trial link',d['demo_url']),('Track',d['track']),('Grant funding requested','10k (proposed, subject to Nick review)'),('What would the grant unlock?',d['unlock']),('Required supporting upload','sentient-grant-proposal.pdf')]
body='SENTIENT APPLICATION COPY - DRAFT, NOT SUBMITTED\n\n'+'\n\n'.join(k+'\n'+v for k,v in fields)+'\n\nBEFORE SUBMISSION\n'+'\n'.join(d['review_before_submission'])+'\n'
(ROOT/'output/pdf/sentient-application-copy.txt').write_text(body)
