from pathlib import Path
import sys

sys.path.insert(0, '/private/tmp/aurekai-pdf-deps')
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'output/pdf/aurekai-project-profile.pdf'
OUT.parent.mkdir(parents=True, exist_ok=True)
c = canvas.Canvas(str(OUT), pagesize=(612, 792))
c.setTitle('Nick Gonzales | Aurekai project profile')
c.setAuthor('Nick Gonzales')
c.setSubject('Independent web development work samples')
INK = '#1b1c1a'
GREEN = '#1b3b2b'
MUTED = '#4b514b'
styles = {
    'body': ParagraphStyle('body', fontName='Helvetica', fontSize=10.5, leading=15, textColor=HexColor(INK)),
    'small': ParagraphStyle('small', fontName='Helvetica', fontSize=9, leading=13, textColor=HexColor(MUTED)),
    'title': ParagraphStyle('title', fontName='Helvetica-Bold', fontSize=15, leading=19, textColor=HexColor(INK)),
}

def para(text, x, top, width=504, style='body'):
    p = Paragraph(text, styles[style])
    _, h = p.wrap(width, 800)
    p.drawOn(c, x, top-h)
    return top-h

def text(s, x, y, size=10, font='Helvetica', color=INK):
    c.setFillColor(HexColor(color))
    c.setFont(font, size)
    c.drawString(x, y, s)

def link(label, url, x, y):
    text(label, x, y, 10, 'Helvetica-Bold', GREEN)
    width = c.stringWidth(label, 'Helvetica-Bold', 10)
    c.linkURL(url, (x, y-3, x+width, y+11), relative=0, thickness=0)

c.setFillColor(HexColor('#fbf9f5'))
c.rect(0, 0, 612, 792, fill=1, stroke=0)
text('AUREKAI', 54, 747, 11, 'Helvetica-Bold', GREEN)
text('Independent project work', 374, 747, 10, color=MUTED)
text('Nick Gonzales', 54, 705, 30, 'Times-Roman')
text('Custom web tools and integrations', 54, 678, 16)
text('Dallas, Texas  /  US Central  /  Remote delivery', 54, 653, 10, color=MUTED)
link('nickgonzales76017@gmail.com', 'mailto:nickgonzales76017@gmail.com', 54, 632)
link('Open the work samples', 'https://aurekai-work-samples.pages.dev/', 366, 632)

para('I build tools for product catalogues, project planning and the manual work between systems. I can take a defined feature through implementation and review alongside a business or agency team.', 54, 605)

def sample(top, title, body, evidence, label, url):
    height = 142
    c.setFillColor(HexColor('#ffffff'))
    c.roundRect(54, top-height, 504, height, 7, fill=1, stroke=0)
    para(title, 72, top-17, 468, 'title')
    para(body, 72, top-44, 468)
    para(evidence, 72, top-85, 468, 'small')
    link(label, url, 72, top-125)

sample(539, 'Product catalogue editor',
       'Search a catalogue, edit descriptions and options, then save a draft and compare it with the original. Submit the draft for review and keep a receipt of that version.',
       "Checked on desktop and mobile, including reload, invalid input and stale edits. Publishing to a merchant's store requires a separate integration.",
       'Try the catalogue editor', 'https://aurekai-work-samples.pages.dev/catalogue/?store=red3-display')
sample(383, 'Retail fixture planner',
       'Arrange fixtures, adjust quantities and compare calculated costs. Keep open questions with the plan, share a copy and print a project brief.',
       'Checked on desktop and mobile, including save and reload. Supplier measurements, final prices and availability still need confirmation.',
       'Try the fixture planner', 'https://aurekai-work-samples.pages.dev/red3-planner/')

para('Working together', 54, 214, style='title')
para('Send the task, your current tools and any deadline. Introductory calls are free. We agree the scope and acceptance checks before pricing, review a working implementation, and arrange deployment and handoff.', 54, 185)
para('These are independent Aurekai demonstrations using public or sample data. Named businesses have not commissioned or endorsed them.', 54, 111, style='small')
text('aurekai-work-samples.pages.dev', 54, 46, 9, color=MUTED)
text('October 2026', 501, 46, 9, color=MUTED)
c.save()

reader = PdfReader(OUT)
assert len(reader.pages) == 1
body = ' '.join(reader.pages[0].extract_text().split())
assert all(s in body for s in ['Nick Gonzales', 'Product catalogue editor', 'Retail fixture planner', 'have not commissioned or endorsed'])
links = [a.get_object()['/A']['/URI'] for a in reader.pages[0].get('/Annots', [])]
assert len(links) == 4
print({'path': str(OUT), 'pages': len(reader.pages), 'links': links, 'bytes': OUT.stat().st_size})
