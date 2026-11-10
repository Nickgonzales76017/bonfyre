#!/usr/bin/env python3
"""Render a prepared product review as a PDF companion to the catalogue workspace.

Usage: python3 product-review-pdf.py INPUT.json OUTPUT.pdf (requires reportlab)
Dependencies: product-review-requirements.txt beside this file.
This renderer reads local material. It never fetches, publishes or contacts anyone.
"""
import json
import sys
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Image


def render(source_path, output_path):
    review = json.loads(Path(source_path).read_text())
    if review.get("schema") != "service-site.product-copy-review.v1" or review.get("state") != "prepared_for_review":
        raise ValueError("Expected a prepared product-copy review.")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    ink, muted, accent = colors.HexColor("#202623"), colors.HexColor("#52605A"), colors.HexColor("#A93329")
    normal = ParagraphStyle("body", fontName="Helvetica", fontSize=9, leading=12, textColor=ink, spaceAfter=5)
    small = ParagraphStyle("small", parent=normal, fontSize=8, leading=10, textColor=muted)
    heading = ParagraphStyle("heading", parent=normal, fontName="Helvetica-Bold", fontSize=11, leading=14, spaceBefore=9, spaceAfter=6)
    title = ParagraphStyle("title", parent=normal, fontName="Helvetica-Bold", fontSize=21, leading=24, spaceAfter=8)
    label = ParagraphStyle("label", parent=small, fontName="Helvetica-Bold", textColor=accent)
    p = lambda text, style=normal: Paragraph(escape(text), style)
    story = [p(review["client"] + " / PRODUCT COPY REVIEW", label), p(review["title"], title), p(review["summary"])]
    image_path = review["source"]["image_path"]
    width, height = ImageReader(image_path).getSize()
    photograph = Image(image_path, width=112, height=112 * height / width)
    copy = review["proposed_copy"]
    introduction = [p("Proposed product copy", heading), p(copy["description"])]
    introduction.extend(p("• " + text) for text in copy["features"])
    panel = Table([[introduction, photograph]], colWidths=[365, 130])
    panel.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 10)]))
    story += [panel, p("Search listing and image", heading)]
    rows = [[p(key, small), p(value)] for key, value in [
        ("Search title", copy["seo_title"]), ("Search description", copy["seo_description"]),
        ("Image filename", copy["image_filename"]), ("Image alt text", copy["image_alt"]),
        ("Collection candidates", "; ".join(copy["collection_proposals"])),
        ("Legacy price", review["source"]["price_observed"] + ". " + review["source"]["price_note"])
    ]]
    table = Table(rows, colWidths=[112, 383])
    table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 4), ("LINEBELOW", (0, 0), (-1, -1), .3, colors.HexColor("#DFE3DF"))]))
    story += [table, p("Before this goes into Shopify", heading)]
    story.extend(p("• " + question) for question in review["questions"])
    story.append(p(review["review_notes"][2], small))
    story.append(p(review["handoff"]))
    url, image_url = review["source"]["url"], review["source"]["image_url"]
    date = datetime.fromisoformat(review["source"]["observed_at"].replace("Z", "+00:00")).astimezone(ZoneInfo("America/Chicago")).strftime("%B %d, %Y")
    story.append(Paragraph('Sources: <link href="' + escape(url, {'"': '&quot;'}) + '">Legacy product page</link> and <link href="' + escape(image_url, {'"': '&quot;'}) + '">product photo</link>. Read ' + date + ', Central Time. Proposed copy; pending client review.', small))

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#DFE3DF"))
        canvas.line(50, 39, A4[0] - 50, 39)
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(muted)
        canvas.drawString(50, 25, "Prepared by Nick / Aurekai")
        canvas.drawRightString(A4[0] - 50, 25, str(doc.page))
        canvas.restoreState()

    SimpleDocTemplate(str(output), pagesize=A4, rightMargin=50, leftMargin=50, topMargin=36, bottomMargin=50, title=review["client"] + " product copy review", author="Nick Gonzales / Aurekai").build(story, onFirstPage=footer, onLaterPages=footer)
    print(json.dumps({"output": str(output.resolve()), "state": review["state"], "transmitted": False, "merchant_published": False}))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: product-review-pdf.py INPUT.json OUTPUT.pdf")
    render(sys.argv[1], sys.argv[2])
