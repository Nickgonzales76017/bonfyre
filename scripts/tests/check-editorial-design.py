#!/usr/bin/env python3
"""Check every public Aurekai Journal story has a unique illustrated cover.

Run after render-editorial.sh; offline and stdlib-only.
"""
from pathlib import Path
from html.parser import HTMLParser
from hashlib import sha256
import re
import sys
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[2]
SRC=ROOT/"editorial/posts"
BLOG=ROOT/"site/blog"
layouts=set()
art_digests=set()
pages=0
class CheckMarkup(HTMLParser):
    def __init__(self):
        super().__init__()
        self.h1=0; self.hero=0; self.cover=0; self.toc=0
        self.layouts=[]
    def handle_starttag(self,tag,attrs):
        d=dict(attrs)
        if tag=="h1": self.h1+=1
        if tag=="body":
            self.layouts=[x for x in d.get("class","").split() if x.startswith("garden-") and x!="garden-hero"]
        if tag=="section" and "garden-hero" in d.get("class","").split(): self.hero+=1
        if tag=="img" and d.get("src")=="./cover.svg": self.cover+=1
        if tag=="aside" and "journal-article-side" in d.get("class",""): self.toc+=1

for source in sorted(SRC.glob("*.md")):
    if not re.search(r"(?m)^status: edited-public\s*$",source.read_text(encoding="utf-8")):
        continue
    slug=source.stem
    page=BLOG/slug/"index.html"
    art=BLOG/slug/"cover.svg"
    assert page.is_file() and art.is_file(), f"missing design for {slug}"
    markup=page.read_text(encoding="utf-8")
    parser=CheckMarkup()
    parser.feed(markup)
    assert parser.h1==1 and parser.hero==1 and parser.cover>=2, f"page structure: {slug}"
    assert parser.toc==1 and len(parser.layouts)==1, f"missing variant: {slug}"
    assert 'href="../article-garden.css"' in markup, f"missing stylesheet: {slug}"
    layouts.add(parser.layouts[0])
    data=art.read_bytes()
    assert len(data)>10000, f"cover too small: {slug}"
    digest=sha256(data).hexdigest()
    assert digest not in art_digests, f"duplicate tree artwork: {slug}"
    art_digests.add(digest)
    root=ET.fromstring(data)
    assert root.tag.endswith("svg"), f"invalid SVG root: {slug}"
    assert len([el for el in root.iter() if el.tag.endswith("path")])>=100, f"insufficient linework: {slug}"
    assert "<script" not in data.decode("utf-8").lower(), f"unexpected SVG script: {slug}"
    pages+=1

index=(BLOG/"index.html").read_text(encoding="utf-8")
assert index.count('class="journal-card-cover"')==pages, "journal previews not complete"
assert 'href="./article-garden.css"' in index, "missing journal cover styles"
css=(BLOG/"article-garden.css").read_text(encoding="utf-8")
assert "@media(max-width:780px)" in css, "missing small screen compositions"
assert "prefers-reduced-motion" in css, "missing reduced motion behavior"
assert pages>=46 and len(layouts)>=7, f"not enough independent designs: {pages} / {layouts}"
print(f"EDITORIAL_DESIGN_PASS {pages} unique tree drawings, {len(layouts)} editorial compositions, 1 masthead each, reading navigation and card art")
