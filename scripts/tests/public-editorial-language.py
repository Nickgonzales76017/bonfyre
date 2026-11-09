#!/usr/bin/env python3
"""Guard reader-facing Bonfÿre pages against opaque project jargon.

Checks visible copy only. Do not rename native binaries, Matrix/WorkGraph APIs,
repository slugs, approved technical documentation, or original-source titles.
"""
from html.parser import HTMLParser
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[2]
SITE=ROOT/"site"
PAGES=[
    "index.html", "explore.html", "market.html", "fieldwork/index.html",
    "discover/index.html", "series/index.html", "matrix/index.html",
]
PHRASES=re.compile(r"\b(?:atlas|matrix|workgraph|constellation|fabric|aperture|estate)\b",re.I)
OLD_DEMO_TITLES=[
    "Podcast Repurposing Plant","Sales Call Distiller","Customer Voice Observatory",
    "Release-Note Radio","Async Standup Newspaper","Competitive Intelligence Scrapbook",
    "Explain This Repo","OSS Maintainer Cockpit","Incident Postmortem Atlas",
    "Shift Handoff Board","Personal Memory Atlas","Family Oral History Museum",
    "Local Archive Explorer","Museum Exhibit Builder","Town in a Box",
    "Grant-Writing Evidence Base","Procurement Memory Site","Freelancer Evidence Machine",
    "Micro-Consulting Storefront","Personal Legal Prep Binder",
]
class Visible(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden_depth=0
        self.text=[]
        self.titles=[]
        self.in_h3=False
    def handle_starttag(self,tag,attrs):
        if tag in ("script","style","template","noscript"):
            self.hidden_depth+=1
        elif tag=="h3" and not self.hidden_depth:
            self.in_h3=True
    def handle_endtag(self,tag):
        if tag in ("script","style","template","noscript"):
            self.hidden_depth=max(0,self.hidden_depth-1)
        elif tag=="h3":
            self.in_h3=False
    def handle_data(self,data):
        if self.hidden_depth or not data.strip():return
        self.text.append(data)
        if self.in_h3:self.titles.append(data.strip())
def parse(path):
    html=path.read_text(encoding="utf-8")
    document=Visible()
    document.feed(html)
    return html,document
def require(condition,reason):
    if not condition:raise AssertionError(reason)
for path in PAGES:
    html,page=parse(SITE/path)
    text=" ".join(page.text)
    match=PHRASES.search(text)
    if match:
        raise AssertionError(f"Reader-facing jargon in {path}: {match.group(0)!r}, context={text[max(0,match.start()-42):match.end()+45]!r}")
    require("<h1" in html,f"Missing page heading: {path}")
directory,disc=parse(SITE/"discover"/"index.html")
require(directory.count('class="discovery-card"')==24,"Directory must retain 20 public examples plus 4 tools")
require(directory.count('data-kind="studio"')==20,"Missing a public application example")
for title in OLD_DEMO_TITLES:
    require(title not in disc.titles, f"Old demo title exposed as reader-facing copy: {title}")
for title in ("Make clips and articles from a podcast","Review customer interviews",
              "Review incidents and timelines","Organize recordings and memories",
              "Code and reference libraries"):
    require(title in " ".join(disc.text),f"Missing clear application name: {title}")
fieldwork,fw=parse(SITE/"fieldwork"/"index.html")
require("46 published essays" not in fieldwork and "46 authored articles" not in fieldwork,"Stale Journal count in Fieldwork")
public_root,_=parse(SITE/"matrix"/"index.html")
for attribute in ('hx-get="/_surface/intake"','href="/studio/#matrix"','href="/studio/#work"'):
    require(attribute in public_root,f"Public page lost functioning entry: {attribute}")
modules=(SITE/"modules"/"index.html").read_text(encoding="utf-8")
require("<title>Bonfÿre — Code and Reference Libraries</title>" in modules,"Source library title regressed")
guide=SITE/"blog"/"connections"/"index.html"
require(guide.exists(),"Generated Reading Guide missing")
markup,g=parse(guide)
require("<title>Reading Guide — Aurekai Journal</title>" in markup,"Reading Guide title regressed")
require("Reading Guide" in " ".join(g.text),"Reading Guide lacks a visible name")
require("atlas" not in " ".join(g.text).lower(),"Internal label appears in public Reading Guide")
print("PUBLIC_EDITORIAL_LANGUAGE_PASS: 7 general pages, 20 application examples, source libraries and generated Reading Guide")
