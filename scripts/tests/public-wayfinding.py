#!/usr/bin/env python3
"""Check that public navigation and action labels are useful and point somewhere.

Run on authored pages, before heavyweight studio compilation; the Journal
renderer and its own contract tests independently verify generated articles.
Native /matrix/ hosts are a separate authenticated route authority.
"""
from html.parser import HTMLParser
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site"
PAGES = {
    "index.html": "",
    "explore.html": "",
    "market.html": "",
    "docs.html": "",
    "discover/index.html": "../",
    "fieldwork/index.html": "../",
    "series/index.html": "../",
    "modules/index.html": "../",
    "blog/index.html": "../",
}
LABELS = [
    "Home", "Businesses", "Applications", "Journal",
    "Four series", "Technology", "Discuss a project ↗"
]
CONTACT = "https://aurekai-work-samples.pages.dev/#ask"

class Navigation(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_navigation = False
        self.links = []
        self.current = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "nav" and attrs.get("aria-label") in (
            "Main navigation", "Site navigation", "Technical navigation"
        ):
            self.in_navigation = True
        elif tag == "a" and self.in_navigation:
            self.current = {"href": attrs.get("href", ""), "text": ""}

    def handle_data(self, data):
        if self.current is not None:
            self.current["text"] += data

    def handle_endtag(self, tag):
        if tag == "a" and self.current is not None:
            self.current["text"] = " ".join(self.current["text"].split())
            self.links.append(self.current)
            self.current = None
        elif tag == "nav" and self.in_navigation:
            self.in_navigation = False

def check(condition, message):
    if not condition:
        raise AssertionError(message)

for name, prefix in PAGES.items():
    src = (SITE / name).read_text(encoding="utf-8")
    doc = Navigation()
    doc.feed(src)
    links = doc.links
    expected = [
        prefix, prefix + "explore.html", prefix + "discover/",
        prefix + "blog/", prefix + "series/",
        prefix + "docs.html", CONTACT,
    ]
    check(len(links) == len(expected), f"{name}: expected {len(expected)} primary navigation links, got {links}")
    check([link["text"] for link in links] == LABELS, f"{name}: labels drifted: {links}")
    check([link["href"] for link in links] == expected, f"{name}: destinations drifted")
    for href in expected[:-1]:
        dest = (SITE / Path(name).parent / href)
        if href.endswith("/") or href == "":
            dest = dest / "index.html"
        check(dest.resolve().is_file(), f"{name}: dead local navigation target: {href}")

directory = (SITE / "discover/index.html").read_text(encoding="utf-8")
check(directory.count('data-kind="studio"') == 20, "Public application count changed")
check(directory.count(">Try this application ↗</a>") == 20, "Public demo entry actions aren't clear")
check(directory.count(">View source code ↗</a>") == 20, "Public demo source actions aren't clear")
check(directory.count(">Open editing workspace ↗</a>") >= 5, "Editing workspaces lost their clear labels")
check(directory.count(">Open this tool ↗</a>") == 4, "Public tool actions aren't clear")
home = (SITE / "index.html").read_text(encoding="utf-8")
check("Browse public application examples" in home, "Homepage main action lost")
check("Read the four essay series" in home, "Homepage series entry lost")
market = (SITE / "market.html").read_text(encoding="utf-8")
check("Read the related article" in market, "Research references lost")
workflow = (ROOT / ".github/workflows/pages.yml").read_text(encoding="utf-8")
check("name:'Application examples',exact:true" in workflow,
      "Browser validation still expects the old Studios filter name")
check("scripts/tests/public-wayfinding.py" in workflow, "Pages publication lacks navigation test")
check("needs.current.outputs.latest" in workflow and "steps.retention.outputs.superseded" in workflow,
      "Outdated Pages build could publish stale public copy")
check("github.actor != 'github-actions[bot]'" in workflow,
      "Generated release commits may recursively restart the expensive pipeline")
retention = (ROOT / "scripts/retain-tested-pages.mjs").read_text(encoding="utf-8")
check("PAGES_RETENTION_SUPERSEDED" in retention and "superseded=true" in retention,
      "Superseded builds must stop before publication instead of retrying indefinitely")
renderer = (ROOT / "scripts/render-editorial.sh").read_text(encoding="utf-8")
check("Discuss a project ↗" in renderer and "Journal homepage navigation is missing" in renderer,
      "Generated Journal navigation isn't tied to public navigation")

print(f"PUBLIC_WAYFINDING_PASS {len(PAGES)} entry pages, 20 demo actions, 20 source links and verified local destinations")
