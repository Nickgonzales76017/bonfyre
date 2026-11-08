#!/usr/bin/env python3
"""Fail a Journal release when public search/readability contracts drift.

This is a local artifact test, not proof of Google indexing or AI citation.
"""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site"
BLOG = SITE / "blog"
BASE = "https://nickgonzales76017.github.io/bonfyre/"
EXPECTED = "https://nickgonzales76017.github.io/bonfyre/blog/"


class Page(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.links = []
        self.tags = []
        self.scripts = []
        self.script = None
        self.h1 = 0
        self.text_nodes = 0

    def handle_starttag(self, tag, attrs):
        props = dict(attrs)
        self.tags.append((tag, props))
        if tag == "meta":
            name = props.get("name") or props.get("property")
            if name:
                self.meta[name] = props.get("content", "")
        if tag == "link":
            self.links.append(props)
        if tag == "h1":
            self.h1 += 1
        if tag == "script" and props.get("type") == "application/ld+json":
            self.script = ""

    def handle_data(self, data):
        if self.script is not None:
            self.script += data
        if data.strip():
            self.text_nodes += 1

    def handle_endtag(self, tag):
        if tag == "script" and self.script is not None:
            self.scripts.append(json.loads(self.script))
            self.script = None

def check(predicate, message):
    if not predicate:
        raise AssertionError(message)

def get_url_loc(node):
    return node.findtext("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")

def validate():
    manifest = BLOG / "post-index.tsv"
    rows = [r.split("\t") for r in manifest.read_text().strip().splitlines()[1:]]
    check(len(rows) >= 66, "Article manifest unexpectedly shrank")
    slugset = set()
    for row in rows:
        slug = row[1]
        check(slug not in slugset, f"Duplicate slug: {slug}")
        slugset.add(slug)
        path = BLOG / slug / "index.html"
        check(path.is_file(), f"Missing generated article: {slug}")
        markup = path.read_text(encoding="utf-8")
        doc = Page()
        doc.feed(markup)
        url = EXPECTED + slug + "/"
        canonical = [x.get("href") for x in doc.links if x.get("rel") == "canonical"]
        check(canonical == [url], f"Canonical mismatch: {slug}")
        check(doc.h1 == 1, f"Expected exactly one h1: {slug}")
        check(doc.meta.get("description"), f"No meta description: {slug}")
        check(doc.meta.get("og:title"), f"No OpenGraph title: {slug}")
        check(doc.meta.get("og:image"), f"No article OG image: {slug}")
        check(doc.meta.get("og:url") == url, f"OG URL mismatch: {slug}")
        check("noindex" not in doc.meta.get("robots", "").lower(), f"Accidental noindex: {slug}")
        scripts = doc.scripts
        article = next((x for x in scripts if x.get("@type") == "BlogPosting"), None)
        breadcrumb = next((x for x in scripts if x.get("@type") == "BreadcrumbList"), None)
        check(article is not None and breadcrumb is not None, f"Article/breadcrumb structured data missing: {slug}")
        check(article["url"] == url and article["headline"] == doc.meta["og:title"], f"Structured title/URL mismatch: {slug}")
        check(article.get("author",{}).get("name") == "Aurekai editorial", f"Author mismatch: {slug}")
        check(article.get("publisher",{}).get("name") == "Aurekai", f"Publisher mismatch: {slug}")
        check(article.get("datePublished") and article.get("dateModified"), f"Date metadata missing: {slug}")
        check(article.get("image") and article["image"][0] == doc.meta["og:image"], f"Article image mismatch: {slug}")
        check("journal-audio-player" in markup and 'data-narration-play' in markup and 'data-narration-seek' in markup and 'data-narration-wave' in markup, f"Missing playable audio edition: {slug}")
        check("reader-outline-menu" in markup and "journal-author-row" in markup, f"Missing outline/byline: {slug}")
        check("journal-sources" in markup, f"Missing references section: {slug}")
        check('src="../reader.js"' in markup, f"Missing reader script: {slug}")
        check('href="../reader.css"' in markup, f"Missing reader stylesheet: {slug}")
        check('href="../about/"' in markup, f"Missing About/author link: {slug}")
        check(doc.text_nodes > 25, f"Article rendered without readable prose: {slug}")
    sitemaps = [SITE / "sitemap.xml", BLOG / "sitemap.xml"]
    for path in sitemaps:
        check(path.exists(), f"Missing sitemap: {path}")
        urls = {get_url_loc(el) for el in ElementTree.parse(path).getroot()}
        check(all(EXPECTED + slug + "/" in urls for slug in slugset),
              f"Not every published article appears in {path}")
    check((BLOG / "reader.js").is_file(), "Narration script is missing")
    check((BLOG / "reader.css").is_file(), "Reader styles are missing")
    check((BLOG / "about" / "index.html").is_file(), "Editorial attribution page is missing")
    print(f"JOURNAL_SEARCH_READINESS_PASS {len(slugset)} crawlable HTML essays, structured data, footnotes, narration, bylines, and two sitemaps")
    print("This checks public-build readiness only; Google indexing requires Search Console verification.")

if __name__ == "__main__":
    try:
        validate()
    except Exception as exc:
        print(f"JOURNAL_SEARCH_READINESS_FAIL: {exc}",file=sys.stderr)
        sys.exit(1)
