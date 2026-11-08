#!/usr/bin/env python3
"""Deterministic Aurekai Journal crawl-readiness test. This is NOT proof of Google indexing."""
from pathlib import Path
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
import re

root = Path("site/blog")
index = root / "post-index.tsv"
rows = index.read_text(encoding="utf-8").strip().splitlines()
assert rows and rows[0].startswith("subject_id\t"), "missing canonical source index"
slugs = [line.split("\t")[1] for line in rows[1:]]
assert len(slugs) == 66, f"unexpected authored article count: {len(slugs)}"
assert len(set(slugs)) == len(slugs), "duplicate slugs"
assert (root / "about/index.html").exists(), "editorial identity unavailable"
assert (root / "reader.js").exists() and (root / "reader.css").exists(), "reader assets missing"
assert (Path("site") / "series/index.html").exists(), "series landing missing"

class ArticleParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta, self.links, self.ids = {}, [], set()
        self.headlines, self.articles = 0, 0
        self.widgets, self.outlines, self.authors = 0, 0, 0
        self.backlinks = []
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if a.get("id"):
            assert a["id"] not in self.ids, f"duplicate anchor {a['id']}"
            self.ids.add(a["id"])
        if tag == "h1": self.headlines += 1
        if "itemscope" in a and a.get("itemtype") == "https://schema.org/BlogPosting": self.articles += 1
        if a.get("itemprop") == "author": self.authors += 1
        if "data-narration" in a: self.widgets += 1
        if "data-outline-menu" in a: self.outlines += 1
        if tag == "meta": self.meta[(a.get("name") or a.get("property") or a.get("itemprop") or "")] = a.get("content","")
        if tag == "link": self.links.append(a)
        if tag == "a" and a.get("href","").startswith("#source-"): self.backlinks.append(a["href"][1:])

base = "https://nickgonzales76017.github.io/bonfyre/blog/"
for slug in slugs:
    page = root / slug / "index.html"
    assert page.exists(), f"missing rendered page {slug}"
    p = ArticleParser()
    p.feed(page.read_text(encoding="utf-8"))
    canonicals = [a.get("href") for a in p.links if a.get("rel") == "canonical"]
    assert canonicals == [base + slug + "/"], f"canonical mismatch {slug}"
    assert p.headlines == 1 and p.articles == 1, f"invalid semantic article {slug}"
    assert p.authors >= 1 and p.widgets == 1 and p.outlines == 1, f"missing editorial tools {slug}"
    assert p.meta.get("description") and p.meta.get("og:image"), f"missing searchable metadata {slug}"
    assert p.meta.get("robots","").startswith("index,follow"), f"indexing inhibited {slug}"
    assert p.meta.get("article:published_time") and p.meta.get("article:modified_time"), f"dates missing {slug}"
    assert "journal-sources" in p.ids, f"source section missing {slug}"
    for ref in p.backlinks:
        assert ref in p.ids, f"broken source link {slug} #{ref}"

ns = {"sm":"http://www.sitemaps.org/schemas/sitemap/0.9"}
urls = {el.text for el in ET.parse(root / "sitemap.xml").findall(".//sm:loc",ns)}
expected = {base,base+"about/","https://nickgonzales76017.github.io/bonfyre/series/"}
expected.update(base+slug+"/" for slug in slugs)
assert urls == expected, f"sitemap drift: missing={expected-urls}, extra={urls-expected}"
feed = (root / "feed.xml").read_text(encoding="utf-8")
assert feed.count("<item>") == len(slugs), "RSS inventory mismatch"
print(f"JOURNAL_CRAWL_READINESS_PASS {len(slugs)} pages / {len(urls)} sitemap URLs / source-linked HTML")
print("Search Console verification remains necessary: crawl readiness does not prove indexing or AI Overview inclusion.")
