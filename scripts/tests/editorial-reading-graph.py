#!/usr/bin/env python3
"""Verify editorial reading connections and truthful index signals.

No claims about Google crawling or inclusion in AI-generated search answers.
"""
from html.parser import HTMLParser
from pathlib import Path
from xml.etree import ElementTree
import json

site=Path("site")
blog=site/"blog"
source=Path("editorial/reading-bridges.tsv")
base="https://nickgonzales76017.github.io/bonfyre/blog/"
rows=[x.split("\t") for x in (blog/"post-index.tsv").read_text().splitlines()[1:]]
posts={x[1]:x for x in rows if x and x[0].startswith("post:")}
edges=[]
for n,line in enumerate(source.read_text().splitlines(),1):
    if not line.strip() or line.startswith("#"): continue
    fields=line.split("\t")
    assert len(fields)==4,f"bad relation at {n}"
    a,relation,b,reason=fields
    assert a in posts and b in posts and a!=b and relation and reason,f"bad relation at {n}"
    assert (a,b) not in {(e[0],e[2]) for e in edges},f"duplicate relation at {n}"
    edges.append(fields)
assert len(edges)>=40 and len({e[0] for e in edges})>=20

class Article(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids=set();self.links=[];self.schema=[];self.collecting=False;self.payload=""
    def handle_starttag(self,tag,attrs):
        attr=dict(attrs)
        if attr.get("id"):self.ids.add(attr["id"])
        if tag=="a":self.links.append(attr.get("href",""))
        if tag=="script" and attr.get("type")=="application/ld+json":
            self.collecting=True;self.payload=""
    def handle_data(self,data):
        if self.collecting:self.payload+=data
    def handle_endtag(self,tag):
        if tag=="script" and self.collecting:
            self.schema.append(json.loads(self.payload))
            self.collecting=False

refs={}
for a,_,b,_ in edges:
    refs.setdefault(a,set()).add(b)
    refs.setdefault(b,set()).add(a)
for slug, targets in refs.items():
    path=blog/slug/"index.html"
    assert path.is_file(),f"no rendered article {slug}"
    p=Article();p.feed(path.read_text())
    assert "journal-connections" in p.ids,f"missing editorial conversation: {slug}"
    urls=set(p.links)
    for target in targets:
        assert "../"+target+"/" in urls,f"missing cross-reading link {slug} → {target}"
    article=next((x for x in p.schema if x.get("@type")=="BlogPosting"),None)
    assert article,f"missing JSON-LD article {slug}"
    metadata=set(article.get("relatedLink",[]))
    for target in targets:
        assert base+target+"/" in metadata,f"missing structured relationship {slug} → {target}"

ns={"s":"http://www.sitemaps.org/schemas/sitemap/0.9"}
for sitemap in [site/"sitemap.xml",blog/"sitemap.xml"]:
    raw=ElementTree.parse(sitemap)
    data={}
    for item in raw.findall(".//s:url",ns):
        loc=item.findtext("s:loc",namespaces=ns)
        date=item.findtext("s:lastmod",namespaces=ns)
        data[loc]=date
    for slug,row in posts.items():
        url=base+slug+"/"
        assert url in data,f"not listed in {sitemap}: {slug}"
        if data[url] is not None:
            assert len(data[url])==10 and data[url][4]=="-" and data[url][7]=="-",f"bad lastmod {slug}"
    assert data.get("https://nickgonzales76017.github.io/bonfyre/series/") is None,"unverified static landing lastmod"
print(f"JOURNAL_READING_GRAPH_PASS {len(edges)} edited links / {len(refs)} connected existing essays / both directions / truthful sitemap dates")
