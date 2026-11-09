#!/usr/bin/env python3
"""End-to-end source → public author pages → native CMS draft checks.

This verifies the current 66 credited essays without claiming they have been
imported into or published from the native BonfyreCMS runtime.
"""
import csv
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re

root=Path(".")
blog=root/"site/blog"
build=root/"build/publication"

def csv_rows(filename):
    with (build/filename).open(encoding="utf-8",newline="") as handle:
        return list(csv.DictReader(handle))

pages=csv_rows("cms-site-page.csv")
fragments=csv_rows("cms-site-fragment.csv")
assert len(pages)==67, f"Expected exactly 66 essays and one organizational author; got {len(pages)}"
assert len(set(p["route"] for p in pages))==67, "Repeated canonical route"
assert len(set(f["coordinate"] for f in fragments))==len(fragments), "Repeated fragment coordinate"
assert all(row["namespace"]=="aurekai-journal" and row["status"]=="draft" for row in pages), "Unsafe automatic publication"
assert all(row["namespace"]=="aurekai-journal" and row["status"]=="draft" for row in fragments), "Unsafe fragment publication"
assert all(f["page"] in {p["route"] for p in pages} for f in fragments), "Orphaned CMS fragment"
assert all(f["source_ref"] and f["source_digest"] for f in fragments), "Missing source provenance"
assert len([f for f in fragments if f["kind"]=="article-body"])==66, "Lost article bodies"
assert len([f for f in fragments if f["kind"]=="article-credit"])==66, "Lost author relationships"
assert all(f["audience"]=="public" for f in fragments), "Accidental private projection"

source_count=0
identity="https://nickgonzales76017.github.io/bonfyre/blog/authors/aurekai-editorial/"
for p in pages:
    if "/authors/" in p["route"]:
        assert p["route"]=="/blog/authors/aurekai-editorial/"
        continue
    slug=p["route"].removeprefix("/blog/").strip("/")
    path=root/"editorial/posts"/(slug+".md")
    assert path.exists(), "Missing authored source: "+slug
    raw=path.read_text(encoding="utf-8")
    digest=sha256(path.read_bytes()).hexdigest()
    body=next(f for f in fragments if f["coordinate"]=="journal:post:"+slug+":body")
    author=next(f for f in fragments if f["coordinate"]=="journal:post:"+slug+":attribution")
    assert body["source_ref"]==path.as_posix() and body["source_digest"]==digest, slug
    assert body["body"]==raw.split("\n---\n",1)[1].strip(), "Lost exact article body: "+slug
    assert author["href"]==identity and "Aurekai editorial" in author["body"], "Authorship mismatch: "+slug
    html=(blog/slug/"index.html").read_text(encoding="utf-8")
    assert '<link rel="author" href="../authors/aurekai-editorial/">' in html, slug
    scripts=re.findall(r'<script type="application/ld\+json">(.*?)</script>',html,re.S)
    schemas=[json.loads(x) for x in scripts]
    article=next(x for x in schemas if x.get("@type")=="BlogPosting")
    assert article["author"]["@id"]==identity+"#identity"
    assert article["author"]["@type"]=="Organization", "No inferred individual authors"
    assert "citation" not in article or all(x.startswith("https://") for x in article["citation"])
    source_count+=1
assert source_count==66
author_page=blog/"authors/aurekai-editorial/index.html"
author_markup=author_page.read_text(encoding="utf-8")
assert author_markup.count('class="author-feature"')==4
assert author_markup.count('<li><a href="../../')==66
assert 'Organization' in author_markup and 'not an individual person' in author_markup
assert (blog/"authors/index.html").exists()
for page in [blog/"authors/index.html",author_page]:
    assert 'rel="canonical"' in page.read_text(encoding="utf-8")
source_lines=(build/"source-receipts.tsv").read_text(encoding="utf-8").splitlines()
assert len(source_lines)==68 and all(line.endswith("\tdraft") for line in source_lines[1:])

print(f"EDITORIAL_PUBLICATION_BRIDGE_PASS {source_count} original essays / {len(pages)} native draft pages / {len(fragments)} sourced fragments / 1 real organizational credit")
print("Publication remains held for owner review; source rows do not attest live native CMS admission.")
