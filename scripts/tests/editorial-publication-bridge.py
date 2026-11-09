#!/usr/bin/env python3
"""End-to-end source → public author pages → native CMS draft checks.

This verifies the full credited essay inventory without claiming they have been
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
article_count=sum(1 for p in (root/"editorial/posts").glob("*.md") if "\nstatus: edited-public\n" in p.read_text(encoding="utf-8"))
assert article_count>=90, f"Published article inventory unexpectedly small: {article_count}"
assert len(pages)==article_count+1, f"Expected {article_count} essays and one organizational author; got {len(pages)}"
assert len(set(p["route"] for p in pages))==article_count+1, "Repeated canonical route"
assert len(set(f["coordinate"] for f in fragments))==len(fragments), "Repeated fragment coordinate"
assert all(row["namespace"]=="aurekai-journal" and row["status"]=="draft" for row in pages), "Unsafe automatic publication"
assert all(row["namespace"]=="aurekai-journal" and row["status"]=="draft" for row in fragments), "Unsafe fragment publication"
assert all(f["page"] in {p["route"] for p in pages} for f in fragments), "Orphaned CMS fragment"
assert all(f["source_ref"] and f["source_digest"] for f in fragments), "Missing source provenance"
assert len([f for f in fragments if f["kind"]=="article-body"])==article_count, "Lost article bodies"
assert len([f for f in fragments if f["kind"]=="article-credit"])==article_count, "Lost author relationships"
assert len([f for f in fragments if f["kind"]=="article-presentation"])==article_count, "Lost editorial art direction"
media_path=root/"editorial/media-sources.tsv"
art_path=root/"editorial/art-direction.tsv"
def tsv_rows(path):
    return list(csv.DictReader([line for line in path.read_text(encoding="utf-8").splitlines()
               if line.strip() and not line.startswith("#")],delimiter="\t"))
directions={r["slug"]:r for r in tsv_rows(art_path)}
media={r["key"]:r for r in tsv_rows(media_path)}
assert len(directions)==article_count and len(media)==12
assert len([f for f in fragments if f["kind"]=="article-media-credit"])==sum(bool(x["media"]) for x in directions.values()), "Lost source media"
assert len([f for f in fragments if f["kind"]=="article-audio-edition"])==sum(bool((root/"editorial/posts"/(s+".md")).read_text(encoding="utf-8").split("---",2)[1].find("audio_file:")>=0) for s in directions), "Lost recorded edition"
art_sha=sha256(art_path.read_bytes()).hexdigest()
media_sha=sha256(media_path.read_bytes()).hexdigest()

assert all(f["audience"]=="public" for f in fragments), "Accidental private projection"

source_count=0
identity="https://nickgonzales76017.github.io/bonfyre/blog/authors/aurekai-editorial/"
for p in pages:
    if "/authors/" in p["route"]:
        assert p["route"]=="/blog/authors/aurekai-editorial/"
        assert p["editorial_kind"]=="author-profile" and p["author_subject_ref"]=="author:aurekai-editorial"
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
    art=directions[slug]
    selected=next(f for f in fragments if f["coordinate"]=="journal:post:"+slug+":presentation")
    assert selected["source_ref"]=="editorial/art-direction.tsv" and selected["source_digest"]==art_sha, slug
    assert selected["body"]=="format: "+art["format"]+"\ncolorway: "+art["colorway"]+"\nmedia: "+(art["media"] or "none"), slug
    contract=json.loads(selected["projection_contract"])
    assert contract["schema"]=="aurekai.journal.edition.v1" and contract["review_required"] is True
    assert contract["subject_id"]=="post:"+slug and contract["canonical_url"].endswith("/blog/"+slug+"/")
    assert contract["format"]==art["format"] and contract["colorway"]==art["colorway"]
    assert contract["media_key"]==(art["media"] or None) and contract["authored_source_sha256"]==digest
    assert selected["slot"]=="cover" and selected["variant_key"]=="journal-"+art["format"]
    assert json.loads(selected["semantic_tags"])[-1]==art["format"]
    assert p["editorial_kind"]=="article" and p["author_subject_ref"]=="author:aurekai-editorial"
    assert p["author_credit"]=="Aurekai editorial" and p["editorial_published_on"] and p["editorial_updated_on"]
    if art["media"]:
        record=media[art["media"]]
        credited=next(f for f in fragments if f["coordinate"]=="journal:post:"+slug+":media")
        assert credited["source_ref"]=="editorial/media-sources.tsv" and credited["source_digest"]==media_sha, slug
        assert record["caption"] in credited["body"] and record["uri"] in credited["body"], slug
    html=(blog/slug/"index.html").read_text(encoding="utf-8")
    assert "format-"+art["format"]+" tone-"+art["colorway"] in html, slug
    assert '<link rel="author" href="../authors/aurekai-editorial/">' in html, slug
    scripts=re.findall(r'<script type="application/ld\+json">(.*?)</script>',html,re.S)
    schemas=[json.loads(x) for x in scripts]
    article=next(x for x in schemas if x.get("@type")=="BlogPosting")
    assert article["author"]["@id"]==identity+"#identity"
    assert article["author"]["@type"]=="Organization", "No inferred individual authors"
    assert "citation" not in article or all(x.startswith("https://") for x in article["citation"])
    source_count+=1
assert source_count==article_count
author_page=blog/"authors/aurekai-editorial/index.html"
author_markup=author_page.read_text(encoding="utf-8")
assert author_markup.count('class="author-feature"')==4
assert author_markup.count('<li><a href="../../')==article_count
assert 'Organization' in author_markup and 'not an individual person' in author_markup
assert (blog/"authors/index.html").exists()
for page in [blog/"authors/index.html",author_page]:
    assert 'rel="canonical"' in page.read_text(encoding="utf-8")
source_lines=(build/"source-receipts.tsv").read_text(encoding="utf-8").splitlines()
assert len(source_lines)==article_count+2 and all(line.endswith("\tdraft") for line in source_lines[1:])

print(f"EDITORIAL_PUBLICATION_BRIDGE_PASS {source_count} original essays / {len(pages)} native draft pages / {len(fragments)} sourced fragments / 1 real organizational credit")
print("Publication remains held for owner review; source rows do not attest live native CMS admission.")
