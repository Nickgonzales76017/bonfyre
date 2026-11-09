"""Shared Aurekai Journal authorship and review-gated native CMS draft projection.

Current edited Markdown is the publication source of record. This module
derives public author pages and *draft* BonfyreCMS site_page/site_fragment rows
from those same exact source bytes. It does not execute CMS writes or publish.
All external source claims retain their original references.
"""
from collections import Counter, defaultdict
from datetime import datetime
from html import escape as esc
from pathlib import Path
from hashlib import sha256
from io import StringIO
import csv
import json
import re

DEFAULT_AUTHOR = "aurekai-editorial"
NAMESPACE = "aurekai-journal"
PUBLIC_BASE = "https://nickgonzales76017.github.io/bonfyre/"
SAFE_ID = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")


def load_authors(root):
    directory = Path(root) / "editorial" / "authors"
    authors = {}
    for path in sorted(directory.glob("*.md")):
        text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
        match = re.fullmatch(r"---\n(.*?)\n---\n(.*)", text, re.S)
        if not match:
            raise ValueError("Authorship requires structured metadata: " + str(path))
        meta = {}
        for line in match.group(1).splitlines():
            if ":" not in line:
                raise ValueError("Invalid author field: " + line)
            key, value = (piece.strip() for piece in line.split(":", 1))
            if key in meta:
                raise ValueError("Duplicate author field: " + key)
            meta[key] = value
        required = {"slug", "name", "kind", "description", "website", "source"}
        if not required.issubset(meta) or any(not meta.get(k) for k in required):
            raise ValueError("Incomplete author profile: " + str(path))
        slug = meta["slug"]
        if not SAFE_ID.fullmatch(slug) or slug != path.stem or slug in authors:
            raise ValueError("Invalid or duplicated author identity: " + slug)
        if meta["kind"] not in ("Person", "Organization"):
            raise ValueError("Unverified author type: " + slug)
        if any(not meta[k].startswith("https://") for k in ("website", "source")):
            raise ValueError("External author links must use HTTPS")
        meta["body"] = match.group(2).strip()
        if not meta["body"]:
            raise ValueError("Empty author biography: " + slug)
        meta["source_path"] = path.relative_to(root).as_posix()
        meta["source_sha256"] = sha256(path.read_bytes()).hexdigest()
        meta["url"] = PUBLIC_BASE + "blog/authors/" + slug + "/"
        authors[slug] = meta
    if DEFAULT_AUTHOR not in authors:
        raise ValueError("Aurekai Journal has no accountable editorial author")
    return authors


def bind_authorship(posts, authors):
    """Refuse invented people or unspecified identities; legacy desk credit is explicit."""
    for post in posts:
        author_id = post.get("author", DEFAULT_AUTHOR)
        if author_id not in authors:
            raise ValueError("Unknown article author, not an identity: " + post["slug"])
        post["author_id"] = author_id
        post["author_profile"] = authors[author_id]
    return posts


def public_author_page(author, articles):
    """Static HTML, no JS or claimed personal identity. Relative links remain stable."""
    canonical = author["url"]
    ordered = sorted(articles, key=lambda p: (p["published"], p["title"]), reverse=True)
    categories = Counter(p["category"] for p in ordered)
    featured = ordered[:4]
    nav = (
        '<nav aria-label="Site navigation">'
        '<a href="../../../">Home</a><a href="../../../explore.html">Businesses</a>'
        '<a href="../../../discover/">Applications</a><a href="../../">Journal</a>'
        '<a href="../../../series/">Four series</a><a href="../../../docs.html">Technology</a>'
        '<a href="https://aurekai-work-samples.pages.dev/#ask">Discuss a project ↗</a>'
        '</nav>'
    )
    bio = "".join("<p>" + esc(para.strip()) + "</p>" for para in author["body"].split("\n\n") if para.strip())
    selected = "".join(
        '<article class="author-feature"><span>' + esc(p["category"]) + " · " + esc(p["published"]) + '</span>'
        '<h2><a href="../../' + esc(p["slug"]) + '/">' + esc(p["title"]) + '</a></h2>'
        '<p>' + esc(p["description"]) + '</p></article>' for p in featured
    )
    directory = "".join(
        '<li><a href="../../' + esc(p["slug"]) + '/"><strong>' + esc(p["title"]) + '</strong>'
        '<span>' + esc(p["category"]) + ' · ' + esc(p["published"]) + '</span></a></li>' for p in ordered
    )
    topics = " · ".join(esc(category) + " (" + str(count) + ")" for category, count in sorted(categories.items()))
    schema = {
        "@context": "https://schema.org", "@type": "ProfilePage", "@id": canonical + "#page", "url": canonical,
        "name": author["name"] + " — Aurekai Journal", "description": author["description"],
        "mainEntity": {"@type": author["kind"], "@id": canonical + "#identity",
                       "name": author["name"], "description": author["description"],
                       "url": canonical, "sameAs": [author["website"], author["source"]]},
        "isPartOf": {"@type": "WebSite", "name": "Aurekai Journal",
                     "url": PUBLIC_BASE + "blog/"}
    }
    ld = json.dumps(schema, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    return '''<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#1b4033">
<title>''' + esc(author["name"]) + ''' — Aurekai Journal</title>
<meta name="description" content="''' + esc(author["description"]) + '''">
<meta property="og:type" content="profile"><meta property="og:title" content="''' + esc(author["name"]) + ''' — Aurekai Journal">
<meta property="og:description" content="''' + esc(author["description"]) + '''">
<meta property="og:url" content="''' + esc(canonical) + '''">
<link rel="canonical" href="''' + esc(canonical) + '''">
<link rel="stylesheet" href="../../../assets/aurekai-field-guide.css">
<link rel="stylesheet" href="../../blog.css"><link rel="stylesheet" href="../../authors.css">
<script type="application/ld+json">''' + ld + '''</script></head>
<body class="author-page"><a class="skip" href="#main">Skip to articles</a>
<header class="top"><div class="shell"><a class="brand" href="../../../"><span class="mark">A</span> AUREKAI <small>/ JOURNAL</small></a>''' + nav + '''</div></header>
<main id="main">
<section class="author-opening"><div class="shell author-opening-inner">
<div><span class="author-kicker">Journal / Authorship</span><h1>''' + esc(author["name"]) + '''</h1>
<p class="author-role">''' + esc(author["description"]) + '''</p>
<div class="author-links"><a href="../../about/">Editorial standards and corrections</a>
<a href="''' + esc(author["website"]) + '''">Aurekai website ↗</a>
<a href="''' + esc(author["source"]) + '''">Review published source ↗</a></div></div>
<aside class="author-attribution" aria-label="Authorship and publication facts">
<span>A documented editorial identity</span><strong>''' + esc(author["kind"]) + '''</strong>
<p>''' + str(len(ordered)) + ''' articles credited to this publication identity</p>
<small>Organization credit is not an individual person's byline, and it does not prove who wrote each passage.</small>
</aside></div></section>
<section class="author-intro shell"><div><span class="author-kicker">About the editorial desk</span>
<h2>Where these articles come from</h2>''' + bio + '''</div>
<aside><span class="author-kicker">Subjects covered</span><p>''' + topics + '''</p>
<p>Articles have separate sources, dates, citations, and updates. The original author credit is consistent across versions.</p></aside></section>
<section class="author-selection shell"><div class="author-section-title"><span class="author-kicker">Recent publication</span>
<h2>Start with an article</h2></div><div class="author-feature-grid">''' + selected + '''</div></section>
<section class="author-archive shell"><div class="author-section-title"><span class="author-kicker">Published archive</span>
<h2>''' + str(len(ordered)) + ''' articles</h2><p>Each article links to its sources, publication date and any related reading.</p></div>
<ol>''' + directory + '''</ol></section>
</main><footer class="footer"><div class="shell"><span>Aurekai Journal · Authorship is credited, not inferred.</span>
<span><a href="../../">Journal</a> · <a href="../../about/">Editorial policies</a> · <a href="../../../">Home</a></span></div></footer>
</body></html>'''


def author_index(authors, posts):
    items = []
    for slug, author in sorted(authors.items()):
        count = sum(p["author_id"] == slug for p in posts)
        items.append('<li><a href="./' + esc(slug) + '/"><strong>' + esc(author["name"]) + '</strong>'
                     ' <span>' + str(count) + ' articles · ' + esc(author["kind"]) + '</span></a></li>')
    return '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Authors and editors — Aurekai Journal</title><meta name="description" content="The credited editorial identities for articles in the Aurekai Journal.">
<link rel="canonical" href="''' + PUBLIC_BASE + '''blog/authors/">
<link rel="stylesheet" href="../../assets/aurekai-field-guide.css">
<link rel="stylesheet" href="../blog.css"><link rel="stylesheet" href="../authors.css"></head><body class="author-page">
<main id="main" class="shell author-index"><span class="author-kicker">Aurekai Journal / Attribution</span>
<h1>Authors and editors</h1><p>The Journal credits each article to a declared publication identity. No individual author is inferred from a repository account, writing style or AI-assisted production.</p>
<ul>''' + "".join(items) + '''</ul>
<p><a href="../about/">Editorial policies and corrections</a> · <a href="../">All articles</a></p></main></body></html>'''


PAGE_FIELDS = ("namespace","route","title","eyebrow","heading","lede","door_page","note","media_subject_ref","editorial_kind","author_credit","author_subject_ref","editorial_published_on","editorial_updated_on","lenses","aperture_subject","aperture_operation","authenticated_pane","machine_view","status")
FRAGMENT_FIELDS = ("namespace","coordinate","page","slot","audience","kind","position","eyebrow","heading","body","label","href","status","variant_key","semantic_tags","projection_contract","source_ref","source_digest","evidence_ref","notes")


def as_csv(fields, rows):
    buffer = StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="raise", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def build_native_draft(root, posts, authors, relations, media_sources=None, check=False):
    """Projection for existing BonfyreCMS site_page/site_fragment owners.

    Never connects to production or invokes a CMS effect. Records are drafts,
    and no route can be published until its native owner reviews the records.
    """
    root = Path(root)
    templates = {
        "page": json.loads((root / "cmd/BonfyreCMS/content-types/site_page.json").read_text()),
        "fragment": json.loads((root / "cmd/BonfyreCMS/content-types/site_fragment.json").read_text())
    }
    for label, fields in (("page", PAGE_FIELDS),("fragment", FRAGMENT_FIELDS)):
        if set(fields)-{"namespace"} != set(templates[label]["attributes"]):
            raise ValueError("Native CMS schema drift: " + label)
    article_lookup = {p["slug"]:p for p in posts}
    if len(article_lookup) != len(posts):
        raise ValueError("Duplicate article source identity")
    outgoing = defaultdict(list)
    for line in Path(relations).read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields)!=4 or fields[0] not in article_lookup or fields[2] not in article_lookup:
            raise ValueError("Invalid authored reading relationship")
        outgoing[fields[0]].append(fields)
    pages, fragments = [], []
    media_sources = media_sources or {}
    art_ref = "editorial/art-direction.tsv"
    art_digest = sha256((root/art_ref).read_bytes()).hexdigest()
    media_ref = "editorial/media-sources.tsv"
    media_digest = sha256((root/media_ref).read_bytes()).hexdigest()
    manifest=["subject_id\tcanonical_url\tauthor_id\tseries\tcategory\tsource_ref\tsource_sha256\tstatus"]
    def page(route,title,lede,note,**metadata):
        row={"namespace":NAMESPACE,"route":route,"title":title,"eyebrow":"Aurekai Journal",
             "heading":title,"lede":lede,"note":note,"lenses":"public","machine_view":"authored","status":"draft"}
        if set(metadata)-set(PAGE_FIELDS):
            raise ValueError("Unrecognized page metadata")
        row.update(metadata)
        pages.append(row)
    def fragment(route,key,position,kind,body,source_ref,digest,label="",href="",**presentation):
        row={"namespace":NAMESPACE,"coordinate":"journal:"+key,"page":route,
             "slot":"main","audience":"public","kind":kind,"position":position,
             "body":body,"label":label,"href":href,"status":"draft",
             "source_ref":source_ref,"source_digest":digest}
        if set(presentation)-set(FRAGMENT_FIELDS):
            raise ValueError("Unrecognized presentation metadata")
        row.update(presentation)
        fragments.append(row)
    for p in sorted(posts,key=lambda x:x["slug"]):
        slug=p["slug"];route="/blog/"+slug+"/"
        source_ref="editorial/posts/"+slug+".md"
        digest=sha256((root/source_ref).read_bytes()).hexdigest()
        url=PUBLIC_BASE+route.lstrip("/")
        author=authors[p["author_id"]]
        page(route,p["title"],p["description"],
             "Article | "+p["published"]+" | "+p["category"]+" | author:"+p["author_id"],
             editorial_kind="article",author_credit=author["name"],
             author_subject_ref="author:"+p["author_id"],
             editorial_published_on=p["published"],
             editorial_updated_on=p.get("updated",p["published"]))
        fragment(route,"post:"+slug+":body",1,"article-body",p["body"],source_ref,digest)
        fragment(route,"post:"+slug+":attribution",2,"article-credit",
                 "By "+author["name"]+" · Published "+p["published"]+
                 " · From "+(p.get("series") or "Aurekai Journal"),source_ref,digest,
                 author["name"],author["url"])
        if p.get("source_url"):
            fragment(route,"post:"+slug+":source",3,"primary-reference",
                     "Originally cited source: "+p["source_url"],source_ref,digest,
                     "View original source",p["source_url"])
        if outgoing[slug]:
            lines = [f"- [{article_lookup[target]['title']}]({PUBLIC_BASE}blog/{target}/): {relation} — {reason}"
                     for _,relation,target,reason in outgoing[slug]]
            fragment(route,"post:"+slug+":related",4,"editorial-related", "\n".join(lines),
                     "editorial/reading-bridges.tsv",
                     sha256(Path(relations).read_bytes()).hexdigest())
        # Presentation is a source-bound, review-held fragment, not a grant to
        # publish or a substitute for the native SitePresentation/SurfaceIR owner.
        fmt, colorway, media_key = p["art"]
        contract={"schema":"aurekai.journal.edition.v1","subject_id":"post:"+slug,
                  "canonical_url":url,"template":"journal-"+fmt,"format":fmt,
                  "colorway":colorway,"media_key":media_key or None,
                  "authored_source_ref":source_ref,"authored_source_sha256":digest,
                  "review_required":True}
        fragment(route,"post:"+slug+":presentation",5,"article-presentation",
                 "format: "+fmt+"\ncolorway: "+colorway+"\nmedia: "+(media_key or "none"),
                 art_ref,art_digest,"Editorial presentation",
                 slot="cover",variant_key="journal-"+fmt,
                 semantic_tags=json.dumps(["journal",p["category"].lower(),fmt]),
                 projection_contract=json.dumps(contract,sort_keys=True,separators=(",",":")),
                 notes="Source-pinned presentation draft; no publication or media-rights claim")
        if media_key:
            if media_key not in media_sources:
                raise ValueError("Article media has no source-owned credits: "+slug)
            uri, caption, credit_url = media_sources[media_key]
            fragment(route,"post:"+slug+":media",6,"article-media-credit",
                     "key: "+media_key+"\nuri: "+uri+"\ncredit: "+caption+"\nsource: "+(credit_url or "first-party"),
                     media_ref,media_digest,caption,credit_url)
        if p.get("audio_file"):
            fragment(route,"post:"+slug+":audio",7,"article-audio-edition",
                     "file: "+p["audio_file"]+"\nvoice: "+p.get("audio_voice","")
                     +"\nmodel: "+p.get("audio_model","")+"\nreview: "+p.get("audio_review","")
                     +"\nmanuscript_sha256: "+p.get("audio_body_sha256",""),
                     source_ref,digest,"Recorded narration (review-held)")
        manifest.append("\t".join(["post:"+slug,url,p["author_id"],p.get("series",""),p["category"],
                                   source_ref,digest,"draft"]))
    for slug, author in sorted(authors.items()):
        route="/blog/authors/"+slug+"/"
        page(route,author["name"],author["description"],"Credited "+author["kind"]+" profile",
             editorial_kind="author-profile",author_credit=author["name"],
             author_subject_ref="author:"+slug)
        fragment(route,"author:"+slug+":bio",1,"author-biography",author["body"],
                 author["source_path"],author["source_sha256"])
        fragment(route,"author:"+slug+":articles",2,"authored-articles",
                 "\n".join(f"- [{p['title']}]({PUBLIC_BASE}blog/{p['slug']}/)"
                     for p in posts if p["author_id"]==slug),author["source_path"],author["source_sha256"])
        manifest.append("\t".join(["author:"+slug,author["url"],slug,"","Authors",
                              author["source_path"],author["source_sha256"],"draft"]))
    if len(set(p["route"] for p in pages))!=len(pages) or len(set(f["coordinate"] for f in fragments))!=len(fragments):
        raise ValueError("Nonunique CMS route or fragment coordinate")
    if any(p["status"]!="draft" for p in pages) or any(f["status"]!="draft" for f in fragments):
        raise ValueError("Native publication requires a separate review")
    out=root/"build"/"publication"
    outputs={
        "cms-site-page.csv":as_csv(PAGE_FIELDS,pages),
        "cms-site-fragment.csv":as_csv(FRAGMENT_FIELDS,fragments),
        "source-receipts.tsv":"\n".join(manifest)+"\n"
    }
    for name,body in outputs.items():
        path=out/name
        if check:
            if not path.exists() or path.read_text(encoding="utf-8")!=body:
                raise AssertionError("Stale native draft projection: "+name)
        else:
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(body,encoding="utf-8")
    return len(pages),len(fragments)
