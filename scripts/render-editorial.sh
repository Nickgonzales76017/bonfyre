#!/bin/sh
# Build Aurekai Journal from edited Markdown without adding an installed JS runtime.
# Run at repository root: sh scripts/render-editorial.sh [--check]
set -eu
python3 - "$@" <<'PY'
import hashlib, html, os, pathlib, re, sys
from datetime import datetime, timezone
from xml.sax.saxutils import escape as xml_escape

ROOT = pathlib.Path.cwd()
SOURCE = ROOT / "editorial" / "posts"
TARGET = ROOT / "site" / "blog"
BASE_URL = "https://nickgonzales76017.github.io/bonfyre/blog/"
CHECK = "--check" in sys.argv[1:]
ALLOWED_ICONS = set(re.findall(r'<symbol id="([^"]+)"', (ROOT / "site/assets/product-icons.svg").read_text()))
CATEGORIES = ["All", "Debates", "Mathematics", "Standards", "Markets", "Science", "Business", "Applications", "Publishing", "Commerce", "Engineering", "Research"]

def esc(s):
    return html.escape(str(s), quote=True)

def sid(s):
    return re.sub(r'(^-+|-+$)', '', re.sub(r'[^a-z0-9]+', '-', s.lower()))

def parse_doc(path):
    raw = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    match = re.match(r'\A---\n(.*?)\n---\n(.*)\Z', raw, re.S)
    if not match:
        raise ValueError(f"Missing front matter: {path}")
    meta = {}
    for row in match.group(1).splitlines():
        if ":" not in row:
            raise ValueError(f"Invalid front matter line in {path}")
        key, value = row.split(":", 1)
        if key.strip() in meta:
            raise ValueError(f"Duplicate metadata key: {key}")
        meta[key.strip()] = value.strip()
    for key in ("slug","title","description","category","icon","published","status","topics","origin"):
        if not meta.get(key):
            raise ValueError(f"Missing {key} in {path}")
    if meta["slug"] != path.stem or not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', path.stem):
        raise ValueError(f"Invalid slug: {path}")
    if meta["category"] not in CATEGORIES or meta["category"] == "All":
        raise ValueError(f"Unrecognized category in {path}")
    if meta["icon"] not in ALLOWED_ICONS:
        raise ValueError(f"Missing icon in {path}")
    datetime.strptime(meta["published"], "%Y-%m-%d")
    if meta["status"] != "edited-public":
        return None
    meta["body"] = match.group(2).strip()
    if len(meta["body"].split()) < 140:
        raise ValueError(f"Article too short for public journal: {path}")
    meta["source_path"] = str(path.relative_to(ROOT))
    meta["source_sha256"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return meta

posts = [p for p in (parse_doc(path) for path in sorted(SOURCE.glob("*.md"))) if p]
if len(posts) < 46:
    raise ValueError(f"Expected 46 edited articles, found {len(posts)}")
if len({p["slug"] for p in posts}) != len(posts):
    raise ValueError("Duplicate public article slug")
posts.sort(key=lambda p: (p["published"], p["title"].lower()), reverse=True)

def inline(s):
    s = esc(s)
    # Only URLs with an http(s) prefix are allowed in authored links.
    s = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', lambda m:
        '<a href="' + m.group(2) + '" rel="noopener noreferrer">' + m.group(1) + '</a>', s)
    s = re.sub(r'`([^`]+)`', lambda m: '<code>' + m.group(1) + '</code>', s)
    s = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', s)
    s = re.sub(r'(?<!\*)\*([^*]+)\*(?!\*)', r'<em>\1</em>', s)
    return s

def body_html(source):
    rendered = []
    for block in re.split(r'\n[ \t]*\n', source.strip()):
        block = block.strip()
        if not block: continue
        if block.startswith("## "):
            title = block[3:].strip()
            rendered.append('<h2 id="' + sid(title) + '">' + inline(title) + '</h2>')
        elif block.startswith("### "):
            title = block[4:].strip()
            rendered.append('<h3 id="' + sid(title) + '">' + inline(title) + '</h3>')
        elif all(x.startswith("- ") for x in block.splitlines()):
            rendered.append('<ul>' + ''.join('<li>' + inline(x[2:]) + '</li>' for x in block.splitlines()) + '</ul>')
        else:
            rendered.append('<p>' + inline(block.replace("\n", " ")) + '</p>')
    return "\n".join(rendered)

def write(relative, content):
    output = TARGET / relative
    if CHECK:
        if not output.exists() or output.read_text(encoding="utf-8") != content:
            raise AssertionError(f"Out-of-date generated article: {relative}")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content, encoding="utf-8")

def icon(p, prefix):
    return '<span class="glyph" aria-hidden="true"><svg viewBox="0 0 24 24"><use href="' + prefix + 'assets/product-icons.svg#' + esc(p["icon"]) + '"></use></svg></span>'

def article_page(p):
    slug = p["slug"]
    mins = max(2, (len(p["body"].split()) + 209) // 210)
    toc = "".join('<a href="#'+sid(line[3:])+'">'+esc(line[3:])+'</a>'
        for line in p["body"].splitlines() if line.startswith("## "))
    prim = p.get("source_url")
    external = '<p><a href="'+esc(prim)+'" rel="noopener noreferrer">Original outside source ↗</a></p>' if prim and prim.startswith("https://") else ""
    related = [q for q in posts if q["slug"] != slug and q["category"] == p["category"]][:3]
    if len(related) < 3:
        related.extend(q for q in posts if q["slug"] != slug and q not in related)
    related = related[:3]
    recommendations = ''.join('<a class="journal-related" href="../'+q["slug"]+'/">'+esc(q["title"])+' ↗</a>' for q in related)
    canonical = BASE_URL + slug + "/"
    topic_labels = ''.join('<span class="journal-topic">'+esc(t.strip())+'</span>' for t in p["topics"].split(",") if t.strip())
    return f'''<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#f5f3eb">
<title>{esc(p["title"])} — Aurekai Journal</title>
<meta name="description" content="{esc(p["description"])}">
<meta property="og:title" content="{esc(p["title"])}"><meta property="og:description" content="{esc(p["description"])}"><meta property="og:type" content="article">
<meta name="author" content="Aurekai editorial"><meta property="article:published_time" content="{esc(p["published"])}T12:00:00Z"><meta property="article:section" content="{esc(p["category"])}"><meta name="twitter:card" content="summary">
<link rel="canonical" href="{canonical}"><link rel="alternate" type="application/rss+xml" title="Aurekai Journal" href="../feed.xml">
<link rel="stylesheet" href="../../assets/aurekai-field-guide.css"><link rel="stylesheet" href="../../assets/aurekai-research.css"><link rel="stylesheet" href="../blog.css">
</head><body><a class="skip" href="#main">Skip to article</a>
<header class="top"><div class="shell"><a class="brand" href="../../"><span class="mark">A</span> AUREKAI <small>/ JOURNAL</small></a>
<nav aria-label="Site navigation"><a href="../../">Home</a><a href="../../discover/">Explore</a><a href="../">Journal</a><a href="../../fieldwork/">Fieldwork</a><a href="../../series/">Series</a><a href="../../docs.html">Bonfÿre</a></nav></div></header>
<main id="main"><section class="journal-article-hero"><div class="shell journal-article-head">
<nav class="journal-breadcrumb" aria-label="Breadcrumb"><a href="../">Journal</a><span>/</span><span>{esc(p["category"])}</span></nav>
{icon(p, "../../")}<div class="eyebrow">{esc(p["category"])} · {mins} min read</div>
<h1>{esc(p["title"])}</h1><p class="lead">{esc(p["description"])}</p>
<div class="journal-meta"><span>Aurekai / Bonfÿre</span><span>{esc(p["published"])}</span><span>Edited public article</span></div>
</div></section>
<section class="journal-article-shell"><div class="shell journal-article-layout">
<article class="article-body" itemscope itemtype="https://schema.org/BlogPosting"><meta itemprop="headline" content="{esc(p["title"])}"><meta itemprop="datePublished" content="{esc(p["published"])}">{body_html(p["body"])}
<div class="journal-source-note"><strong>About this article.</strong> {esc(p["origin"])}. {("The article responds to or examines the linked original source." if prim else "Adapted and researched for public reading.")} <a href="https://github.com/Nickgonzales76017/bonfyre/blob/main/{esc(p["source_path"])}">Editable source ↗</a></div>
</article>
<aside class="journal-article-side"><div class="journal-side-sticky"><span class="eyebrow">In this article</span>{toc}<hr>
{external}<div class="journal-topics">{topic_labels}</div><p><a href="../../discover/">All applications ↗</a> · <a href="../">All articles ↗</a></p></div></aside>
</div></section>
<section class="block alt"><div class="shell"><div class="head"><div><span class="eyebrow">Keep reading</span><h2>Related articles</h2></div><p>More research from Aurekai and Bonfÿre.</p></div>
<div class="journal-footer-links">{recommendations}<a href="../">All {len(posts)} articles ↗</a></div></div></section>
</main><footer class="footer"><div class="shell"><span>Aurekai / Bonfÿre · Journal</span><span><a href="../">Journal</a> · <a href="../../market.html">Research</a> · <a href="../../docs.html">Technology</a></span></div></footer></body></html>
'''

for p in posts:
    write(p["slug"] + "/index.html", article_page(p))

def card(p):
    search = ' '.join([p["title"], p["description"], p["topics"], p["category"]]).lower()
    return f'''<article class="journal-card" data-article data-category="{esc(p["category"])}" data-search="{esc(search)}">
<a class="journal-card-link" href="./{p["slug"]}/"><div class="journal-card-top">{icon(p, "../")}<span class="eyebrow">{esc(p["category"])}</span></div>
<h3>{esc(p["title"])}</h3><p>{esc(p["description"])}</p>
<div class="journal-card-bottom"><span>{esc(p["published"])}</span><span>Read article ↗</span></div></a></article>'''

index_file = TARGET / "index.html"
index = index_file.read_text(encoding="utf-8")
# Preserve the existing journal design and its accessible client-side filter.
start = index.find('<div class="journal-grid">')
end = index.find('<p id="journal-empty"', start)
if start < 0 or end < 0:
    raise ValueError("Blog index does not have expected card region")
index = index[:start] + '<div class="journal-grid">' + '\n'.join(map(card, posts)) + '</div>\n' + index[end:]
index = re.sub(r'All articles / \d+', 'All articles / ' + str(len(posts)), index)
index = re.sub(r'Showing \d+ articles', 'Showing ' + str(len(posts)) + ' articles', index)
fstart = index.find('<div class="journal-filters"')
fend = index.find('</div><label class="journal-search-label"', fstart)
if fstart < 0 or fend < 0:
    raise ValueError("Blog index filter area missing")
categories = [x for x in CATEGORIES if x == "All" or any(p["category"] == x for p in posts)]
buttons = ''.join('<button type="button" class="journal-filter'+(' active' if i == 0 else '')+
    '" data-filter="'+esc(x)+'" aria-pressed="'+('true' if i == 0 else 'false')+'">'+esc(x)+'</button>'
    for i,x in enumerate(categories))
index = index[:fstart] + '<div class="journal-filters" role="group" aria-label="Filter articles">' + buttons + index[fend:]
# The filter helper refers to data attributes that have been built on every card.
assert index.count(' data-article data-category=') == len(posts)
write("index.html", index)

def pubdate(value):
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).strftime("%a, %d %b %Y 12:00:00 GMT")
items = []
for p in posts:
    link = BASE_URL + p["slug"] + "/"
    items.append(f'<item><title>{xml_escape(p["title"])}</title><link>{link}</link><guid isPermaLink="true">{link}</guid><pubDate>{pubdate(p["published"])}</pubDate><category>{xml_escape(p["category"])}</category><description>{xml_escape(p["description"])}</description></item>')
rss = '<?xml version="1.0" encoding="UTF-8"?>\n<rss version="2.0"><channel><title>Aurekai Journal</title><link>'+BASE_URL+'</link><description>Research and articles from Aurekai and Bonfÿre</description><language>en-us</language>\n' + '\n'.join(items) + '\n</channel></rss>\n'
write("feed.xml", rss)
urls = [BASE_URL] + [BASE_URL+p["slug"]+"/" for p in posts]
sitemap = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + '\n'.join('<url><loc>'+u+'</loc><lastmod>2026-11-08</lastmod></url>' for u in urls) + '\n</urlset>\n'
write("sitemap.xml", sitemap)
columns = ["subject_id","slug","category","published","source_path","source_sha256","canonical_url","primary_source_url"]
records = ['\t'.join(columns)]
for p in posts:
    records.append('\t'.join(["post:"+p["slug"],p["slug"],p["category"],p["published"],p["source_path"],p["source_sha256"],BASE_URL+p["slug"]+"/",p.get("source_url","")]))
write("post-index.tsv", '\n'.join(records)+'\n')
# Search records for the public cross-site directory.
discovery_index = ROOT / "site" / "discover" / "article-index.tsv"
discovery_rows = ["slug\tcategory\ttitle\tdescription\ttopics\tpublished\tcanonical_url"]
for post in posts:
    discovery_rows.append("\t".join([post["slug"],post["category"],post["title"],post["description"],post["topics"],post["published"],BASE_URL+post["slug"]+"/"]))
discovery_content = "\n".join(discovery_rows)+"\n"
if CHECK:
    if not discovery_index.exists() or discovery_index.read_text(encoding="utf-8") != discovery_content:
        raise AssertionError("Outdated public article search index")
else:
    discovery_index.parent.mkdir(parents=True,exist_ok=True)
    discovery_index.write_text(discovery_content,encoding="utf-8")

print(f'Aurekai Journal: {len(posts)} authored articles, {len(categories)} categories, RSS, sitemap, and native-import index ' + ('verified' if CHECK else 'built'))
PY
