#!/bin/sh
# Build Aurekai Journal from edited Markdown without adding an installed JS runtime.
# Run at repository root: sh scripts/render-editorial.sh [--check]
set -eu
python3 - "$@" <<'PY'
import hashlib, html, os, pathlib, re, sys
from datetime import datetime, timezone
from xml.sax.saxutils import escape as xml_escape

# Article-specific tree etchings and cover compositions, no external assets or dependencies.


ROOT = pathlib.Path.cwd()
SOURCE = ROOT / "editorial" / "posts"
TARGET = ROOT / "site" / "blog"
BASE_URL = "https://nickgonzales76017.github.io/bonfyre/blog/"
CHECK = "--check" in sys.argv[1:]
ALLOWED_ICONS = set(re.findall(r'<symbol id="([^"]+)"', (ROOT / "site/assets/product-icons.svg").read_text()))
CATEGORIES = ["All", "Debates", "Mathematics", "Standards", "Markets", "Science", "Business", "Applications", "Publishing", "Commerce", "Engineering", "Research"]

# Original, deterministic cover illustration and composition grammar.
import hashlib
import html
import math
import random

PALETTES = (
    ("#153e31", "#e7d6ad", "#e8ba86", "#f8f2e4"),
    ("#173a32", "#cfddbc", "#dda17e", "#f8f0e0"),
    ("#214738", "#e0d3aa", "#e8bb9c", "#fbf4e6"),
    ("#15352e", "#d8e2c7", "#cfa96a", "#f6f0e4"),
    ("#244c3d", "#d4e4bd", "#dfbb8d", "#f7f2e9"),
    ("#103c34", "#d8d3a8", "#d79c77", "#fcf3e3"),
    ("#2b4838", "#e3d9c3", "#e3b58b", "#fff6e9"),
    ("#1b443d", "#d5e0c7", "#d8ad85", "#f8efe1"),
)
SPECIES = ("cedar", "pine", "oak", "birch", "fern", "willow")
COMPOSITIONS = (
    "folio", "reverse", "centered", "framed",
    "panorama", "offset", "letterpress", "diptych"
)

def style_for(post):
    seed = int(hashlib.sha256(post["slug"].encode("utf-8")).hexdigest()[:16], 16)
    dark, ink, accent, paper = PALETTES[(seed >> 12) % len(PALETTES)]
    family = SPECIES[(seed >> 6) % len(SPECIES)]
    variant = COMPOSITIONS[(seed >> 20) % len(COMPOSITIONS)]
    return {
        "seed": seed, "dark": dark, "ink": ink, "accent": accent,
        "paper": paper, "family": family, "variant": variant,
        "index": f"{seed % 997 + 1:03d}",
        "rotation": (seed % 13) - 6,
    }

def _point(x, y):
    return f"{x:.1f},{y:.1f}"

def _path(d, color, width=1, opacity=1, extra=""):
    return f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width:.2f}" opacity="{opacity:.3f}" stroke-linecap="round" stroke-linejoin="round" {extra}/>'

def _leaf(cx, cy, angle, scale, color, opacity):
    rad = math.radians(angle)
    dx, dy = math.cos(rad) * 11 * scale, math.sin(rad) * 11 * scale
    nx, ny = -math.sin(rad) * 4 * scale, math.cos(rad) * 4 * scale
    d = (f"M {_point(cx-dx, cy-dy)} "
         f"Q {_point(cx+nx, cy+ny)} {_point(cx+dx, cy+dy)} "
         f"Q {_point(cx-nx, cy-ny)} {_point(cx-dx, cy-dy)}")
    return _path(d, color, .72, opacity)

def _tree(rng, x, bottom, height, spread, family, ink, accent, density):
    result = []
    sway = rng.uniform(-35, 35)
    top = bottom-height
    main = f"M {_point(x,bottom)} C {_point(x+sway*.2,bottom-height*.36)} {_point(x+sway*.75,top+height*.22)} {_point(x+sway,top)}"
    result.append(_path(main, ink, rng.uniform(1.6, 2.6), .8))
    result.append(_path(f"M {_point(x-3,bottom)} Q {_point(x+sway*.3,top+height*.4)} {_point(x+sway-2,top+18)}", accent,.45,.48))
    branch_count = density + rng.randint(-1, 2)
    for j in range(branch_count):
        u=(j+1)/(branch_count+1)
        y=bottom-height*(.12+.79*u)
        tx=x+sway*u
        canopy = (1-u)**.72
        for side in (-1,1):
            if family == "willow":
                bx=tx+side*spread*canopy*.82
                by=y-height*.14*canopy
                d=f"M {_point(tx,y)} Q {_point(tx+side*spread*.51*canopy,y-height*.20*canopy)} {_point(bx,by)}"
                result.append(_path(d,ink,.85,.64))
                for k in range(3):
                    ox=bx+side*(k*6+3)
                    result.append(_path(f"M {_point(ox,by)} Q {_point(ox+side*5,by+23)} {_point(ox+side*2,by+55+12*k)}",accent,.7,.63))
            elif family == "fern":
                bx=tx+side*spread*canopy*.63
                by=y-height*.07
                d=f"M {_point(tx,y)} Q {_point(tx+side*spread*.27,y-height*.10)} {_point(bx,by)}"
                result.append(_path(d,ink,.82,.69))
                for k in range(5):
                    t=(k+1)/6
                    lx=tx+(bx-tx)*t
                    ly=y+(by-y)*t
                    ll=10+17*(1-t)
                    result.append(_path(f"M {_point(lx,ly)} l {side*ll*.42:.1f},{-ll:.1f}",accent,.6,.6))
                    result.append(_path(f"M {_point(lx,ly)} l {side*ll*.5:.1f},{ll*.64:.1f}",accent,.6,.53))
            else:
                reach=spread*canopy*(.80 if family=="pine" else .58 if family=="birch" else .72)
                bx=tx+side*reach
                by=y-height*(.025 if family=="pine" else .17 if family=="oak" else .10)*canopy
                d=f"M {_point(tx,y)} Q {_point(tx+side*reach*.52,y-height*.08*canopy)} {_point(bx,by)}"
                result.append(_path(d,ink,rng.uniform(.6,1.08),.72))
                result.append(_path(f"M {_point(tx+side*reach*.3,y-3)} Q {_point(tx+side*reach*.47,y-height*.21*canopy)} {_point(tx+side*reach*.66,by-17*canopy)}",accent,.52,.53))
                if family in ("cedar","pine"):
                    for k in range(5):
                        t=(k+1)/6
                        nx=tx+(bx-tx)*t
                        ny=y+(by-y)*t
                        length=(9+13*canopy)*(1-.4*t)
                        for up in (-1,1):
                            result.append(_path(f"M {_point(nx,ny)} l {side*length*.38:.1f},{up*length:.1f}",ink,.48,.48))
                else:
                    for k in range(4):
                        t=(k+1)/5
                        nx=tx+(bx-tx)*t
                        ny=y+(by-y)*t
                        for sign in (-1,1):
                            result.append(_leaf(nx+side*5,ny+sign*7,sign*36+side*17,.75*canopy+.4,accent,.62))
    if family=="birch":
        for j in range(11):
            yy=bottom-(j+1)*height/13
            xx=x+sway*(j/13)
            result.append(_path(f"M {_point(xx-2,yy)} l {-5-rng.random()*4:.1f},{-2-rng.random()*4:.1f}", accent,.5,.53))
    if family=="oak":
        for j in range(9):
            cx=x+rng.uniform(-spread*.55,spread*.55)
            cy=top+rng.uniform(height*.1,height*.52)
            result.append(_leaf(cx,cy,rng.randint(0,180),rng.uniform(.75,1.2),ink,.54))
    return ''.join(result)

def illustration_svg(post):
    st = style_for(post)
    rng = random.Random(st["seed"])
    width, height = 760, 900
    ink, accent, dark = st["ink"], st["accent"], st["dark"]
    mode = st["variant"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="art-title">',
        f'<title id="art-title">Original botanical study of {html.escape(post["title"])}</title>',
        f'<defs><linearGradient id="shadow" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="{dark}"/><stop offset=".53" stop-color="{dark}"/><stop offset="1" stop-color="#0d2925"/></linearGradient></defs>',
        '<rect width="760" height="900" fill="url(#shadow)"/>',
        f'<rect x="28" y="28" width="704" height="844" fill="none" stroke="{ink}" opacity=".44"/>',
    ]
    # Antique field-guide ruling, inset rules, and unique register marks.
    for j in range(23):
        y = 64 + j * 34
        for x, direction in ((51, 1), (709, -1)):
            size = 20 if j % 5 == 0 else 10
            parts.append(_path(f"M {_point(x,y)} h {size*direction}", accent, .9, .55))
    if mode in ("folio", "reverse"):
        # Window of negative space with trees rising from both page edges.
        halo_x = 390 if mode == "folio" else 310
        parts.append(f'<circle cx="{halo_x}" cy="410" r="196" fill="none" stroke="{accent}" stroke-width="1.1" opacity=".49"/>')
        for k in range(9):
            rr = 80 + k*23
            parts.append(f'<circle cx="{halo_x}" cy="410" r="{rr}" fill="none" stroke="{ink}" stroke-width=".55" opacity="{.10+k*.013:.3f}"/>')
        forest = (
            [(74, 847, 680, 188, "cedar", 15), (685, 860, 630, 181, "willow", 12), (28, 824, 500, 128, "pine", 11)]
            if mode == "folio" else
            [(665, 860, 730, 229, "oak", 15), (95, 855, 525, 144, "birch", 12), (732, 830, 610, 167, "fern", 11)]
        )
    elif mode == "centered":
        # A single collected botanical specimen surrounded by a field diagram.
        cx, cy = 380+rng.randint(-50,50), 360
        for k in range(15):
            rr = 70+k*20
            parts.append(f'<ellipse cx="{cx}" cy="{cy}" rx="{rr}" ry="{rr*.83:.1f}" transform="rotate(-17 {cx} {cy})" fill="none" stroke="{accent}" stroke-width=".65" opacity="{.40-k*.018:.2f}"/>')
        forest=[(380, 840, 685, 220, "cedar", 17),(50,840,450,115,"fern",13),(710,840,470,120,"willow",11)]
    elif mode == "framed":
        # A layered birch grove. Vertical trunks lend this issue a different rhythm.
        for x in range(105,750,90):
            parts.append(_path(f"M {x},102 V 824", accent, .55, .21))
        for k in range(8):
            y=152+k*90
            parts.append(_path(f"M 79,{y} H 681", ink, .55, .19))
        forest=[(104,851,675,85,"birch",10),(274,847,585,115,"birch",12),(484,851,708,120,"oak",12),(664,850,605,95,"birch",11)]
    elif mode == "panorama":
        # Distant mountain contours and a low forest at dusk.
        for k in range(6):
            y=370+k*45
            d=f"M 34,{y+40} Q 190,{y-90+rng.randint(-20,20)} 374,{y+10} T 735,{y-20}"
            parts.append(_path(d,accent,1.0,.2+k*.06))
        parts.append(f'<circle cx="555" cy="253" r="89" fill="none" stroke="{accent}" stroke-width="1.2" opacity=".58"/>')
        forest=[(70,861,460,115,"pine",11),(210,851,610,145,"cedar",13),(370,853,565,150,"pine",12),(536,859,680,160,"cedar",15),(704,849,490,135,"fern",12)]
    elif mode == "offset":
        # Broad overhanging boughs and fine lower-edge shrubs.
        for k in range(10):
            parts.append(_path(f"M 30,{105+k*22} C 260,{72+k*20} 450,{200+k*9} 730,{70+k*26}",ink,.7,.22))
        forest=[(120,859,735,260,"oak",15),(683,852,510,165,"willow",12),(430,861,315,140,"fern",11)]
    elif mode == "letterpress":
        # Opposing pressed specimens, with inset instrument lines.
        for k in range(13):
            rr=75+k*20
            parts.append(f'<ellipse cx="380" cy="438" rx="{rr}" ry="{rr*.77:.1f}" transform="rotate(24 380 438)" fill="none" stroke="{accent}" opacity=".19" stroke-width=".75"/>')
        forest=[(105,859,640,200,"fern",17),(660,842,650,190,"willow",16),(370,861,265,100,"birch",10)]
        parts.append(_path("M 150 128 H 610 M 150 772 H 610",ink,1.1,.62))
    else:  # diptych
        # Two contrasting tree studies on opposite sides of a ruled page.
        parts.append(_path("M 380 55 V 845", accent, 1.15, .49))
        for k in range(10):
            y=110+k*72
            parts.append(_path(f"M 363,{y} h 34", ink, .9, .48))
        forest=[(156,862,715,180,"cedar",16),(335,850,515,110,"birch",12),(512,846,645,198,"oak",17),(710,853,555,138,"willow",12)]
    # Stable but distinct geometry within each editorial composition.
    for i,(x,y,h,spread,family,density) in enumerate(forest):
        x += rng.uniform(-19,19)
        h += rng.randint(-40,40)
        y += rng.randint(-10,10)
        if (st["seed"] >> (i*5)) % 7 == 0 and mode not in ("framed","diptych"):
            family = SPECIES[(SPECIES.index(family)+1+st["seed"]%4)%len(SPECIES)]
        parts.append(_tree(rng,x,y,h,spread,family,ink,accent,density))
    # Distinct constellation of crosshairs and microscopic specimen marks.
    for i in range(32):
        x,y=rng.uniform(57,703),rng.uniform(73,810)
        size=rng.uniform(1.8,5.0)
        if i % 4 == 0:
            parts.append(_path(f"M {_point(x-size,y)} h {2*size:.1f} M {_point(x,y-size)} v {2*size:.1f}",accent,.9,.44))
        else:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{size*.32:.2f}" fill="{accent}" opacity=".34"/>')
    parts.append(_path("M 58 819 H 702 M 58 82 H 187 M 575 82 H 702", ink, 1.15, .65))
    parts.append(f'<circle cx="380" cy="82" r="6.5" fill="none" stroke="{accent}" stroke-width="1.1" opacity=".82"/>')
    parts.append('</svg>\n')
    return ''.join(parts)


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
    series_id = p.get("series", "")
    series_names = {"authority":"Authority and judgment", "living":"Beyond websites", "access":"Expression and participation", "operators":"Internet operators"}
    if series_id and series_id not in series_names:
        raise ValueError(f"Unknown editorial series: {series_id}")
    series_html = ('<a class="journal-series-link" href="../../series/#'+series_id+'">Explore '+esc(series_names[series_id])+' ↗</a>') if series_id else ""
    epigraph_text = p.get("epigraph", "")
    epigraph_credit = p.get("epigraph_credit", "")
    epigraph_url = p.get("epigraph_url", "")
    if epigraph_url and not epigraph_url.startswith("https://"):
        raise ValueError(f"Unsupported epigraph source URL: {slug}")
    attribution = ('<a href="'+esc(epigraph_url)+'" rel="noopener noreferrer">'+esc(epigraph_credit)+'</a>') if epigraph_url else esc(epigraph_credit)
    epigraph_html = ('<figure class="journal-epigraph"><blockquote>“'+esc(epigraph_text.strip("“”"))+'”</blockquote><figcaption>'+attribution+'</figcaption></figure>') if epigraph_text and epigraph_credit else ""
    question = p.get("parting_question", "")
    reflection_html = ('<aside class="journal-afterword"><span class="eyebrow">A parting thought</span><h2>'+esc(question)+'</h2>'+series_html+'</aside>') if question else ""
    external = '<p><a href="'+esc(prim)+'" rel="noopener noreferrer">Original outside source ↗</a></p>' if prim and prim.startswith("https://") else ""
    related = [q for q in posts if q["slug"] != slug and q["category"] == p["category"]][:3]
    if len(related) < 3:
        related.extend(q for q in posts if q["slug"] != slug and q not in related)
    related = related[:3]
    recommendations = ''.join('<a class="journal-related garden-related" href="../'+q["slug"]+'/"><img src="../'+q["slug"]+'/cover.svg" alt="" loading="lazy"><span>'+esc(q["title"])+' ↗</span></a>' for q in related)
    canonical = BASE_URL + slug + "/"
    topic_labels = ''.join('<span class="journal-topic">'+esc(t.strip())+'</span>' for t in p["topics"].split(",") if t.strip())
    garden = style_for(p)
    garden_css = ('--garden-deep:'+garden["dark"]+';--garden-ink:'+garden["ink"]+
                  ';--garden-accent:'+garden["accent"]+';--garden-paper:'+garden["paper"])
    return f'''<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#f5f3eb">
<title>{esc(p["title"])} — Aurekai Journal</title>
<meta name="description" content="{esc(p["description"])}">
<meta property="og:title" content="{esc(p["title"])}"><meta property="og:description" content="{esc(p["description"])}"><meta property="og:type" content="article">
<meta name="author" content="Aurekai editorial"><meta property="article:published_time" content="{esc(p["published"])}T12:00:00Z"><meta property="article:section" content="{esc(p["category"])}"><meta name="twitter:card" content="summary">
<link rel="canonical" href="{canonical}"><link rel="alternate" type="application/rss+xml" title="Aurekai Journal" href="../feed.xml">
<link rel="stylesheet" href="../../assets/aurekai-field-guide.css"><link rel="stylesheet" href="../../assets/aurekai-research.css"><link rel="stylesheet" href="../blog.css"><link rel="stylesheet" href="../article-garden.css">
</head><body class="journal-garden garden-{garden["variant"]}" style="{garden_css}"><a class="skip" href="#main">Skip to article</a>
<header class="top"><div class="shell"><a class="brand" href="../../"><span class="mark">A</span> AUREKAI <small>/ JOURNAL</small></a>
<nav aria-label="Site navigation"><a href="../../">Home</a><a href="../../discover/">Explore</a><a href="../">Journal</a><a href="../../fieldwork/">Fieldwork</a><a href="../../series/">Series</a><a href="../../docs.html">Bonfÿre</a></nav></div></header>
<main id="main"><section class="garden-hero" aria-labelledby="article-title">
<div class="garden-side-ornament" aria-hidden="true"></div>
<div class="shell garden-hero-grid">
<div class="garden-hero-copy">
<nav class="journal-breadcrumb" aria-label="Breadcrumb"><a href="../">Aurekai Journal</a><span>/</span><span>{esc(p["category"])}</span></nav>
<p class="garden-folio">Field Notes <span>Study {garden["index"]} / {esc(garden["family"].title())}</span></p>
<h1 id="article-title">{esc(p["title"])}</h1>
<p class="garden-subtitle">{esc(p["description"])}</p>
<div class="garden-tags"><span>{esc(p["category"])}</span><span>{mins} minute read</span><span>{esc(p["published"])}</span></div>
<a class="garden-read-link" href="#read-story">Read the story <span aria-hidden="true">↓</span></a>
</div>
<figure class="garden-figure"><img src="./cover.svg" width="760" height="900" loading="eager" fetchpriority="high" alt="Original {esc(garden["family"])} tree and botanical line illustration created for this article.">
<figcaption>Aurekai / Botanical study {garden["index"]}</figcaption></figure>
</div>
<div class="shell garden-hero-serial"><span>An Aurekai Journal essay</span><span>Independent research · original illustrations</span><a href="../../series/">Explore the series ↗</a></div>
</section>
<section class="journal-article-shell" id="read-story"><div class="shell journal-article-layout">
<article class="article-body" itemscope itemtype="https://schema.org/BlogPosting"><meta itemprop="headline" content="{esc(p["title"])}"><meta itemprop="datePublished" content="{esc(p["published"])}"><div class="garden-chapterline"><span>Journal / {esc(p["category"])}</span><span>Reading room · {garden["index"]}</span></div>{epigraph_html}{body_html(p["body"])}\n{reflection_html}
<div class="journal-source-note"><strong>About this article.</strong> {esc(p["origin"])}. {("The article responds to or examines the linked original source." if prim else "Adapted and researched for public reading.")} <a href="https://github.com/Nickgonzales76017/bonfyre/blob/main/{esc(p["source_path"])}">Editable source ↗</a></div>
</article>
<aside class="journal-article-side"><div class="journal-side-sticky"><div class="garden-margin-art" aria-hidden="true"><img src="./cover.svg" alt="" loading="lazy"></div><span class="eyebrow">In this article</span>{toc}<hr>
{external}<div class="journal-topics">{topic_labels}</div><p><a href="../../discover/">All applications ↗</a> · <a href="../">All articles ↗</a></p></div></aside>
</div></section>
<section class="block alt garden-close"><div class="shell"><div class="head"><div><span class="eyebrow">Keep reading</span><h2>Related articles</h2></div><p>More research from Aurekai and Bonfÿre.</p></div>
<div class="journal-footer-links garden-related-group">{recommendations}<a href="../">All {len(posts)} articles ↗</a></div></div></section>
</main><footer class="footer"><div class="shell"><span>Aurekai / Bonfÿre · Journal</span><span><a href="../">Journal</a> · <a href="../../market.html">Research</a> · <a href="../../docs.html">Technology</a></span></div></footer></body></html>
'''

for p in posts:
    write(p["slug"] + "/cover.svg", illustration_svg(p))
    write(p["slug"] + "/index.html", article_page(p))

def card(p):
    search = ' '.join([p["title"], p["description"], p["topics"], p["category"]]).lower()
    return f'''<article class="journal-card" data-article data-category="{esc(p["category"])}" data-search="{esc(search)}">
<a class="journal-card-link" href="./{p["slug"]}/"><div class="journal-card-top">{icon(p, "../")}<span class="eyebrow">{esc(p["category"])}</span></div>
<div class="journal-card-cover" aria-hidden="true"><img src="./{p["slug"]}/cover.svg" alt="" loading="lazy" width="760" height="900"></div><h3>{esc(p["title"])}</h3><p>{esc(p["description"])}</p>
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


# Public design invariant: every retained article has a unique tree engraving,
# one accessible reading layout, and the same art in Journal discovery.
if CHECK:
    from xml.etree import ElementTree as ET
    arts = set()
    variants = set()
    for p in posts:
        cover_path = TARGET / p["slug"] / "cover.svg"
        html_path = TARGET / p["slug"] / "index.html"
        content = cover_path.read_bytes()
        markup = html_path.read_text(encoding="utf-8")
        digest = hashlib.sha256(content).hexdigest()
        if digest in arts:
            raise AssertionError(f"Duplicate botanical artwork: {p['slug']}")
        arts.add(digest)
        root = ET.fromstring(content)
        paths = sum(1 for el in root.iter() if el.tag.endswith("path"))
        if paths < 100:
            raise AssertionError(f"Botanical illustration needs more linework: {p['slug']}: {paths}")
        composition = style_for(p)["variant"]
        variants.add(composition)
        if (markup.count('<h1 id="article-title">') != 1 or
            f'class="journal-garden garden-{composition}"' not in markup or
            markup.count('src="./cover.svg"') < 2 or
            'href="../article-garden.css"' not in markup or
            'id="read-story"' not in markup or
            '<aside class="journal-article-side">' not in markup):
            raise AssertionError(f"Botanical reading layout incomplete: {p['slug']}")
    index = (TARGET / "index.html").read_text(encoding="utf-8")
    if index.count('class="journal-card-cover"') != len(posts):
        raise AssertionError("Journal artwork missing from some discovery cards")
    if len(variants) < 7:
        raise AssertionError(f"Expected editorial layout diversity, found: {sorted(variants)}")
    print(f"EDITORIAL_DESIGN_PASS {len(arts)} unique tree drawings, {len(variants)} cover compositions")

print(f'Aurekai Journal: {len(posts)} authored articles, {len(categories)} categories, RSS, sitemap, and native-import index ' + ('verified' if CHECK else 'built'))
PY
