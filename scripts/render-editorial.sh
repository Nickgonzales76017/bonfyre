#!/bin/sh
# Build Aurekai Journal from edited Markdown without adding an installed JS runtime.
# Run at repository root: sh scripts/render-editorial.sh [--check]
set -eu
python3 - "$@" <<'PY'
import hashlib, html, os, pathlib, re, sys, json
from datetime import datetime, timezone
from xml.sax.saxutils import escape as xml_escape

ROOT = pathlib.Path.cwd()
SOURCE = ROOT / "editorial" / "posts"
TARGET = ROOT / "site" / "blog"
BASE_URL = "https://nickgonzales76017.github.io/bonfyre/blog/"
CHECK = "--check" in sys.argv[1:]
ALLOWED_ICONS = set(re.findall(r'<symbol id="([^"]+)"', (ROOT / "site/assets/product-icons.svg").read_text()))
CATEGORIES = ["All", "Debates", "Mathematics", "Standards", "Markets", "Science", "Business", "Applications", "Publishing", "Commerce", "Engineering", "Research"]

# The editorial desk chooses the presentation per story. No hash, randomness,
# generated illustration, placeholder photo, or fabricated magazine issue.
# Format: slug | opening format | colorway | licensed/owned visual (or none).
EDITION_SPEC = """
a-council-agenda-can-be-useful|field-report|forest|research
a-job-posting-has-more-than-one-job|business-review|olive|
a-library-of-alexandria-selected-by-demand|archive|ink|library
a-publisher-deserves-a-home|cover-spread|forest|
a-union-for-the-internet|broadsheet|brick|printing
a-wholesale-lot-is-an-equation-not-a-discount|market-ledger|copper|
ai-lowered-entry-cost-not-customer-service-cost|business-review|ink|
ai-operating-laboratory-equipment-needs-a-real-safety-boundary|research-note|slate|
ai-written-code-is-not-the-whole-delivery|system-brief|forest|process
an-electric-vehicle-can-be-a-mobile-battery-but-availability-is-the-product|field-report|olive|
auth-md-and-the-agent-registration-problem|dossier|slate|
coding-a-quickbooks-replacement-does-not-replace-accounting|business-review|copper|
connecting-an-account-is-not-acting-on-it|dossier|forest|
creators-need-operating-infrastructure-not-only-better-media-models|culture-feature|brick|
differential-privacy-is-not-a-substitute-for-data-governance|research-note|slate|
digital-identity-is-not-a-single-login-box|column|forest|
duckdb-in-the-browser-changes-who-needs-a-server|system-brief|olive|schema
frappe-as-an-application-language|dossier|olive|
from-audio-to-something-people-can-use|culture-feature|ink|radio
graded-collectibles-are-an-identity-market|market-ledger|brick|
machine-payments-are-not-machine-purchasing-authority|business-review|slate|
mcp-a2a-activitypub-and-atproto-are-different-tools|comparative|ink|
metr-study-and-the-missing-counterfactual|research-note|copper|
minecraft-built-a-world-what-about-everything-else|manifesto|forest|
nobody-called-the-pizza-shops-slop|culture-feature|brick|
productized-service-first-is-not-every-founders-rule|column|copper|
property-listings-need-rights-not-just-fields|field-report|ink|
rate-distortion-for-executable-agent-skills|research-note|olive|
rss-is-still-a-great-source-of-time|broadsheet|forest|
satellite-models-can-see-change-not-insurance-liability|field-report|slate|
screenshots-cannot-test-a-distributed-system|system-brief|ink|
shopifys-ai-memo-needs-an-outcome-metric|business-review|brick|
sites-are-dead-long-live-the-web|cover-spread|forest|forest
source-available-is-not-the-same-as-open-source|comparative|olive|
stable-identifiers-are-equivalence-classes|research-note|ink|
the-archive-is-not-the-owner|archive|copper|
the-catalogue-and-the-room|studio-edition|forest|room
the-clip-is-not-the-whole-work|culture-feature|slate|
the-deployment-was-green-but-the-site-was-old|system-brief|brick|
the-internet-has-an-inheritance-problem|archive|forest|
the-internet-of-bad-ideas|manifesto|copper|
the-local-archive-that-never-got-uploaded|archive|olive|books
the-machine-told-me-so|dossier|ink|
the-math-behind-sharing-software-across-businesses|business-review|olive|
the-memory-wall-in-local-models|system-brief|slate|
the-profile-is-bigger-than-the-person|cover-spread|brick|
the-spreadsheet-that-became-a-business|dossier|forest|calculator
the-world-is-not-one-embedding|comparative|slate|
three-brands-one-foundation|studio-edition|olive|
three-headlines-can-still-be-one-source|broadsheet|brick|
two-coding-agents-one-worktree|system-brief|copper|
useful-things-are-already-out-there|field-report|olive|
vibe-coding-is-not-a-production-method|manifesto|ink|
we-didnt-run-out-of-ideas|culture-feature|forest|
websites-should-open-real-applications|studio-edition|forest|product
what-a-legacy-migration-really-moves|field-report|copper|
what-happens-when-anyone-can-make-an-app|manifesto|olive|
when-everyone-agrees-who-is-thinking|column|ink|
when-reading-becomes-participating|cover-spread|forest|
when-the-agent-finishes-coding|system-brief|slate|
who-gets-to-have-sophisticated-software|business-review|copper|
who-keeps-the-internet-alive|culture-feature|forest|broadcast
why-ai-needs-an-operating-company|business-review|forest|
why-base-rates-change-camera-matching-claims|research-note|brick|
why-does-every-app-want-to-be-everything|comparative|forest|
why-the-cheapest-token-isnt-the-cheapest-answer|market-ledger|ink|
"""
FORMATS = frozenset(("cover-spread", "archive", "dossier", "field-report", "culture-feature",
    "business-review", "market-ledger", "research-note", "system-brief", "studio-edition",
    "comparative", "manifesto", "broadsheet", "column"))
PALETTES = {
    "forest": ("#204b3d", "#f3eee6", "#d5ab88", "#253d34"),
    "ink": ("#1d2930", "#f5f0e9", "#b6a78a", "#26353b"),
    "olive": ("#48533a", "#f5f0e5", "#c4a679", "#394331"),
    "brick": ("#753d34", "#f4eee7", "#cda888", "#5f332d"),
    "copper": ("#734d39", "#f6f0e7", "#d7ae83", "#5b4136"),
    "slate": ("#314753", "#eff1ed", "#c2ba9d", "#273941")
}
# Verified originals or existing real product screenshots only.
# Wikimedia FilePath redirects to the named image; caption links to the
# authoritative file page and identifies illustrative historical imagery.
MEDIA = {
 "forest": ("https://commons.wikimedia.org/wiki/Special:FilePath/Mist_around_a_forest_%28Unsplash%29.jpg?width=1200",
   "Real forest photograph, Austin Schmid, 2016 (CC0). Illustrative cover photograph.",
   "https://commons.wikimedia.org/wiki/File:Mist_around_a_forest_(Unsplash).jpg"),
 "library": ("https://commons.wikimedia.org/wiki/Special:FilePath/A_reading_room_at_the_Library_of_Congress%2C_Washington%2C_D.C._LCCN2011648351.jpg?width=1200",
   "Library of Congress reading room, historic photograph, public domain. Illustrative archival image.",
   "https://commons.wikimedia.org/wiki/File:A_reading_room_at_the_Library_of_Congress,_Washington,_D.C._LCCN2011648351.jpg"),
 "books": ("https://commons.wikimedia.org/wiki/Special:FilePath/Bookshelves_at_the_library.jpg?width=1200",
   "Library shelves, Westcott Phillip / U.S. Fish and Wildlife Service, public domain. Illustrative photograph.",
   "https://commons.wikimedia.org/wiki/File:Bookshelves_at_the_library.jpg"),
 "radio": ("https://commons.wikimedia.org/wiki/Special:FilePath/Radio_studio._Controll_%28sic%29_room._Looking_into_studio_LOC_matpc.03328.jpg?width=1200",
   "Historic radio control room, Matson Collection / Library of Congress, public domain. Illustrative image.",
   "https://commons.wikimedia.org/wiki/File:Radio_studio._Controll_(sic)_room._Looking_into_studio_LOC_matpc.03328.jpg"),
 "broadcast": ("https://commons.wikimedia.org/wiki/Special:FilePath/Radio_studio._Controll_%28sic%29_room._Looking_into_studio_LOC_matpc.03328.jpg?width=1200",
   "Historic radio studio, Matson Collection / Library of Congress, public domain. Illustrative image.",
   "https://commons.wikimedia.org/wiki/File:Radio_studio._Controll_(sic)_room._Looking_into_studio_LOC_matpc.03328.jpg"),
 "printing": ("https://commons.wikimedia.org/wiki/Special:FilePath/Printing_press.jpg?width=1200",
   "Printing press engraving by W. Lowry after J. Farey, 1819, public domain (Wellcome Collection).",
   "https://commons.wikimedia.org/wiki/File:Printing_press.jpg"),
 "calculator": ("https://commons.wikimedia.org/wiki/Special:FilePath/Old_produx_mechanical_calculator.jpg?width=1200",
   "Old Produx mechanical calculator photographed by Drummyfish, CC0. Illustrative photograph.",
   "https://commons.wikimedia.org/wiki/File:Old_produx_mechanical_calculator.jpg"),
 "room": ("local:spaces/shots/studio-room-selection.png", "Aurekai / Bonfÿre, real public room-planning studio screenshot.", ""),
 "product": ("local:spaces/shots/studio-product.png", "Aurekai / Bonfÿre, real public product studio screenshot.", ""),
 "research": ("local:instruments/shots/research-desktop.png", "Aurekai / Bonfÿre, real research instrument screenshot.", ""),
 "process": ("local:instruments/shots/process-desktop.png", "Aurekai / Bonfÿre, real process instrument screenshot.", ""),
 "schema": ("local:instruments/shots/schema-desktop.png", "Aurekai / Bonfÿre, real schema instrument screenshot.", "")
}

def editorial_specs():
    result = {}
    for line in EDITION_SPEC.strip().splitlines():
        slug, fmt, tone, media = line.strip().split("|")
        if slug in result or fmt not in FORMATS or tone not in PALETTES or (media and media not in MEDIA):
            raise ValueError("Invalid article-level art direction: "+slug)
        result[slug] = (fmt, tone, media)
    return result

def media_url(media, prefix):
    if not media:
        return ""
    url = MEDIA[media][0]
    return prefix + url[6:] if url.startswith("local:") else url

def media_figure(p, prefix, with_credit=True):
    key = p["art"][2]
    if not key:
        return ""
    image, caption, link = MEDIA[key]
    credit = ('<a href="'+esc(link)+'" rel="noopener noreferrer">'+esc(caption)+'</a>') if link else esc(caption)
    return ('<figure class="ed-photo"><img src="'+esc(media_url(key, prefix))+'" alt="'+esc(caption)+'"'
            ' width="1200" height="900" loading="eager" decoding="async">'
            + ('<figcaption>'+credit+'</figcaption>' if with_credit else '')+'</figure>')

def editorial_detail(p):
    # Typographic covers ARE the article's own table of contents, not
    # duplicated stock illustration or invented aphorism.
    headings = [line[3:].strip() for line in p["body"].splitlines() if line.startswith("## ")]
    lead = headings[0] if headings else p["description"]
    chapters = ''.join(
        '<li><a href="#'+sid(title)+'"><span>'+f"{i:02d}"+'</span><em>'+esc(title)+'</em></a></li>'
        for i, title in enumerate(headings[1:4], 2))
    section = p.get("series") or p["category"].lower()
    return ('<div class="ed-editor-note"><span class="ed-editor-label">The article / Contents</span>'
            '<strong>'+esc(lead)+'</strong>'
            + ('<ol class="ed-editor-chapters">'+chapters+'</ol>' if chapters else '')
            + '<span>'+esc(section.replace("-", " ").title())+' / Original editorial</span></div>')

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
    if meta.get("updated"):
        datetime.strptime(meta["updated"], "%Y-%m-%d")
        if meta["updated"] < meta["published"]:
            raise ValueError(f"Update date precedes publication: {path}")
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
ART_DIRECTION = editorial_specs()
if set(ART_DIRECTION) != {p["slug"] for p in posts}:
    missing = sorted({p["slug"] for p in posts} - set(ART_DIRECTION))
    stale = sorted(set(ART_DIRECTION) - {p["slug"] for p in posts})
    raise ValueError(f"Editorial plan not exhaustive: missing={missing} stale={stale}")
for p in posts:
    p["art"] = ART_DIRECTION[p["slug"]]

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
            rendered.append('<h2 id="' + sid(title) + '">' + inline(title) + ' <a href="#'+sid(title)+'" class="section-permalink" aria-label="Permanent link to section: '+esc(title)+'">§</a></h2>')
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


ARGUMENT_TURNS = {"the-machine-told-me-so":["Every checkpoint passes","The goal never gets questioned","Review the purpose independently"],"when-everyone-agrees-who-is-thinking":["Everyone reports progress","The same premise guides every reviewer","Protect evidence-backed disagreement"],"the-internet-of-bad-ideas":["A prototype works","Usefulness is untested","Let real-world feedback change the plan"],"sites-are-dead-long-live-the-web":["A homepage represents a business","Its work lives across disconnected systems","One subject, several legitimate views"],"the-profile-is-bigger-than-the-person":["A profile lists a biography","Work and relationships are elsewhere","Connect authorship, projects, and community"],"when-reading-becomes-participating":["A reader reaches the final paragraph","The story has no meaningful continuation","Offer optional, authorized participation"],"why-does-every-app-want-to-be-everything":["Each app wants the whole user","Institutions cannot move their work","Compose tools while retaining boundaries"],"nobody-called-the-pizza-shops-slop":["More people can publish","Most tools stop at generating a page","Support relationships and real services"],"we-didnt-run-out-of-ideas":["Specialist knowledge stays invisible","Publishing tools do not support the work","Make those interests operable together"],"minecraft-built-a-world-what-about-everything-else":["Simple creative building blocks","Serious activity still needs infrastructure","Let communities invent their own uses"],"the-spreadsheet-that-became-a-business":["A flexible grid becomes a business model","The model is trapped in one file","Connect decisions, records, and operations"],"who-gets-to-have-sophisticated-software":["Capability exists somewhere","Cost and complexity ration access","Make the capability understandable and usable"],"what-happens-when-anyone-can-make-an-app":["An app arrives quickly","Operations, rights, and maintenance remain","Give creation an accountable lifecycle"],"who-keeps-the-internet-alive":["Activity and audiences grow","Small social institutions lose operators","Support the people who maintain continuity"],"the-local-archive-that-never-got-uploaded":["A community holds recordings and records","The search index cannot see them","Preserve context with lawful discovery"],"a-library-of-alexandria-selected-by-demand":["Popular material gets copied","Low-attention history disappears","Preserve without ranking by popularity alone"],"the-archive-is-not-the-owner":["A copy has been saved","Possession is mistaken for permission","Record custody, rights, and access separately"],"the-clip-is-not-the-whole-work":["An excerpt travels widely","Original meaning and credits fall away","Connect the clip to its people and sources"],"the-internet-has-an-inheritance-problem":["A founder leaves an institution","Logins, rights, and memory have no successor","Plan stewardship before the emergency"],"a-union-for-the-internet":["Each operator negotiates alone","Platforms concentrate leverage","Build lawful collective infrastructure"]}
SERIES_MEDIA = {"authority":["authority-loop.svg","The loop and the missing question","A review can prove that the requested task was completed without establishing that the request was right.","A completed task and successful verification feed back into the original brief. Independent purpose and impact questions interrupt the closed loop."],"living":["living-subject.svg","One subject, different legitimate views","A publisher, person, community, and application can point to the same source without becoming the same owner.","One central subject connects to four separate views: profile, publication, community, and authorized applications."],"access":["expression-ecosystem.svg","From output to participation","A prompt can generate a page. An ecosystem needs tools that sustain relationships and institutions.","A generated page reaches one artifact while a common tool layer branches into collections, communities, and sustainable services."],"operators":["operators-continuity.svg","What survives a change of operator?","Preservation succeeds when material, people, authority, and continuity are not confused.","A timeline connects creation, maintenance, stewardship transfer, and new contributions, while custody, rights, access, and governance remain distinct."]}

def editorial_argument(post):
    values = ARGUMENT_TURNS.get(post["slug"])
    series = post.get("series", "")
    visual = SERIES_MEDIA.get(series)
    if not values or not visual:
        return ""
    media, title, caption, alt = visual
    stages = ("The familiar path", "What falls through", "The design opportunity")
    steps = ''.join('<li><span class="argument-step">'+esc(stages[i])+'</span><strong>'+esc(v)+'</strong></li>' for i,v in enumerate(values))
    return ('<figure class="journal-argument-figure">'
            '<div class="argument-head"><span>Visual argument / '+esc(series)+'</span><strong>'+esc(title)+'</strong></div>'
            '<img loading="lazy" decoding="async" width="1200" height="700" src="../../series/media/'+esc(media)+'" alt="'+esc(alt)+'">'
            '<figcaption>'+esc(caption)+' <a href="../../series/#'+esc(series)+'">Explore this series ↗</a></figcaption>'
            '<ol class="argument-turns">'+steps+'</ol></figure>')

def editorial_body(post):
    rendered = body_html(post["body"])
    visual = editorial_argument(post)
    if not visual:
        return rendered
    headings = list(re.finditer(r'<h2 id="', rendered))
    if len(headings) >= 2:
        insert_at = headings[1].start()
    elif headings:
        insert_at = headings[0].start()
    else:
        insert_at = len(rendered)
    return rendered[:insert_at] + visual + rendered[insert_at:]


# Editorial source trails are built only from real authored outbound links.
from urllib.parse import urlsplit
SERIES_READING_ORDER = {
 "authority":["the-machine-told-me-so","when-everyone-agrees-who-is-thinking","the-internet-of-bad-ideas"],
 "living":["sites-are-dead-long-live-the-web","the-profile-is-bigger-than-the-person","when-reading-becomes-participating","why-does-every-app-want-to-be-everything"],
 "access":["nobody-called-the-pizza-shops-slop","we-didnt-run-out-of-ideas","minecraft-built-a-world-what-about-everything-else","the-spreadsheet-that-became-a-business","who-gets-to-have-sophisticated-software","what-happens-when-anyone-can-make-an-app"],
 "operators":["who-keeps-the-internet-alive","the-local-archive-that-never-got-uploaded","a-library-of-alexandria-selected-by-demand","the-archive-is-not-the-owner","the-clip-is-not-the-whole-work","the-internet-has-an-inheritance-problem","a-union-for-the-internet"]
}
SERIES_LABELS = {"authority":"Authority and judgment","living":"Beyond websites","access":"Expression and participation","operators":"Internet operators"}

def build_citation_notes(markup, post):
    citations, labels, mentions = {}, {}, {}
    link_re = re.compile(r'<a href="(https?://[^"]+)" rel="noopener noreferrer">([^<]+)</a>')
    def annotate(match):
        url, label = match.group(1), match.group(2)
        if "nickgonzales76017.github.io/bonfyre/" in url:
            return match.group(0)
        if url not in citations:
            citations[url] = len(citations)+1
            labels[url] = label
            mentions[url] = []
        n = citations[url]
        ref = "source-ref-%d-%d" % (n, len(mentions[url])+1)
        mentions[url].append(ref)
        return match.group(0) + '<sup class="journal-cite"><a href="#source-%d" id="%s" aria-label="Reference %d">[%d]</a><span class="journal-cite-preview" aria-hidden="true">%s</span></sup>' % (n,ref,n,n,esc(label))
    marked = link_re.sub(annotate, markup)
    rows = []
    for url,n in citations.items():
        domain = urlsplit(html.unescape(url)).netloc.removeprefix("www.")
        back = ''.join('<a href="#'+m+'" class="journal-cite-back" aria-label="Back to reference '+str(n)+'">↩</a>' for m in mentions[url])
        rows.append('<li id="source-'+str(n)+'"><a itemprop="citation" href="'+url+'" rel="noopener noreferrer">'+labels[url]+'</a><span class="source-domain">'+esc(domain)+'</span><span class="source-backlinks">'+back+'</span></li>')
    primary = post.get("source_url","")
    context = ('<p class="journal-primary-source"><strong>Related source context:</strong> <a itemprop="citation" href="'+esc(primary)+'" rel="noopener noreferrer">'+esc(urlsplit(primary).netloc.removeprefix("www."))+'</a>. Source links do not imply endorsement.</p>') if primary.startswith("https://") and primary not in citations else ""
    empty = '<p>There are no separately linked external references in the body of this editorial essay. See the retained publication lineage below.</p>' if not rows and not context else ""
    return marked, ('<section id="journal-sources" class="journal-sources" aria-labelledby="journal-sources-heading"><h2 id="journal-sources-heading">Sources and further reading</h2><p class="journal-sources-intro">References point to original material linked in the essay, not independently invented citations.</p>'+
                    ('<ol>'+''.join(rows)+'</ol>' if rows else "")+context+empty+'</section>')

def reader_outline(post):
    headings=[line[3:].strip() for line in post["body"].splitlines() if line.startswith("## ")]
    links=''.join('<a href="#'+sid(h)+'">'+esc(h)+'</a>' for h in headings)
    links+='<a href="#journal-sources">Sources and references</a>'
    if EDITORIAL_BRIDGES.get(post["slug"]) or EDITORIAL_BACKLINKS.get(post["slug"]):
        links+='<a href="#journal-connections">Ideas in conversation</a>'
    return '<div class="reader-outline-menu" data-outline-menu><button type="button" class="reader-outline-trigger" aria-expanded="false" aria-controls="reader-outline-list">Essay outline <span aria-hidden="true">⌄</span></button><nav id="reader-outline-list" class="reader-outline-links" aria-label="Essay sections">'+links+'</nav></div>'

def narrator(post):
    # The waveform-like phrase map for browser speech is based on authored
    # sentence lengths, never disguised as measured audio amplitude.
    sentences = [x for x in re.split(r'(?<=[.!?])\s+', re.sub(r'\[[^\]]+\]\(https?://[^)]+\)', '',post["body"])) if x.strip()]
    if not sentences:
        sentences = [post["description"]]
    total = len(post["body"].split())
    pattern = []
    for i in range(72):
        words = len(sentences[min(len(sentences)-1, i*len(sentences)//72)].split())
        height = 12 + min(40, int(words*1.3))
        pattern.append('<i style="--level:'+str(height)+'px"></i>')
    estimated = max(1, round(total/165))
    file_name = post.get("audio_file", "")
    media = ""
    if file_name:
        if not re.fullmatch(r'[\w-]+\.(?:mp3|m4a|ogg|wav|opus)',file_name):
            raise ValueError(f"Invalid audio asset name for {post['slug']}")
        source_audio = TARGET / post["slug"] / file_name
        if not source_audio.is_file():
            raise ValueError(f"Audio declared but absent for {post['slug']}: {file_name}")
        media = '<audio data-narration-media preload="metadata" src="./'+esc(file_name)+'"></audio>'
    return ('''<section class="journal-audio-player" id="audio-edition" data-narration data-state="idle" data-mode="'''
        + ('recording' if media else 'browser') + '''" aria-label="Listen to this article">
      <div class="journal-audio-topline"><span class="journal-audio-kicker">The listening edition</span>
      <span class="journal-audio-format">''' + ('Recorded narration' if media else 'Browser voice / no recording required') + '''</span></div>
      <div class="journal-audio-main">
        <button class="journal-audio-play" type="button" data-narration-play aria-label="Play article narration"><span aria-hidden="true" data-narration-symbol>▶</span></button>
        <div class="journal-audio-content"><div class="journal-audio-heading"><strong>Listen to this article</strong>
          <span data-narration-status role="status" aria-live="polite">Ready to listen</span></div>
          <div class="journal-audio-wave-wrap"><div class="journal-audio-wave" data-narration-wave aria-hidden="true">'''
        + ''.join(pattern) + '''</div><input type="range" min="0" max="1000" value="0" step="1"
        data-narration-seek aria-label="Seek article narration by position" aria-valuetext="Beginning" /></div>
          <div class="journal-audio-time"><span data-narration-elapsed>0:00</span><span data-narration-total>About '''
        + str(estimated) + ''' min</span></div></div></div>
      <div class="journal-audio-actions">
        <div class="journal-audio-navigation">
          <button type="button" data-narration-back aria-label="Previous spoken passage">↶ <span>Back</span></button>
          <button type="button" data-narration-forward aria-label="Next spoken passage"><span>Next</span> ↷</button>
          <button type="button" data-narration-stop aria-label="Stop narration" disabled>■ <span>Stop</span></button>
        </div>
        <label class="journal-audio-rate">Speed <select data-narration-rate aria-label="Narration playback speed">
         <option value="0.85">0.85×</option><option value="1" selected>1×</option>
         <option value="1.15">1.15×</option><option value="1.35">1.35×</option><option value="1.5">1.5×</option></select></label>
        <button type="button" data-narration-copy class="journal-audio-copy" aria-label="Copy a link to this spoken passage">Copy passage link</button>
        <a href="#read-story" class="journal-audio-transcript">Read along ↓</a>
      </div>
      <progress data-narration-progress max="100" value="0" class="journal-audio-progress">0%</progress>
      <p class="journal-audio-disclosure">'''
      + ('Waveform represents the published audio recording. Reading remains available below.' if media else
         'Uses your browser’s speech voice. The bars map the text’s spoken passages, not a measured audio signal; seeking skips between passages.') + '''</p>'''
      + media + '''</section>''')


def byline(post):
    series=post.get("series","")
    label=SERIES_LABELS.get(series,"Aurekai Journal")
    href='../../series/#'+series if series else '../'
    return ('<div class="journal-author-row"><span class="journal-author-mark" aria-hidden="true">A</span><div>'
            '<p class="journal-byline">By <span itemprop="author" itemscope itemtype="https://schema.org/Organization"><a href="../about/" itemprop="url"><span itemprop="name">Aurekai editorial</span></a></span></p>'
            '<p class="journal-from">From <a href="'+href+'">'+esc(label)+'</a> · <time datetime="'+esc(post["published"])+'" itemprop="datePublished">'+esc(post["published"])+'</time> · Edited public essay</p></div></div>')



# Human-edited relationships join existing essays. They are not produced by
# keyword matching, claimed as citations, or confused with author endorsement.
EDITORIAL_BRIDGES_PATH = ROOT / "editorial" / "reading-bridges.tsv"
EDITORIAL_BRIDGES = {}
EDITORIAL_BACKLINKS = {}
known_slugs = {post["slug"] for post in posts}
for number, raw in enumerate(EDITORIAL_BRIDGES_PATH.read_text(encoding="utf-8").splitlines(), 1):
    if not raw.strip() or raw.startswith("#"):
        continue
    fields = raw.split("\t")
    if len(fields) != 4:
        raise ValueError(f"Invalid reading-bridge field count at line {number}")
    source, relation, destination, reason = [v.strip() for v in fields]
    if (source not in known_slugs or destination not in known_slugs
        or source == destination or not relation or not reason):
        raise ValueError(f"Reading bridge has missing article or explanation at line {number}")
    if any(x["to"] == destination for x in EDITORIAL_BRIDGES.get(source, [])):
        raise ValueError(f"Duplicate reading bridge at line {number}")
    EDITORIAL_BRIDGES.setdefault(source, []).append(
        {"to": destination, "relation": relation, "reason": reason})
    EDITORIAL_BACKLINKS.setdefault(destination, []).append(source)
if len(EDITORIAL_BRIDGES) < 20 or sum(map(len,EDITORIAL_BRIDGES.values())) < 40:
    raise ValueError("The editorial conversation graph is incomplete")
POST_BY_SLUG = {post["slug"]: post for post in posts}

def editorial_conversation(post):
    outgoing = EDITORIAL_BRIDGES.get(post["slug"], [])
    incoming = [ref for ref in EDITORIAL_BACKLINKS.get(post["slug"], [])
                if ref not in {e["to"] for e in outgoing}]
    if not outgoing and not incoming:
        return ""
    cards = []
    for edge in outgoing[:2]:
        target = POST_BY_SLUG[edge["to"]]
        cards.append('<a class="journal-conversation-card" href="../'+esc(target["slug"])+'/">'
            '<span>'+esc(edge["relation"])+'</span><strong>'+esc(target["title"])+'</strong>'
            '<p>'+esc(edge["reason"])+'</p><small>Follow this connection ↗</small></a>')
    for ref in incoming:
        origin = POST_BY_SLUG[ref]
        cards.append('<a class="journal-conversation-card journal-conversation-return" href="../'+esc(origin["slug"])+'/">'
            '<span>Another essay leads here</span><strong>'+esc(origin["title"])+'</strong>'
            '<p>'+esc(origin["description"])+'</p><small>Read the earlier argument ↗</small></a>')
    return ('<section class="journal-conversation" id="journal-connections" aria-labelledby="journal-connections-title">'
            '<div class="journal-conversation-intro"><span class="eyebrow">Across the Journal</span>'
            '<h2 id="journal-connections-title">Ideas in conversation</h2>'
            '<p>Read these essays alongside this one. Each takes its own position; a connection is not an endorsement or an additional source citation.</p></div>'
            '<div class="journal-conversation-grid">'+''.join(cards)+'</div></section>')


def article_page(p):
    slug = p["slug"]
    fmt, tone, media = p["art"]
    dark, cream, accent, ink = PALETTES[tone]
    mins = max(2, (len(p["body"].split()) + 209) // 210)
    rendered_body, notes_html = build_citation_notes(editorial_body(p), p)
    conversation = editorial_conversation(p)
    modified = p.get("updated", p["published"])
    image_url = media_url(media, "https://nickgonzales76017.github.io/bonfyre/") if media else "https://nickgonzales76017.github.io/bonfyre/series/media/"+({"authority":"authority-loop.svg","living":"living-subject.svg","access":"expression-ecosystem.svg","operators":"operators-continuity.svg"}.get(p.get("series"),"living-subject.svg"))
    series_order = SERIES_READING_ORDER.get(p.get("series",""), [])
    series_position = series_order.index(slug)+1 if slug in series_order else 0
    series_badge = "Essay "+str(series_position)+" of "+str(len(series_order)) if series_position else "Aurekai Journal"
    toc = "".join('<a href="#'+sid(line[3:])+'">'+esc(line[3:])+'</a>'
        for line in p["body"].splitlines() if line.startswith("## "))
    prim = p.get("source_url")
    series_id = p.get("series", "")
    series_names = {"authority":"Authority and judgment", "living":"Beyond websites",
      "access":"Expression and participation", "operators":"Internet operators"}
    if series_id and series_id not in series_names:
        raise ValueError(f"Unknown editorial series: {series_id}")
    series_html = ('<a class="journal-series-link" href="../../series/#'+series_id+'">Explore '+esc(series_names[series_id])+' ↗</a>') if series_id else ""
    epigraph_text = p.get("epigraph","")
    epigraph_credit = p.get("epigraph_credit","")
    epigraph_url = p.get("epigraph_url","")
    if epigraph_url and not epigraph_url.startswith("https://"):
        raise ValueError(f"Unsupported epigraph source URL: {slug}")
    attribution = ('<a href="'+esc(epigraph_url)+'" rel="noopener noreferrer">'+esc(epigraph_credit)+'</a>') if epigraph_url else esc(epigraph_credit)
    epigraph_html = ('<figure class="journal-epigraph"><blockquote>“'+esc(epigraph_text.strip("“”"))+'”</blockquote><figcaption>'+attribution+'</figcaption></figure>') if epigraph_text and epigraph_credit else ""
    question = p.get("parting_question","")
    reflection_html = ('<aside class="journal-afterword"><span class="eyebrow">A question worth keeping</span><h2>'+esc(question)+'</h2>'+series_html+'</aside>') if question else ""
    external = ('<p><a href="'+esc(prim)+'" rel="noopener noreferrer">Related reference ↗</a></p>') if prim and prim.startswith("https://") else ""
    named_posts = {q["slug"]:q for q in posts}
    if slug in series_order:
        pos = series_order.index(slug)
        related = [named_posts[n] for n in (series_order[pos+1:]+series_order[:pos]) if n in named_posts][:3]
    else:
        related = [q for q in posts if q["slug"] != slug and q["category"] == p["category"]][:3]
    if len(related) < 3:
        related.extend(q for q in posts if q["slug"] != slug and q not in related)
    related = related[:3]
    recommendations = ''.join('<a class="ed-related-item" href="../'+q["slug"]+'/"><span>'+esc(q["category"])+'</span><strong>'+esc(q["title"])+'</strong><small>Read essay ↗</small></a>' for q in related)
    canonical = BASE_URL+slug+"/"
    topic_labels = ''.join('<span class="journal-topic">'+esc(t.strip())+'</span>' for t in p["topics"].split(",") if t.strip())
    cover_visual = media_figure(p, "../../") if media else editorial_detail(p)
    colorvars = "--ed-dark:"+dark+";--ed-paper:"+cream+";--ed-accent:"+accent+";--ed-ink:"+ink

    source_urls = list(dict.fromkeys(
        re.findall(r'\]\((https?://[^\s)]+)\)',p["body"])))
    source_urls = [u for u in source_urls if "nickgonzales76017.github.io/bonfyre/" not in u]
    if prim and prim.startswith("https://") and prim not in source_urls:
        source_urls.append(prim)
    schema={"@context":"https://schema.org","@type":"BlogPosting",
      "@id":canonical+"#article","url":canonical,
      "mainEntityOfPage":{"@type":"WebPage","@id":canonical},
      "headline":p["title"],"description":p["description"],"inLanguage":"en-US",
      "datePublished":p["published"]+"T12:00:00+00:00",
      "dateModified":modified+"T12:00:00+00:00",
      "author":{"@type":"Organization","name":"Aurekai editorial",
        "url":"https://nickgonzales76017.github.io/bonfyre/blog/about/"},
      "publisher":{"@type":"Organization","name":"Aurekai","url":"https://aurekai.work/"},
      "articleSection":p["category"],
      "keywords":[w.strip() for w in p["topics"].split(",") if w.strip()],
      "wordCount":len(p["body"].split()),
      "image":[image_url]}
    dialogue_links = [e["to"] for e in EDITORIAL_BRIDGES.get(slug, [])]
    dialogue_links += EDITORIAL_BACKLINKS.get(slug, [])
    dialogue_links = list(dict.fromkeys(link for link in dialogue_links if link != slug))[:8]
    if dialogue_links:
        schema["relatedLink"]=[BASE_URL+link+"/" for link in dialogue_links]
    if source_urls:
        schema["citation"]=source_urls
    if series_id:
        schema["isPartOf"]={"@type":"CollectionPage",
          "url":"https://nickgonzales76017.github.io/bonfyre/series/#"+series_id,
          "name":series_names[series_id]}
    else:
        schema["isPartOf"]={"@type":"Blog",
          "url":"https://nickgonzales76017.github.io/bonfyre/blog/",
          "name":"Aurekai Journal"}
    jsonld=json.dumps(schema,ensure_ascii=False,separators=(",",":")).replace("<","\\u003c")
    breadcrumb={"@context":"https://schema.org","@type":"BreadcrumbList","itemListElement":[
      {"@type":"ListItem","position":1,"name":"Aurekai Journal","item":"https://nickgonzales76017.github.io/bonfyre/blog/"},
      {"@type":"ListItem","position":2,"name":p["title"],"item":canonical}]}
    breadcrumb_ld=json.dumps(breadcrumb,ensure_ascii=False,separators=(",",":")).replace("<","\\u003c")

    page_label = p["category"]+" / "+p["published"]
    return f'''<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="{dark}">
<title>{esc(p["title"])} — Aurekai Journal</title>
<meta name="description" content="{esc(p["description"])}"><meta name="robots" content="index,follow,max-image-preview:large">
<meta property="og:site_name" content="Aurekai Journal"><meta property="og:url" content="{canonical}"><meta property="og:image" content="{esc(image_url)}"><meta property="og:image:alt" content="Editorial image for {esc(p["title"])}">
<meta name="twitter:title" content="{esc(p["title"])}"><meta name="twitter:description" content="{esc(p["description"])}"><meta name="twitter:image" content="{esc(image_url)}">
<meta property="article:modified_time" content="{esc(modified)}T12:00:00Z">
<meta property="og:title" content="{esc(p["title"])}"><meta property="og:description" content="{esc(p["description"])}"><meta property="og:type" content="article">
<meta name="author" content="Aurekai editorial"><meta property="article:published_time" content="{esc(p["published"])}T12:00:00Z">
<meta property="article:section" content="{esc(p["category"])}"><meta name="twitter:card" content="summary_large_image">
<link rel="canonical" href="{canonical}"><link rel="alternate" type="application/rss+xml" title="Aurekai Journal" href="../feed.xml">
<link rel="sitemap" type="application/xml" href="../../sitemap.xml"><script type="application/ld+json">{jsonld}</script><script type="application/ld+json">{breadcrumb_ld}</script>
<link rel="stylesheet" href="../../assets/aurekai-field-guide.css"><link rel="stylesheet" href="../../assets/aurekai-research.css">
<link rel="author" href="../about/"><link rel="stylesheet" href="../blog.css"><link rel="stylesheet" href="../article-editions.css"><link rel="stylesheet" href="../reader.css"><script defer src="../reader.js"></script>
</head><body class="ed-journal format-{fmt} tone-{tone} {'ed-with-media' if media else 'ed-type-led'}" style="{colorvars}">
<a class="skip" href="#main">Skip to article</a>
<header class="top"><div class="shell"><a class="brand" href="../../"><span class="mark">A</span> AUREKAI <small>/ JOURNAL</small></a>
<nav aria-label="Site navigation"><a href="../../">Home</a><a href="../../discover/">Explore</a><a href="../">Journal</a><a href="../../fieldwork/">Fieldwork</a><a href="../../series/">Series</a><a href="../../docs.html">Bonfÿre</a></nav></div></header>
<main id="main">
<section class="ed-opening" aria-labelledby="article-title">
<div class="ed-opening-grid">
<div class="ed-cover-panel">
<div class="ed-cover-top"><span class="ed-wordmark">AUREKAI<span>JOURNAL</span></span><span>{esc(p["category"])}</span></div>
<div class="ed-cover-art">{cover_visual}</div>
<div class="ed-cover-bottom"><span>Independent editorial / {esc(p["published"])}</span><span>01 — {mins} min read</span></div>
</div>
<div class="ed-title-panel">
<div class="ed-title-utility"><a href="../">← Journal</a><span>{esc(page_label)}</span><a href="#audio-edition" class="journal-audio-jump">▶ Listen</a></div>
<p class="ed-section-label">The Aurekai Journal <span> / An essay</span></p>
<h1 id="article-title">{esc(p["title"])}</h1>
<p class="ed-deck">{esc(p["description"])}</p>
{narrator(p)}
<div class="ed-title-foot"><span>{esc(series_badge)}</span><span>{mins} minute read</span><a href="#read-story">Begin reading ↓</a></div>{byline(p)}
</div>
</div>
</section>
<section class="ed-edition-rule"><div class="shell"><span>{esc(p["category"])} / {esc(p["published"])}</span><span>Independent sources and original analysis</span><a href="../../series/">Explore the four series ↗</a></div></section>
<div class="reader-scroll-track" aria-hidden="true"><span data-reader-scroll></span></div>
<div class="reader-toolbar" aria-label="Article reading tools"><div class="shell"><span class="reader-toolbar-label">Continue reading · <a href="#audio-edition">Listen to this article ↑</a></span>{reader_outline(p)}</div></div>
<section class="journal-article-shell ed-reading" id="read-story"><div class="shell journal-article-layout">
<article class="article-body" itemscope itemtype="https://schema.org/BlogPosting"><meta itemprop="headline" content="{esc(p["title"])}"><meta itemprop="datePublished" content="{esc(p["published"])}"><meta itemprop="dateModified" content="{esc(modified)}"><meta itemprop="mainEntityOfPage" content="{canonical}"><meta itemprop="image" content="{esc(image_url)}"><span itemprop="author" itemscope itemtype="https://schema.org/Organization"><meta itemprop="name" content="Aurekai editorial"><meta itemprop="url" content="https://nickgonzales76017.github.io/bonfyre/blog/about/"></span><span itemprop="publisher" itemscope itemtype="https://schema.org/Organization"><meta itemprop="name" content="Aurekai"><meta itemprop="url" content="https://aurekai.work/"></span>
{epigraph_html}{rendered_body}
{reflection_html}
{notes_html}
{conversation}
<div class="journal-source-note"><strong>About this article.</strong> {esc(p["origin"])}. {("The linked external material is cited for context, not as an endorsement." if prim else "Original analysis prepared for public reading.")}
<a href="https://github.com/Nickgonzales76017/bonfyre/blob/main/{esc(p["source_path"])}">View editorial source ↗</a></div>
</article>
<aside class="journal-article-side"><div class="journal-side-sticky"><span class="eyebrow">In this article</span>{toc}<hr>
{external}<div class="journal-topics">{topic_labels}</div><p><a href="../../discover/">Applications ↗</a> · <a href="../">All articles ↗</a></p>
</div></aside>
</div></section>
<section class="ed-next"><div class="shell"><div class="ed-next-heading"><span>Continue reading</span><h2>Related articles</h2></div><div class="ed-related-row">{recommendations}<a class="ed-related-all" href="../">Browse all {len(posts)} articles ↗</a></div></div></section>
</main>
<footer class="footer"><div class="shell"><span>Aurekai / Bonfÿre · Journal</span><span><a href="../">Journal</a> · <a href="../../market.html">Research</a> · <a href="../../docs.html">Technology</a></span></div></footer></body></html>
'''

for p in posts:
    old_cover = TARGET / p["slug"] / "cover.svg"
    if not CHECK and old_cover.exists():
        old_cover.unlink()
    write(p["slug"] + "/index.html", article_page(p))

def card(p):
    fmt, tone, media = p["art"]
    search = ' '.join([p["title"], p["description"], p["topics"], p["category"]]).lower()
    art = ('<img src="'+esc(media_url(media,"../"))+'" alt="" width="720" height="480" loading="lazy">' if media else
       '<span class="ed-card-feature">'+esc(p["title"])+'</span>')
    outline = [line[3:].strip() for line in p["body"].splitlines() if line.startswith("## ")][:3]
    outline_preview = ('<div class="journal-card-outline" aria-hidden="true"><strong>Inside the essay</strong>'+''.join('<span>'+esc(h)+'</span>' for h in outline)+'</div>') if outline else ""
    return f'''<article class="journal-card ed-card card-{tone}" data-article data-category="{esc(p["category"])}" data-search="{esc(search)}">
<a class="journal-card-link" href="./{p["slug"]}/"><div class="journal-card-top"><span class="eyebrow">{esc(p["category"])}</span><span class="ed-card-kind">{esc(fmt.replace("-"," "))}</span></div>
<div class="ed-card-cover">{art}</div><h3>{esc(p["title"])}</h3><p>{esc(p["description"])}</p>{outline_preview}
<div class="journal-card-bottom"><span>{esc(p["published"])}</span><span>Read / listen ↗</span></div></a></article>'''

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
# Use individual authored dates, never a synthetic site-wide modification date.
# Static landing pages omit lastmod when their exact edit time is not known.
article_sitemap_dates = {
    BASE_URL+post["slug"]+"/":post.get("updated",post["published"]) for post in posts
}
urls = [BASE_URL, BASE_URL+"about/", "https://nickgonzales76017.github.io/bonfyre/series/"] + list(article_sitemap_dates)
def sitemap_entry(url):
    date = article_sitemap_dates.get(url)
    return "<url><loc>"+xml_escape(url)+"</loc>" + (
        "<lastmod>"+date+"</lastmod>" if date else "") + "</url>"
sitemap = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + '\n'.join(
    sitemap_entry(url) for url in urls) + '\n</urlset>\n'
write("sitemap.xml", sitemap)

# Serve the same per-article dates from the site-wide sitemap. All other
# pages remain discoverable without inventing a modification timestamp.
site_urls = ["https://nickgonzales76017.github.io/bonfyre/",
             "https://nickgonzales76017.github.io/bonfyre/discover/",
             "https://nickgonzales76017.github.io/bonfyre/fieldwork/"]
site_urls += urls
root_sitemap = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + '\n'.join(
    sitemap_entry(url) for url in dict.fromkeys(site_urls)) + '\n</urlset>\n'
root_sitemap_path = ROOT / "site" / "sitemap.xml"
if CHECK:
    if not root_sitemap_path.exists() or root_sitemap_path.read_text(encoding="utf-8") != root_sitemap:
        raise AssertionError("Out-of-date site-wide sitemap")
else:
    root_sitemap_path.write_text(root_sitemap,encoding="utf-8")

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


# Every article has explicit, human-picked art direction. No SVG covers
# generated from a slug, no fabricated quotes, no filler artwork.
if CHECK:
    formats, photo_pages = set(), 0
    for p in posts:
        fmt, tone, media = p["art"]
        markup=(TARGET/p["slug"]/"index.html").read_text(encoding="utf-8")
        formats.add(fmt)
        if (markup.count('<h1 id="article-title">')!=1 or
            markup.count('data-narration data-state="idle"')!=1 or
            'data-narration-seek' not in markup or
            'data-narration-wave' not in markup or
            'href="#audio-edition"' not in markup or
            f'class="ed-journal format-{fmt} tone-{tone} ' not in markup or
            'href="../article-editions.css"' not in markup or
            'id="read-story"' not in markup or
            '<aside class="journal-article-side">' not in markup):
            raise AssertionError(f"Editorial article contract failed: {p['slug']}")
        if 'cover.svg' in markup or 'garden-' in markup:
            raise AssertionError(f"Retired generated artwork in {p['slug']}")
        if (TARGET/p["slug"]/"cover.svg").exists():
            raise AssertionError(f"Orphan procedural cover: {p['slug']}")
        if media:
            photo_pages+=1
            if MEDIA[media][1] not in markup:
                raise AssertionError(f"Unattributed source image: {p['slug']}")
    index=(TARGET/"index.html").read_text(encoding="utf-8")
    if index.count('class="ed-card-cover"')!=len(posts):
        raise AssertionError("Directory does not include all article editions")
    if 'href="./article-editions.css"' not in index or 'article-garden' in index:
        raise AssertionError("Journal retains retired cover styles")
    if len(formats)<12 or photo_pages<8:
        raise AssertionError(f"Editorial variety too narrow: {len(formats)} formats and {photo_pages} source images")
    print(f"EDITORIAL_DESIGN_PASS {len(posts)} individually assigned editions / {len(formats)} formats / {photo_pages} credited photographic or first-party features / zero generated drawings")

print(f'Aurekai Journal: {len(posts)} authored articles, {len(categories)} categories, RSS, sitemap, and native-import index ' + ('verified' if CHECK else 'built'))
PY
