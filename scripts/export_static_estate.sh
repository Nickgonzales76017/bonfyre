#!/bin/sh
# The estate, as bytes anyone can serve.
#
# A composed page is already settled for minutes at a time: the projection
# cache proves it, serving the same bytes to every anonymous visitor until the
# material underneath changes.  A thing that is the same for everyone for
# minutes does not need a server per visitor -- it needs a server once, and an
# edge after that.
#
# So this walks the estate exactly as a visitor would, over its own HTTP, and
# writes what comes back as a tree of files.  What it produces can be put on
# any static host on earth and answers in milliseconds anywhere, which is what
# lets one small machine's work reach an enormous number of people.
#
# It is an export of what the estate composed, never a second estate: it reads
# only, invents nothing, and every page it writes says the same thing the
# estate said at that moment.
#
#   export_static_estate.sh --from http://127.0.0.1:28801 --out dist [--items 8000]
set -eu

FROM="http://127.0.0.1:28801"
OUT="dist"
HOST="aurekai.life"
ITEM_LIMIT=20000
PARALLEL=12
ARCHIVE="${BONFYRE_ITEM_ARCHIVE_DB:-}"
FRONTIER="${BONFYRE_SOURCE_FRONTIER_DB:-}"
FABRIC="${BONFYRE_SUBJECT_FABRIC_DB:-}"
CMS="${BONFYRE_CMS_DB:-}"
SITE_ROOT="${BONFYRE_SITE_ROOT:-site/matrix}"
# The stores the subject rooms read. A room's addresses come from the same store
# the room itself queries, so the export cannot drift from what the estate holds.
MODEL_CATALOG="${BONFYRE_MODEL_CATALOG:-$HOME/.local/share/bonfyre/catalog.db}"
FRAPPE_DB="${BONFYRE_FRAPPE_SURFACE_DB:-}"
REPO_ROOT="${BONFYRE_REPO:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}"
RECORD_KIND_LIMIT="${RECORD_KIND_LIMIT:-400}"
INSTITUTION_LIMIT="${INSTITUTION_LIMIT:-600}"
# The page organ accepts a bounded walk of 400 lengths.  The static export must
# not stop at an unrelated twelfth page while the route itself still offers a
# next length: that silently strands admitted sources from public circulation.
MAX_DEPTH="${MAX_DEPTH:-400}"
SOURCE_LEDGER_ONLY=0
PEOPLE_LEDGER_ONLY=0

while [ $# -gt 0 ]; do
    case "$1" in
        --from) FROM=$2; shift 2 ;;
        --out) OUT=$2; shift 2 ;;
        --host) HOST=$2; shift 2 ;;
        --items) ITEM_LIMIT=$2; shift 2 ;;
        --parallel) PARALLEL=$2; shift 2 ;;
        --archive) ARCHIVE=$2; shift 2 ;;
        --frontier) FRONTIER=$2; shift 2 ;;
        --cms) CMS=$2; shift 2 ;;
        --site-root) SITE_ROOT=$2; shift 2 ;;
        --max-depth) MAX_DEPTH=$2; shift 2 ;;
        --source-ledger-only) SOURCE_LEDGER_ONLY=1; shift ;;
        --people-ledger-only) PEOPLE_LEDGER_ONLY=1; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

[ "$SOURCE_LEDGER_ONLY" = 0 ] || [ "$PEOPLE_LEDGER_ONLY" = 0 ] || {
    echo "choose only one ledger-only mode" >&2; exit 2;
}

case "$MAX_DEPTH" in
    ''|*[!0-9]*) echo "--max-depth must be a positive whole number" >&2; exit 2 ;;
esac
[ "$MAX_DEPTH" -ge 1 ] || { echo "--max-depth must be at least 1" >&2; exit 2; }
[ "$MAX_DEPTH" -le 400 ] || MAX_DEPTH=400

mkdir -p "$OUT"
written=0
failed=0

# SourceDepth's query coordinates are public addresses, not a private database
# view.  Carry every admitted source profile and the bounded admitted index
# that leads to it.  This reads the page organ over HTTP, so static bytes stay
# an export of what WebAddress said rather than a second renderer.
source_ledger() {
    [ -n "$FRONTIER" ] && [ -f "$FRONTIER" ] || return 0
    echo "every admitted source, with its own address"
    manifest="$OUT/.source-ledger.list"
    tool_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../cmd/BonfyreSourceFrontier/tools" && pwd)
    make -s -C "$tool_dir/.." tools/source-ledger-static-manifest
    "$tool_dir/source-ledger-static-manifest" --frontier "$FRONTIER" > "$manifest"
    count=$(wc -l < "$manifest" | tr -d ' ')
    echo "  $count source-ledger coordinates, asked $PARALLEL at a time"
    helper="$OUT/.fetch-source-ledger.sh"
    cat > "$helper" <<'WORKER'
#!/bin/sh
path=$1
key=$2
mkdir -p "$OUT/$(dirname "$key")"
attempt=1
code=000
while [ "$attempt" -le 3 ]; do
    rm -f "$OUT/$key.part"
    code=$(curl -s -m 30 -o "$OUT/$key.part" -w '%{http_code}' -H "Host: $HOST" "$FROM$path" 2>/dev/null || echo 000)
    if [ "$code" = 200 ] && [ -s "$OUT/$key.part" ]; then
        mv "$OUT/$key.part" "$OUT/$key"
        exit 0
    fi
    attempt=$((attempt + 1))
done
rm -f "$OUT/$key.part"
echo "  $path did not answer after 3 attempts ($code)" >&2
exit 1
WORKER
    chmod +x "$helper"
    export OUT FROM HOST
    tr '\t' '\n' < "$manifest" | xargs -P "$PARALLEL" -n 2 "$helper" || failed=$((failed + 1))
    rm -f "$helper" "$manifest"
    actual=$(find "$OUT/channels/__q" -type f -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
    written=$((written + actual))
    echo "  $actual source-ledger pages written"
    # A release is a finite generation, not a best-effort sample.  xargs can
    # report only that one or more children failed; the manifest cardinality is
    # the actual contract.  Refuse before sync/deploy when it is incomplete.
    if [ "$actual" -ne "$count" ]; then
        echo "  source-ledger incomplete: expected $count coordinates, materialized $actual" >&2
        return 1
    fi
}

# The people directory has a finite, public derivation closure too. A name is
# never a global identity key: every exact profile carries the source that
# printed it, while an old name-only address lowers to an explicit chooser.
people_ledger() {
    [ -n "$ARCHIVE" ] && [ -f "$ARCHIVE" ] || return 0
    echo "every retained credited-name observation, with its own address"
    # The root is the finite directory into this closure. A ledger-only
    # generation without it has every leaf but no public way to begin.
    mkdir -p "$OUT"
    code=$(curl -s -m 30 -o "$OUT/people.html.part" -w '%{http_code}' -H "Host: $HOST" "$FROM/people" 2>/dev/null || echo 000)
    if [ "$code" != 200 ] || [ ! -s "$OUT/people.html.part" ]; then
        rm -f "$OUT/people.html.part"
        echo "  /people directory did not answer ($code)" >&2
        return 1
    fi
    mv "$OUT/people.html.part" "$OUT/people.html"
    written=$((written + 1))
    manifest="$OUT/.people-ledger.list"
    tool_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../cmd/BonfyreWebAddress/tools" && pwd)
    make -s -C "$tool_dir/.." tools/people-static-manifest
    "$tool_dir/people-static-manifest" --archive "$ARCHIVE" --null > "$manifest"
    fields=$(tr -cd '\000' < "$manifest" | wc -c | tr -d ' ')
    [ $((fields % 2)) -eq 0 ] || { echo "people ledger has an incomplete coordinate pair" >&2; return 1; }
    count=$((fields / 2))
    echo "  $count credited-name coordinates, asked $PARALLEL at a time"
    helper="$OUT/.fetch-people-ledger.sh"
    cat > "$helper" <<'WORKER'
#!/bin/sh
path=$1
key=$2
mkdir -p "$OUT/$(dirname "$key")"
attempt=1
code=000
while [ "$attempt" -le 3 ]; do
    rm -f "$OUT/$key.part"
    code=$(curl -s -m 30 -o "$OUT/$key.part" -w '%{http_code}' -H "Host: $HOST" "$FROM$path" 2>/dev/null || echo 000)
    if [ "$code" = 200 ] && [ -s "$OUT/$key.part" ]; then
        mv "$OUT/$key.part" "$OUT/$key"
        exit 0
    fi
    attempt=$((attempt + 1))
done
rm -f "$OUT/$key.part"
echo "  $path did not answer after 3 attempts ($code)" >&2
exit 1
WORKER
    chmod +x "$helper"
    export OUT FROM HOST
    xargs -0 -P "$PARALLEL" -n 2 "$helper" < "$manifest" || failed=$((failed + 1))
    rm -f "$manifest"
    actual=$(find "$OUT/people/__q" -type f -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
    written=$((written + actual))
    echo "  $actual credited-name pages written"
    if [ "$actual" -ne "$count" ]; then
        echo "  people ledger incomplete: expected $count coordinates, materialized $actual" >&2
        return 1
    fi
    if [ "$PEOPLE_LEDGER_ONLY" = 0 ]; then
    # Reuse the same profile renderer for every source-qualified maker host.
    profile_manifest="$OUT/.people-profile-hosts.list"
    if [ -n "$CMS" ]; then
        [ -f "$CMS" ] || { echo "people profile CMS publication source is unavailable" >&2; return 1; }
        "$tool_dir/people-static-manifest" --archive "$ARCHIVE" --cms "$CMS" --profile-hosts --null > "$profile_manifest"
    else
        "$tool_dir/people-static-manifest" --archive "$ARCHIVE" --profile-hosts --null > "$profile_manifest"
    fi
    profile_fields=$(tr -cd '\000' < "$profile_manifest" | wc -c | tr -d ' ')
    [ $((profile_fields % 2)) -eq 0 ] || { echo "profile host coordinate pair incomplete" >&2; return 1; }
    profile_count=$((profile_fields / 2))
    echo "  $profile_count credited profile subdomains"
    xargs -0 -P "$PARALLEL" -n 2 "$helper" < "$profile_manifest" || failed=$((failed + 1))
    rm -f "$profile_manifest" "$helper"
    actual_profiles=$(find "$OUT/hosts" -type f -name index.html 2>/dev/null | grep -E '/hosts/credited-[a-z0-9-]+-[0-9a-f]{20}\.aurekai\.work/index\.html$' | wc -l | tr -d " ")
    written=$((written + actual_profiles))
    echo "  $actual_profiles credited profile hosts materialized"
    if [ "$actual_profiles" -ne "$profile_count" ]; then
        echo "  profile closure incomplete: expected $profile_count, materialized $actual_profiles" >&2
        return 1
    fi
    fi
}

if [ "$SOURCE_LEDGER_ONLY" = 1 ]; then
    source_ledger
    bytes=$(du -sk "$OUT" | cut -f1)
    echo "wrote $written source-ledger pages ($failed refused) into $OUT, ${bytes}K on disk"
    exit 0
fi

if [ "$PEOPLE_LEDGER_ONLY" = 1 ]; then
    people_ledger
    bytes=$(du -sk "$OUT" | cut -f1)
    echo "wrote $written people-ledger pages ($failed refused) into $OUT, ${bytes}K on disk"
    exit 0
fi

# A page is written as <path>.html, which a static host serves at the address
# people were actually given.  Writing <path>/index.html instead costs every
# visitor a redirect to a trailing slash they never typed.
fetch() {
    path=$1
    case "$path" in
        /) file="$OUT/index.html" ;;
        *) mkdir -p "$OUT$(dirname "$path")"; file="$OUT${path}.html" ;;
    esac
    code=$(curl -s -m 30 -o "$file.tmp" -w '%{http_code}' -H "Host: $HOST" "$FROM$path" 2>/dev/null || echo 000)
    if [ "$code" = "200" ] && [ -s "$file.tmp" ]; then
        mv "$file.tmp" "$file"
        written=$((written + 1))
        [ $((written % 250)) -eq 0 ] && echo "  ...$written pages"
        return 0
    fi
    rm -f "$file.tmp"
    failed=$((failed + 1))
    echo "  $path did not answer ($code)" >&2
    return 1
}

# The pages name one stylesheet rather than carrying a copy each; the export
# has to carry the stylesheet too, or every page it writes is unstyled.
echo "what every page is dressed in"
mkdir -p "$OUT/assets"
# A member's and a publisher's pages also name the bar's sheet, the site
# program's, an independent site's, and the sheets of the surfaces they open;
# leaving those behind sent every one of those pages out unstyled.
for asset in tailwind.css world.css matrix.css matrix-aperture.css aurekai-native.css aurekai-composed.css \
             aurekai-bar.css aurekai-site-program.css own-site.css \
             donor-lemmy.css donor-mobilizon.css donor-nextcommerce.css donor-pinry.css donor-pixelfed.css \
             donor-plane.css donor-zulip.css donor-maplibre.css; do
    if [ -f "$SITE_ROOT/$asset" ]; then
        cp "$SITE_ROOT/$asset" "$OUT/assets/$asset"
        echo "  $asset ($(wc -c < "$SITE_ROOT/$asset" | tr -d ' ') bytes, carried once for every page)"
        written=$((written + 1))
    fi
done

# A MediaProgram may only name a digest-verified same-origin asset twin.  Carry
# those exact bytes into the static closure before pages are exported: without
# this, /_surface/listen-program worked only on the native origin and the
# released R2 estate held an HTML player pointing at a missing asset.  The
# suffix is a MIME hint for object stores, never an identity claim; WebAddress
# re-verifies the digest before serving the native form.
if [ -n "$CMS" ] && [ -f "$CMS" ]; then
    echo "published media twins"
    manifest="$OUT/.media-twins.list"
    sqlite3 "$CMS" "SELECT DISTINCT asset_ref FROM media_program WHERE media_kind='audio' AND publication_state='published' AND status='published' AND asset_ref LIKE '/assets/twin/%' ORDER BY asset_ref;" > "$manifest" 2>/dev/null || true
    while IFS= read -r path; do
        case "$path" in
            /assets/twin/[0123456789abcdef][0123456789abcdef][0123456789abcdef][0123456789abcdef]*.*) ;;
            *) echo "  refused non-static-twin asset: $path" >&2; failed=$((failed + 1)); continue ;;
        esac
        file="$OUT$path"
        mkdir -p "$(dirname "$file")"
        code=$(curl -s -m 180 -o "$file.part" -w '%{http_code}' -H "Host: $HOST" "$FROM$path" 2>/dev/null || echo 000)
        if [ "$code" = 200 ] && [ -s "$file.part" ]; then
            mv "$file.part" "$file"
            written=$((written + 1))
        else
            rm -f "$file.part"
            echo "  $path did not answer ($code)" >&2
            failed=$((failed + 1))
        fi
    done < "$manifest"
    rm -f "$manifest"
fi

echo "routes"
# Every public route the composition table declares, asked for by name.
for path in / /discover /whats-on /nearby /listen /podcasts /watch /board /channels \
            /community /stage /on-this-day /parcels /releases /advisories /research \
            /people /organisations /search; do
    fetch "$path" || true
done

# A page that advertises the native Atom feed must not become a static page
# pointing at a missing machine address. Carry exactly the host-scoped bytes
# WebAddress answered; the exporter never reconstructs feed membership.
if { [ -f "$OUT/index.html" ] && grep -q 'href="/feed.atom"' "$OUT/index.html"; } ||
   { [ -f "$OUT/discover.html" ] && grep -q 'href="/feed.atom"' "$OUT/discover.html"; }; then
    code=$(curl -s -m 30 -o "$OUT/feed.atom.part" -w '%{http_code}' \
      -H "Host: $HOST" "$FROM/feed.atom" 2>/dev/null || echo 000)
    if [ "$code" != 200 ] || [ ! -s "$OUT/feed.atom.part" ]; then
        rm -f "$OUT/feed.atom.part"
        echo "  advertised /feed.atom did not answer ($code); refusing static release" >&2
        exit 1
    fi
    mv "$OUT/feed.atom.part" "$OUT/feed.atom"
    written=$((written + 1))
fi

echo "each route, as far down as it goes"
# A route is as long as the estate has publishers for it.  The export follows
# the same rel=next chain a reader follows, writing each length at the address
# the page itself names, so the walk works on a host that only serves files.
for path in /discover /watch /board /listen /podcasts /channels /community \
            /nearby /whats-on /on-this-day /research /releases /advisories /parcels; do
    depth=2
    last=1
    while [ "$depth" -le "$MAX_DEPTH" ]; do
        page="$path/$depth"
        mkdir -p "$OUT$path"
        code=$(curl -s -m 30 -o "$OUT$page.html.part" -w '%{http_code}' -H "Host: $HOST" "$FROM$page" 2>/dev/null || echo 000)
        if [ "$code" != "200" ] || [ ! -s "$OUT$page.html.part" ]; then rm -f "$OUT$page.html.part"; break; fi
        # A length with nothing on it ends the walk rather than being published.
        if ! grep -q 'href="/o/' "$OUT$page.html.part"; then rm -f "$OUT$page.html.part"; break; fi
        mv "$OUT$page.html.part" "$OUT$page.html"
        written=$((written + 1))
        last=$depth
        grep -q 'rel="next"' "$OUT$page.html" || break
        depth=$((depth + 1))
    done
    [ "$last" -gt 1 ] && echo "  $path reads $last lengths"
done

echo "the subject rooms"
# Every room the composer serves, exported as bytes an edge can answer with.
#
# This was the whole reason none of the estate's rooms existed on the live sites:
# the export walked the composed routes -- /discover, /whats-on, /channels -- and
# no /s/ room at all, so the institution pages, the governance grammar, the price
# observatory, the compiled record kinds and everything built after them answered
# 404 at the edge while working perfectly on the machine that composed them.
#
# Two kinds of room. An index room has one fixed address and is asked for by
# name. A per-entity room has as many addresses as the estate has entities, so the
# identifiers come from the same stores the room itself reads -- never from a list
# typed here, which would go stale the first time the estate learned something.
#
# Every one is exported as a signed-out reader sees it. A room that shows a reader
# their own household's connections shows this export none of them, which is the
# scoping working rather than a gap in the export.
for path in /s/grammars/all /s/sets/all /s/models/all /s/apps/all /s/work/all \
            /s/whats-on/all /s/prices/observed /s/frontier/cost \
            /s/governance/grammar /s/operators/archetypes; do
    fetch "$path" || true
done

# The interaction grammars and the cells under them: both tables are compiled, so
# the names come out of the generated header rather than being retyped.
CLONE_HEADER="$REPO_ROOT/cmd/BonfyreWebAddress/generated/native_clone_grammars.h"
if [ -f "$CLONE_HEADER" ]; then
    sed -n '/bf_clone_grammars\[\] = {/,/^};/p' "$CLONE_HEADER" |
      sed -n 's/^  {"\([a-z0-9-]*\)".*/\1/p' > "$OUT/.grammars.list"
    while read -r g; do
        [ -n "$g" ] && { fetch "/s/grammar/$g" || true; }
    done < "$OUT/.grammars.list"
    sed -n '/bf_interaction_primitives\[\] = {/,/^};/p' "$CLONE_HEADER" |
      sed -n 's/^  {"\([a-z-]*\)".*/\1/p' > "$OUT/.cells.list"
    while read -r c; do
        [ -n "$c" ] && { fetch "/s/primitive/$c" || true; }
    done < "$OUT/.cells.list"
    echo "  $(wc -l < "$OUT/.grammars.list" | tr -d ' ') grammars and $(wc -l < "$OUT/.cells.list" | tr -d ' ') cells"
    rm -f "$OUT/.grammars.list" "$OUT/.cells.list"
fi

# Working sets and the community bodies' projects, from the record store.
if [ -n "$CMS" ] && [ -f "$CMS" ]; then
    sqlite3 "$CMS" "SELECT set_id FROM discovery_set WHERE status='published';" 2>/dev/null \
        > "$OUT/.sets.list" || : > "$OUT/.sets.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/set/$x" || true; }; done < "$OUT/.sets.list"
    sqlite3 "$CMS" "SELECT project_id FROM project WHERE status='published' LIMIT 200;" 2>/dev/null \
        > "$OUT/.projects.list" || : > "$OUT/.projects.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/project/$x" || true; }; done < "$OUT/.projects.list"
    echo "  $(wc -l < "$OUT/.sets.list" | tr -d ' ') working sets, $(wc -l < "$OUT/.projects.list" | tr -d ' ') projects"
    rm -f "$OUT/.sets.list" "$OUT/.projects.list"
fi

# Public subject rooms are not an operator sample.  Their identifiers come
# from the same owners their native renderers read, so a newly published note
# or program reaches the edge in the next generation without a hand-maintained
# route list.  A private note is intentionally absent: this export is the
# signed-out public projection.
if [ -n "$CMS" ] && [ -f "$CMS" ]; then
    sqlite3 "$CMS" "SELECT substr(route, 7) FROM wiki_page
      WHERE status='published' AND allow_guest=1 AND route LIKE 'notes/%'
      ORDER BY id;" 2>/dev/null > "$OUT/.public-notes.list" || : > "$OUT/.public-notes.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/note/$x" || true; }; done < "$OUT/.public-notes.list"

    sqlite3 "$CMS" "SELECT instance_id FROM program_instance
      WHERE status='published' ORDER BY instance_id;" 2>/dev/null > "$OUT/.programs.list" || : > "$OUT/.programs.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/program/$x" || true; }; done < "$OUT/.programs.list"

    echo "  $(wc -l < "$OUT/.public-notes.list" | tr -d ' ') public notes, $(wc -l < "$OUT/.programs.list" | tr -d ' ') programs"
    rm -f "$OUT/.public-notes.list" "$OUT/.programs.list"
fi

# Capabilities are compiled into WebAddress itself.  Read the one canonical
# catalogue from the source that owns the entries rather than spelling a second
# list into the release script.
CAPABILITY_SOURCE="$REPO_ROOT/cmd/BonfyreWebAddress/src/main.c"
if [ -f "$CAPABILITY_SOURCE" ]; then
    sed -n '/static const X402Capability X402_CATALOG\[\] = {/,/^};/p' "$CAPABILITY_SOURCE" |
      sed -n 's/^[[:space:]]*{"\([a-z0-9-]*\)".*/\1/p' > "$OUT/.capabilities.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/capability/$x" || true; }; done < "$OUT/.capabilities.list"
    echo "  $(wc -l < "$OUT/.capabilities.list" | tr -d ' ') compiled capabilities"
    rm -f "$OUT/.capabilities.list"
fi

# The models, from the catalogue the model room reads.
if [ -n "$MODEL_CATALOG" ] && [ -f "$MODEL_CATALOG" ]; then
    sqlite3 "$MODEL_CATALOG" \
      "SELECT external_id FROM catalog_nodes WHERE kind='model' AND external_id <> '';" 2>/dev/null \
        > "$OUT/.models.list" || : > "$OUT/.models.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/model/$x" || true; }; done < "$OUT/.models.list"
    echo "  $(wc -l < "$OUT/.models.list" | tr -d ' ') models"
    rm -f "$OUT/.models.list"
fi

# The compiled record kinds. There are over a thousand, so this exports them
# largest first and stops at a stated bound rather than silently exporting some.
if [ -n "$FRAPPE_DB" ] && [ -f "$FRAPPE_DB" ]; then
    sqlite3 "$FRAPPE_DB" \
      "SELECT slug FROM frappe_program ORDER BY field_count DESC LIMIT $RECORD_KIND_LIMIT;" 2>/dev/null \
        > "$OUT/.kinds.list" || : > "$OUT/.kinds.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/record-kind/$x" || true; }; done < "$OUT/.kinds.list"
    total=$(sqlite3 "$FRAPPE_DB" "SELECT COUNT(*) FROM frappe_program;" 2>/dev/null || echo 0)
    echo "  record kinds: $(wc -l < "$OUT/.kinds.list" | tr -d ' ') of $total, largest first"
    rm -f "$OUT/.kinds.list"
fi

# The institutions a reader can actually reach: publishers this estate has
# admitted a channel from, rather than every root it has only heard of.
if [ -n "$FRONTIER" ] && [ -f "$FRONTIER" ]; then
    sqlite3 "$FRONTIER" \
      "SELECT DISTINCT r.host FROM root r JOIN channel c ON c.root_id = r.root_id
       WHERE c.state = 'admitted' AND r.host <> '' ORDER BY r.host LIMIT $INSTITUTION_LIMIT;" 2>/dev/null \
        > "$OUT/.insts.list" || : > "$OUT/.insts.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/institution/$x" || true; }; done < "$OUT/.insts.list"
    echo "  $(wc -l < "$OUT/.insts.list" | tr -d ' ') institutions with an admitted channel"
    rm -f "$OUT/.insts.list"

    # A publisher is its own public subject rather than merely a source of
    # search rows.  There is deliberately no limit here: an export that gives
    # an address to only a sample of admitted publishers recreates the
    # circulation bottleneck this full-estate publish path exists to remove.
    sqlite3 "$FRONTIER" \
      "SELECT DISTINCT r.host FROM root r JOIN channel c ON c.root_id = r.root_id
       WHERE c.state = 'admitted' AND r.host <> '' ORDER BY r.host;" 2>/dev/null \
        > "$OUT/.publishers.list" || : > "$OUT/.publishers.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/publisher/$x" || true; }; done < "$OUT/.publishers.list"
    echo "  $(wc -l < "$OUT/.publishers.list" | tr -d ' ') admitted publishers"
    rm -f "$OUT/.publishers.list"
fi

# Parcel subjects are read from the public subject manifest exposed by the
# native parcel owner.  The native manifest has its own stated bound so a
# sitemap remains fetchable; this exporter preserves that bound rather than
# inventing a second query over property observations.  It converts the
# machine-facing room coordinates into the canonical public subject address
# used by people and Save actions.
parcel_sitemap="$OUT/.parcel-subjects.xml"
parcel_code=$(curl -s -m 30 -o "$parcel_sitemap" -w '%{http_code}' \
  -H "Host: $HOST" "$FROM/sitemap-subjects.xml" 2>/dev/null || echo 000)
if [ "$parcel_code" = 200 ] && [ -s "$parcel_sitemap" ]; then
    sed -n 's|.*<loc>[^<]*/room/parcel/\([A-Za-z0-9-]*\)</loc>.*|\1|p' "$parcel_sitemap" \
      | sort -u > "$OUT/.parcels.list"
    while read -r x; do [ -n "$x" ] && { fetch "/s/parcel/$x" || true; }; done < "$OUT/.parcels.list"
    echo "  $(wc -l < "$OUT/.parcels.list" | tr -d ' ') parcel subjects from the public manifest"
    rm -f "$OUT/.parcels.list"
fi
rm -f "$parcel_sitemap"

echo "the ledger, a page at a time"
# The way into thousands of publishers: the first pages of each cut, so the
# directory is walkable without a server behind it.
for path in "/channels?roots=1" "/channels?way=rss" "/channels?way=ics" "/channels?publishes=news"; do
    safe=$(printf '%s' "$path" | sed 's|[?&=]|_|g')
    code=$(curl -s -m 30 -o "$OUT$safe.html" -w '%{http_code}' -H "Host: $HOST" "$FROM$path" 2>/dev/null || echo 000)
    [ "$code" = "200" ] && written=$((written + 1)) || rm -f "$OUT$safe.html"
done

if [ -n "$ARCHIVE" ] && [ -f "$ARCHIVE" ]; then
    echo "everything the estate has kept"
    case "$HOST" in
        aurekai.life) subject="institution:aurekai:life" ;;
        aurekai.live) subject="institution:aurekai:live" ;;
        aurekai.work) subject="institution:aurekai:work" ;;
        aurekai.shop) subject="institution:aurekai:shop" ;;
        *) subject="" ;;
    esac
    if [ -n "$FABRIC" ]; then
        [ -f "$FABRIC" ] && [ -n "$subject" ] || {
            echo "cannot derive a public subject for static item export: $HOST" >&2
            exit 2
        }
        fabric_sql=$(printf '%s' "$FABRIC" | sed "s/'/''/g")
        sqlite3 "$ARCHIVE" "ATTACH DATABASE '$fabric_sql' AS compiled;
            SELECT i.source || '/' || i.key FROM item i
            JOIN (SELECT DISTINCT source,item_key FROM compiled.subject_route_item
                  WHERE subject='$subject' AND scope='public') routed
              ON routed.source=i.source AND routed.item_key=i.key
            ORDER BY i.at DESC LIMIT $ITEM_LIMIT;" > "$OUT/.items.list" || {
                echo "public route manifest failed; refusing incomplete export" >&2
                exit 2
            }
    else
        sqlite3 "$ARCHIVE" "SELECT source || '/' || key FROM item ORDER BY at DESC LIMIT $ITEM_LIMIT;" \
            > "$OUT/.items.list" 2>/dev/null || true
    fi
    # Asked in parallel: the estate answers a warm page in milliseconds, so
    # the cost of an export is almost entirely waiting, and waiting serially
    # for tens of thousands of pages is hours of nothing happening.
    count=$(wc -l < "$OUT/.items.list" | tr -d ' ')
    echo "  $count items, asked $PARALLEL at a time"
    export OUT FROM HOST
    # The worker lives in its own file: a shell function cannot cross xargs,
    # and an inline script long enough to do the job is longer than a command
    # line is allowed to be.
helper="$OUT/.fetch-one.sh"
    cat > "$helper" <<'WORKER'
#!/bin/sh
item=$1
mkdir -p "$OUT/o/$(dirname "$item")"
attempt=1
code=000
while [ "$attempt" -le 3 ]; do
    rm -f "$OUT/o/$item.html.part"
    code=$(curl -s -m 30 -o "$OUT/o/$item.html.part" -w '%{http_code}' -H "Host: $HOST" "$FROM/o/$item" 2>/dev/null || echo 000)
    if [ "$code" = 200 ] && [ -s "$OUT/o/$item.html.part" ]; then
        mv "$OUT/o/$item.html.part" "$OUT/o/$item.html"
        exit 0
    fi
    attempt=$((attempt + 1))
done
rm -f "$OUT/o/$item.html.part"
echo "  /o/$item did not answer after 3 attempts ($code)" >&2
exit 1
WORKER
    chmod +x "$helper"
    xargs -P "$PARALLEL" -n 1 "$helper" < "$OUT/.items.list" || failed=$((failed + 1))
    rm -f "$helper"
    actual=$(find "$OUT/o" -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
    written=$((written + actual))
    rm -f "$OUT/.items.list"
    # Retained item pages are a finite release closure, not a best-effort
    # sample.  Without this cardinality gate a dead origin could print a
    # successful-looking export while silently dropping almost the estate.
    if [ "$actual" -ne "$count" ]; then
        echo "  item archive incomplete: expected $count retained items, materialized $actual" >&2
        exit 1
    fi
else
    echo "no item archive given: only routes were written"
fi

source_ledger
people_ledger

# A static host has no /healthz of its own; say plainly what this tree is.
cat > "$OUT/healthz" <<HEALTH
{"schema":"bonfyre-static-estate.v1","composed":"$(date -u +%FT%TZ)","pages":$written,
 "cannot_infer":"a page here is what the estate composed at the moment it was exported, not a live reading of its sources"}
HEALTH

bytes=$(du -sk "$OUT" | cut -f1)
echo
echo "wrote $written pages ($failed refused) into $OUT, ${bytes}K on disk"
