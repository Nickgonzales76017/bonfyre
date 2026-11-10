#!/bin/sh
# Every address this estate gives a subject, as bytes the edge can serve.
#
# The estate answers on hundreds of hosts -- a publisher it reads, a member
# standing in a territory -- and each one composes that subject's own material.
# None of that reached anyone: the static export walked a single host, and the
# edge resolved a request by its path alone, so every subdomain in the world
# would have been handed the territory's front page under the subject's name.
# An address that shows someone else's material is worse than no address.
#
# So each published host is walked on its own, and written under a prefix that
# names it.  The edge looks there first and falls back to the shared estate,
# which is what makes one published tree carry every subject without copying
# the whole estate under each of them.
#
# It exports only hosts the estate already publishes a presentation for, and
# only what those hosts answered.  Being addressed remains this estate's own
# decision to read a subject; it is not a relationship anyone agreed to.
#
#   export_subject_hosts.sh --from URL --out DIR [--routes "/ /about"] [--limit 400] [--only "HOST HOST"]
set -eu

FROM="http://127.0.0.1:28801"
OUT="dist"
ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
ROUTES="/"
LIMIT=600
PARALLEL=8
ONLY=""

while [ $# -gt 0 ]; do
    case "$1" in
        --from) FROM=$2; shift 2 ;;
        --out) OUT=$2; shift 2 ;;
        --estate) ESTATE=$2; shift 2 ;;
        --routes) ROUTES=$2; shift 2 ;;
        --limit) LIMIT=$2; shift 2 ;;
        --parallel) PARALLEL=$2; shift 2 ;;
        --only) ONLY=$2; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

[ -f "$ESTATE/cms.db" ] || { echo "no CMS at $ESTATE/cms.db" >&2; exit 2; }
mkdir -p "$OUT/hosts"

# Every host with a published presentation, and then the territory apexes.
sqlite3 "$ESTATE/cms.db" "
  SELECT DISTINCT host FROM site_presentation
  WHERE status='published' AND host <> '' AND host LIKE '%.aurekai.%'
  ORDER BY host LIMIT $LIMIT;" > "$OUT/.subject-hosts.txt" 2>/dev/null || {
    echo "could not read published presentations" >&2; exit 2; }

# Each territory's own apex, which is a host like any other and was the one
# host nobody exported: a single-host walk wrote aurekai.life's front page and
# every other territory served those same bytes under its own name. A
# territory composes from what it actually holds, and .work holding little is
# a fact about this estate, not a reason to show it somebody else's evening.
for territory in ${TERRITORIES:-life live work shop}; do
    printf 'aurekai.%s\n' "$territory" >> "$OUT/.subject-hosts.txt"
done

# A pass for a few named hosts walks only those.
if [ -n "$ONLY" ]; then
    : > "$OUT/.subject-hosts.txt"
    for host in $ONLY; do printf '%s\n' "$host" >> "$OUT/.subject-hosts.txt"; done
fi

# A site standing at its own address has pages of its own: each standing page
# it published, what it offers, and the way to write to it.  The front page
# alone left every link on it pointing at an address the edge did not hold.
sqlite3 "$ESTATE/cms.db" "
  SELECT s.host || ' ' || p.route FROM site_presentation s
  JOIN site_page p ON p.namespace = s.site_id AND p.status = 'published'
  WHERE s.status='published' AND s.territory='independent' AND s.host LIKE '%.aurekai.%'
  UNION SELECT host || ' /offers' FROM site_presentation WHERE status='published' AND territory='independent' AND host LIKE '%.aurekai.%'
  UNION SELECT host || ' /contact' FROM site_presentation WHERE status='published' AND territory='independent' AND host LIKE '%.aurekai.%';" \
  > "$OUT/.site-routes.txt" 2>/dev/null || : > "$OUT/.site-routes.txt"

export OUT FROM ROUTES

one_host() {
    host=$1
    wrote=0
    for route in $ROUTES $(awk -v h="$host" '$1 == h && $2 ~ /^\/[a-z0-9\/_-]+$/ {print $2}' "$OUT/.site-routes.txt" 2>/dev/null) @own-pages @search; do
        # A member's front page lists the pages at their address.  Those are
        # this host's own pages too, and a list whose every line is a dead
        # link is worse than no list; follow what the front page just named.
        if [ "$route" = "@own-pages" ]; then
            [ -f "$OUT/hosts/$host/index.html" ] || continue
            for own in $(grep -o 'href="/w/[a-z0-9-]*/[a-z0-9-]*"' "$OUT/hosts/$host/index.html" 2>/dev/null | sed 's/^href="//;s/"$//' | sort -u | head -24); do
                mkdir -p "$OUT/hosts/$host$(dirname "$own")"
                file="$OUT/hosts/$host${own}.html"
                code=$(curl -s -m 45 -o "$file.tmp" -w '%{http_code}' \
                            -H "Host: $host" -H "Accept: text/html" "$FROM$own" 2>/dev/null || echo 000)
                if [ "$code" = "200" ] && [ -s "$file.tmp" ]; then mv "$file.tmp" "$file"; wrote=$((wrote + 1)); else rm -f "$file.tmp"; fi
            done
            continue
        fi
        # A site's own search travels with it: the index and passages its
        # sections were embedded into, the model a question is embedded with,
        # and the reader. A site without an index answers 404 and keeps the
        # page-name filter alone.
        if [ "$route" = "@search" ]; then
            mkdir -p "$OUT/hosts/$host/_search" "$OUT/assets"
            for part in passages.json index.bfsi model.bfse; do
                file="$OUT/hosts/$host/_search/$part"
                code=$(curl -s -m 60 -o "$file.tmp" -w '%{http_code}' -H "Host: $host" "$FROM/_search/$part" 2>/dev/null || echo 000)
                if [ "$code" = "200" ] && [ -s "$file.tmp" ]; then mv "$file.tmp" "$file"; else rm -f "$file.tmp"; fi
            done
            [ -s "$OUT/hosts/$host/_search/index.bfsi" ] || rm -rf "$OUT/hosts/$host/_search"
            if [ -d "$OUT/hosts/$host/_search" ] && [ ! -s "$OUT/assets/static-embed.mjs" ]; then
                curl -s -m 30 -o "$OUT/assets/static-embed.mjs.tmp" -H "Host: $host" "$FROM/assets/static-embed.mjs" && mv "$OUT/assets/static-embed.mjs.tmp" "$OUT/assets/static-embed.mjs"
            fi
            continue
        fi
        case "$route" in
            /) file="$OUT/hosts/$host/index.html" ;;
            *) mkdir -p "$OUT/hosts/$host$(dirname "$route")"
               file="$OUT/hosts/$host${route}.html" ;;
        esac
        mkdir -p "$(dirname "$file")"
        code=$(curl -s -m 45 -o "$file.tmp" -w '%{http_code}' \
                    -H "Host: $host" -H "Accept: text/html" "$FROM$route" 2>/dev/null || echo 000)
        if [ "$code" = "200" ] && [ -s "$file.tmp" ]; then
            mv "$file.tmp" "$file"
            wrote=$((wrote + 1))
        else
            rm -f "$file.tmp"
        fi
    done
    [ "$wrote" -gt 0 ] && echo "$host"
    return 0
}
export -f one_host 2>/dev/null || true

count=0
# A subshell per host, bounded: the composing estate is one machine and a
# hundred simultaneous walks would make it slower, not faster.
while IFS= read -r host; do
    [ -n "$host" ] || continue
    one_host "$host" >> "$OUT/.subject-hosts.done" &
    count=$((count + 1))
    [ $((count % PARALLEL)) -eq 0 ] && wait
done < "$OUT/.subject-hosts.txt"
wait
# One published, source-scoped public profile per admitted credited-maker
# coordinate. Reuse the native profile shell and original /o/ detail pages;
# do not manufacture accounts, posts or identities from matching bylines.
if [ -n "$ONLY" ]; then
    : # Named-host export stays bounded; it is not a full People backfill.
elif [ -f "$ESTATE/items.db" ]; then
    root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
    make -s -C "$root/cmd/BonfyreWebAddress" tools/people-static-manifest
    profile_manifest="$OUT/.credited-profile-hosts.nul"
    "$root/cmd/BonfyreWebAddress/tools/people-static-manifest" \
        --archive "$ESTATE/items.db" --cms "$ESTATE/cms.db" --profile-hosts --null > "$profile_manifest"
    fields=$(tr -cd '\000' < "$profile_manifest" | wc -c | tr -d ' ')
    [ $((fields % 2)) -eq 0 ] || { echo 'credited-profile manifest is incomplete' >&2; exit 2; }
    profiles=$((fields / 2))
    [ "$profiles" -gt 0 ] || { echo 'no source-backed credited profiles admitted' >&2; exit 2; }
    profile_worker="$OUT/.credit-profile-export.sh"
    cat > "$profile_worker" <<'CREDIT_WORKER'
#!/bin/sh
set -eu
route=$1
leaf=$2
case "$leaf" in hosts/credited-*.aurekai.work/index.html) ;; *) exit 2 ;; esac
dir=${leaf%/index.html}
for tab in index; do
    if [ "$tab" = index ]; then file="$OUT/$leaf"; query=overview
    else file="$OUT/$dir/$tab.html"; query=$tab; fi
    mkdir -p "$(dirname "$file")"
    part="$file.$$.part"
    code=$(curl -s -m 45 -o "$part" -w '%{http_code}' \
      -H 'Host: aurekai.work' -H 'Accept: text/html' "$FROM$route&tab=$query" 2>/dev/null || echo 000)
    if [ "$code" != 200 ] || [ ! -s "$part" ] ||
       ! grep -q 'data-credit-observation' "$part" ||
       ! grep -q 'data-profile-page' "$part" ||
       ! grep -q 'data-observed-publications' "$part" ||
       ! grep -q "id='organizations'" "$part"; then
        rm -f "$part"
        echo "  credited profile $dir /$tab did not verify ($code)" >&2
        exit 1
    fi
    mv "$part" "$file"
done
CREDIT_WORKER
    chmod +x "$profile_worker"
    export OUT FROM
    xargs -0 -P "$PARALLEL" -n 2 "$profile_worker" < "$profile_manifest" || {
        rm -f "$profile_worker" "$profile_manifest"
        echo 'credited profile export incomplete: refusing release' >&2
        exit 2
    }
    rm -f "$profile_worker" "$profile_manifest"
    actual=$(find "$OUT/hosts" -type f -name index.html | grep -E '/hosts/credited-[a-z0-9-]+-[0-9a-f]{20}\.aurekai\.work/index\.html$' | wc -l | tr -d ' ')
    [ "$actual" -eq "$profiles" ] || {
        echo "credited-profile coverage mismatch expected=$profiles actual=$actual" >&2
        exit 2
    }
    echo "  credited profiles: $actual exact source-hosts, one full public document with six sections each"
else
    echo 'retained archive unavailable: credited profile export refused' >&2
    exit 2
fi


# The shared assets an exported site page links to, so a static host serves
# them without anyone copying them by hand.
first=$(sort -u "$OUT/.subject-hosts.done" 2>/dev/null | head -1)
if [ -n "$first" ]; then
    mkdir -p "$OUT/assets"
    for asset in own-site.css own-site-agent.js own-site-reading.js static-embed.mjs; do
        [ -s "$OUT/assets/$asset" ] && continue
        code=$(curl -s -m 30 -o "$OUT/assets/$asset.tmp" -w '%{http_code}' -H "Host: $first" "$FROM/assets/$asset" 2>/dev/null || echo 000)
        if [ "$code" = "200" ] && [ -s "$OUT/assets/$asset.tmp" ]; then mv "$OUT/assets/$asset.tmp" "$OUT/assets/$asset"; else rm -f "$OUT/assets/$asset.tmp"; fi
    done
fi

hosts=$(find "$OUT/hosts" -name index.html -type f 2>/dev/null | wc -l | tr -d ' ')
pages=$(find "$OUT/hosts" -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
rm -f "$OUT/.subject-hosts.done" "$OUT/.site-routes.txt"
printf '{"hosts":%s,"pages":%s,"credited_profile_expected":%s,"credited_profile_written":%s,"prefix":"hosts/",' "$hosts" "$pages" "${profiles:-0}" "${actual:-0}"
printf '"cannot_infer":"a page under a subject host is what this estate composed for that subject when it was written; it is not that subject own site, not live, and not anything they agreed to"}\n'
