#!/bin/sh
# Everything the estate learned since last time, made public.
#
# The qualifier admits new sources every five minutes and the archive grows
# behind it, but none of that reaches anyone until the estate is read, composed
# and published. Doing that by hand means the public estate is only ever as
# fresh as the last time a person remembered, which is the failure this exists
# to remove.
#
# One pass, in the only order that works:
#   read what is due          the sources the scheduler says are cold
#   take what they published  into the item archive, at archive depth
#   tell the index            so search can answer for the new material
#   compose every route       including each length of the walk
#   publish                   to the object store and the edge
#
# It is safe to run while the qualifier runs: they touch different things, and
# a pass that finds nothing new simply publishes the same estate again.
set -eu

ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
REPO="${BONFYRE_REPO:-$HOME/Developer/Bonfyre}"
WORK="${BONFYRE_PUBLISH_WORK:-$ESTATE/publish}"
PORT="${BONFYRE_PUBLISH_PORT:-28899}"
ITEMS="${BONFYRE_PUBLISH_ITEMS:-24000}"
BUDGET="${BONFYRE_REHEAT_BUDGET:-400}"
REHEAT_MODE="${BONFYRE_PUBLISH_REHEAT:-inline}"

mkdir -p "$WORK"
LOG="$WORK/publish.log"
note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

# launchd does not inherit the login shell's Bun PATH. Resolve the installed
# runtime before mutation, rather than silently skipping publication stages.
if ! command -v bun >/dev/null 2>&1; then
    for candidate in "$HOME/.bun/bin/bun" /opt/homebrew/bin/bun /usr/local/bin/bun; do
        if [ -x "$candidate" ]; then
            PATH="$(dirname "$candidate"):$PATH"
            export PATH
            break
        fi
    done
fi
if ! command -v bun >/dev/null 2>&1; then
    note "publisher unavailable: Bun executable not found in launch environment"
    exit 2
fi

LOCK="$WORK/running.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
    if [ -n "$(find "$LOCK" -maxdepth 0 -mmin +180 2>/dev/null)" ]; then
        note "clearing a stale lock"; rmdir "$LOCK" 2>/dev/null || true; mkdir "$LOCK" 2>/dev/null || exit 0
    else
        exit 0
    fi
fi
server=""
cleanup() { [ -n "$server" ] && kill "$server" 2>/dev/null || true; rmdir "$LOCK" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# publish.log is this pass, not every pass since September. It had reached
# 47 MB, most of it the same 404 list repeated hourly. The previous seven
# passes are kept beside it, compressed, for anyone tracing a regression.
if [ -s "$LOG" ]; then
    mkdir -p "$WORK/logs"
    kept="$WORK/logs/publish-$(date -u +%Y%m%dT%H%M%SZ).log"
    if command -v zstd >/dev/null 2>&1 && zstd -q -19 "$LOG" -o "$kept.zst"; then : > "$LOG"
    else mv "$LOG" "$kept"; fi
    ls -1t "$WORK/logs"/publish-*.log* 2>/dev/null | tail -n +8 | while IFS= read -r old; do rm -f "$old"; done
fi

# Composing a page holds the material it draws on in memory, and this machine
# is somebody's laptop: it runs a model server, an editor and whatever else.
# A pass that starts while memory is already gone does not publish faster, it
# just takes the machine down with it, so it waits for the next hour instead.
# The pass asks the same memory ledger every local model asks before it loads
# (ResourceAdmission, volume "memory"), so a publish and a model load cannot
# both be admitted against the same gigabytes. Free plus inactive plus
# purgeable is what the ledger counts; "pages free" sits near zero whenever
# the machine is healthy, and a guard on it would never let this pass run.
ADMISSION="$REPO/cmd/BonfyreAdmission/bonfyre-admission"
ADMISSION_DB="${BONFYRE_ADMISSION_DB:-$HOME/.bonfyre/admission.db}"
memory_grant=""
GRANT_FILE="$WORK/memory.grant"
if [ -x "$ADMISSION" ]; then
    # A pass killed outright never reaches its cleanup, and its grant stayed
    # committed: three such grants held 2.1 GB from 17-22 October and refused
    # every pass after them. Only the lock holder gets here, so a grant left
    # in the file belongs to a pass that is gone.
    if [ -s "$GRANT_FILE" ]; then
        "$ADMISSION" release --db "$ADMISSION_DB" --grant "$(cat "$GRANT_FILE")" >/dev/null 2>&1 || true
        note "released the memory grant of a pass that did not finish"
    fi
    rm -f "$GRANT_FILE"
    answer=$("$ADMISSION" request --db "$ADMISSION_DB" --plane publish-estate --kind publish-pass \
        --bytes $(( ${BONFYRE_PUBLISH_MIN_FREE_MB:-700} * 1048576 )) --volume memory 2>/dev/null || true)
    case "$answer" in
        *'"verdict":"admit"'*)
            memory_grant=$(printf '%s' "$answer" | sed -n 's/.*"grant_id":\([0-9]*\).*/\1/p')
            printf '%s\n' "$memory_grant" > "$GRANT_FILE" ;;
        *)
            note "memory not admitted this pass ($(printf '%s' "$answer" | sed -n 's/.*"reason":"\([^"]*\)".*/\1/p')); leaving the machine alone"
            exit 0 ;;
    esac
    cleanup() {
        [ -n "$server" ] && kill "$server" 2>/dev/null || true
        [ -n "$memory_grant" ] && "$ADMISSION" release --db "$ADMISSION_DB" --grant "$memory_grant" >/dev/null 2>&1 || true
        rm -f "$GRANT_FILE"
        rmdir "$LOCK" 2>/dev/null || true
    }
else
    free_mb=$(vm_stat 2>/dev/null | awk '
        /Pages free/        {gsub(/\./,"",$3); free=$3}
        /Pages inactive/    {gsub(/\./,"",$3); inactive=$3}
        /Pages purgeable/   {gsub(/\./,"",$3); purgeable=$3}
        END {printf "%d", (free+inactive+purgeable)*16384/1048576}')
    if [ -n "$free_mb" ] && [ "$free_mb" -gt 0 ] \
       && [ "$free_mb" -lt "${BONFYRE_PUBLISH_MIN_FREE_MB:-700}" ]; then
        note "only ${free_mb}MB available; leaving the machine alone this pass"
        exit 0
    fi
fi

WEB="$REPO/cmd/BonfyreWebAddress/bonfyre-web-address"
PROJECT="$REPO/components/BonfyreProject/bonfyre-project"
for needed in "$WEB" "$ESTATE/cms.db"; do
    [ -e "$needed" ] || { note "missing: $needed"; exit 2; }
done

export BONFYRE_CMS_DB="$ESTATE/cms.db"
export BONFYRE_FOREIGN_ROOT="$ESTATE/foreign"
export BONFYRE_SOURCE_FRONTIER_DB="$ESTATE/frontier.db"
export BONFYRE_ITEM_ARCHIVE_DB="$ESTATE/items.db"
export BONFYRE_PRICES_DB="$ESTATE/prices.db"
export BONFYRE_PUBLIC_PROJECTION_DB="$ESTATE/projection.db"
# Whose material a subject's address carries. Without this the composing
# server has no fabric to read, so every subject host published as the
# territory's own front page under someone else's name -- and once that
# fallback was removed, as an empty shell. The estate served these correctly
# all along; only the publication did not.
export BONFYRE_SUBJECT_FABRIC_DB="$ESTATE/subject_fabric.db"
export BONFYRE_LOCAL_ZONE="${BONFYRE_LOCAL_ZONE:-America/Chicago}"

# 1. Read what the scheduler says is due. This is deliberately independent of
# publishing: a bounded external refresh can take sixty seconds per source,
# while a fully retained estate must still be able to compose and deploy. The
# normal scheduled pass remains inline; a release may explicitly publish the
# current retained generation without pretending skipped reheat is fresh data.
case "$REHEAT_MODE" in
    inline)
        if [ -x "$PROJECT" ]; then
            "$PROJECT" foreign reheat --budget "$BUDGET" --per-host 4 > "$WORK/reheat.json" 2>&1 || true
            note "read $(grep -o '"decision":"read[^"]*"' "$WORK/reheat.json" 2>/dev/null | wc -l | tr -d ' ') sources"
        fi
        ;;
    skip)
        note "reheat deferred; publishing the retained generation already held"
        ;;
    *)
        note "unknown BONFYRE_PUBLISH_REHEAT=$REHEAT_MODE; expected inline or skip"
        exit 2
        ;;
esac

# 2. Keep what they published, at archive depth rather than page depth.
if [ -f "$REPO/scripts/project_event_sources.mjs" ]; then
    bun "$REPO/scripts/project_event_sources.mjs" --estate "$ESTATE" --repo "$REPO" --apply \
        >> "$LOG" 2>&1 || note "event source filing refused"
fi
"$WEB" census-items > "$WORK/items.jsonl" 2>/dev/null || true
kept=$("$WEB" archive-items --items "$WORK/items.jsonl" 2>/dev/null | tail -1)
note "archive: $kept"

# 2a. A channel's filing is not a ceiling on what its retained body actually
# carries. Recompute that bounded evidence before selecting public programs so
# a feed filed as news but demonstrably publishing a regular audio, video or
# image stream can reach the corresponding route. This only projects measured
# carriage beside the canonical family; it never changes admission or family.
if [ -f "$REPO/cmd/BonfyreSourceFrontier/tools/measure_carried_material.mjs" ]; then
    bun "$REPO/cmd/BonfyreSourceFrontier/tools/measure_carried_material.mjs" \
        --cms "$ESTATE/cms.db" --foreign "$ESTATE/foreign" --frontier "$ESTATE/frontier.db" \
        --apply --evidence "$WORK/carriage.jsonl" >> "$LOG" 2>&1 \
        && note "measured material carriage" || note "material carriage refused"
fi

# 3. Tell the index, so a question nobody asked in advance can still be answered.
if [ -n "${SUPABASE_PROJECT_REF:-}" ] && [ -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ] \
   && [ -f "$REPO/scripts/load_search_index.mjs" ]; then
    # The index lives on another host. When that host cannot be reached every
    # item is retried and refused in turn, and one pass sat here for six and a
    # half hours with nothing published behind it. The step gets a budget; a
    # pass that runs out of it publishes without the index and says so.
    perl -e 'alarm shift; exec @ARGV' "${BONFYRE_PUBLISH_INDEX_SECONDS:-900}" \
        bun "$REPO/scripts/load_search_index.mjs" --archive "$ESTATE/items.db" >> "$LOG" 2>&1 || note "index refused or out of time"
fi

# 3a. Give an address to every publisher whose material this estate now holds.
#     The archive grows every pass, so the addressed estate has to grow with it
#     or the newest publishers stay invisible behind the material they wrote.
METER="$REPO/cmd/BonfyreMeter/bonfyre-meter"
export BONFYRE_METER_DB="$ESTATE/meter.db"
PUBLISH_STARTED_AT="$(date -u +%FT%TZ)"
PUBLISH_ATTEMPT="attempt:publish:${PUBLISH_STARTED_AT}"
PUBLISH_RECEIPT="receipt:publish:${PUBLISH_STARTED_AT}"
meter() { [ -x "$METER" ] && "$METER" record --key "$1" --op "$2" --provider "$3" \
    --bytes "${4:-0}" --receipt "$PUBLISH_RECEIPT" --attempt-ref "$PUBLISH_ATTEMPT:$2" \
    --realization-ref "r2:${BONFYRE_R2_BUCKET:-aurekai-estate}" --at "$PUBLISH_STARTED_AT" >/dev/null 2>&1 || true; }

if [ -f "$REPO/scripts/address_every_publisher.mjs" ]; then
    bun "$REPO/scripts/address_every_publisher.mjs" --estate "$ESTATE" --repo "$REPO" \
        --min-items 3 --apply >> "$LOG" 2>&1 || note "publisher addressing refused"
fi

# 3b. Give every member and organisation the surfaces bound to their address.
#     A person's page is not a per-user CMS: it is the estate's own surfaces,
#     bound to that person, plus whatever they have put in them.
if [ -f "$REPO/scripts/compose_member_worlds.mjs" ]; then
    bun "$REPO/scripts/compose_member_worlds.mjs" --estate "$ESTATE" --repo "$REPO" --apply \
        >> "$LOG" 2>&1 || note "member worlds refused"
fi

# 4. Compose every route, every length of every walk, and everything kept.
rm -f "$ESTATE/projection.db"
"$WEB" serve --db "$ESTATE/matrix.db" --port "$PORT" --workers "${BONFYRE_PUBLISH_WORKERS:-2}" --site-root "$REPO/site/matrix" \
    > "$WORK/server.log" 2>&1 &
server=$!
ready=0
for _ in $(seq 1 60); do
    curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && { ready=1; break; }
    sleep 1
done
[ "$ready" = "1" ] || { note "the estate did not come up; nothing published"; exit 3; }

rm -rf "$WORK/dist"
sh "$REPO/scripts/export_static_estate.sh" --from "http://127.0.0.1:$PORT" --out "$WORK/dist" \
    --archive "$ESTATE/items.db" --frontier "$ESTATE/frontier.db" --items "$ITEMS" --parallel "${BONFYRE_PUBLISH_PARALLEL:-6}" \
    --site-root "$REPO/site/matrix" >> "$LOG" 2>&1

# 4b. And every address this estate gives a subject, under its own prefix, so
#     the edge can answer on the host rather than on the path alone.
if [ -f "$REPO/scripts/export_subject_hosts.sh" ]; then
    sh "$REPO/scripts/export_subject_hosts.sh" --from "http://127.0.0.1:$PORT" \
        --out "$WORK/dist" --estate "$ESTATE" --routes "/" --limit 4000 >> "$LOG" 2>&1 \
        || { note "subject hosts refused; no partial cohort release"; exit 3; }
fi
# 4b2. Lots this estate has observed, as a chain rather than a card. The
#      projector types marketplace feeds as commerce and establishes only the
#      two stages an observation entitles it to.
if [ -f "$REPO/scripts/project_listings.mjs" ]; then
    bun "$REPO/scripts/project_listings.mjs" --estate "$ESTATE" --repo "$REPO" --apply \
        >> "$LOG" 2>&1 || note "listings refused"
fi
if [ -f "$REPO/scripts/publish_market.mjs" ]; then
    bun "$REPO/scripts/publish_market.mjs" --estate "$ESTATE" --repo "$REPO" \
        --out "$WORK/dist" >> "$LOG" 2>&1 || note "market refused"
    for territory in life live work shop; do
        [ -f "$WORK/dist/market.html" ] && mkdir -p "$WORK/dist/hosts/aurekai.$territory" \
            && cp "$WORK/dist/market.html" "$WORK/dist/hosts/aurekai.$territory/market.html"
    done
fi

# 4b3. One retained-material read compiles the dated edition for every public
# territory, wearing an actually published magazine profile and preserving
# each piece's publisher and observation boundary.
if [ -f "$REPO/scripts/publish-edition.mjs" ]; then
    bun "$REPO/scripts/publish-edition.mjs" --estate "$ESTATE" --out "$WORK/dist" \
        --all-territories >> "$LOG" 2>&1 && note "edition: compiled" || note "edition refused"
fi

# 4c. What the estate's reading costs, at its own address and under each
#     subject running programs. Accrued at admitted rates, never billed.
if [ -f "$REPO/scripts/publish_economics.mjs" ]; then
    bun "$REPO/scripts/publish_economics.mjs" --estate "$ESTATE" --out "$WORK/dist" \
        >> "$LOG" 2>&1 || note "economics refused"
    for territory in life live work shop; do
        if [ -f "$WORK/dist/economics.html" ]; then
            mkdir -p "$WORK/dist/hosts/aurekai.$territory"
            cp "$WORK/dist/economics.html" "$WORK/dist/hosts/aurekai.$territory/economics.html"
        fi
    done
fi

# 4d. A funding route starts with a product a real institution can recognise.
# The ProgrammeFabric compiler turns each published programme into a bounded
# public/member/operator pilot proposal, retains its distinct work packages,
# and then appends public Grants.gov observations.  It opens no issue,
# application, account, payment or spend authority.
if [ -f "$REPO/scripts/bootstrap_funding_frontier.mjs" ]; then
    bun "$REPO/scripts/bootstrap_funding_frontier.mjs" --cms "$ESTATE/cms.db" --out "$WORK/dist" \
        >> "$LOG" 2>&1 && note "funding frontier: compiled" || note "funding frontier: observation refused"
    for territory in life live work shop; do
        if [ -f "$WORK/dist/funding.html" ]; then
            mkdir -p "$WORK/dist/hosts/aurekai.$territory"
            cp "$WORK/dist/funding.html" "$WORK/dist/hosts/aurekai.$territory/funding.html"
            cp "$WORK/dist/funding-frontier.json" "$WORK/dist/hosts/aurekai.$territory/funding-frontier.json"
        fi
    done
fi

pages=$(find "$WORK/dist" -name '*.html' | wc -l | tr -d ' ')
note "composed $pages pages"
kill "$server" 2>/dev/null || true; server=""

# 5. Publish. The object store takes any number of pages; the edge takes what
#    a static host will hold, so the two are not the same publication.
if [ -n "${CLOUDFLARE_ACCESS_KEY_ID:-}" ] && [ -n "${CLOUDFLARE_R2_ENDPOINT:-}" ]; then
    AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION=auto \
    aws s3 sync "$WORK/dist/" "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/" \
        --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" --only-show-errors --no-progress >> "$LOG" 2>&1 \
        && { note "object store: synced"; meter "estate:publish" "r2_sync" "cloudflare" \
                "$(du -sk "$WORK/dist" 2>/dev/null | awk '{print $1*1024}')"; } \
        || note "object store: refused"
    # Direct R2 custom domains do not infer the Worker extension rewrite.
    # Mirror every emitted HTML route to its extensionless material key so the
    # quota-independent material lane can answer the same public route shape.
    if [ -x "$REPO/scripts/publish_r2_route_aliases.sh" ]; then
        "$REPO/scripts/publish_r2_route_aliases.sh" --source "$WORK/dist" \
            >> "$LOG" 2>&1 && note "object store route aliases: synced" \
            || note "object store route aliases: refused"
    fi
fi

# 5b. Every subdomain is served from the edge as static files, not from the
#     object store above. Nothing refreshed those files after they were first
#     laid out on 18 October, so 1,776 of 1,780 pages and the site sheets went
#     stale while this pass kept syncing a bucket no visitor reads. They are
#     composed in-process by the same owners a request reaches, and a short
#     composition never replaces a full one.
EDGE="$REPO/deploy/worker"
if [ -n "${CLOUDFLARE_API_TOKEN:-}" ] && [ -f "$EDGE/wrangler.toml" ]; then
    stage="$WORK/edge"
    rm -rf "$stage" "$WORK/edge-previous"
    mkdir -p "$stage"
    shards="${BONFYRE_EDGE_SHARDS:-4}"; shard=0; pids=""
    while [ "$shard" -lt "$shards" ]; do
        "$WEB" publish-static --db "$ESTATE/matrix.db" --site-root "$REPO/site/matrix" \
            --out "$stage" --shard "$shard/$shards" >> "$LOG" 2>&1 &
        pids="$pids $!"; shard=$((shard + 1))
    done
    composed=1
    for pid in $pids; do wait "$pid" || composed=0; done
    # Machine discovery is part of the release, not an optional side export.
    for discovery_host in aurekai.life aurekai.live aurekai.shop aurekai.work; do
        for discovery_file in robots.txt sitemap.xml sitemap-surfaces.xml; do
            if [ ! -s "$stage/h/$discovery_host/$discovery_file" ]; then
                note "edge discovery missing: $discovery_host/$discovery_file"; composed=0
            fi
        done
        if [ -s "$stage/h/$discovery_host/robots.txt" ] &&
           ! grep -Fq "Sitemap: https://$discovery_host/sitemap.xml" "$stage/h/$discovery_host/robots.txt"; then
            note "edge discovery foreign robots origin: $discovery_host"; composed=0
        fi
        if [ -s "$stage/h/$discovery_host/sitemap.xml" ] &&
           ! grep -Fq "https://$discovery_host/sitemap-surfaces.xml" "$stage/h/$discovery_host/sitemap.xml"; then
            note "edge discovery foreign sitemap origin: $discovery_host"; composed=0
        fi
    done
    # A dynamic discussion URL is not indexed unless its own published file
    # exists in this exact candidate. Partial sitemap cohorts block promotion.
    community_index="$stage/h/aurekai.life/sitemap-community.xml"
    if [ ! -s "$community_index" ]; then
        note "edge dynamic index missing: aurekai.life/sitemap-community.xml"
        composed=0
    else
        for shard_file in $(grep -oE 'sitemap-community-[0-9]+\.xml' "$community_index" || true); do
            if [ ! -s "$stage/h/aurekai.life/$shard_file" ]; then
                note "edge dynamic index shard missing: $shard_file"
                composed=0
            fi
        done
        sitemap_threads=$(grep -h '<url><loc>https://aurekai.life/community/' "$stage/h/aurekai.life"/sitemap-community-[0-9]*.xml 2>/dev/null | wc -l | tr -d ' ' || true)
        published_threads=$(find "$stage/h/aurekai.life/community" -type f -path '*/thread/*/index.html' 2>/dev/null | wc -l | tr -d ' ')
        if [ "$sitemap_threads" -ne "$published_threads" ]; then
            note "edge dynamic discussion mismatch: sitemap $sitemap_threads, published $published_threads"
            composed=0
        fi
    fi
    written=$(find "$stage/h" -name index.html 2>/dev/null | wc -l | tr -d ' ')
    # Stage host-specific discovery and public HTML aliases for the existing
    # Worker R2 fallback. No external writes before edge activation.
    r2_discovery=''
    if [ "$composed" = 1 ]; then
        if [ -n "${CLOUDFLARE_ACCESS_KEY_ID:-}" ] &&
           [ -n "${CLOUDFLARE_SECRET_ACCESS_KEY:-}" ] &&
           [ -n "${CLOUDFLARE_R2_ENDPOINT:-}" ]; then
            r2_discovery="$WORK/r2-host-discovery"
            rm -rf "$r2_discovery"
            mkdir -p "$r2_discovery/hosts"
            for hostdir in "$stage"/h/*; do
                [ -d "$hostdir" ] || continue
                host=${hostdir##*/}
                target="$r2_discovery/hosts/$host"
                mkdir -p "$target"
                for machine in "$hostdir"/robots.txt "$hostdir"/sitemap*.xml; do
                    [ -s "$machine" ] || continue
                    cp "$machine" "$target/${machine##*/}"
                done
                # Native export uses route/index.html; the Worker R2 fallback
                # resolves that exact semantic address to route.html.
                for family in blog community; do
                    [ -d "$hostdir/$family" ] || continue
                    find "$hostdir/$family" -type f -name index.html -exec sh -ec '
                        root=$1; dest=$2; shift 2
                        for file do
                            rel=${file#"$root"/}
                            key=${rel%/index.html}.html
                            mkdir -p "$(dirname "$dest/$key")"
                            cp "$file" "$dest/$key"
                        done
                    ' sh "$hostdir" "$target" {} +
                done
            done
            note "edge host discovery: R2 fallback candidate prepared"
        else
            note "edge discovery refused: R2 fallback credentials missing"
            composed=0
        fi
    fi
    # Refuse BEFORE Worker activation if any required host would still
    # expose a foreign sitemap, missing robots discovery, or no community index.
    if [ "$composed" = 1 ]; then
        for discovery_host in aurekai.life aurekai.live aurekai.shop aurekai.work nick.aurekai.work; do
            candidate="$r2_discovery/hosts/$discovery_host"
            if [ ! -s "$candidate/sitemap.xml" ] ||
               [ ! -s "$candidate/robots.txt" ] ||
               ! grep -Fq "https://$discovery_host/sitemap-surfaces.xml" "$candidate/sitemap.xml" ||
               ! grep -Fq "Sitemap: https://$discovery_host/sitemap.xml" "$candidate/robots.txt"; then
                note "edge discovery candidate incomplete: $discovery_host"
                composed=0
            fi
        done
        if [ ! -s "$r2_discovery/hosts/aurekai.life/sitemap-community.xml" ]; then
            note "edge dynamic community index not staged"
            composed=0
        fi
        nick_journals=$(find "$stage/h/nick.aurekai.work/blog" -type f -name index.html 2>/dev/null |
            grep -E '/blog/[^/]+/index.html$' | wc -l | tr -d ' ')
        note "edge nick released journals: $nick_journals (review-held excluded)"
    fi
    # The R2 snapshot contains every admitted source-qualified credit.
    # The separately published Worker asset tree must carry that same cohort.
    # Hardlink read-only export bytes inside this release staging volume so
    # thousands of profile pages do not consume duplicate disk allocations.
    credit_expected=$(find "$WORK/dist/hosts" -type f -path "*/credited-*.aurekai.work/index.html" 2>/dev/null | wc -l | tr -d " ")
    for credit in "$WORK"/dist/hosts/credited-*.aurekai.work; do
        [ -d "$credit" ] || continue
        name=${credit##*/}
        [ -s "$credit/index.html" ] || { note "credit profile missing index: $name"; composed=0; continue; }
        if [ -e "$stage/h/$name/index.html" ]; then
            cmp -s "$credit/index.html" "$stage/h/$name/index.html" || composed=0
        else
            mkdir -p "$stage/h/$name" && ln "$credit/index.html" "$stage/h/$name/index.html" || composed=0
        fi
    done
    credit_written=$(find "$stage/h" -type f -path "*/credited-*.aurekai.work/index.html" 2>/dev/null | wc -l | tr -d " ")
    if [ "$credit_expected" -ne "$credit_written" ]; then
        note "edge credit cohort incomplete: expected $credit_expected, staged $credit_written"
        composed=0
    fi
    note "edge credited profiles: $credit_written of $credit_expected source-qualified hosts"
    if [ "$credit_expected" -lt "${BONFYRE_EDGE_MIN_CREDIT_PROFILES:-2000}" ]; then
        note "edge credit cohort below minimum: $credit_expected (required ${BONFYRE_EDGE_MIN_CREDIT_PROFILES:-2000})"
        composed=0
    fi
    written=$(find "$stage/h" -name index.html 2>/dev/null | wc -l | tr -d " ")
    if [ "$composed" = 1 ] && [ "$written" -ge "${BONFYRE_EDGE_MIN_PAGES:-1500}" ] && \
       BONFYRE_WORKSPACE_CLIENT="${BONFYRE_WORKSPACE_CLIENT:-$REPO/integrations/browser}" \
       "$WEB" publish-assets --site-root "$REPO/site/matrix" --out "$stage" >> "$LOG" 2>&1; then
        for shared in assets apps; do
            [ ! -d "$stage/$shared" ] || { mkdir -p "$EDGE/edge-assets/$shared"; cp -R "$stage/$shared/." "$EDGE/edge-assets/$shared/"; }
        done
        cp "$REPO/site/matrix/own-site-reading.js" "$REPO/site/matrix/static-embed.mjs" "$EDGE/edge-assets/assets/"
        if [ -d "$EDGE/edge-assets/h" ]; then
            mv "$EDGE/edge-assets/h" "$WORK/edge-previous" || {
                note "edge: could not preserve existing host tree"; exit 3;
            }
        fi
        if ! mv "$stage/h" "$EDGE/edge-assets/h"; then
            note "edge: candidate swap failed, restoring previous host tree"
            [ ! -d "$WORK/edge-previous" ] || mv "$WORK/edge-previous" "$EDGE/edge-assets/h"
            exit 3
        fi
        if (cd "$EDGE" && bunx wrangler@4 deploy) >> "$LOG" 2>&1; then
            if [ -n "$r2_discovery" ]; then
                # The active Worker now has the same validated static snapshot.
                # Only then refresh its host-keyed object-store fallback.
                AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
                AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
                AWS_DEFAULT_REGION=auto \
                aws s3 sync "$r2_discovery/" \
                    "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/" \
                    --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" \
                    --only-show-errors --no-progress >> "$LOG" 2>&1 || {
                    note "edge discovery fallback sync failed; public readback is unverified"
                    exit 4
                }
                note "edge host discovery: current origin-specific fallback synced"
                # A removed or moderated thread must not survive as a stale
                # fallback object after its new static tree stops publishing it.
                # Delete only the community subtree owned by this publisher;
                # never --delete against the shared estate bucket.
                mkdir -p "$r2_discovery/hosts/aurekai.life/community"
                AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
                AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
                AWS_DEFAULT_REGION=auto \
                aws s3 sync "$r2_discovery/hosts/aurekai.life/community/" \
                    "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/hosts/aurekai.life/community/" \
                    --delete --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" \
                    --only-show-errors --no-progress >> "$LOG" 2>&1 || {
                    note "edge discussion withdrawal reconciliation refused"
                    exit 4
                }
                note "edge community fallback: withdrawn thread aliases reconciled"
            fi
            # A Cloudflare deploy receipt is not public acceptance. Query-unique
            # readbacks bypass stale URL cache entries without purging a zone.
            discovery_readback=1
            cache_key=$(date +%s)
            for discovery_host in aurekai.life aurekai.live aurekai.shop aurekai.work nick.aurekai.work; do
                if [ ! -s "$r2_discovery/hosts/$discovery_host/sitemap.xml" ]; then
                    note "edge discovery host absent from candidate: $discovery_host"
                    discovery_readback=0
                    continue
                fi
                attempt=0; visible=0
                while [ "$attempt" -lt 3 ]; do
                    attempt=$((attempt + 1))
                    if curl -fsS --max-time 12 \
                        "https://$discovery_host/sitemap.xml?bonfyre_readback=$cache_key-$attempt" \
                        2>> "$LOG" | grep -Fq "https://$discovery_host/sitemap-surfaces.xml"; then
                        visible=1
                        break
                    fi
                    [ "$attempt" -ge 3 ] || sleep 2
                done
                if [ "$visible" -ne 1 ]; then
                    note "edge discovery live origin mismatch: $discovery_host"
                    discovery_readback=0
                fi
                if ! curl -fsS --max-time 12 \
                    "https://$discovery_host/robots.txt?bonfyre_readback=$cache_key" \
                    2>> "$LOG" | grep -Fq "Sitemap: https://$discovery_host/sitemap.xml"; then
                    note "edge discovery robots missing host sitemap: $discovery_host"
                    discovery_readback=0
                fi
            done
            if ! curl -fsS --max-time 15 \
                "https://aurekai.life/sitemap-community.xml?bonfyre_readback=$cache_key" \
                2>> "$LOG" | grep -Eq '<(sitemapindex|urlset)([[:space:]]|>)'; then
                note "edge discovery live community index missing"
                discovery_readback=0
            fi
            if [ "$discovery_readback" -ne 1 ]; then
                note "edge public discovery readback refused; deployment not accepted"
                exit 4
            fi
            note "edge public discovery: 5 host-specific sitemaps + community index verified"
            note "edge: deployed and public discovery verified for $written pages"; rm -rf "$WORK/edge-previous"
        else
            note "edge: deploy refused; the edge keeps its last pages"
            # Restore the previous public host tree after a rejected upload.
            # The refused candidate is derived, not an active release.
            rm -rf "$EDGE/edge-assets/h"
            if [ -d "$WORK/edge-previous" ]; then
                mv "$WORK/edge-previous" "$EDGE/edge-assets/h" || {
                    note "edge: rollback failed, manual recovery required"
                    exit 3
                }
            fi
        fi
    else
        note "edge: only $written pages composed; the edge keeps its last pages"
    fi
    rm -rf "$stage"
fi

# 6. The same material as tables, so a reader can ask a question instead of
#    guessing at addresses. Small: the whole source topology fits in megabytes.
if [ -f "$REPO/scripts/publish_catalog.sh" ]; then
    sh "$REPO/scripts/publish_catalog.sh" --upload >> "$LOG" 2>&1 \
        && note "catalogue published" || note "catalogue refused"
fi

note "pass complete"
