#!/bin/sh
# Every human grammar must be server-lowered from the CUE program table.  This
# is intentionally a route test, rather than a generated-header test: it
# catches a route that compiles but falls through to the legacy browser page.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'native surface routes: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-native-surface.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
schemas="$tmp/schemas"
mkdir -p "$schemas"
for type in world world_fragment community_body community_post pack_demand capability_program; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db" matrix="$tmp/matrix.db" authdb="$tmp/auth.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world '{"world":"field-season","title":"Field season","character":"field-notes","lede":"A real composed World.","sources":"","grammars":"stage,channels,community,board,spatial,listings,inbox,observations,packs","cannot_infer":"a World is not an effect","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world_fragment '{"fragment_id":"field-season:community","world":"field-season","ordinal":1,"subject_address":"community:field-notes","surface_grammar":"community","renderer_family":"community","extent":"page","cannot_infer":"fragment is not authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_body '{"community":"field-notes","subject_address":"topic:field","title":"Field Notes","description":"Human context.","visibility":"public","cannot_infer":"community is not federation","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_post '{"post":"field-notes:one","community":"field-notes","author_subject":"person:ada","title":"A field note","body":"Rain changed the work window.","published_at":"2026-09-10T12:00:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"post is not authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create pack_demand '{"pack":"field-response","demand_kind":"observation","needs":"weather observation","preferred_sources":"weather","body_regime":"retained-public","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create capability_program '{"program_id":"field-service","application_kind":"ServiceRequest","action":"Request service","consumes":[],"produces":[],"carrier_candidates":[],"capability_requirements":[],"authority_requirement":"service.request","transition_conditions":[],"failure_projection":"show refusal","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" "$web" serve --db "$matrix" --auth-db "$authdb" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"; attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "$base/healthz"; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 80 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; exit 1; }; sleep 0.05
done

# The exact fifteen presentation grammars are server-lowered from their CUE
# declarations. Routes with retained public material may take the
# PageComposition compatibility adapter before the sparse native donor, but
# neither route class may fall through to a browser-owned document.
for route in /discover /stage /channels /community /board /parcels /listings /inbox /territory /sources /w /packs /link /studio /console; do
  printf 'native surface routes: checking %s\n' "$route" >&2
  page=$(curl -sSi -H 'Host: aurekai.live' -H 'Accept: text/html' "$base$route")
  if [ "$route" = /studio ] || [ "$route" = /territory ]; then
    printf '%s' "$page" | grep -q 'HTTP/1.1 200 OK'
  else
    printf '%s' "$page" | grep -Eqi 'X-Bonfyre-(Surface-Program: (CUE-native|native-source-workspace)|World-Projection: native)'
  fi
  case "$route" in
    /link|/sources) printf '%s' "$page" | grep -q 'data-source-desk'; printf '%s' "$page" | grep -q '/assets/composition.css'; printf '%s' "$page" | grep -q 'id="sx-add"'; ! printf '%s' "$page" | grep -q 'localStorage\|bonfyre-surface-bridge';;
    /studio|/territory) printf '%s' "$page" | grep -q 'data-ak-player'; ! printf '%s' "$page" | grep -q 'localStorage\|bonfyre-surface-bridge';;
    *) ! printf '%s' "$page" | grep -q '<script\|localStorage\|bonfyre-surface-bridge';;
  esac
printf 'native surface routes: cleared core-route-sweep\n' >&2
done
# Shared page chrome is icon-bearing, not text-only. The native owner must emit
# a local semantic SVG vocabulary on ordinary public routes.
icon_page=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/discover")
printf '%s' "$icon_page" | grep -q 'class="glyph ak-icon"' || { echo "native surface routes: FAIL (missing native icon class)" >&2; exit 1; }
printf '%s' "$icon_page" | grep -q 'href="/whats-on"' || { echo "native surface routes: FAIL (missing dated-events navigation)" >&2; exit 1; }
printf '%s' "$icon_page" | grep -q 'href="/watch"' || { echo "native surface routes: FAIL (missing watch navigation)" >&2; exit 1; }
printf '%s' "$icon_page" | grep -q 'href="/s/work/all"' || { echo "native surface routes: FAIL (missing Work board continuation)" >&2; exit 1; }
icon_count=$(printf '%s' "$icon_page" | grep -o 'class="glyph ak-icon"' | wc -l | tr -d ' ')
printf 'native surface routes: cleared shared-icon-contract\n' >&2
[ "$icon_count" -ge 12 ] || { echo "native surface routes: FAIL (only $icon_count shared icons)" >&2; exit 1; }

# Native /link exposes the real ForeignTwin working desk rather than leaving
# a user inside the generic Accounts section.
native_link=$(curl -sSi -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/link")
printf '%s' "$native_link" | grep -q 'data-source-desk'
printf '%s' "$native_link" | grep -q 'href="/link?sort=most"'
filtered=$(curl -sS -G -H 'Host: aurekai.live' -H 'Accept: text/html' --data-urlencode 'q=field note' --data-urlencode 'family=research' "$base/sources")
printf '%s' "$filtered" | grep -q 'sort=most&amp;q=field%20note&amp;family=research'
style=$(curl -sS -H 'Host: aurekai.live' "$base/assets/composition.css")
printf '%s' "$style" | grep -q '.sx-finder'
printf 'native surface routes: cleared link-source-desk\n' >&2
printf '%s' "$native_link" | grep -q 'href="/s/sets/all"'
for route in /foreign/workbench /s/work/foreign-twins; do
  printf 'native surface routes: checking %s\n' "$route" >&2
  foreign=$(curl -sSi -H 'Host: aurekai.live' -H 'Accept: text/html' "$base$route")
  printf '%s' "$foreign" | grep -q 'data-source-desk'
  printf '%s' "$foreign" | grep -q '/assets/composition.css'
  printf '%s' "$foreign" | grep -q 'Bring a web address'
  printf '%s' "$foreign" | grep -q 'Browse known sources'
printf 'native surface routes: cleared foreign-workbench-routes\n' >&2
  ! printf '%s' "$foreign" | grep -q 'async function cmsDraft('
done
# Machine clients retain their distinct CUE and site-level contracts.
printf 'native surface routes: cleared machine-link-contract\n' >&2
machine=$(curl -sSi -H 'Host: aurekai.live' -H 'Accept: application/json' "$base/link")
! printf '%s' "$machine" | grep -q '<script'
packs=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/packs")
printf '%s' "$packs" | grep -q 'field-response\|weather observation'
printf 'native surface routes: cleared account-programs\n' >&2
inbox=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/inbox")
printf '%s' "$inbox" | grep -q 'Request service\|service.request'
printf 'native surface routes: cleared worlds-index\n' >&2
worlds=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w")
printf '%s' "$worlds" | grep -q 'Field season'
# A Community post is addressed through the World that published the
# community fragment; the global Channels route must not invent that owner.
printf 'native surface routes: cleared community-world\n' >&2
channels=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/field-season/community")
printf '%s' "$channels" | grep -q 'Rain changed the work window'
echo 'native surface routes: PASS'
