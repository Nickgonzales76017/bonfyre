#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin="$root/cmd/BonfyreWebAddress/bonfyre-web-address"
tmp=$(mktemp -d)
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
db="$tmp/matrix.db"
digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

# The Matrix fixture must not inherit live content or identity stores from MCP.
# Individual owner fixtures below can still supply their own explicit state.
export BONFYRE_CMS_DB="$tmp/cms.db"
export BONFYRE_CMS_SCHEMAS="$root/cmd/BonfyreCMS/content-types"
export BONFYRE_AUTH_DB="$tmp/auth.db"
export BONFYRE_FOREIGN_ROOT="$tmp/foreign"
mkdir -p "$BONFYRE_FOREIGN_ROOT/bodies" "$BONFYRE_FOREIGN_ROOT/receipts"

"$bin" subject --db "$db" --id object:plant:luna --kind PlantPassport --digest "$digest" --evidence receipt:plant
"$bin" bootstrap-owned --db "$db" | grep -q '"bootstrapped_owned_domains":6'
for matrix_root in 'aurekai.life institution:aurekai:life' 'aurekai.live institution:aurekai:live' 'aurekai.shop institution:aurekai:shop' 'aurekai.work institution:aurekai:work' 'internet.aurekai.work institution:aurekai:internet' 'theleaflounge.store institution:leaf-lounge'; do
    set -- $matrix_root
    "$bin" resolve --db "$db" --host "$1" --path / | grep -q "\"subject_id\":\"$2\""
done
"$bin" subject --db "$db" --id standing:plants:life --kind StandingAddress --digest bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb --evidence receipt:standing
"$bin" address --db "$db" --subject object:plant:luna --kind mcp --address mcp://bonfyre/object/plant/luna --evidence receipt:mcp
"$bin" address --db "$db" --subject object:plant:luna --kind fs_path --address /Bonfyre/Life/Plants/luna --evidence receipt:fs
"$bin" address --db "$db" --subject standing:plants:life --kind standing_query --address https://aurekai.life/plants --evidence receipt:standing:url
"$bin" domain --db "$db" --host aurekai.life --territory life --scope self --world live --repr HTML --evidence receipt:domain:life
"$bin" domain --db "$db" --host aurekai.live --territory live --scope public --world live --repr HTML --evidence receipt:domain:live
"$bin" domain --db "$db" --host aurekai.shop --territory shop --scope market --world live --repr HTML --evidence receipt:domain:shop
"$bin" domain --db "$db" --host aurekai.work --territory work --scope institution --world live --repr HTML --evidence receipt:domain:work
"$bin" domain --db "$db" --host theleaflounge.store --territory independent --scope public --world live --repr HTML --evidence receipt:domain:leaf
"$bin" bootstrap-owned --db "$db" | grep -q '"bootstrapped_owned_domains":0'
# A non-Matrix owner can attach a receipted reference to this exact presence.
# It changes no subject, visibility, authority or body ownership; the shared
# RO-Crate/Zarr closure simply becomes able to point all carriers at it.
"$bin" closure-contribution --db "$db" --subject institution:aurekai:life \
  --presence presence:institution:aurekai:life:root --plane material \
  --owner ObjectFabric --resource bonfyre://objectfabric/aurekai-life/public-index \
  --representation Zarr --digest ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  --evidence receipt:objectfabric:aurekai-life
"$bin" closure-contributions --db "$db" --subject institution:aurekai:life \
  --presence presence:institution:aurekai:life:root --plane material | \
  grep -q 'bonfyre://objectfabric/aurekai-life/public-index'
for pair in 'life self' 'live public' 'shop market' 'work institution'; do
    set -- $pair
    "$bin" address --db "$db" --subject object:plant:luna --kind web_url --address "https://leaflounge.aurekai.$1/plants/luna" --evidence "receipt:url:$1"
    "$bin" presence --db "$db" --id "presence:luna:$1" --subject object:plant:luna --territory "$1" --scope "$2" --world live --repr HTML --evidence "receipt:presence:$1"
done
"$bin" closure-contribution --db "$db" --subject object:plant:luna \
  --presence presence:luna:life --plane latent --owner LatentCoordinateFabric \
  --resource bonfyre://latent/plant-luna/life-passport --representation Zarr \
  --digest eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
  --evidence receipt:latent:plant-luna
"$bin" presence --db "$db" --id presence:plants:life --subject standing:plants:life --territory life --scope self --world live --repr HTML --evidence receipt:standing:presence
"$bin" standing --db "$db" --address https://aurekai.life/plants --subject-kind PlantPassport --territory life --scope self --world live --repr HTML --evidence receipt:standing:definition
"$bin" standing-members --db "$db" --address https://aurekai.life/plants | grep -q '"members":1'
"$bin" address --db "$db" --subject object:plant:luna --kind web_url --address https://theleaflounge.store/plants/luna --evidence receipt:url:independent
"$bin" presence --db "$db" --id presence:luna:independent --subject object:plant:luna --territory independent --scope public --world live --repr HTML --evidence receipt:presence:independent
"$bin" address --db "$db" --subject object:plant:luna --kind historical_alias --address https://luna.aurekai.shop/plants/luna --evidence receipt:url:alias
"$bin" alias --db "$db" --alias https://luna.aurekai.shop/plants/luna --canonical https://leaflounge.aurekai.shop/plants/luna --evidence receipt:alias
for carrier in 'api API /api/plants/luna' 'mcp MCP /mcp/resources/plant/luna' 'a2a A2A /a2a/resources/plant/luna' 'cms_route CMS /cms/plants/luna' 'habitat Habitat /habitat/plants/luna'; do
    set -- $carrier
    "$bin" address --db "$db" --subject object:plant:luna --kind "$1" --address "https://leaflounge.aurekai.shop$3" --evidence "receipt:url:$1"
    "$bin" presence --db "$db" --id "presence:luna:shop:$1" --subject object:plant:luna --territory shop --scope market --world live --repr "$2" --evidence "receipt:presence:shop:$1"
done
"$bin" constellation --db "$db" --subject object:plant:luna | grep -q '"kind":"mcp"'
"$bin" constellation --db "$db" --subject object:plant:luna | grep -q '"canonical_address":"https://leaflounge.aurekai.shop/plants/luna"'

"$bin" resolve --db "$db" --host leaflounge.aurekai.life --path /plants/luna | grep -q '"presence_id":"presence:luna:life"'
"$bin" resolve --db "$db" --host aurekai.life --path /plants | grep -q '"standing_members":1'
"$bin" resolve --db "$db" --host leaflounge.aurekai.live --path /plants/luna | grep -q '"presence_id":"presence:luna:live"'
"$bin" resolve --db "$db" --host leaflounge.aurekai.shop --path /plants/luna | grep -q '"audience":"market"'
"$bin" resolve --db "$db" --host luna.aurekai.shop --path /plants/luna | grep -q '"is_alias":true'
"$bin" resolve --db "$db" --host luna.aurekai.shop --path /plants/luna | grep -q '"canonical_address":"https://leaflounge.aurekai.shop/plants/luna"'
"$bin" resolve --db "$db" --host leaflounge.aurekai.work --path /plants/luna | grep -q '"presence_id":"presence:luna:work"'
"$bin" resolve --db "$db" --host theleaflounge.store --path /plants/luna | grep -q '"territory":"independent"'
"$bin" resolve --db "$db" --host leaflounge.aurekai.shop --path /plants/luna --world live --audience market --repr HTML | grep -q '"presence_id":"presence:luna:shop"'
if "$bin" resolve --db "$db" --host leaflounge.aurekai.shop --path /plants/luna --query audience=public >/dev/null 2>&1; then exit 1; fi

"$bin" serve --db "$db" --auth-db "$BONFYRE_AUTH_DB" --port 18787 --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
curl -fsS 'http://127.0.0.1:18787/healthz' | grep -q '"verdict":"healthy"'
life_root=$(curl -fsS -H 'Host: aurekai.life' 'http://127.0.0.1:18787/')
echo "$life_root" | grep -q '<body class="life">'
! echo "$life_root" | grep -q 'Keep what <em>matters.</em>'
echo "$life_root" | grep -q 'Prepare route'
echo "$life_root" | grep -q 'hx-get="/_surface/intake"'
# Root HTML is a real Matrix carrier. The reusable visual grammar receives the
# exact resolve result; it must not replace an institution with a host guess.
root_html=$life_root
echo "$root_html" | grep -q 'BonfyreMatrixContext'
echo "$root_html" | grep -q '"subject":"institution:aurekai:life"'
echo "$root_html" | grep -q '"presence":"presence:institution:aurekai:life:root"'
echo "$root_html" | grep -q '"canonicalAddress":"https://aurekai.life/"'
echo "$root_html" | grep -q 'rel="describedby" type="application/ld+json" href="https://aurekai.life/ro-crate/ro-crate-metadata.json"'
echo "$root_html" | grep -q 'rel="alternate" type="application/vnd.zarr+json" href="https://aurekai.life/zarr/zarr.json"'
live_root=$(curl -fsS -H 'Host: aurekai.live' 'http://127.0.0.1:18787/')
shop_root=$(curl -fsS -H 'Host: aurekai.shop' 'http://127.0.0.1:18787/')
work_root=$(curl -fsS -H 'Host: aurekai.work' 'http://127.0.0.1:18787/')
echo "$live_root" | grep -q '<body class="live">'
echo "$shop_root" | grep -q '<body class="shop">'
echo "$work_root" | grep -q '<body class="work">'
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/' | grep -q '<body class="independent">'
echo "$work_root" | grep -q '"subject":"institution:aurekai:work"'
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/' | grep -q '"subject":"institution:leaf-lounge"'
# The Internet Observatory is a Matrix-owned Work institution, not a static
# dashboard.  Its public routes are closure-backed maintained views and never
# enumerate the candidate frontier; that needs the authenticated Operator Lab.
internet_root=$(curl -fsS -H 'Host: internet.aurekai.work' 'http://127.0.0.1:18787/')
echo "$internet_root" | grep -q 'Aurekai Internet Observatory'
echo "$internet_root" | grep -q '"subject":"institution:aurekai:internet"'
echo "$internet_root" | grep -q 'CandidateAddresses with receipts'
echo "$internet_root" | grep -q 'operator frontier, raw query inputs'
if echo "$internet_root" | grep -q 'href="/property">Property'; then
  echo "Internet Observatory retained generic cross-institution navigation"; exit 1
fi
internet_a2a=$(curl -fsS -H 'Host: internet.aurekai.work' 'http://127.0.0.1:18787/agents/a2a')
echo "$internet_a2a" | grep -q 'maintained agent cut'
echo "$internet_a2a" | grep -q 'discovery-only'
curl -fsS -H 'Host: internet.aurekai.work' 'http://127.0.0.1:18787/ro-crate/ro-crate-metadata.json?address=/agents/a2a' | grep -q '"subject":{"id":"institution:aurekai:internet"'
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: internet.aurekai.work' 'http://127.0.0.1:18787/operators/')
[ "$code" = "401" ] || { echo "Internet Operator Lab was public: $code"; exit 1; }
# A controlled Observatory host must not fall through into generic global
# property/market flows that were never compiled into its address closure.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: internet.aurekai.work' 'http://127.0.0.1:18787/property/')
[ "$code" = "404" ] || { echo "Internet host fell through to global property flow: $code"; exit 1; }
curl -fsS -H 'Host: internet.aurekai.work' 'http://127.0.0.1:18787/agents' | grep -q 'maintained agent cut'
curl -fsS -H 'Host: aurekai.work' 'http://127.0.0.1:18787/studio/' | grep -q 'Institution Studio'
curl -fsS -H 'Host: aurekai.work' 'http://127.0.0.1:18787/studio/' | grep -q 'matrix-htmx.js'
if curl -fsS -H 'Host: aurekai.work' 'http://127.0.0.1:18787/studio/' | grep -q '<pre'; then
  echo 'Studio still exposes raw receipt wells'; exit 1
fi
# A browser/HTMX request is a human carrier: even a refused request must be an
# accessible fragment, never a JSON body rendered into an operator page.
studio_human=$(curl -sS -H 'Host: aurekai.work' -H 'Accept: text/html' \
  'http://127.0.0.1:18787/studio/v1/profile')
echo "$studio_human" | grep -q 'studio-result'
if echo "$studio_human" | grep -q '{"error"'; then
  echo 'Studio human carrier leaked raw JSON'; exit 1
fi
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/assets/matrix.css' | grep -q -- '--ink:#10201a'
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/assets/matrix-aperture.css' | grep -q 'intake-result'
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/assets/matrix.js' | grep -q "Plant culture"
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/assets/matrix-htmx.js' | grep -q 'htmx'
curl -fsS -H 'Host: theleaflounge.store' 'http://127.0.0.1:18787/assets/matrix-vgpu.js' | grep -q "matrix-gpu"
plant_html=$(curl -fsS -H 'Host: leaflounge.aurekai.life' 'http://127.0.0.1:18787/plants/luna')
echo "$plant_html" | grep -q 'ro-crate/ro-crate-metadata.json?address=/plants/luna'
plant_crate=$(curl -fsS -H 'Host: leaflounge.aurekai.life' 'http://127.0.0.1:18787/ro-crate/ro-crate-metadata.json?address=/plants/luna')
echo "$plant_crate" | grep -q '"subject":{"id":"object:plant:luna"'
echo "$plant_crate" | grep -q '"name":"latent","state":"referenced"'
echo "$plant_crate" | grep -q 'bonfyre://latent/plant-luna/life-passport'
plant_zarr=$(curl -fsS -H 'Host: leaflounge.aurekai.life' 'http://127.0.0.1:18787/zarr/zarr.json?address=/plants/luna')
echo "$plant_zarr" | grep -q '"subject":{"id":"object:plant:luna"'
echo "$plant_zarr" | grep -q '"latent":"referenced"'
if curl -fsS -H 'Host: leaflounge.aurekai.life' 'http://127.0.0.1:18787/ro-crate/ro-crate-metadata.json?address=https%3A%2F%2Fforeign.example' >/dev/null 2>&1; then
  echo 'closure accepted a foreign URL rather than a local Matrix path'; exit 1
fi
curl -fsS -G -H 'Host: aurekai.life' --data-urlencode 'url=https://example.org/opportunity?q=one&two=three' 'http://127.0.0.1:18787/_surface/intake' | grep -q 'Review in Studio'
curl -fsS -G -H 'Host: aurekai.life' --data-urlencode 'url=https://example.org/opportunity?q=one&two=three' --data-urlencode 'occurrence=external:ext-00001' 'http://127.0.0.1:18787/_surface/intake' | grep -q 'name="foreign_occurrence" value="external:ext-00001"'
curl -fsS -G -H 'Host: aurekai.life' --data-urlencode 'url=https://example.org/opportunity?q=one&two=three' --data-urlencode 'occurrence=external:ext-00001' 'http://127.0.0.1:18787/_surface/intake' | grep -q 'Candidate route context'
curl -fsS -G -H 'Host: aurekai.life' --data-urlencode 'url=file:///private-state' 'http://127.0.0.1:18787/_surface/intake' | grep -q 'http:// or https://'
curl -sS -i -G -H 'Host: foreign.example' --data-urlencode 'url=https://example.org/' 'http://127.0.0.1:18787/_surface/intake' | grep -q '404 Not Found'
curl -sS -i -H 'Host: foreign.example' 'http://127.0.0.1:18787/assets/matrix.css' | grep -q '404 Not Found'
# The descriptor and the served surface must agree by construction.
#
# This assertion previously required the mcp URL to be present unconditionally.
# That encoded the defect: every host advertised api, mcp, a2a, cms and habitat
# while all five answered 404 in production, which is precisely the inference
# this organ's contract forbids -- "discovery metadata -> a claim that any named
# resource is publicly readable or executable".
#
# Liveness is now derived from the filesystem: a surface is advertised if and
# only if $SITE_ROOT/surfaces/<name>.json exists, and the same predicate decides
# what is served. An operator flag can drift from reality; a file cannot.
descriptor=$(curl -fsS -H 'Host: aurekai.life' 'http://127.0.0.1:18787/.well-known/bonfyre')
echo "$descriptor" | grep -q '"web":"https://aurekai.life/"'
echo "$descriptor" | grep -q '"bonfyrefs":'
echo "$descriptor" | grep -q '"ro_crate":"https://aurekai.life/ro-crate/ro-crate-metadata.json"'
echo "$descriptor" | grep -q '"zarr":"https://aurekai.life/zarr/zarr.json"'

# RO-Crate and Zarr are generated from the same native resolve, not static
# per-host catalog files. Matrix is the exact plane. Other owners can attach
# evidence-bound references to the same Presence without becoming Matrix data.
crate=$(curl -fsS -H 'Host: aurekai.life' 'http://127.0.0.1:18787/ro-crate/ro-crate-metadata.json')
echo "$crate" | grep -q '"@context":"https://w3id.org/ro/crate/1.1/context"'
echo "$crate" | grep -q '"subject":{"id":"institution:aurekai:life"'
echo "$crate" | grep -q '"name":"exact-semantic","state":"resolved"'
echo "$crate" | grep -q '"name":"material","state":"referenced"'
echo "$crate" | grep -q 'bonfyre://objectfabric/aurekai-life/public-index'
echo "$crate" | grep -q '"name":"economic","state":"unresolved"'
zarr=$(curl -fsS -H 'Host: aurekai.life' 'http://127.0.0.1:18787/zarr/zarr.json')
echo "$zarr" | grep -q '"zarr_format":3'
echo "$zarr" | grep -q '"bonfyre:virtual":true'
echo "$zarr" | grep -q '"subject":{"id":"institution:aurekai:life"'
echo "$zarr" | grep -q '"material":"referenced"'
"$bin" closure --db "$db" --host aurekai.life | grep -q '"BonfyreMatrixClosure"'
"$bin" closure --db "$db" --host aurekai.life --format zarr | grep -q '"zarr_format":3'
"$bin" closure --db "$db" --host leaflounge.aurekai.life --path /plants/luna | grep -q 'bonfyre://latent/plant-luna/life-passport'

# Every surface named in the descriptor must answer 200.
for surface in api mcp a2a cms habitat; do
  case "$surface" in
    api) path=/api/ ;; mcp) path=/mcp/resources/ ;; a2a) path=/a2a/resources/ ;;
    cms) path=/cms/ ;; habitat) path=/habitat/ ;;
  esac
  code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.life' \
    "http://127.0.0.1:18787$path")
  if echo "$descriptor" | grep -q "\"$surface\":"; then
    [ "$code" = "200" ] || { echo "advertised $surface but $path returned $code"; exit 1; }
  else
    [ "$code" = "404" ] || { echo "unadvertised $surface but $path returned $code"; exit 1; }
  fi
done

# An unknown host gets no surface content even when the file exists.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: foreign.example' \
  'http://127.0.0.1:18787/api/')
[ "$code" = "404" ] || { echo "unknown host served a surface: $code"; exit 1; }

echo "web address matrix: PASS"
