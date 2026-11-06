#!/bin/sh
# A site's own search, from an empty directory: the published sections become
# a static index the page reads in the browser, the index is bound to the
# model that made it, a CMS edit is in the next index with no rebuild step,
# and a missing model answers 404 while the pages keep serving.
# Needs the distilled model (~/BonfyreModels/static/nomic-static-128.bfse);
# without it the test reports itself skipped.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
model=${BONFYRE_STATIC_MODEL:-$HOME/BonfyreModels/static/nomic-static-128.bfse}
[ -s "$model" ] || { echo "site search: SKIP (no static model at $model)"; exit 0; }
for organ in BonfyreCMS/bonfyre-cms BonfyreIngest/bonfyre-ingest BonfyreParagraph/bonfyre-paragraph BonfyreRecall/bonfyre-recall; do
  [ -x "$root/cmd/$organ" ] || { echo "site search: SKIP ($organ not built)"; exit 0; }
done
export BONFYRE_CMS_BINARY="$root/cmd/BonfyreCMS/bonfyre-cms" BONFYRE_INGEST_BINARY="$root/cmd/BonfyreIngest/bonfyre-ingest"
export BONFYRE_PARAGRAPH_BINARY="$root/cmd/BonfyreParagraph/bonfyre-paragraph" BONFYRE_RECALL_BINARY="$root/cmd/BonfyreRecall/bonfyre-recall"
export BONFYRE_CMS_SCHEMAS="$root/cmd/BonfyreCMS/content-types" BONFYRE_SITE_ROOT="$root/site/matrix" BONFYRE_STATIC_MODEL="$model"
home=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-search.XXXXXX")
export BONFYRE_SEARCH_CACHE="$home/search-cache"
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
cleanup() { "$web" site stop --home "$home" >/dev/null 2>&1 || true; rm -rf "$home"; }
trap cleanup EXIT INT TERM
fail() { echo "site search: $1" >&2; exit 1; }
site() { "$web" site "$@" --home "$home" --port "$port"; }
fetch() { curl -s -m 20 --resolve "manual.localhost:$port:127.0.0.1" -o "$2" -w '%{http_code}' "http://manual.localhost:$port$1"; }

site new manual --title 'Field Manual' --no-open >/dev/null
cat > "$home/wifi.md" <<'PAGE'
# Home network

Where the network details are kept.

## Guest access

The router label has the Wi-Fi password and the guest network name.
PAGE
cat > "$home/heater.md" <<'PAGE'
# Water heater

Service history for the tank.

## Maintenance

The water heater was serviced in March; the next flush is due in September.
PAGE
site page manual --title 'Home network' --group 'House' --file "$home/wifi.md" --no-open | grep -q '"page_served":true' || fail "first page"
site page manual --title 'Water heater' --group 'House' --file "$home/heater.md" --no-open | grep -q '"page_served":true' || fail "second page"

[ "$(fetch /_search/passages.json "$home/p.json")" = 200 ] || fail "passages route"
[ "$(fetch /_search/index.bfsi "$home/i.bfsi")" = 200 ] || fail "index route"
[ "$(fetch /_search/model.bfse "$home/m.bfse")" = 200 ] || fail "model route"
cmp -s "$home/m.bfse" "$model" || fail "the served model is not the installed model"
grep -q '"schema":"bonfyre.site-search.v1"' "$home/p.json" || fail "passages schema"
grep -q '"heading":"Guest access"' "$home/p.json" || fail "a section is missing from the passages"
grep -q '"anchor":"maintenance"' "$home/p.json" || fail "a section anchor is missing"

# The page reader, given exactly what the browser gets, finds the section.
answer=$(cd "$root" && bun -e "
import { loadStaticModel, loadIndex, keywordIndex, hybridTop } from './site/matrix/static-embed.mjs';
import { readFileSync } from 'node:fs';
const mb = readFileSync('$home/m.bfse'), m = loadStaticModel(mb), idx = await loadIndex(readFileSync('$home/i.bfsi'), mb);
const P = JSON.parse(readFileSync('$home/p.json', 'utf8')).passages;
if (idx.count !== P.length) throw new Error('index and passages differ');
const top = q => { const [s, i] = hybridTop(q, m, idx, keywordIndex(P), 1)[0]; return P[i].route + '#' + P[i].anchor; };
console.log([top('where is the wifi password'), top('when is the tank due for a flush')].join(' '));")
[ "$answer" = "/home-network#guest-access /water-heater#maintenance" ] || fail "questions found the wrong sections: $answer"

# The index is the content: an edit in the CMS is in the next index.
before=$(grep -o '"index_sha256":"[0-9a-f]*"' "$home/p.json")
cat > "$home/heater.md" <<'PAGE'
# Water heater

Service history for the tank.

## Maintenance

The water heater was replaced in October with a tankless unit.
PAGE
site page manual --title 'Water heater' --group 'House' --file "$home/heater.md" --replace --no-open >/dev/null
fetch /_search/passages.json "$home/p2.json" >/dev/null
after=$(grep -o '"index_sha256":"[0-9a-f]*"' "$home/p2.json")
[ "$before" != "$after" ] || fail "an edited page did not change the index"
grep -q 'tankless' "$home/p2.json" || fail "the edit is not in the passages"

# No model: search says so and the pages still serve.
"$web" site stop --home "$home" >/dev/null 2>&1 || true
BONFYRE_STATIC_MODEL="$home/none.bfse" "$web" site serve --home "$home" --port "$port" >/dev/null 2>&1 || fail "site serve"
for _ in 1 2 3 4 5 6 7 8 9 10; do [ "$(fetch / "$home/front.html")" = 200 ] && break; sleep 0.5; done
[ "$(fetch /water-heater "$home/page.html")" = 200 ] || fail "pages stopped serving without a model"
[ "$(fetch /_search/index.bfsi "$home/none")" = 404 ] || fail "a missing model did not refuse search"
grep -q 'search model unavailable' "$home/none" || fail "the refusal does not say why"

echo "site search: PASS"
