#!/bin/sh
# The frontier, reachable.
#
# The spatial owner could parse and render /w/<world>/spatial?at=... before this
# route existed, and nothing served it. What is proved here is that the address
# reaches the owner, that the owner's answer is what comes back rather than a
# second implementation of it, and that a request without a frontier is left
# alone for the surface that already answers it.
set -eu
web=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
spatial=$(cd "$(dirname -- "$2")" && pwd)/$(basename -- "$2")
estate=$(cd "$(dirname -- "$3")" && pwd)/$(basename -- "$3")
tmp=$(mktemp -d)
port=${BONFYRE_SPATIAL_ROUTE_PORT:-8793}
cleanup() { if [ -n "${pid:-}" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf "$tmp"; }
trap cleanup EXIT
mkdir -p "$tmp/bin"
cp "$spatial" "$tmp/bin/bonfyre-spatial"

j() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(eval(sys.argv[1],{"d":d}))' "$1"; }

# Two fixes. The second was seen at 22:00:30 and reached this estate at
# 22:00:45, and only the first is ever advertised.
"$tmp/bin/bonfyre-spatial" observe --db "$tmp/spatial.db" --observation fix-a \
  --subject subject:aircraft:abc123 --geometry 'POINT(-96.7836 32.7843)' \
  --lon -96.7836 --lat 32.7843 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:00Z --recorded-at 2026-09-12T22:00:02Z \
  --mode observed --source-occurrence o --provenance opensky > /dev/null
"$tmp/bin/bonfyre-spatial" observe --db "$tmp/spatial.db" --observation fix-b \
  --subject subject:aircraft:abc123 --geometry 'POINT(-96.7836 32.7933)' \
  --lon -96.7836 --lat 32.7933 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:30Z --recorded-at 2026-09-12T22:00:45Z \
  --mode observed --source-occurrence o --provenance opensky > /dev/null
sqlite3 "$tmp/cms.db" "CREATE TABLE spatial_observation(observation_id TEXT,subject_address TEXT,geometry_kind TEXT,geometry TEXT,crs TEXT,horizontal_datum TEXT,observed_at TEXT,source_occurrence TEXT,observation_mode TEXT,derived_from TEXT,mobility TEXT,scale TEXT,region TEXT,cell TEXT,provenance TEXT,status TEXT); INSERT INTO spatial_observation VALUES('fix-a','subject:aircraft:abc123','point','g','EPSG:4326','WGS84','2026-09-12T22:00:00Z','o','observed','[]','moving','city','dfw','dfw-0912','opensky','published');"
"$estate" emit-cms --cms-db "$tmp/cms.db" > "$tmp/ads.jsonl"
"$estate" import-advertisements --db "$tmp/plane.db" --advertisements "$tmp/ads.jsonl" > /dev/null

"$web" bootstrap-owned --db "$tmp/matrix.db" > /dev/null 2>&1 || true
BONFYRE_SPATIAL_DB="$tmp/spatial.db" \
BONFYRE_EXTERNAL_ESTATE_DB="$tmp/plane.db" \
BONFYRE_BIN_DIR="$tmp/bin" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" > "$tmp/server.log" 2>&1 &
pid=$!
i=0; while [ $i -lt 60 ]; do
  curl -sS -m 2 -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break
  i=$((i+1)); sleep 0.2
done

base="http://127.0.0.1:$port/w/deep-ellum/spatial"

# The same address at the same valid instant is a different world depending on
# what the estate had learned. This is the whole reason the route carries two
# instants instead of one.
curl -sS -m 10 -H 'Accept: application/json' \
  "$base?at=2026-09-12T22:00:40Z&as-of=2026-09-12T22:00:40Z&cell=dfw-0912" > "$tmp/early.json"
[ "$(j 'd["present"][0]["observation"]' < "$tmp/early.json")" = "fix-a" ]
curl -sS -m 10 -H 'Accept: application/json' \
  "$base?at=2026-09-12T22:00:40Z&as-of=2026-09-12T22:01:00Z&cell=dfw-0912" > "$tmp/later.json"
# fix-b was never advertised: the plane named the subject and the owner answered.
[ "$(j 'd["present"][0]["observation"]' < "$tmp/later.json")" = "fix-b" ]
[ "$(j 'd["coarse_considered"]' < "$tmp/later.json")" = "1" ]
[ "$(j 'd["frontier"]["as_of"]' < "$tmp/later.json")" = "2026-09-12T22:01:00Z" ]

# A browser gets the frontier stated, not just the data.
curl -sS -m 10 -H 'Accept: text/html' \
  "$base?at=2026-09-12T22:00:40Z&as-of=2026-09-12T22:01:00Z" > "$tmp/page.html"
grep -q 'as it stood at 2026-09-12T22:00:40Z' "$tmp/page.html"
grep -q 'recompiles the closure' "$tmp/page.html"

# A half-read frontier would show a reader an instant they did not ask for.
[ "$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "$base?at=2026-09-12")" = "400" ]
[ "$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "$base?at=2026-09-12T22:00:40Z&as-of=nonsense")" = "400" ]

# Without a frontier this is somebody else's route and is left alone. The
# surface answers 404 for a World that has not published that grammar, which is
# the surface's answer and not this route's.
[ "$(curl -sS -m 10 -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$base")" = "404" ]
# Unrelated routes are untouched.
[ "$(curl -sS -m 10 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz")" = "200" ]

echo "spatial frontier route: ok"
