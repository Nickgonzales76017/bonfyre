#!/bin/sh
# Carrier paging past the first page (parcel-projection no-cursor gap).
#
# What this proves: every SQL-backed machine projection walks its whole
# collection -- ?offset= skips rows, a full page carries a next link in the
# carrier's own grammar (OGC links, OData nextLink, ActivityPub next), a
# short page ends the walk, three parcel dialects agree on every page (not
# just the first), and a bad offset is refused with 400 rather than served
# as page one.
#
# The fixture is a miniature Fleming estate (hot SQLite only, no Parquet)
# plus a miniature work graph, served by the tree binary on a private port.
# Nothing here touches live stores.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
qbin="$root/cmd/BonfyreQuery/bonfyre-query"
[ -x "$qbin" ] || { echo 'carrier paging: SKIP (bonfyre-query not built)'; exit 0; }
command -v sqlite3 >/dev/null || { echo 'carrier paging: SKIP (sqlite3 missing)'; exit 0; }
command -v python3 >/dev/null || { echo 'carrier paging: SKIP (python3 missing)'; exit 0; }
command -v curl >/dev/null || { echo 'carrier paging: SKIP (curl missing)'; exit 0; }
ext="$HOME/.duckdb/extensions/v1.5.1/osx_arm64/sqlite_scanner.duckdb_extension"
[ -f "$ext" ] || { echo 'carrier paging: SKIP (no cached DuckDB sqlite extension and no network to fetch it)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-carrier-paging.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
fail() { echo "carrier paging: FAIL: $*" >&2; [ -f "$tmp/server.log" ] && tail -20 "$tmp/server.log" >&2; exit 1; }
pass=0; ok() { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

# ---- miniature Fleming estate -------------------------------------------
flem="$tmp/fleming"; mkdir -p "$flem"
sqlite3 "$flem/estate-lite.db" <<'SQL'
CREATE TABLE observation(observation_id INTEGER PRIMARY KEY, src_id TEXT, observed TEXT, observed_at TEXT);
CREATE TABLE page(id INTEGER);
CREATE TABLE harvest(id INTEGER);
CREATE TABLE parcel_flat(id INTEGER);
INSERT INTO observation VALUES
 (1,'src:tad-parcel-view','{"Account_Nu":"00000051","Situs_Addr":"51 Main St","City":"Fort Worth","ZipCode":"76101","Owner_Name":"Ada","Year_Built":"1999","Num_Bedroo":"3","Num_Bathro":"2","Living_Are":"1500","Land_SqFt":"6000","Land_Value":"20000","Total_Valu":"150000"}','2026-01-01'),
 (2,'src:tad-parcel-view','{"Account_Nu":"00000052","Situs_Addr":"52 Main St","City":"Fort Worth","ZipCode":"76102","Owner_Name":"Bob","Year_Built":"2001","Num_Bedroo":"4","Num_Bathro":"2.5","Living_Are":"1800","Land_SqFt":"6500","Land_Value":"22000","Total_Valu":"170000"}','2026-01-02'),
 (3,'src:tad-parcel-view','{"Account_Nu":"00000053","Situs_Addr":"53 Main St","City":"Fort Worth","ZipCode":"76103","Owner_Name":"Cy","Year_Built":"1980","Num_Bedroo":"3","Num_Bathro":"1","Living_Are":"1200","Land_SqFt":"5000","Land_Value":"18000","Total_Valu":"130000"}','2026-01-03'),
 (4,'src:tad-parcel-view','{"Account_Nu":"00000054","Situs_Addr":"54 Main St","City":"Fort Worth","ZipCode":"76104","Owner_Name":"Dee","Year_Built":"2010","Num_Bedroo":"5","Num_Bathro":"3","Living_Are":"2200","Land_SqFt":"7000","Land_Value":"25000","Total_Valu":"200000"}','2026-01-04'),
 (5,'src:tad-parcel-view','{"Account_Nu":"00000055","Situs_Addr":"55 Main St","City":"Fort Worth","ZipCode":"76105","Owner_Name":"Eli","Year_Built":"1975","Num_Bedroo":"2","Num_Bathro":"1","Living_Are":"1000","Land_SqFt":"4500","Land_Value":"15000","Total_Valu":"110000"}','2026-01-05'),
 (6,'src:dallas-tax-parcels','{"GIS_ACCT":"D-001","ST_NUM":"1","ST_NAME":"Elm St","CITY":"Dallas","TAXPANAME1":"Fay","AREA_FEET":"5500","COUNTY":"DALLAS COUNTY"}','2026-01-06'),
 (7,'src:dallas-tax-parcels','{"GIS_ACCT":"D-002","ST_NUM":"2","ST_NAME":"Elm St","CITY":"Dallas","TAXPANAME1":"Gus","AREA_FEET":"5600","COUNTY":"DALLAS COUNTY"}','2026-01-07'),
 (8,'src:dallas-tax-parcels','{"GIS_ACCT":"D-003","ST_NUM":"3","ST_NAME":"Elm St","CITY":"Dallas","TAXPANAME1":"Hal","AREA_FEET":"5700","COUNTY":"DALLAS COUNTY"}','2026-01-08'),
 (9,'src:dallas-tax-parcels','{"GIS_ACCT":"D-004","ST_NUM":"4","ST_NAME":"Elm St","CITY":"Dallas","TAXPANAME1":"Ivy","AREA_FEET":"5800","COUNTY":"DALLAS COUNTY"}','2026-01-09'),
 (10,'src:dallas-tax-account-points','{"AccountID":"A-101","ParcelID":"P-101","SiteAddrNum":"10","SiteStreetname":"Oak","CityJuris":"DALLAS","Legal1":"LOT 1 AIR RIGHTS RESERVE","Legal2":"","Legal3":"","Legal4":"","Owner1":"Jo"}','2026-01-10'),
 (11,'src:dallas-tax-account-points','{"AccountID":"A-102","ParcelID":"P-102","SiteAddrNum":"11","SiteStreetname":"Oak","CityJuris":"DALLAS","Legal1":"LOT 2","Legal2":"SUBSURFACE EASEMENT","Legal3":"","Legal4":"","Owner1":"Kay"}','2026-01-11'),
 (12,'src:dallas-tax-account-points','{"AccountID":"A-103","ParcelID":"P-103","SiteAddrNum":"12","SiteStreetname":"Oak","CityJuris":"DALLAS","Legal1":"LOT 3","Legal2":"NOTHING OF NOTE","Legal3":"","Legal4":"","Owner1":"Lou"}','2026-01-12'),
 (13,'src:stray-a','{"x":1}','2026-01-13'),
 (14,'src:stray-b','{"x":2}','2026-01-14');
SQL
sqlite3 "$flem/fleming-work.db" <<'SQL'
CREATE TABLE missions(id TEXT PRIMARY KEY, created_at TEXT);
CREATE TABLE workgraph_nodes(status TEXT, mission_id TEXT, operator_id TEXT, input_uri TEXT);
INSERT INTO missions VALUES ('m1','2026-01-01T00:00:01Z'),('m2','2026-01-01T00:00:02Z'),('m3','2026-01-01T00:00:03Z');
INSERT INTO workgraph_nodes VALUES
 ('done','m1','op.a','in:1'),('done','m2','op.b','in:2'),
 ('running','m3','op.c','in:3'),('queued','m3','op.d','in:4');
SQL

# DuckDB resolves its sqlite extension under its home directory; point it at
# a copy of this machine's cache so the test needs no network.
mkdir -p "$tmp/dhome/.duckdb/extensions/v1.5.1/osx_arm64"
cp "$ext" "$ext.info" "$tmp/dhome/.duckdb/extensions/v1.5.1/osx_arm64/" 2>/dev/null || cp "$ext" "$tmp/dhome/.duckdb/extensions/v1.5.1/osx_arm64/"
mkdir -p "$tmp/bin" && ln -s "$qbin" "$tmp/bin/bonfyre-query"

# ---- serve ---------------------------------------------------------------
matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$((20000 + $$ % 30000))
BONFYRE_FLEMING_ROOT="$flem" BONFYRE_DUCKDB_HOME="$tmp/dhome" BONFYRE_BIN_DIR="$tmp/bin" \
  BONFYRE_CMS_DB="$tmp/cms.db" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -s -o /dev/null -m 1 "$base/healthz" 2>/dev/null; do
  kill -0 "$pid" 2>/dev/null || fail "server exited on boot"
  i=$((i+1)); [ $i -gt 100 ] && fail "server did not start"
  sleep 0.2
done
get() { curl -sS -m 25 -H 'Accept: application/json' "$base$1"; }
code() { curl -sS -m 25 -o /dev/null -w '%{http_code}' -H 'Accept: application/json' "$base$1"; }
json_ok() { printf '%s' "$2" | python3 -m json.tool >/dev/null 2>&1 || fail "$1 is not valid JSON: $2"; }

echo "== the outbox walks every source =="
out=$(get '/ap/outbox') || fail "GET /ap/outbox"
json_ok outbox "$out"
[ "$(printf '%s' "$out" | J 'print(d["limit"],d["offset"])')" = "50 0" ] || fail "default page is not limit=50 offset=0"
[ "$(printf '%s' "$out" | J 'print(len(d["orderedItems"]))')" = "5" ] || fail "default page misses sources: $out"
printf '%s' "$out" | grep -q '"next"' && fail "short default page carries a next link" || ok "default page lists all 5 sources with no next link"
[ "$(printf '%s' "$out" | J 'print([r["src_id"] for r in d["orderedItems"]])')" = \
  "['src:tad-parcel-view', 'src:dallas-tax-parcels', 'src:dallas-tax-account-points', 'src:stray-a', 'src:stray-b']" ] \
  || fail "outbox order is not count DESC, src_id: $out"
ok "outbox orders by count then source id"
p1=$(get '/ap/outbox?limit=2') p2=$(get '/ap/outbox?limit=2&offset=2') p3=$(get '/ap/outbox?limit=2&offset=4')
for p in "$p1" "$p2" "$p3"; do json_ok outbox-page "$p"; done
[ "$(printf '%s' "$p1" | J 'print([r["src_id"] for r in d["orderedItems"]])')" = "['src:tad-parcel-view', 'src:dallas-tax-parcels']" ] || fail "outbox page 1: $p1"
[ "$(printf '%s' "$p2" | J 'print([r["src_id"] for r in d["orderedItems"]])')" = "['src:dallas-tax-account-points', 'src:stray-a']" ] || fail "outbox page 2: $p2"
[ "$(printf '%s' "$p3" | J 'print([r["src_id"] for r in d["orderedItems"]])')" = "['src:stray-b']" ] || fail "outbox page 3: $p3"
ok "limit=2 walks all 5 sources across 3 disjoint pages"
printf '%s' "$p1" | grep -q '"next":"[^"]*offset=2"' || fail "page 1 next link misses offset=2: $p1"
printf '%s' "$p2" | grep -q '"next":"[^"]*offset=4"' || fail "page 2 next link misses offset=4: $p2"
printf '%s' "$p3" | grep -q '"next"' && fail "final page carries a next link: $p3" || ok "next links chain pages and stop at the end"
past=$(get '/ap/outbox?limit=2&offset=99')
[ "$(printf '%s' "$past" | J 'print(d["orderedItems"])')" = "[]" ] || fail "past-the-end page is not empty: $past"
printf '%s' "$past" | grep -q '"next"' && fail "empty page carries a next link" || ok "past-the-end page is empty with no next link"

echo "== a bad offset is refused, on every carrier =="
for path in '/ap/outbox?offset=abc' '/ap/outbox?offset=-1' '/ap/outbox?offset=1000001' \
            '/ogc/collections/parcels/items?offset=abc' '/reso/Property?offset=-1' \
            '/mismo/subject-property?offset=1oops' '/events?offset=99x'; do
  [ "$(code "$path")" = "400" ] || fail "$path is not 400"
  get "$path" | grep -q '"error":"invalid_offset"' || fail "$path misses invalid_offset"
done
ok "abc, -1, 1000001 and 1oops are 400 invalid_offset on outbox, OGC, RESO, MISMO and events"
curl -s -i -m 25 -H 'Accept: application/json' "$base/ap/outbox?offset=abc" | head -1 | grep -q '400 Bad Request' \
  || fail "400 status line misnames itself"
ok "the 400 status line says Bad Request"
[ "$(code '/ap/outbox?limit=0')" = "200" ] || fail "limit=0 is not clamped to a page"
[ "$(get '/ap/outbox?limit=0' | J 'print(len(d["orderedItems"]))')" = "1" ] || fail "limit=0 page is not one row"
ok "limit=0 still clamps to one row"

echo "== the three parcel dialects agree on every page =="
t0=$(get '/ogc/collections/parcels/items?territory=tarrant&limit=2')
t1=$(get '/ogc/collections/parcels/items?territory=tarrant&limit=2&offset=2')
t2=$(get '/ogc/collections/parcels/items?territory=tarrant&limit=2&offset=4')
for p in "$t0" "$t1" "$t2"; do json_ok ogc-page "$p"; done
[ "$(printf '%s' "$t0" | J 'print([r["account"] for r in d["properties_rows"]],d["numberReturned"])')" = "['00000051', '00000052'] 2" ] || fail "tarrant page 1: $t0"
[ "$(printf '%s' "$t1" | J 'print([r["account"] for r in d["properties_rows"]],d["numberReturned"])')" = "['00000053', '00000054'] 2" ] || fail "tarrant page 2: $t1"
[ "$(printf '%s' "$t2" | J 'print([r["account"] for r in d["properties_rows"]],d["numberReturned"])')" = "['00000055'] 1" ] || fail "tarrant page 3: $t2"
ok "tarrant walks 51-55 with honest numberReturned"
printf '%s' "$t0" | grep -q '"rel":"next".*offset=2' || fail "OGC page 1 misses next offset=2: $t0"
printf '%s' "$t1" | grep -q '"rel":"next".*offset=4' || fail "OGC page 2 misses next offset=4: $t1"
printf '%s' "$t2" | grep -q '"rel":"next"' && fail "OGC final page carries next: $t2" || ok "OGC next links chain and stop"
for off in 0 2 4; do
  o=$(get "/ogc/collections/parcels/items?territory=tarrant&limit=2&offset=$off" | J 'print([r["account"] for r in d["properties_rows"]])')
  r=$(get "/reso/Property?territory=tarrant&limit=2&offset=$off" | J 'print([x["ListingKey"] for x in d["value"]])')
  m=$(get "/mismo/subject-property?territory=tarrant&limit=2&offset=$off" | J 'print([x["account"] for x in d["MESSAGE"]["DEAL_SETS"]["SUBJECT_PROPERTIES"]])')
  [ "$o" = "$r" ] && [ "$r" = "$m" ] || fail "carriers disagree at offset=$off: $o / $r / $m"
done
ok "OGC, RESO and MISMO return the same accounts at offsets 0, 2 and 4"
reso_n=$(get '/reso/Property?territory=tarrant&limit=2' | J 'print(d["@odata.nextLink"])')
case "$reso_n" in *"/reso/Property?territory=tarrant&limit=2&offset=2") ok "RESO next page is @odata.nextLink";; *) fail "RESO nextLink: $reso_n";; esac
get '/reso/Property?territory=tarrant&limit=2&offset=4' | grep -q 'nextLink' && fail "RESO final page carries nextLink" || ok "RESO final page omits nextLink"
mismo_p=$(get '/mismo/subject-property?territory=tarrant&limit=2&offset=2')
[ "$(printf '%s' "$mismo_p" | J 'print(d["paging"]["limit"],d["paging"]["offset"])')" = "2 2" ] || fail "MISMO paging echo: $mismo_p"
printf '%s' "$mismo_p" | grep -q '"next":"[^"]*offset=4"' || fail "MISMO next link: $mismo_p"
ok "MISMO echoes paging and links next"

echo "== the combined-territory walk crosses the union =="
a0=$(get '/ogc/collections/parcels/items?territory=all&limit=3')
a1=$(get '/ogc/collections/parcels/items?territory=all&limit=3&offset=3')
a2=$(get '/ogc/collections/parcels/items?territory=all&limit=3&offset=6')
a3=$(get '/ogc/collections/parcels/items?territory=all&limit=3&offset=9')
[ "$(printf '%s' "$a0" | J 'print([(r["territory"],r["account"]) for r in d["properties_rows"]])')" = \
  "[('dallas', 'D-001'), ('dallas', 'D-002'), ('dallas', 'D-003')]" ] || fail "all page 1: $a0"
[ "$(printf '%s' "$a1" | J 'print([(r["territory"],r["account"]) for r in d["properties_rows"]])')" = \
  "[('dallas', 'D-004'), ('tarrant', '00000051'), ('tarrant', '00000052')]" ] || fail "all page 2: $a1"
[ "$(printf '%s' "$a2" | J 'print([(r["territory"],r["account"]) for r in d["properties_rows"]])')" = \
  "[('tarrant', '00000053'), ('tarrant', '00000054'), ('tarrant', '00000055')]" ] || fail "all page 3: $a2"
[ "$(printf '%s' "$a3" | J 'print(d["properties_rows"],d["numberReturned"])')" = "[] 0" ] || fail "all page 4: $a3"
printf '%s' "$a3" | grep -q '"rel":"next"' && fail "empty union page carries next" || ok "all-territory walk covers 9 rows then ends empty"

echo "== recorded rights and events page too =="
rr0=$(get '/ogc/collections/recorded-rights/items?limit=2')
rr1=$(get '/ogc/collections/recorded-rights/items?limit=2&offset=2')
[ "$(printf '%s' "$rr0" | J 'print([r["account_id"] for r in d["properties_rows"]],d["numberReturned"])')" = "['A-101', 'A-102'] 2" ] || fail "recorded-rights page 1: $rr0"
[ "$(printf '%s' "$rr1" | J 'print([r["account_id"] for r in d["properties_rows"]],d["numberReturned"])')" = "[] 0" ] || fail "recorded-rights page 2 (A-103 has no noted right): $rr1"
printf '%s' "$rr0" | grep -q '"rel":"next"' || fail "recorded-rights full page misses next"
printf '%s' "$rr1" | grep -q '"rel":"next"' && fail "recorded-rights empty page carries next" || ok "recorded-rights pages with self/next links"
e0=$(get '/events?limit=2') e1=$(get '/events?limit=2&offset=2') e2=$(get '/events?limit=2&offset=4')
for p in "$e0" "$e1" "$e2"; do json_ok events-page "$p"; done
[ "$(printf '%s' "$e0" | J 'print([e["subject"] for e in d])')" = "['op.c', 'op.d']" ] || fail "events page 1: $e0"
[ "$(printf '%s' "$e1" | J 'print([e["subject"] for e in d])')" = "['op.b', 'op.a']" ] || fail "events page 2: $e1"
[ "$(printf '%s' "$e2" | J 'print(d)')" = "[]" ] || fail "events page 3: $e2"
ok "events walks the bare batch array to []"

echo
echo "carrier paging: $pass passed"
