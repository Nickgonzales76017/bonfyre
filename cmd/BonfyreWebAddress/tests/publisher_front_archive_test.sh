#!/bin/sh
# A publisher's own address carries what the estate kept from that publisher
# even when its compiled "/" route holds no items. From the 19 October 2026
# recompile every publisher's "/" cut was empty, and all 1,700 publisher hosts
# published as a bare heading with their host for a name.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'publisher front archive: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-publisher-front.XXXXXX")
trap '[ -n "${KEEP:-}" ] || rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies" "$tmp/out"
for type in internet_source site_presentation; do
    cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/"
done
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null
"$cms" entry create internet_source \
  '{"family":"news","source":"feed-fieldwire-test-abc123","label":"Fieldwire Journal","says":"fixture","url":"https://fieldwire.test/feed","twin":"fieldwire-twin","state":"observed","extract":"items","title_field":"title","meta_field":"published","link_field":"url","summary_field":"body","status":"published"}' \
  --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null
"$cms" entry create site_presentation \
  '{"site_id":"publisher-fieldwire-test","host":"fieldwire-test.aurekai.life","subject_address":"institution:publisher:fieldwire-test","profile":"newsroom","territory":"life","navigation":[],"status":"published"}' \
  --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null
cat > "$tmp/foreign/receipts/fieldwire-twin.json" <<EOF
{"body_path":"$tmp/foreign/bodies/fieldwire.body"}
EOF
cat > "$tmp/foreign/bodies/fieldwire.body" <<'EOF'
{"items":[{"title":"RAIN CHANGES THE HARVEST WINDOW","body":"Growers adjust.","published":"2026-10-01T00:00:00Z","url":"https://fieldwire.test/rain"}]}
EOF

export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
       BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" BONFYRE_SUBJECT_FABRIC_DB="$tmp/subject_fabric.db"
"$web" census-items > "$tmp/items.jsonl"
"$web" archive-items --items "$tmp/items.jsonl" >/dev/null

# The fabric names the publisher's reader and compiles "/" with no items.
sqlite3 "$tmp/subject_fabric.db" "
CREATE TABLE subject_bundle(subject TEXT,scope TEXT,bundle_digest TEXT);
CREATE TABLE subject_reader(subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,material TEXT,
  protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT);
CREATE TABLE subject_route(subject TEXT,scope TEXT,route TEXT,territory TEXT,title TEXT,lede TEXT,character TEXT,cannot_infer TEXT);
CREATE TABLE subject_route_item(subject TEXT,scope TEXT,route TEXT,ordinal INTEGER,source TEXT,item_key TEXT,material TEXT);
CREATE TABLE subject_material(subject TEXT,scope TEXT,source TEXT,item_key TEXT,material TEXT,title TEXT,byline TEXT,publisher TEXT,
  href TEXT,group_name TEXT,image TEXT,asset TEXT,poster TEXT,body TEXT,ident TEXT,about TEXT,rights TEXT,rights_href TEXT,origin TEXT,
  origin_href TEXT,people TEXT,where_text TEXT,transcript_href TEXT,chapters_href TEXT,talk_href TEXT,at INTEGER,duration_ms INTEGER,
  placed INTEGER,lon REAL,lat REAL);
INSERT INTO subject_bundle VALUES('institution:publisher:fieldwire-test','public','fixture');
INSERT INTO subject_reader VALUES('institution:publisher:fieldwire-test','public','feed-fieldwire-test-abc123','fieldwire-test',
  'feed-fieldwire-test-abc123','news','RSS','admitted','',1,'');
INSERT INTO subject_route VALUES('institution:publisher:fieldwire-test','public','/','life','Today','','','');"

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
"$web" publish-static --db "$tmp/matrix.db" --site-root "$root/site/matrix" --out "$tmp/out" \
    --only "fieldwire-test.aurekai.life" >/dev/null
page="$tmp/out/h/fieldwire-test.aurekai.life/index.html"
[ -s "$page" ] || { echo 'publisher front archive: no page written' >&2; exit 1; }
grep -q 'RAIN CHANGES THE HARVEST WINDOW' "$page" || { echo 'publisher front archive: the archive did not reach the front' >&2; exit 1; }
grep -q '<title>Fieldwire Journal' "$page" || { echo 'publisher front archive: the page is not named for the publisher' >&2; exit 1; }
grep -q 'href="/o/feed-fieldwire-test-abc123/' "$page" || { echo 'publisher front archive: the item does not open' >&2; exit 1; }
grep -q 'fieldwire.test</span>' "$page" || { echo 'publisher front archive: the item does not name where it came from' >&2; exit 1; }
echo 'publisher front archive: PASS'
