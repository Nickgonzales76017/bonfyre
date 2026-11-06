#!/bin/sh
# The product catalogue is an owner projection, not a receipt directory scan.
# It must show the three real stages, preserve the unbound public-program
# frontier, and work even where a forensic receipt closure is intentionally
# unavailable.
set -eu

bin=${1:-./bonfyre-project}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-foreign-release.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

cms="$tmp/cms.db"
frontier="$tmp/frontier.db"
items="$tmp/items.db"
matrix="$tmp/matrix.db"
prices="$tmp/prices.db"

sqlite3 "$cms" "
  CREATE TABLE internet_source(
    url TEXT, status TEXT, source TEXT, family TEXT, carries TEXT,
    body_regime TEXT, tier TEXT, last_seen TEXT, cadence_seconds INTEGER,
    place_field TEXT, media_field TEXT, rights_field TEXT, publisher_ident TEXT
  );
  INSERT INTO internet_source VALUES
    ('https://parts.example/catalog','published','parts-catalog','commerce','product',
     'REMOTE','WARM','2026-09-26T12:00:00Z',900,'geometry.value','image.url','license','parts-example'),
    ('https://quiet.example/feed','published','quiet-feed','publication','article',
     'REMOTE','COLD','',21600,'','','','');
  CREATE TABLE source_collection(collection_id TEXT, status TEXT, source_selector TEXT);
  CREATE TABLE site_program(program_id TEXT, status TEXT, source_collections TEXT);
  INSERT INTO source_collection VALUES
    ('parts-catalogue','published','{\"source_refs\":[\"parts-catalog\"]}');
  INSERT INTO site_program VALUES
    ('parts-reading','published','[\"parts-catalogue\"]');
"
sqlite3 "$frontier" "
  CREATE TABLE channel(address TEXT, exact_address TEXT, state TEXT);
  INSERT INTO channel VALUES
    ('https://normalized.parts.example/catalog','https://parts.example/catalog','admitted'),
    ('https://normalized.parts.example/catalog','https://parts.example/catalog','eligible'),
    ('https://normalized.quiet.example/feed','https://quiet.example/feed','eligible');
"
sqlite3 "$items" "
  CREATE TABLE item(source TEXT);
  INSERT INTO item VALUES ('parts-catalog'),('parts-catalog'),('parts-catalog');
"
sqlite3 "$prices" "
  CREATE TABLE observed_price(source TEXT, publisher TEXT, item TEXT, amount REAL,
    currency TEXT, unit TEXT, region TEXT, kind TEXT, evidence_ref TEXT, observed_at TEXT);
  INSERT INTO observed_price VALUES
    ('parts-catalog','Parts Example','brake lever',18.50,'USD','each','Dallas','retail','source stated', '2026-09-26T12:00:00Z'),
    ('unbound-merchant','Unbound Merchant','ticket',25.00,'USD','each','Dallas','ticket','source stated', '2026-09-26T13:00:00Z');
"

# A release address is semantic identity, not a display label.  URLs longer
# than the old receipt-reader buffer must stay exact in the compact catalogue.
long_url=$(python3 -c "print('https://long.example/catalog?' + 'facet=' + 'x'*1800)")
sqlite3 "$cms" "INSERT INTO internet_source VALUES ('$long_url','published','long-catalog','commerce','product','REMOTE','WARM','',900,'','','','');"
sqlite3 "$frontier" "INSERT INTO channel VALUES ('https://normalized.long.example/catalog','$long_url','admitted');"

BONFYRE_CMS_DB="$cms" \
BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
BONFYRE_ITEM_ARCHIVE_DB="$items" \
BONFYRE_PRICES_DB="$prices" \
BONFYRE_MATRIX_DB="$matrix" \
BONFYRE_FOREIGN_RECEIPTS="$tmp/receipts-deliberately-absent" \
"$bin" foreign twins >"$tmp/catalogue.json"

BONFYRE_CMS_DB="$cms" \
BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
BONFYRE_ITEM_ARCHIVE_DB="$items" \
BONFYRE_PRICES_DB="$prices" \
BONFYRE_MATRIX_DB="$matrix" \
"$bin" foreign twins --page 1 --limit 1 >"$tmp/catalogue-page.json"

# Find is literal source-contract narrowing, not a semantic search claim.  The
# result-set count changes; whole-estate closure and readiness do not.
BONFYRE_CMS_DB="$cms" \
BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
BONFYRE_ITEM_ARCHIVE_DB="$items" \
BONFYRE_PRICES_DB="$prices" \
BONFYRE_MATRIX_DB="$matrix" \
"$bin" foreign twins --query PARTS --page 0 --limit 1 >"$tmp/catalogue-query.json"

generation=$(python3 - "$tmp/catalogue-query.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['catalogue_generation'])
PY
)
BONFYRE_CMS_DB="$cms" \
BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
BONFYRE_ITEM_ARCHIVE_DB="$items" \
BONFYRE_PRICES_DB="$prices" \
BONFYRE_MATRIX_DB="$matrix" \
"$bin" foreign twins --query PARTS --page 0 --limit 1 --expect-generation "$generation" >"$tmp/catalogue-pinned.json"
BONFYRE_CMS_DB="$cms" \
BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
BONFYRE_ITEM_ARCHIVE_DB="$items" \
BONFYRE_PRICES_DB="$prices" \
BONFYRE_MATRIX_DB="$matrix" \
"$bin" foreign twins --query PARTS --page 0 --limit 1 --expect-generation "wrong-generation" >"$tmp/catalogue-mismatch.json"

python3 - "$tmp/catalogue.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d['schema'] == 'bonfyre.foreign.twins.v2'
assert d['count'] == 3
assert d['receipt_ledger']['state'] == 'separate_forensic_projection'
assert d['release_summary']['public_program_bound'] == 0
assert d['release_summary']['public_program_input_bound'] == 1
assert d['release_summary']['public_program_delivery_verified'] == 0
assert d['economic_observations'] == {
    'state':'partially_unbound', 'observations':2, 'source_identities':2,
    'bound_source_contracts':1, 'unbound_source_identities':1,
    'latest_observed_at':'2026-09-26T13:00:00Z',
    'cannot_infer':'an observed amount is not a current offer, price available to this viewer, purchase authority, payment, funding commitment, or settlement'
}
parts = next(t for t in d['twins'] if t['url'] == 'https://parts.example/catalog')
release = parts['release']
assert release['source_contract']['source'] == 'parts-catalog'
assert release['stages']['frontier'] == {'state':'current','channels':2,'admitted':1,'eligible':1}
assert release['stages']['archive'] == {'state':'current','items':3}
assert release['stages']['public_program'] == {
    'state':'input_bound','bindings':1,'programs':['parts-reading'],
    'additional_programs':0,'delivery_state':'not_verified'
}
assert release['facets']['spatial'] == {'state':'declared','extractor':'geometry.value'}
assert release['facets']['media'] == {'state':'declared','extractor':'image.url'}
assert release['facets']['rights'] == {'state':'declared','extractor':'license'}
assert release['facets']['publisher'] == {'state':'declared','identifier':'parts-example'}
assert release['readiness'] == {
    'spatial': {'state':'ready_for_extraction','blocked_by':''},
    'media': {'state':'ready_for_extraction','blocked_by':''},
    'rights': {'state':'ready_for_extraction','blocked_by':''},
    'publisher': {'state':'declared','blocked_by':''},
    'economics': {'state':'observed','observations':1,
                  'latest_observed_at':'2026-09-26T12:00:00Z','blocked_by':''},
    'public_program': {'state':'input_bound','blocked_by':'delivery_not_verified'}
}
quiet = next(t for t in d['twins'] if t['url'] == 'https://quiet.example/feed')
assert quiet['release']['stages']['frontier']['state'] == 'eligible'
assert quiet['release']['stages']['archive'] == {'state':'empty','items':0}
assert quiet['release']['facets']['spatial']['state'] == 'unbound'
assert quiet['release']['readiness']['economics'] == {
    'state':'unbound','observations':0,'latest_observed_at':'',
    'blocked_by':'no_exact_source_binding'
}
assert quiet['release']['readiness']['rights'] == {
    'state':'unbound','blocked_by':'extractor_not_declared'
}
assert quiet['release']['stages']['public_program'] == {
    'state':'unbound','bindings':0,'programs':[],
    'additional_programs':0,'delivery_state':'not_verified'
}
assert quiet['release']['readiness']['public_program'] == {
    'state':'unbound','blocked_by':'source_collection_not_bound_to_program'
}
expected_readiness = {
    'spatial_ready_for_extraction':1, 'media_ready_for_extraction':1,
    'rights_ready_for_extraction':1, 'publisher_declared':1,
    'economic_observed':1, 'public_program_input_bound':1,
    'public_program_delivery_verified':0
}
assert d['release_summary']['readiness'] == expected_readiness
assert 'does not create a spatial' in quiet['release']['cannot_infer']
long = next(t for t in d['twins'] if len(t['url']) > 1024)
assert long['url'].endswith('x' * 1800)
assert long['release']['source_contract']['source'] == 'long-catalog'
assert long['release']['stages']['frontier']['state'] == 'current'
PY

python3 - "$tmp/catalogue-page.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert (d['offset'], d['limit'], d['total'], d['count'], d['next_offset']) == (1, 1, 3, 1, 2)
# Ordered by URL, so the three rows are long.example, parts.example,
# quiet.example. This expected quiet-feed here, which stopped being true when
# the long-URL row was added to the fixture ahead of it and the expectation was
# not moved with it.
assert d['twins'][0]['release']['source_contract']['source'] == 'parts-catalog'
# Summary facts remain estate-wide even though one rich row crossed the carrier.
assert d['release_summary']['archive_current'] == 1
assert d['release_summary']['public_program_input_bound'] == 1
PY

python3 - "$tmp/catalogue-query.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert (d['query'], d['total'], d['estate_total'], d['count'], d['next_offset']) == ('PARTS', 1, 3, 1, None)
assert d['state'] == 'current' and d['catalogue_generation'].startswith('foreign-v1-')
assert d['twins'][0]['release']['source_contract']['source'] == 'parts-catalog'
# A narrow operator working set does not rewrite the estate's actual closure.
assert d['release_summary']['source_contracts'] == 3
assert d['release_summary']['frontier_current'] == 2
assert d['release_summary']['archive_current'] == 1
PY

python3 - "$tmp/catalogue-mismatch.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d['state'] == 'snapshot_mismatch'
assert d['expected_generation'] == 'wrong-generation'
assert d['catalogue_generation'].startswith('foreign-v1-')
assert 'cannot safely continue' in d['cannot_infer']
PY

python3 - "$tmp/catalogue-pinned.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d['state'] == 'current' and d['count'] == 1
PY

printf '%s\n' 'foreign release projection: pass'
