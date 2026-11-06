#!/bin/sh
set -eu
catalog=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
tmp=$(mktemp -d "${TMPDIR:-/tmp}/connection-catalog.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

duckdb "$tmp/fixture.db" <<SQL
CREATE TABLE source AS SELECT *, sha256(raw_line) AS row_sha256 FROM (VALUES
 ('0123456789abcdef0123456789abcdef','goal-a',1,'Alpha | https://example.org/a','Alpha | https://example.org/a','https://example.org/a','user-direct-20261102-connection-studio-hunt'),
 ('0123456789abcdef0123456789abcdef','goal-a',2,'Beta | http://example.org/b','Beta | http://example.org/b','http://example.org/b','user-direct-20261102-connection-studio-hunt'),
 ('fedcba9876543210fedcba9876543210','goal-b',1,'Gamma | https://example.org/c','Gamma | https://example.org/c','https://example.org/c','user-direct-20261102-connection-studio-hunt')
) t(source_goal_id,source_goal_key,original_ordinal,raw_line,source_entry,source_url_candidate,goal_source);
CREATE TABLE facets AS SELECT CASE WHEN original_ordinal=2 THEN 'Other' ELSE 'Nango' END AS catalogue,
 split_part(source_entry,' | ',1) AS raw_key,source_url_candidate,source_goal_id,source_goal_key,
 original_ordinal,row_sha256,source_entry,'unverified_source_candidate' AS status FROM source;
COPY source TO '$tmp/entries.parquet' (FORMAT PARQUET);
COPY facets TO '$tmp/facets.parquet' (FORMAT PARQUET);
COPY (SELECT * REPLACE ('wrong' AS source_entry) FROM source) TO '$tmp/bad.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM source UNION ALL SELECT * FROM source WHERE original_ordinal=1 AND source_goal_key='goal-a') TO '$tmp/duplicate.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM facets UNION ALL SELECT * REPLACE ('Other' AS catalogue) FROM facets WHERE original_ordinal=1 AND source_goal_key='goal-a') TO '$tmp/cross-ecosystem.parquet' (FORMAT PARQUET);
COPY (SELECT * REPLACE ('Impostor | https://example.org/a' AS source_entry) FROM source) TO '$tmp/impostor-entries.parquet' (FORMAT PARQUET);
COPY (SELECT * REPLACE ('Impostor | https://example.org/a' AS source_entry, 'Impostor' AS raw_key) FROM facets) TO '$tmp/impostor-facets.parquet' (FORMAT PARQUET);
SQL

"$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --limit 2 > "$tmp/page1.json"
"$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --limit 2 --offset 2 > "$tmp/page2.json"
"$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --ecosystem Other > "$tmp/http.json"
"$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --view app > "$tmp/app.json"
"$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --search aLpH > "$tmp/search.json"
"$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --search "' OR 1=1 --" > "$tmp/injection.json"
python3 - "$tmp" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
def read(name):
    return json.loads((p / name).read_text())
a, b, http = map(read, ('page1.json', 'page2.json', 'http.json'))
assert a['schema'] == 'bonfyre.connection-catalog.v1'
assert len(a['entries']) == 2 and a['next_offset'] == 2
assert len(b['entries']) == 1 and b['next_offset'] is None
assert len({(r['goal_id'], r['ordinal']) for r in a['entries'] + b['entries']}) == 3
assert all(r['safe_link'] == r['source_url'] for r in a['entries'])
assert len(http['entries']) == 1 and http['entries'][0]['safe_link'] is None
assert http['entries'][0]['status'] == 'unverified_source_candidate'
app = read('app.json')
assert len(app['entries']) == 3
assert all(set(r) == {'ecosystem', 'name', 'safe_link', 'status'} for r in app['entries'])
assert next(r for r in app['entries'] if r['name'] == 'Beta')['safe_link'] is None
assert [r['name'] for r in read('search.json')['entries']] == ['Alpha']
assert read('injection.json')['entries'] == []
PY

for entries in bad duplicate; do
  if "$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/$entries.parquet" > "$tmp/refused.json" 2>/dev/null; then
    echo "accepted $entries source" >&2; exit 1
  fi
  test ! -s "$tmp/refused.json"
done
for pair in 'cross-ecosystem entries' 'impostor-facets impostor-entries'; do
  set -- $pair
  if "$catalog" list --facets "$tmp/$1.parquet" --entries "$tmp/$2.parquet" --ecosystem Nango > "$tmp/refused.json" 2>/dev/null; then
    echo "accepted inconsistent facets $1" >&2; exit 1
  fi
  test ! -s "$tmp/refused.json"
done
for limit in 0 101 -1; do
  if "$catalog" list --facets "$tmp/facets.parquet" --entries "$tmp/entries.parquet" --limit "$limit" > "$tmp/refused.json" 2>/dev/null; then
    echo "accepted invalid limit $limit" >&2; exit 1
  fi
  test ! -s "$tmp/refused.json"
done
echo 'connection catalog tests passed'
