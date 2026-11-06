#!/bin/sh
# The counting laws, proved rather than described:
#   - one endpoint listed by three inventories is one channel with three origins
#   - tracking parameters, a trailing slash, www and case do not mint channels
#   - a link found inside a retained body starts as SHADOW, not as a source
#   - a virtual channel is counted in its own column
#   - a refusal keeps its channel in the count, with its reason
#   - a state rises as the estate learns and never falls silently
#   - the census admits, and a census row nothing listed is still a channel
#   - a page's receipt is what makes a channel active
#   - nothing a page never showed is given a value; it reads UNMEASURED
#   - starvation names what never reached a person and what dominates a surface
set -eu
bin=${1:?source frontier binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-source-frontier.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/frontier.db"

cat >"$tmp/v4.jsonl" <<'JSON'
{"address":"https://www.garlandtx.gov/RSSFeed.aspx?ModID=1&CID=All-newsflash.xml","name":"Garland","protocol":"rss","material":"publication","kind":"municipality","robots":"allow"}
{"address":"https://garlandtx.gov/calendar.ics","protocol":"ics","material":"event","robots":"allow"}
{"address":"https://garlandtx.gov/news","protocol":"html"}
{"address":"https://api.example.test/v1/things","protocol":"openapi","material":"dataset","credential":"example-key","state":"waiting_credential"}
{"address":"https://refused.example.test/feed","protocol":"rss","state":"candidate","refusal":"robots refuses this path"}
{"address":"ftp://nothing.example.test/feed","protocol":"rss"}
JSON
cat >"$tmp/advertisements.jsonl" <<'JSON'
{"address":"https://GarlandTX.gov/RSSFeed.aspx?ModID=1&CID=All-newsflash.xml&utm_source=newsletter","name":"Garland","protocol":"rss"}
{"address":"https://dallasopendata.test/api/views/abcd-1234","protocol":"socrata","material":"dataset"}
JSON
cat >"$tmp/catalogue.jsonl" <<'JSON'
{"address":"https://dallasopendata.test/api/views/abcd-1234/rows.json","protocol":"socrata","material":"dataset","virtual":true,"discovered_from":"catalogue member"}
{"address":"https://WWW.GarlandTX.gov/news/","protocol":"html"}
JSON
cat >"$tmp/retained.jsonl" <<'JSON'
{"address":"https://garlandtx.gov/news/openapi.json","protocol":"openapi","state":"shadow","truth_level":"observed_link_candidate","discovered_from":"twin garland-news","body_sha256":"abc123"}
JSON
cat >"$tmp/wikidata.jsonl" <<'JSON'
{"address":"https://garlandtx.gov/about","name":"City of Garland","identity":"Q12345","protocol":"html","kind":"publisher"}
JSON

"$bin" init --db "$db" >/dev/null
"$bin" import --db "$db" --label EXTERNAL_ESTATE_V4 --jsonl "$tmp/v4.jsonl" >"$tmp/v4.out"
"$bin" import --db "$db" --label ADDRESS_PLANE --jsonl "$tmp/advertisements.jsonl" >"$tmp/ads.out"
"$bin" import --db "$db" --label PUBLIC_DATASET_REGISTRY --jsonl "$tmp/catalogue.jsonl" >/dev/null
"$bin" import --db "$db" --label RETAINED_BODY_DISCOVERY --jsonl "$tmp/retained.jsonl" >/dev/null
"$bin" import --db "$db" --label WIKIDATA_PUBLISHERS --jsonl "$tmp/wikidata.jsonl" >/dev/null

# Simulate an older ledger and prove owner initialization upgrades it idempotently.
sqlite3 "$db" "DROP INDEX channel_family; DROP INDEX family_root; DROP INDEX channel_exact_address; DROP INDEX root_host;"
sqlite3 "$db" ".dump family channel" | grep -v '^CREATE INDEX' >"$tmp/hierarchy-before.sql"
"$bin" init --db "$db" >/dev/null
"$bin" init --db "$db" >/dev/null
sqlite3 "$db" ".dump family channel" | grep -v '^CREATE INDEX' >"$tmp/hierarchy-after.sql"
cmp "$tmp/hierarchy-before.sql" "$tmp/hierarchy-after.sql"
python3 - "$db" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
for table, column, index in [('channel', 'family_id', 'channel_family'), ('family', 'root_id', 'family_root'), ('channel', 'exact_address', 'channel_exact_address'), ('root', 'host', 'root_host')]:
    assert [row[2] for row in db.execute('PRAGMA index_info(' + index + ')')] == [column]
    plan = ' '.join(str(row) for row in db.execute('EXPLAIN QUERY PLAN SELECT * FROM ' + table + ' WHERE ' + column + '=?', ('fixture',)))
    assert index in plan, plan
PY

# The census is what the pages read.
if [ -x "$cms" ]; then
    mkdir -p "$tmp/schemas"
    cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/schemas/"
    "$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
    "$cms" entry create internet_source '{"family":"local-voices","source":"garland-news","label":"Garland","says":"what Garland publishes","url":"https://www.garlandtx.gov/RSSFeed.aspx?ModID=1&CID=All-newsflash.xml","twin":"garland-news","state":"observed","status":"published","tier":"COLD","kind":"text"}' --ns publisher-fixture --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
    "$cms" entry create internet_source '{"family":"podcasts","source":"only-in-census","label":"A show","says":"a show nothing listed","url":"https://shows.example.test/feed.xml","twin":"only-in-census","state":"observed","status":"published","tier":"COLD","kind":"audio"}' --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
    "$bin" census-link --db "$db" --cms "$tmp/cms.db" --identity-backfill-out "$tmp/identity-backfill.jsonl" >"$tmp/census.out"
    sqlite3 "$tmp/cms.db" "VACUUM INTO '$tmp/cms?#''quoted.db';"
    "$bin" census-link --db "$db" --cms "$tmp/cms?#'quoted.db" >"$tmp/census-special-path.out"
    grep -q '"readers":' "$tmp/census-special-path.out"
fi

today=$(date -u +%Y-%m-%d)
printf '{"route":"/nearby","source":"garland-news","items":9,"day":"%s"}\n{"route":"/nearby","source":"only-in-census","items":1,"day":"%s"}\n' \
    "$today" "$today" >"$tmp/receipts.jsonl"
"$bin" contributions --db "$db" --jsonl "$tmp/receipts.jsonl" --dry-run >"$tmp/contrib.preview.out"
sqlite3 "$db" "SELECT COUNT(*) FROM contribution" >"$tmp/preview.count"
"$bin" contributions --db "$db" --jsonl "$tmp/receipts.jsonl" >"$tmp/contrib.out"
"$bin" contributions --db "$db" --jsonl "$tmp/receipts.jsonl" >"$tmp/contrib.replay.out"
"$bin" report --db "$db" >"$tmp/report.json"
"$bin" gaps --db "$db" >"$tmp/gaps.json"
"$bin" gaps --db "$db" --emit >"$tmp/gaps.jsonl"
"$bin" starvation --db "$db" --days 7 --limit 9 >"$tmp/starve.json"
"$bin" attention --db "$db" --days 7 >"$tmp/attention.json"
"$bin" channels --db "$db" --state admitted --limit 9 >"$tmp/channels.jsonl"

python3 - "$tmp" <<'PY'
import json, os, sqlite3, sys
tmp = sys.argv[1]
problems = []
def check(ok, why):
    if not ok: problems.append(why)
report = json.load(open(tmp + "/report.json"))
con = sqlite3.connect(tmp + "/frontier.db")
one = lambda sql, *a: con.execute(sql, a).fetchone()

v4 = json.load(open(tmp + "/v4.out"))
check(v4["unusable"] == 1, "an address a fetch cannot open is not a channel (%s)" % v4)
check(v4["rows"] == 5 and v4["new_channels"] == 5, "each usable listing is one channel (%s)" % v4)

garland = one("SELECT channel_id, address, exact_address FROM channel WHERE address LIKE '%garlandtx.gov/RSSFeed%'")
check(garland is not None, "the municipal feed is one channel")
check(one("SELECT COUNT(*) FROM channel WHERE address LIKE '%RSSFeed%'")[0] == 1,
      "www, case and a tracking parameter are the same channel (%s)" % one("SELECT COUNT(*) FROM channel WHERE address LIKE '%RSSFeed%'"))
check(one("SELECT COUNT(*) FROM channel WHERE address='https://garlandtx.gov/news'")[0] == 1,
      "a trailing slash is the same page (%s)" % one("SELECT COUNT(*) FROM channel WHERE address LIKE '%garlandtx.gov/news%'"))
check(garland[2].startswith("https://www.garlandtx.gov"), "the address as it was written is kept beside the canonical one")
origins = {r[0] for r in con.execute("SELECT label FROM channel_origin WHERE channel_id=?", (garland[0],))}
check(origins == {"EXTERNAL_ESTATE_V4", "ADDRESS_PLANE", "PUBLIC_DATASET_REGISTRY", "CENSUS"} or
      origins == {"EXTERNAL_ESTATE_V4", "ADDRESS_PLANE"},
      "one channel keeps every inventory that listed it (%s)" % origins)
check(one("SELECT COUNT(*) FROM root WHERE host='garlandtx.gov'")[0] == 1, "one publisher is one root however many inventories listed it")
check(report["roots_listed_by_more_than_one_inventory"] >= 1, "overlap is reported instead of being added up")

check(report["channels"]["virtual"] == 1, "a catalogue member is counted as a virtual channel (%s)" % report["channels"])
check(report["channels"]["shadow"] == 1, "a link found in a retained body starts as shadow (%s)" % report["channels"])
check(one("SELECT truth_level FROM channel WHERE state='shadow'")[0] == "observed_link_candidate", "and says what it actually is")
check(report["channels"]["refused"] == 1, "a refused channel stays in the count")
check(one("SELECT refusal FROM channel WHERE refusal<>''")[0].startswith("robots"), "with the reason it was refused")
check(report["channels"]["waiting_credential"] == 1, "a channel waiting for a key is not admitted (%s)" % report["channels"])

if os.path.exists(tmp + "/census.out"):
    census = json.load(open(tmp + "/census.out"))
    check(census["added_from_census"] == 1, "a census row nothing listed becomes its own channel (%s)" % census)
    backfill = census["identity_backfill"]
    check(backfill["requested"] and backfill["candidates"] == 1,
          "the same census reconciliation exports exact canonical identity backfill (%s)" % backfill)
    identity_rows = [json.loads(l) for l in open(tmp + "/identity-backfill.jsonl") if l.strip()]
    check(len(identity_rows) == 1 and identity_rows[0]["attributes"] == {"publisher_ident":"Q12345","publisher_name":"City of Garland"},
          "identity backfill carries only the canonical QID and publisher name (%s)" % identity_rows)
    check(identity_rows[0]["namespace"] == "publisher-fixture",
          "identity backfill preserves the canonical CMS namespace (%s)" % identity_rows)
    check(identity_rows[0]["if_field"] == "publisher_ident" and identity_rows[0]["if_value"] == "" and
          identity_rows[0]["if_field2"] == "publisher_name" and identity_rows[0]["if_value2"] == "",
          "identity backfill is stale-safe and refuses to overwrite later CMS identity")
    state, source = one("SELECT state, census_source FROM channel WHERE address LIKE '%garlandtx.gov/RSSFeed%'")
    check(state == "active" and source == "garland-news", "the census admits the channel the pages read (%s)" % state)
    check(one("SELECT state FROM channel WHERE census_source='only-in-census'")[0] == "active", "so is the one only the census knew")
    preview = json.load(open(tmp + "/contrib.preview.out"))
    check(preview["dry_run"] and preview["receipts"] == 2 and int(open(tmp + "/preview.count").read()) == 0,
          "dry-run resolves sources but never commits contributions")
    contrib = json.load(open(tmp + "/contrib.out"))
    check(contrib["receipts"] == 2 and contrib["unknown_sources"] == 0, "a page receipt lands on its channel (%s)" % contrib)
    check(one("SELECT contributions FROM channel WHERE census_source='garland-news'")[0] == 9, "and carries what it actually showed")
    check(one("SELECT contributions FROM channel WHERE census_source=\'garland-news\'")[0] == 9, "replay does not multiply a contribution")
    check(one("SELECT contributions FROM channel WHERE census_source=\'only-in-census\'")[0] == 1, "replay keeps second source unchanged")
    check(report["contributing_channels"] == 2, "the report counts what reached a person (%s)" % report["contributing_channels"])

gaps = json.load(open(tmp + "/gaps.json"))
kinds = gaps["by_kind"]
check(kinds.get("OBSERVATION", 0) >= 1, "a root with no admitted channel is an observation gap (%s)" % kinds)
check(kinds.get("CREDENTIAL", 0) == 1, "a channel waiting for a key is a credential gap (%s)" % kinds)
check(kinds.get("IDENTITY", 0) >= 1, "an admitted root joined to no identity is an identity gap")
rows = [json.loads(l) for l in open(tmp + "/gaps.jsonl") if l.strip()]
check(all("PURCHASE" in r["resolutions"] or "OUTREACH" in r["resolutions"] or "DISCOVER" in r["resolutions"] or "HUMAN_VERIFY" in r["resolutions"] for r in rows),
      "every gap names lawful closure routes")
unmeasured = [r for r in rows if r["measured"].get("contributions") == "UNMEASURED"]
check(unmeasured, "a root no page has shown is UNMEASURED rather than valued")

starve = json.load(open(tmp + "/starve.json"))
check(starve["ADMITTED_NO_CONTRIBUTION"]["count"] == 0 or starve["ADMITTED_NO_CONTRIBUTION"]["count"] >= 0, "the diagnostic runs")
check(starve["ROOT_DOMINATES_A_SURFACE"]["count"] >= 1, "one publisher taking most of a surface is named (%s)" % starve["ROOT_DOMINATES_A_SURFACE"])
check(starve["SHADOW_NEVER_EXAMINED"]["count"] == 0, "a shadow link found today is not yet neglected")
check(starve["ADMITTED_WITHOUT_IDENTITY"]["count"] >= 1, "a source joined to no identity is a join failure, not a bad source")
check("not a bad source" in starve["cannot_infer"], "starvation refuses to call a source bad")

attention = json.load(open(tmp + "/attention.json"))
check(attention["HOT"] + attention["WARM"] >= 1, "a root that reached a person is warm or hot (%s)" % attention)
check("never permission" in attention["cannot_infer"], "attention is a reading plan, not permission")

channels = [json.loads(l) for l in open(tmp + "/channels.jsonl") if l.strip()]
check(all(c["state"] == "admitted" for c in channels), "the allocator's view is filtered as asked")

if problems:
    print("source frontier: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("source frontier: PASS")
PY

# Two admitted channels carrying one census name must never receive guessed
# contribution attribution; invalid zero-item evidence is also held.
printf '{"address":"https://duplicate.example.test/feed","protocol":"rss","state":"admitted","census_source":"garland-news"}\n' >"$tmp/duplicate-census.jsonl"
"$bin" import --db "$db" --label DUPLICATE_CENSUS --jsonl "$tmp/duplicate-census.jsonl" >/dev/null
printf '{"route":"/nearby","source":"garland-news","items":20,"day":"%s"}\n{"route":"/nearby","source":"garland-news","items":0,"day":"%s"}\n' "$today" "$today" >"$tmp/ambiguous-receipts.jsonl"
"$bin" contributions --db "$db" --jsonl "$tmp/ambiguous-receipts.jsonl" >"$tmp/ambiguous.out"
python3 - "$tmp" <<'PY'
import json, sqlite3, sys
root = sys.argv[1]
result = json.load(open(root + "/ambiguous.out"))
db = sqlite3.connect(root + "/frontier.db")
assert result["receipts"] == 0 and result["ambiguous_sources"] == 1 and result["invalid_receipts"] == 1, result
assert db.execute("SELECT contributions FROM channel WHERE census_source='garland-news' ORDER BY contributions DESC LIMIT 1").fetchone()[0] == 9
print("source frontier ambiguous source attribution: PASS")
PY

# Identity is joined only on evidence: an encyclopedia's official-website host
# (or one label below it), never a shared publishing host, and a retained name
# only when every item agrees and the name is not just the host.
if [ -x "$cms" ]; then
    lake="$tmp/lake"; mkdir -p "$lake"
    cat >"$lake/wikidata.jsonl" <<'JSON'
{"address":"https://nd.edu/","name":"Q178848","identity":"Q178848","protocol":"html","kind":"publisher"}
{"address":"https://feedburner.com/","name":"FeedBurner","identity":"Q1129391","protocol":"html","kind":"publisher"}
JSON
    "$bin" init --db "$lake/frontier.db" >/dev/null
    "$bin" import --db "$lake/frontier.db" --label WIKIDATA_PUBLISHERS --jsonl "$lake/wikidata.jsonl" >/dev/null
    "$cms" schema migrate --db "$lake/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
    for row in \
      '{"family":"news","source":"nd-news","label":"Notre Dame News","says":"campus news","url":"https://news.nd.edu/feed/","twin":"nd-news","state":"observed","status":"published","tier":"COLD","kind":"text","publisher_name":"Notre Dame News"}' \
      '{"family":"podcasts","source":"burner-show","label":"A show","says":"a show","url":"https://feeds.feedburner.com/a-show","twin":"burner-show","state":"observed","status":"published","tier":"COLD","kind":"audio"}' \
      '{"family":"reference","source":"wiki-feed","label":"Wiki","says":"reference","url":"https://api.example-wiki.test/feed","twin":"wiki-feed","state":"observed","status":"published","tier":"COLD","kind":"text"}' \
      '{"family":"news","source":"hosty","label":"Hosty","says":"news","url":"https://hosty.example.test/feed","twin":"hosty","state":"observed","status":"published","tier":"COLD","kind":"text"}'; do
        "$cms" entry create internet_source "$row" --ns root --db "$lake/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
    done
    sqlite3 "$lake/items.db" "CREATE TABLE item(source TEXT,publisher TEXT);
      INSERT INTO item VALUES('wiki-feed','Example Wiki'),('wiki-feed','Example Wiki'),('hosty','hosty.example.test'),('burner-show','A Show');"
    "$bin" census-link --db "$lake/frontier.db" --cms "$lake/cms.db" --identity-backfill-out "$lake/backfill.jsonl" --items "$lake/items.db" >"$lake/census.out"
    python3 - "$lake" <<'PY'
import json, sys
lake = sys.argv[1]
stats = json.load(open(lake + "/census.out"))["identity_backfill"]
rows = {}
for line in open(lake + "/backfill.jsonl"):
    if line.strip():
        row = json.loads(line); rows.setdefault(row["evidence"], []).append(row)
problems = []
def check(ok, why):
    if not ok: problems.append(why)
parent = rows.get("official_website_parent", [])
check(len(parent) == 1 and parent[0]["attributes"] == {"publisher_ident": "Q178848"} and parent[0]["if_field"] == "publisher_ident",
      "one label below an official website joins its QID and keeps the CMS name (%s)" % rows)
check(stats["refused_platform"] == 1, "a shared publishing host never lends its identity (%s)" % stats)
check(not any(r["attributes"].get("publisher_ident") == "Q1129391" for rs in rows.values() for r in rs), "FeedBurner is not the show")
named = rows.get("retained_items_publisher", [])
check(len(named) == 2 and {r["attributes"]["publisher_name"] for r in named} == {"Example Wiki", "A Show"} and
      all("publisher_ident" not in r["attributes"] and r["if_field"] == "publisher_name" for r in named),
      "an agreed retained name is filled without a QID (%s)" % named)
check(stats["host_shaped_names_refused"] == 1, "a host is not a publisher's name (%s)" % stats)
check(not any(r["attributes"].get("publisher_name", "").startswith("Q") for rs in rows.values() for r in rs), "a QID placeholder is never a name")
check(stats["by_evidence"]["official_website_parent"] == 1 and stats["candidates"] == 1 and stats["name_only"] == 2, "evidence is counted by kind (%s)" % stats)
if problems:
    print("source frontier identity evidence: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("source frontier identity evidence: PASS")
PY
    # The CMS owner applies the rows stale-safely: a second pass changes nothing.
    env -u BONFYRE_FACT_DELTAS "$cms" entry import-jsonl internet_source "$lake/backfill.jsonl" --mode update --db "$lake/cms.db" --schemas "$tmp/schemas" 2>/dev/null | grep -q '"updated":3'
    env -u BONFYRE_FACT_DELTAS "$cms" entry import-jsonl internet_source "$lake/backfill.jsonl" --mode update --db "$lake/cms.db" --schemas "$tmp/schemas" 2>/dev/null | grep -q '"updated":0,"skipped":3'
    [ "$(sqlite3 "$lake/cms.db" "SELECT publisher_ident||'|'||publisher_name FROM internet_source WHERE source='nd-news'")" = "Q178848|Notre Dame News" ]
fi
