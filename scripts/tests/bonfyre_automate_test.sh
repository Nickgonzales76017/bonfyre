#!/bin/sh
# The standing automation runs every step even when one fails, records each
# step's outcome, keeps the words chart current without re-encoding, and never
# writes a credential value into its run record.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
organs="cmd/BonfyreWebAddress/bonfyre-web-address cmd/BonfyreCMS/bonfyre-cms cmd/BonfyreLatentAtlas/bonfyre-latent-atlas cmd/BonfyreCredentials/bonfyre-credentials cmd/BonfyreSourceFrontier/bonfyre-source-frontier"
for b in $organs; do
    [ -x "$root/$b" ] || { echo "automation runner: SKIP ($b not built)"; exit 0; }
done
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-automate.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/bin" "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies" "$tmp/deltas"
for b in $organs; do ln -s "$root/$b" "$tmp/bin/"; done
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/schemas/"
cp "$root/cmd/BonfyreCMS/content-types/publication_occurrence.json" "$tmp/schemas/"
"$root/cmd/BonfyreCMS/bonfyre-cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
bun "$root/cmd/BonfyreWebAddress/tools/apply_census_sources.mjs" --cms "$root/cmd/BonfyreCMS/bonfyre-cms" --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
body() {
  # The digest is written by the fixture's observed-body owner, not inferred
  # from a timestamp. An unchanged observation can skip redundant promotion.
  cat >"$tmp/foreign/bodies/$1.body"
  digest=$(shasum -a 256 "$tmp/foreign/bodies/$1.body" | awk '{print $1}')
  printf '{"body_path":"%s/bodies/%s.body","content_sha256":"%s"}\n' "$tmp/foreign" "$1" "$digest" >"$tmp/foreign/receipts/$1.json"
}
body wikidata-places-dfw <<'JSON'
{"head":{"vars":[]},"results":{"bindings":[
 {"place":{"value":"http://www.wikidata.org/entity/Q16557"},"name":{"value":"Dallas"},"about":{"value":"city in Texas"},"geometry":{"value":"POINT(-96.8 32.78)"},"page":{"value":"https://en.wikipedia.org/wiki/Dallas"}},
 {"place":{"value":"http://www.wikidata.org/entity/Q16558"},"name":{"value":"Fort Worth"},"about":{"value":"city in Texas"},"geometry":{"value":"POINT(-97.33 32.75)"},"page":{"value":"https://en.wikipedia.org/wiki/Fort_Worth,_Texas"}}
]}}
JSON
body podcasting-2-0 <<'XML'
<?xml version="1.0"?><rss><channel><title>Podcasting 2.0</title>
<item><title>Episode 271: Live from Fort Worth</title><link>https://example.test/271</link><pubDate>Fri, 11 Sep 2026 20:00:00 GMT</pubDate><description>Recorded in front of an audience.</description></item>
</channel></rss>
XML
# The CUE census also declares Buzzcast RSS. Give every initial published
# source exact retained evidence, so a clean generation really is complete.
body buzzcast <<'RSS'
<?xml version="1.0"?><rss><channel><item><title>Buzzcast Episode: Independent Podcasting</title><link>https://example.test/buzzcast</link><pubDate>Fri, 11 Sep 2026 18:00:00 GMT</pubDate></item></channel></rss>
RSS
# One native publication generation over two different publisher families.
"$root/cmd/BonfyreCMS/bonfyre-cms" entry create internet_source '{"family":"news","source":"newsroom-twin","label":"Fixture newsroom","says":"public stories","url":"https://news.example.test/rss","twin":"newsroom-twin","state":"observed","extract":"rss:item","status":"published"}' --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
"$root/cmd/BonfyreCMS/bonfyre-cms" entry create internet_source '{"family":"podcast","source":"podcast-twin","label":"Fixture podcast","says":"public episodes","url":"https://podcast.example.test/rss","twin":"podcast-twin","state":"observed","extract":"rss:item","status":"published"}' --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
body newsroom-twin <<'RSS'
<?xml version="1.0"?><rss><channel><item><title>City funding announced</title><link>https://news.example.test/item1</link><pubDate>Fri, 11 Sep 2026 10:00:00 GMT</pubDate></item></channel></rss>
RSS
body podcast-twin <<'RSS'
<?xml version="1.0"?><rss><channel><item><title>Episode on open data</title><link>https://podcast.example.test/episode1</link><pubDate>Fri, 11 Sep 2026 11:00:00 GMT</pubDate></item></channel></rss>
RSS
cat >"$tmp/providers.json" <<'JSON'
[{"provider":"webhooks","name":"Webhooks","purpose":"signs outgoing webhooks","get_it":"","terms":"","fields":[{"field":"signing_secret","label":"Signing secret","kind":"secret","sensitive":true,"required":true,"generator":"random-hex-32","help":""}],"apply":{"headers":[],"query":[]},"verify":{},"tells_provider":""}]
JSON
printf '%s' "0123456789abcdef0123456789abcdef" >"$tmp/loose.key"; chmod 644 "$tmp/loose.key"

runner() { BONFYRE_FACT_DELTAS="${BONFYRE_TEST_DELTA_SINK-$tmp/deltas}" bun "$root/scripts/bonfyre_automate.mjs" --root "$root" --state "$tmp/state" --cms "$tmp/cms.db" --cms-schemas "$tmp/schemas" \
    --foreign "$tmp/foreign" --atlas-db "$tmp/atlas.db" --credentials-db "$tmp/credentials.db" --providers "$tmp/providers.json" \
    --bin "$tmp/bin" --acoustic-per-run 0 --frontier-db "$tmp/frontier.db" --archive-db "$tmp/items.db" \
    --only publication-material,civic-index,census-items,archive,neighbourhood,atlas,link,acoustic,contributions,frontier-state,credentials "$@"; }

set +e
runner --credentials-key "$tmp/loose.key" >"$tmp/first.out" 2>&1; first=$?
"$root/cmd/BonfyreCredentials/bonfyre-credentials" init-key --key "$tmp/private.key" >/dev/null 2>&1
runner --credentials-key "$tmp/private.key" >"$tmp/second.out" 2>&1; second=$?
set -e
bun "$root/scripts/tests/bonfyre_automate_verify.mjs" "$tmp" "$first" "$second"

# Admit a third source declaration without retained evidence. The current
# generation must remain retryable; a partial native promotion is never a
# cache-hit despite other newsroom/podcast sources having already succeeded.
"$root/cmd/BonfyreCMS/bonfyre-cms" entry create internet_source '{"family":"public-records","source":"civic-bulletin","label":"Public grants bulletin","says":"published civic grant notices","url":"https://records.example.test/rss","twin":"civic-bulletin","state":"observed","extract":"rss:item","status":"published"}' --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
set +e
runner --only publication-material >"$tmp/held-first.out" 2>&1; held_first=$?
runner --only publication-material >"$tmp/held-second.out" 2>&1; held_second=$?
set -e
# Now the exact published source emits a retained receipt and body. It is
# admitted once, without any CMS manual row creation or provider crawl.
body civic-bulletin <<'RSS'
<?xml version="1.0"?><rss><channel><item><title>Public street grant notice</title><link>https://records.example.test/grants/1</link><pubDate>Wed, 14 Oct 2026 10:00:00 GMT</pubDate></item></channel></rss>
RSS
set +e
runner --only publication-material >"$tmp/recovered.out" 2>&1; recovered=$?
runner --only publication-material >"$tmp/replayed.out" 2>&1; replayed=$?
set -e
bun "$root/scripts/tests/bonfyre_automate_verify.mjs" "$tmp" "$held_first" "$held_second" "$recovered" "$replayed" --retries

# A source may be published while delta forwarding is not configured. Its
# material is owned and visible, but the outbox remains a durable obligation.
"$root/cmd/BonfyreCMS/bonfyre-cms" entry create internet_source '{"family":"community-notices","source":"community-notices","label":"Community notices","says":"public civic participation bulletin","url":"https://community.example.test/rss","twin":"community-notices","state":"observed","extract":"rss:item","status":"published"}' --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
body community-notices <<'RSS'
<?xml version="1.0"?><rss><channel><item><title>Community garden signup</title><link>https://community.example.test/join</link><pubDate>Wed, 14 Oct 2026 12:00:00 GMT</pubDate></item></channel></rss>
RSS
set +e
BONFYRE_TEST_DELTA_SINK="" runner --only publication-material >"$tmp/offline-delta.out" 2>&1; offline_delta=$?
BONFYRE_TEST_DELTA_SINK="" runner --only publication-material >"$tmp/offline-noop.out" 2>&1; offline_noop=$?
BONFYRE_TEST_DELTA_SINK="$tmp/deltas" runner --only publication-material >"$tmp/reconnected-delta.out" 2>&1; reconnected_delta=$?
BONFYRE_TEST_DELTA_SINK="$tmp/deltas" runner --only publication-material >"$tmp/reconnected-noop.out" 2>&1; reconnected_noop=$?
set -e
bun "$root/scripts/tests/bonfyre_automate_verify.mjs" "$tmp" "$offline_delta" "$offline_noop" "$reconnected_delta" "$reconnected_noop" --sink-reconnect
