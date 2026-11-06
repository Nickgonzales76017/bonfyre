#!/bin/sh
# A retained feed body is content, not a catalogue row.
#
# This estate observed 500 syndication feeds, proved their carriers against the
# bytes, and kept a gigabyte of their bodies on disk. The native promoter that
# turns one of those bodies into publication items -- with the twin whose
# receipt holds it, a digest that identifies it, and the rights basis it was
# retained under -- was complete, correct, and had never run: `internet_source`
# held zero rows. Having the feeds had been mistaken for having what they said.
#
# What this pins down is the part that is easy to get wrong once it does run:
# identity, so re-promotion is idempotent; encoding, because a publisher
# declaring ISO-8859-1 put raw high bytes into a TEXT column and SQLite stored
# them without complaint; and the refusal to turn an observation into a claim.
set -eu

bin=${1:?binary required}
bin=$(CDPATH='' cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd -P)
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
db="$root/cms.db"
foreign="$root/foreign"
mkdir -p "$foreign/receipts" "$foreign/bodies" "$root/deltas"

fail() { echo "FAIL: $1"; exit 1; }

# A feed whose declaration says ISO-8859-1 and whose bytes are latin-1.
{
    printf '<?xml version="1.0" encoding="ISO-8859-1"?><rss version="2.0"><channel>'
    printf '<item><title>Palmeiras bate o S\343o Paulo</title><link>https://e.test/a</link><pubDate>Sat, 13 Sep 2026 10:00:00 +0000</pubDate></item>'
    printf '<item><title>Second story</title><link>https://e.test/b</link><pubDate>Sat, 13 Sep 2026 11:00:00 +0000</pubDate></item>'
    printf '<item><title>Europe says &quot;no time&quot; &#8211; &#x41C;&#x438;&#x440;</title><link>https://e.test/c?a=1&amp;b=2</link><pubDate>Sat, 13 Sep 2026 12:00:00 +0000</pubDate></item>'
    printf '</channel></rss>'
} > "$foreign/bodies/t1.body"
printf '{"twin_id":"t1","body_path":"%s/bodies/t1.body","body_bytes":400}\n' "$foreign" > "$foreign/receipts/t1.json"

"$bin" schema migrate --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" >/dev/null 2>&1
"$bin" entry create internet_source --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
  '{"family":"external_estate","source":"t1","label":"probe","says":"retained rss observation","url":"https://e.test/","twin":"t1","state":"observed","extract":"rss:item","status":"published"}' >/dev/null 2>&1 \
  || fail "could not register the source"

out=$(BONFYRE_FACT_DELTAS="$root/deltas" "$bin" publication promote-rss --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
      --source t1 --foreign-root "$foreign" --limit 50 2>/dev/null | grep '^{')
echo "$out" | bun -e '
import { strict as assert } from "node:assert";
const d = JSON.parse(await Bun.stdin.text());
assert.equal(d.created, 3);
assert(d.entity_decoded_fields >= 2, "entities in a title and a link were stored undecoded");
assert(d.transcoded_fields >= 1, "a latin-1 title was stored without transcoding");
' || fail "first promotion"
# A successful source write has one exact published dependency delta.
test "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 1 || fail "new publication fact delta missing"
grep -rl '"cms:publication_source:t1"' "$root/deltas" >/dev/null || fail "source-edge delta missing"

# Every stored field is valid UTF-8. Raw feed bytes in a TEXT column are how a
# reader ends up guessing what the publisher meant.
sqlite3 "$db" "select title from publication_occurrence" | bun -e '
new TextDecoder("utf-8", { fatal: true }).decode(await Bun.stdin.arrayBuffer());
' || fail "a stored title is not valid UTF-8"

sqlite3 "$db" "select title from publication_occurrence" | grep -q "São Paulo" \
  || fail "the transcoded title lost its content"

# Entities are text, not markup: the title and link are stored decoded.
sqlite3 "$db" "select title||'|'||canonical_href from publication_occurrence where canonical_href like '%e.test/c%'" \
  | bun -e '
import { strict as assert } from "node:assert";
const row = (await Bun.stdin.text()).trim();
const separator = row.indexOf("|");
assert(separator >= 0, "missing title/link separator");
assert.equal(row.slice(0, separator), "Europe says \"no time\" \u2013 \u041c\u0438\u0440");
assert.equal(row.slice(separator + 1), "https://e.test/c?a=1&b=2");
' || fail "entities were not decoded"

# Re-promotion adds nothing: an item is identified by what it is.
again=$(BONFYRE_FACT_DELTAS="$root/deltas" "$bin" publication promote-rss --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
        --source t1 --foreign-root "$foreign" --limit 50 2>/dev/null | grep '^{')
echo "$again" | bun -e '
import { strict as assert } from "node:assert";
const d = JSON.parse(await Bun.stdin.text());
assert.equal(d.created, 0, "re-promotion created duplicate items");
assert.equal(d.already_present, 3, "identity changed when entities were decoded");
' || fail "promotion is not idempotent"
# No new CMS facts means no new World/Work invalidation for this source.
test "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 1 || fail "no-op single promotion emitted FactDelta"

# An observation is not a claim, and the row has to say so itself.
sqlite3 "$db" "select assertion_state,rights_basis,cannot_infer from publication_occurrence limit 1" \
  | bun -e '
import { strict as assert } from "node:assert";
const [state, rights, ...rest] = (await Bun.stdin.text()).trim().split("|");
const cannot = rest.join("|");
assert.equal(state, "OBSERVED", "a foreign feed is not an assertion of this estate");
assert.equal(rights, "retained-public-rss-observation");
assert(cannot.includes("endorsement") && cannot.includes("effect"), cannot);
' || fail "a promoted item does not refuse the claims it must refuse"

# The batch form promotes every published source in one process. It streams one
# line per source and then a summary, so the summary is the last line.
batch=$(BONFYRE_FACT_DELTAS="$root/deltas" "$bin" publication promote-rss --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
        --all-published --foreign-root "$foreign" --limit 50 2>/dev/null | grep '^{' | tail -1)
echo "$batch" | bun -e '
import { strict as assert } from "node:assert";
const d = JSON.parse(await Bun.stdin.text());
assert.equal(d.sources, 1);
assert.equal(d.items_created, 0, "the batch form re-created items the single form already had");
' || fail "batch promotion"

# Delivery can fail AFTER canonical CMS insertion commits. The outbox must
# retain this revision even when a later promoter finds zero newly created
# rows; it is the only safe way to avoid flooding downstream on clean replays.
printf '%s\n' '<?xml version="1.0"?><rss><channel><item><title>Public grant dispatch</title><link>https://public.test/notice-1</link></item></channel></rss>' > "$foreign/bodies/t2.body"
printf '{"twin_id":"t2","body_path":"%s/bodies/t2.body"}\n' "$foreign" > "$foreign/receipts/t2.json"
"$bin" entry create internet_source --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
  '{"family":"civic-grants","source":"t2","label":"Public grants","says":"retained rss notice","url":"https://public.test/rss","twin":"t2","state":"observed","extract":"rss:item","status":"published"}' >/dev/null 2>&1 || fail "could not register second publisher"
if BONFYRE_FACT_DELTAS="$root/missing" "$bin" publication promote-rss --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
  --source t2 --foreign-root "$foreign" --limit 50 >"$root/delivery-failed.json" 2>/dev/null; then
  fail "missing delta sink succeeded after committing a source"
fi
grep -q '"fact_delta":"DELIVERY_FAILED"' "$root/delivery-failed.json" || fail "delivery failure hidden"
test "$(sqlite3 "$db" "SELECT count(*) FROM publication_occurrence WHERE source_ref='t2';")" = 1 || fail "committed source row lost after delta failure"
test "$(sqlite3 "$db" "SELECT revision FROM _publication_delta_outbox WHERE namespace='root' AND source='t2';")" = 1 || fail "post-commit delta outbox lost its source revision"
test "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 1 || fail "failure affected existing source delta"

# A replay with zero new occurrence rows heals the durable outbox.
healed=$(BONFYRE_FACT_DELTAS="$root/deltas" "$bin" publication promote-rss --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
  --source t2 --foreign-root "$foreign" --limit 50 2>/dev/null | grep '^{')
echo "$healed" | grep -q '"created":0' || fail "outbox healing duplicated source material"
echo "$healed" | grep -q '"fact_delta":"DELIVERED"' || fail "pending dependency was not delivered"
test "$(sqlite3 "$db" "SELECT count(*) FROM _publication_delta_outbox WHERE namespace='root' AND source='t2';")" = 0 || fail "delivered revision was not acknowledged"
test "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 2 || fail "retry failed to emit exactly one new source delta"
grep -rl '"cms:publication_source:t2"' "$root/deltas" >/dev/null || fail "recovered fact delta lost its source coordinate"

# All-published replay is now an actual zero-work change-notice generation.
quiet=$(BONFYRE_FACT_DELTAS="$root/deltas" "$bin" publication promote-rss --db "$db" --schemas "$repo/cmd/BonfyreCMS/content-types" \
  --all-published --foreign-root "$foreign" --limit 50 2>/dev/null | grep '^{' | tail -1)
echo "$quiet" | grep -q '"items_created":0' || fail "all-published recovery replay duplicated material"
test "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 2 || fail "no-op all-published replay emitted spurious delta"

echo "PASS publication: items promoted, latin-1 transcoded, identity idempotent, observation not claimed, batch form agrees"
