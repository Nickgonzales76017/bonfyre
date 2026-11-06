#!/bin/sh
# Receipts packed into one compressed store say exactly what the loose files
# said: every packed receipt is byte-identical to its file, every question the
# owner answers comes out the same from the pack alone, a loose receipt written
# after packing is still seen and wins over a packed one of its name, and a
# damaged pack is reported rather than read around.
set -eu
bin=${1:-./bonfyre-project}
[ -x "$bin" ] || { echo "SKIP: $bin not built"; exit 0; }
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT INT TERM
fail() { echo "FAIL: $1" >&2; exit 1; }
"$bin" foreign pack "$tmp" "$tmp/none.pack" >/dev/null 2>&1 || true
if "$bin" foreign pack /nonexistent "$tmp/x.pack" 2>&1 | grep -q "carries no zstd"; then echo "SKIP: built without zstd"; exit 0; fi

loose="$tmp/loose"; mkdir -p "$loose" "$tmp/packed" "$tmp/mixed" "$tmp/bad"
i=0
while [ $i -lt 300 ]; do
  host="site$((i % 40)).example"; day=$(printf '%02d' $((1 + i % 27)))
  kind=source_observed; sha=$(printf '%064d' $((i % 7))); [ $((i % 11)) -eq 0 ] && { kind=source_unreachable; sha=null; }
  printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"t-%s","source_url":"https://%s/feed","observed_at":"2026-09-%sT0%d:00:00Z","observe_right":"public","robots_transport_exit":0,"robots_http_status":200,"policy_decision":"allow","fetch_attempted":true,"http_status":200,"observation_kind":"%s","content_sha256":"%s","body_path":"/nowhere/%s.body"}\n' \
    "$i" "$host" "$day" $((i % 10)) "$kind" "$sha" "$i" > "$loose/obs-$(printf '%04d' $i).json"
  i=$((i + 1))
done
# One receipt much longer than the eight kilobytes a reader looks at.
{ printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"https://long.example/","observed_at":"2026-09-01T00:00:00Z","observation_kind":"source_observed","policy_decision":"allow","content_sha256":"%064d","http_status":200,"padding":"' 9; head -c 30000 /dev/zero | tr '\0' 'x'; printf '"}\n'; } > "$loose/long.json"

receipt=$("$bin" foreign pack "$loose" "$tmp/packed/.receipts.pack")
echo "$receipt" | grep -q '"receipts":301' || fail "the pack did not take every receipt: $receipt"
echo "$receipt" | grep -q '"loose_files_changed":false' || fail "packing claims to have changed the loose files"
"$bin" foreign pack-verify "$loose" "$tmp/packed/.receipts.pack" | grep -q '"identical":301,"different":0,"packed_without_a_loose_file":0,"loose_not_packed":0' \
  || fail "a packed receipt differs from its loose file"
raw=$(cat "$loose"/* | wc -c | tr -d ' '); packed=$(wc -c < "$tmp/packed/.receipts.pack" | tr -d ' ')
# The receipts themselves shrink many times over; the file also carries the
# dictionary and whole database pages, which dominate an estate this small.
compressed=$(echo "$receipt" | sed -n 's/.*"compressed_bytes":\([0-9]*\).*/\1/p')
[ "$compressed" -lt $((raw / 5)) ] || fail "receipts did not compress ($compressed of $raw bytes)"
[ "$packed" -lt "$raw" ] || fail "the pack file is larger than the loose text ($packed of $raw bytes)"

# The owner's answers, from the loose directory and from the pack alone.
for question in conflicts admissible hydration "history https://site3.example/feed" "resolve https://site3.example/feed" "resolve https://long.example/" "twins --receipts"; do
  a=$(BONFYRE_FOREIGN_RECEIPTS="$loose" "$bin" foreign $question 2>&1 || true)
  b=$(BONFYRE_FOREIGN_RECEIPTS="$tmp/packed" "$bin" foreign $question 2>&1 || true)
  [ -n "$a" ] && [ "$a" = "$b" ] || fail "'foreign $question' answers differently from the pack"
done

# After packing: a new loose receipt is seen, and a loose file of a packed name wins.
cp "$tmp/packed/.receipts.pack" "$tmp/mixed/"
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"https://later.example/","observed_at":"2026-10-03T00:00:00Z","observation_kind":"source_observed","policy_decision":"allow","content_sha256":"%064d","http_status":200}\n' 5 > "$tmp/mixed/later.json"
BONFYRE_FOREIGN_RECEIPTS="$tmp/mixed" "$bin" foreign history https://later.example/ | grep -q '"observations":1' || fail "a receipt written after packing is not seen"
sed 's/"http_status":200/"http_status":451/' "$loose/long.json" > "$tmp/mixed/long.json"
BONFYRE_FOREIGN_RECEIPTS="$tmp/mixed" "$bin" foreign history https://long.example/ | grep -q '"observations":1.*"http_status":451' || fail "a loose receipt did not win over the packed one of its name"
"$bin" foreign pack-verify "$tmp/mixed" "$tmp/mixed/.receipts.pack" >/dev/null && fail "verify passed although a loose file differs from its packed copy"

# Retiring loose files: only those whose bytes are still the packed bytes go.
retire="$tmp/retire"; mkdir -p "$retire"; cp "$loose"/* "$retire/"; cp "$tmp/packed/.receipts.pack" "$retire/"
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"https://site3.example/feed","observed_at":"2026-10-04T00:00:00Z","observation_kind":"source_observed","policy_decision":"allow","content_sha256":"%064d","http_status":200}\n' 8 >> "$retire/obs-0003.json"
before=$(BONFYRE_FOREIGN_RECEIPTS="$retire" "$bin" foreign conflicts)
"$bin" foreign pack-retire "$retire" "$retire/.receipts.pack" | grep -q '"loose_removed":300,.*"kept_because_changed_since_packing":1,"already_absent":0' || fail "retire did not remove exactly the unchanged files"
[ "$(ls "$retire" | wc -l | tr -d ' ')" = 1 ] && [ -f "$retire/obs-0003.json" ] || fail "the receipt that grew after packing was not the one file left"
[ "$(BONFYRE_FOREIGN_RECEIPTS="$retire" "$bin" foreign conflicts)" = "$before" ] || fail "answers changed after the loose files were retired"
"$bin" foreign receipt "$retire" obs-0007.json | cmp -s - "$loose/obs-0007.json" || fail "a retired receipt does not read back from the pack"
"$bin" foreign receipt "$retire" obs-0003.json | cmp -s - "$retire/obs-0003.json" || fail "the grown receipt is not read from its loose file"
"$bin" foreign pack-retire "$retire" "$retire/.receipts.pack" | grep -q '"loose_removed":0,.*"already_absent":300' || fail "retiring twice removed something"

# Damage is said, not skipped: one byte changed inside one packed receipt.
cp "$tmp/packed/.receipts.pack" "$tmp/bad/.receipts.pack"
sqlite3 "$tmp/bad/.receipts.pack" "UPDATE receipt SET z = substr(z, 1, length(z) - 6) || x'5A' || substr(z, length(z) - 4) WHERE name = 'obs-0005.json'"
"$bin" foreign pack-verify "$loose" "$tmp/bad/.receipts.pack" >/dev/null 2>&1 && fail "a damaged pack verified clean"
BONFYRE_FOREIGN_RECEIPTS="$tmp/bad" "$bin" foreign conflicts 2>&1 | grep -q "unreadable or damaged" || fail "a damaged pack was read without saying so"
# Bodies: stored once per distinct content, read back exactly, damage refused.
bodies="$tmp/bodies"; mkdir -p "$bodies"
i=0; while [ $i -lt 40 ]; do { printf '<html><title>page %s</title><body>' $((i % 30)); head -c $((2000 + (i % 30) * 137)) /dev/zero | tr '\0' 'a'; printf '</body></html>'; } > "$bodies/page-$(printf '%02d' $i).body"; i=$((i + 1)); done
head -c 50000 /dev/urandom > "$bodies/binary.bin"; : > "$bodies/empty.body"
made=$("$bin" foreign pack-bodies "$bodies" "$tmp/bodies.pack")
echo "$made" | grep -q '"bodies":42,"distinct_contents":32,' || fail "bodies were not stored once per distinct content: $made"
"$bin" foreign pack-bodies-verify "$bodies" "$tmp/bodies.pack" | grep -q '"identical":42,"different":0,"damaged":0,"packed_without_a_loose_file":0,"loose_not_packed":0' || fail "a packed body differs from its file"
for name in page-07.body page-37.body binary.bin empty.body; do "$bin" foreign body "$tmp/bodies.pack" "$name" | cmp -s - "$bodies/$name" || fail "$name did not read back exactly"; done
"$bin" foreign body "$tmp/bodies.pack" nothing-by-this-name >/dev/null 2>&1 && fail "an absent body was returned"
cp "$tmp/bodies.pack" "$tmp/bodies-bad.pack"
sqlite3 "$tmp/bodies-bad.pack" "UPDATE blob SET z = substr(z, 1, length(z) - 6) || x'5A' || substr(z, length(z) - 4) WHERE sha256 = (SELECT sha256 FROM body WHERE name = 'page-07.body')"
"$bin" foreign body "$tmp/bodies-bad.pack" page-07.body >/dev/null 2>&1 && fail "a damaged body was returned"
"$bin" foreign pack-bodies-verify "$bodies" "$tmp/bodies-bad.pack" >/dev/null 2>&1 && fail "a damaged body pack verified clean"
cmp -s "$bodies/page-07.body" "$bodies/page-37.body" || fail "fixture: the two names were meant to hold the same content"
echo "PASS body pack: 42 bodies, 32 distinct contents"
# The same receipts as a typed table, where the tool and an engine to ask it are here.
table=$(dirname "$bin")/bonfyre-receipt-table
if [ -x "$table" ] && command -v duckdb >/dev/null 2>&1; then
  "$table" "$tmp/packed/.receipts.pack" "$tmp/receipts.parquet" | grep -q '"observations":301,"receipts_without_an_observation":0' || fail "the table does not hold every observation"
  ask() { duckdb -noheader -list -c "$1"; }
  [ "$(ask "SELECT count(*), count(DISTINCT source_url), count(*) FILTER (WHERE retained) FROM read_parquet('$tmp/receipts.parquet')")" = "301|41|273" ] || fail "table counts differ from the receipts written"
  [ "$(ask "SELECT typeof(http_status), typeof(retained), typeof(body_bytes) FROM read_parquet('$tmp/receipts.parquet') LIMIT 1")" = "INTEGER|BOOLEAN|BIGINT" ] || fail "the table's columns are not typed"
  # The owner's own answer and the table's agree on how many observations one address has.
  owner=$(BONFYRE_FOREIGN_RECEIPTS="$tmp/packed" "$bin" foreign history https://site3.example/feed | sed -n 's/.*"observations":\([0-9]*\).*/\1/p')
  [ "$owner" = "$(ask "SELECT count(*) FROM read_parquet('$tmp/receipts.parquet') WHERE source_url='https://site3.example/feed'")" ] || fail "the table and the owner disagree about one address"
  echo "PASS receipt table: 301 observations, typed"
else echo "SKIP receipt table: tool or duckdb not present"; fi
echo "PASS receipt pack: 301 receipts, $raw bytes loose text, $packed bytes packed"
