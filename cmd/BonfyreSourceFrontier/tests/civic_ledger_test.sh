#!/bin/sh
# civic-ledger: receipts in, Markdown out, and every refusal it promises.
# The bodies below are made-up fixtures in the shapes Overpass and Legistar
# answer with; none of it is published or presented as a real record.
set -eu
BIN=${1:-./tools/civic-ledger}
T=$(mktemp -d "${TMPDIR:-/tmp}/civic-ledger.XXXXXX")
trap 'rm -rf "$T"' 0 1 2 15
fail=0; pass=0
ok() { pass=$((pass+1)); }
no() { echo "FAIL: $1" >&2; fail=$((fail+1)); }

receipt() { # name body-file decision status
  sha=$(shasum -a 256 "$T/$2" | cut -d' ' -f1)
  printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"t","source_url":"https://example.test/q","observed_at":"2026-10-30T01:02:03Z","policy_decision":"%s","http_status":%s,"content_sha256":"%s","body_path":"%s"}' "$3" "$4" "$sha" "$2" > "$T/$1"
}

cat > "$T/towns.body" <<'EOF'
{"osm3s":{"copyright":"The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."},
"elements":[
{"type":"city","id":1,"tags":{"county":"North County","name":"Alder","cameras":"12"}},
{"type":"city","id":2,"tags":{"county":"North County","name":"Birch","cameras":"0"}},
{"type":"city","id":3,"tags":{"county":"South County","name":"Alder","cameras":"12"}},
{"type":"city","id":4,"tags":{"county":"South County","name":"Cedar","cameras":"30"}},
{"type":"county","id":5,"tags":{"name":"North County","cameras":"14"}},
{"type":"county","id":6,"tags":{"name":"South County","cameras":"35"}}]}
EOF
receipt towns.json towns.body allow 200
out=$("$BIN" towns "$T/towns.json")
echo "$out" | grep -q '^| Cedar | 30 |$' && ok || no "town row"
echo "$out" | grep -q '^| Alder | 12 |$' && ok || no "town counted once across counties"
[ "$(echo "$out" | grep -c '^| Alder ')" = 1 ] && ok || no "Alder listed twice"
echo "$out" | grep -q 'Birch' && no "zero-count town listed" || ok
echo "$out" | grep -q 'put 49 license plate readers on the public map across 2 counties' && ok || no "county total"
echo "$out" | grep -q 'Open Database License' && ok || no "ODbL attribution"
[ "$(echo "$out" | grep -n 'Cedar' | cut -d: -f1)" -lt "$(echo "$out" | grep -n '| Alder' | cut -d: -f1)" ] && ok || no "sorted by count"

# A body with a single position is refused whole.
sed 's/"id":4,/"id":4,"lat":32.7,"lon":-96.8,/' "$T/towns.body" > "$T/pos.body"
receipt pos.json pos.body allow 200
rc=0; "$BIN" towns "$T/pos.json" > "$T/pos.out" 2>&1 || rc=$?; [ "$rc" = 3 ] && ok || no "position exit code"
grep -q 'carries a position' "$T/pos.out" && ok || no "position reason"
[ "$(grep -c '^|' "$T/pos.out" || true)" = 0 ] && ok || no "partial output after refusal"
sed 's/"type":"city","id":4/"type":"node","id":4/' "$T/towns.body" > "$T/node.body"
receipt node.json node.body allow 200
"$BIN" towns "$T/node.json" >/dev/null 2>&1 && no "raw element accepted" || ok

# Drifted body, robots refusal, failed read, missing ODbL notice.
receipt drift.json towns.body allow 200; printf ' ' >> "$T/towns.body"
"$BIN" towns "$T/drift.json" 2>"$T/e" && no "drifted body accepted" || grep -q 'no longer hashes' "$T/e" && ok || no "drift reason"
receipt robots.json towns.body robots_disallow 0
"$BIN" towns "$T/robots.json" 2>"$T/e" && no "robots refusal ignored" || grep -q 'robots did not allow' "$T/e" && ok || no "robots reason"
receipt e500.json towns.body allow 500
"$BIN" towns "$T/e500.json" 2>/dev/null && no "500 accepted" || ok
printf '{"elements":[]}' > "$T/bare.body"; receipt bare.json bare.body allow 200
"$BIN" towns "$T/bare.json" 2>"$T/e" && no "unlicensed body accepted" || grep -q 'ODbL' "$T/e" && ok || no "ODbL reason"

cat > "$T/c1.body" <<'EOF'
[{"MatterId":7,"MatterGuid":"AAAA-1111","MatterFile":"26-1","MatterStatusName":"Approved","MatterIntroDate":"2026-03-01T00:00:00","MatterTitle":"Authorize a contract with Example Vendor for license plate reading cameras\r\n[Presenter Name, Title]"},
 {"MatterId":8,"MatterGuid":"BBBB-2222","MatterFile":"24-9","MatterStatusName":"Approved","MatterIntroDate":"2024-01-01T00:00:00","MatterTitle":"An ordinance on expired license plates"},
 {"MatterId":9,"MatterGuid":"bad guid<","MatterFile":"25-4","MatterStatusName":"Agenda Ready","MatterIntroDate":"2025-05-05T00:00:00","MatterTitle":"Flock camera update with “quotes” and a [bracket] | pipe"}]
EOF
cat > "$T/c2.body" <<'EOF'
[{"MatterId":7,"MatterGuid":"AAAA-1111","MatterFile":"26-1","MatterStatusName":"Approved","MatterIntroDate":"2026-03-01T00:00:00","MatterTitle":"Authorize a contract with Example Vendor for license plate reading cameras"}]
EOF
receipt c1.json c1.body allow 200; receipt c2.json c2.body allow 200
out=$("$BIN" council "Alder" alder.example.test "$T/c1.json" "$T/c2.json")
echo "$out" | grep -q '^## Alder$' && ok || no "place heading"
echo "$out" | grep -q '^2 items in Alder' && ok || no "dedupe and filter count"
echo "$out" | grep -q 'expired license plates' && no "registration ordinance kept" || ok
echo "$out" | grep -q 'Presenter Name' && no "second title line kept" || ok
echo "$out" | grep -q '“quotes”' && ok || no "unicode escapes decoded"
echo "$out" | grep -q '(bracket)' && ok || no "brackets neutralised"
echo "$out" | grep -q 'https://alder.example.test/LegislationDetail.aspx?ID=7&GUID=AAAA-1111' && ok || no "record link"
echo "$out" | grep 'ID=9' && no "unsafe guid linked" || ok
[ "$(echo "$out" | grep -n '2026-03-01' | cut -d: -f1)" -lt "$(echo "$out" | grep -n '2025-05-05' | cut -d: -f1)" ] && ok || no "newest first"
"$BIN" council "Alder" 'evil.test/x?' "$T/c2.json" >/dev/null 2>&1 && no "unsafe host accepted" || ok
printf '[]' > "$T/none.body"; receipt none.json none.body allow 200
"$BIN" council "Birch" birch.example.test "$T/none.json" | grep -q 'worth a records request' && ok || no "empty place"
out=$("$BIN" reads "Map=$T/towns.json" "Blocked=$T/robots.json" "Down=$T/e500.json")
echo "$out" | grep -q '^| Map (example.test) | 2026-10-30 | Read in full |$' && ok || no "read row"
echo "$out" | grep -q 'Blocked.*stay out, so it was not read' && ok || no "robots row"
echo "$out" | grep -q 'Down.*answered with an error' && ok || no "error row"
rc=0; "$BIN" >/dev/null 2>&1 || rc=$?; [ "$rc" = 2 ] && ok || no "usage exit"

echo "civic-ledger: $pass passed, $fail failed"
[ "$fail" = 0 ]
