#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
program=${BONFYRE_OFFER_BINARY:-$root/cmd/BonfyreOffer/bonfyre-offer}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-offer-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
"$program" generate "$root/components/BonfyreProof/outputs/founder-sample-pickfu/proof-bundle.json" "$tmp/pack" >"$tmp/out" 2>"$tmp/err"
jq -e '. as $doc | .proofSlug == "founder-sample-pickfu" and .proofLabel == "Founder Sample - PickFu" and .qualityStatus == "strong" and .commercialState == "not_quoted" and .sendable == false and (["price","priceCents","pricingFactors","turnaround","promise","wordCount","sentenceCount"] | all(. as $key | $doc | has($key) and .[$key] == null)) and .requirements == ["recipient_need","deliverable_scope","delivery_capacity","source_permissions","real_quote","human_approval"]' "$tmp/pack/offer.json" >/dev/null
grep -q 'not a priced offer or a delivery commitment' "$tmp/pack/offer.md"
grep -q 'INTERNAL PREPARATION ONLY' "$tmp/pack/outreach.md"
if grep -Eq '\$18|\$8|same day|24 hours|48 hours' "$tmp/pack/offer.md" "$tmp/pack/outreach.md"; then exit 1; fi
printf '%s\n' '{"sourceSystem":"BonfyreProof","proofLabel":"Unidentified"}' > "$tmp/unidentified.json"
if "$program" generate "$tmp/unidentified.json" "$tmp/refused" >"$tmp/out" 2>"$tmp/err"; then exit 1; fi
grep -q 'Proof identity is required' "$tmp/err"
[ ! -e "$tmp/refused" ]
printf '%s\n' '{"proofSlug":"technical-evidence","proofLabel":"Review: \"quoted\" \\ test","recommendation":"needs review","qualityStatus":"strong"}' > "$tmp/quoted.json"
"$program" generate "$tmp/quoted.json" "$tmp/quoted" >"$tmp/out" 2>"$tmp/err"
jq -e '. as $doc | .proofLabel == "Review: \"quoted\" \\ test" and (["qualityScore","reviewScore","priceCents"] | all(. as $key | $doc | has($key) and .[$key] == null)) and .sendable == false' "$tmp/quoted/offer.json" >/dev/null
echo 'commercial safety: 3 cases PASS'
