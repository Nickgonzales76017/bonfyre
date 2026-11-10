#!/usr/bin/env bash
set -euo pipefail

binary="$1"
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
mkdir -p "$root/household/food" "$root/household/housing"
printf '%s\n' '{"offer_id":"beans","cash_price_usd":"1.79","merchant":"Target","source_url":"https://example.test/beans"}' > "$root/household/food/current-offers.jsonl"
printf '%s\n%s\n' \
  '{"listing_id":"milo-a","property_id":"property:milo","unit_identity":"floorplan:A","advertised_base_rent_monthly_usd":925,"availability_status":"reported_available_now","source_refs":["https://example.test/milo"],"evidence_ref":"housing/receipts#milo"}' \
  '{"listing_id":"milo-a1","property_id":"property:milo","unit_identity":"floorplan:A1","advertised_base_rent_monthly_usd":975,"availability_status":"reported_available_now","source_refs":["https://example.test/milo"],"evidence_ref":"housing/receipts#milo"}' > "$root/household/housing/current-listings.jsonl"
"$binary" --state "$root" select --kind housing.listing --where availability_status=reported_available_now | grep -q '"count": 2'
"$binary" --state "$root" reconcile household:housing:listing:milo-a household:housing:listing:milo-a1 | grep -q '"disposition": "coexisting_distinct_units"'
"$binary" --state "$root" prove household:housing:listing:milo-a --claim availability | grep -q '"proof_state": "not_proven"'
echo "operator smoke: PASS"
