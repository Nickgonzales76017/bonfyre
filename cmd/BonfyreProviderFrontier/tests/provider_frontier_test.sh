#!/bin/sh
set -eu

binary=${1:?binary}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
provider="$root/provider.db"
address="$root/address.db"
foreign="$root/foreign.db"
receipts="$root/foreign.jsonl"
out="$root/exact.jsonl"
manifest="$root/context-material.jsonl"

sqlite3 "$provider" "
CREATE TABLE provider_embedding_run(run_id TEXT PRIMARY KEY,representation_id TEXT,requested_count INTEGER,stored_count INTEGER,failed_count INTEGER,dimensions INTEGER,status TEXT);
CREATE TABLE provider_embedding_query(query_id TEXT PRIMARY KEY,run_id TEXT,candidate_count INTEGER,top_k INTEGER);
CREATE TABLE provider_embedding_rank(query_id TEXT,rank INTEGER,subject_id TEXT,source_digest TEXT,PRIMARY KEY(query_id,rank));
INSERT INTO provider_embedding_run VALUES('run:1','mistral:test',2,2,0,4,'complete');
INSERT INTO provider_embedding_query VALUES('query:1','run:1',2,2);
INSERT INTO provider_embedding_rank VALUES('query:1',1,'external:a','source-a'),('query:1',2,'external:b','source-b');"
sqlite3 "$address" "
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT);
INSERT INTO candidate_advertisement VALUES('external:a','external_occurrence','ExternalEstate:external:a','ForeignTwin:admission','MaterialCompiler:external');
INSERT INTO candidate_advertisement VALUES('external:b','external_occurrence','ExternalEstate:external:b','ForeignTwin:admission','MaterialCompiler:external');"
printf 'provider frontier body a\n' > "$root/body-a"
printf 'provider frontier body b\n' > "$root/body-b"
sha_a=$(shasum -a 256 "$root/body-a" | awk '{print $1}')
sha_b=$(shasum -a 256 "$root/body-b" | awk '{print $1}')
printf '%s\n' \
 "{\"schema\":\"bonfyre-foreign-observation.v1\",\"observation_kind\":\"source_observed\",\"policy_decision\":\"allow\",\"twin_id\":\"external:a\",\"source_url\":\"https://a.example\",\"observed_at\":\"2026-09-15T00:00:00+00:00\",\"content_sha256\":\"$sha_a\",\"body_path\":\"$root/body-a\"}" \
 "{\"schema\":\"bonfyre-foreign-observation.v1\",\"observation_kind\":\"source_observed\",\"policy_decision\":\"allow\",\"twin_id\":\"external:b\",\"source_url\":\"https://b.example\",\"observed_at\":\"2026-09-15T00:00:01+00:00\",\"content_sha256\":\"$sha_b\",\"body_path\":\"$root/body-b\"}" > "$receipts"
"$binary" verify --provider-db "$provider" --run-id run:1 --query-id query:1 --address-db "$address" --foreign-evidence "$receipts" --foreign-db "$foreign" --out "$out" > "$root/result.json"
grep -q '"ranked_handles":2' "$root/result.json"
grep -q '"exact_owner_queries":2' "$root/result.json"
grep -q '"retained_observations":2' "$root/result.json"
[ "$(wc -l < "$out" | tr -d ' ')" = 2 ]
grep -q '"outcome":"observed"' "$out"
grep -q '"lowerer_ref":"MaterialCompiler:external"' "$out"
if grep -q 'lowerer_handoff' "$out"; then echo 'provider frontier verifier emitted a lowerer handoff' >&2; exit 1; fi
"$binary" material-manifest --provider-db "$provider" --run-id run:1 --query-id query:1 --address-db "$address" --foreign-evidence "$receipts" --exact-owner-evidence "$out" --information-policy-class public --out "$manifest" > "$root/manifest.json"
grep -q '"exactly_retained_materials":2' "$root/manifest.json"
[ "$(wc -l < "$manifest" | tr -d ' ')" = 2 ]
grep -q '"information_policy_class":"public"' "$manifest"
if grep -q 'lowerer_handoff' "$manifest"; then echo 'provider context manifest emitted a lowerer handoff' >&2; exit 1; fi
# The later material form must recheck every advertised owner/lowerer too.  A
# valid old exact-owner ledger cannot keep feeding context after a descriptor
# has been retired or reassigned.
sqlite3 "$address" "UPDATE candidate_advertisement SET owner_ref='ExternalEstate:retired' WHERE identity='external:b';"
if "$binary" material-manifest --provider-db "$provider" --run-id run:1 --query-id query:1 --address-db "$address" --foreign-evidence "$receipts" --exact-owner-evidence "$out" --information-policy-class public --out "$root/owner-drift.jsonl" >/dev/null 2>&1; then
  echo 'provider context manifest accepted an AddressPlane owner drift' >&2; exit 1
fi
sqlite3 "$address" "UPDATE candidate_advertisement SET owner_ref='ExternalEstate:external:b' WHERE identity='external:b';"
# A non-ForeignTwin exact verifier is not interchangeable merely because a
# provider rank happened to name it.
sqlite3 "$address" "UPDATE candidate_advertisement SET exact_verifier='CMS:publication' WHERE identity='external:b';"
if "$binary" verify --provider-db "$provider" --run-id run:1 --query-id query:1 --address-db "$address" --foreign-evidence "$receipts" --foreign-db "$root/foreign-reject.db" --out "$root/reject.jsonl" >/dev/null 2>&1; then
  echo 'provider frontier verifier accepted a mismatched exact owner' >&2; exit 1
fi
echo 'ProviderEmbeddingRank -> AddressPlane -> ForeignTwin exact verifier: PASS'
