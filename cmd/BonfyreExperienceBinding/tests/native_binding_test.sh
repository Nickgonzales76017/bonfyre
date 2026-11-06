#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin=${1:-"$root/cmd/BonfyreExperienceBinding/bonfyre-experience-binding"}
evidence="$root/cmd/BonfyreExperienceMap/generated/participation_evidence.jsonl"
ingest="$root/cmd/BonfyreExperienceMap/generated/ingest_protocol-0304"
native="$root/cmd/BonfyreExperienceMap/generated/protocol_observation_bridge_receipt.json"
[ -x "$bin" ] || { echo "experience binding: SKIP (binary not built)"; exit 0; }
[ -f "$evidence" ] || { echo "experience binding: SKIP (retained evidence unavailable)"; exit 0; }
[ -f "$ingest/intake-manifest.json" ] || { echo "experience binding: SKIP (ingest unavailable)"; exit 0; }
[ -f "$native" ] || { echo "experience binding: SKIP (protocol receipt unavailable)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
domain="$tmp/domain.jsonl"

"$bin" domain external:protocol-0304 \
  --observation-receipts "$evidence" --ingest-dir "$ingest" --output "$domain" >"$tmp/domain.out"
grep -q '"owner":"DomainProfile"' "$tmp/domain.out"
grep -q '"profile":"reference-standard"' "$tmp/domain.out"
grep -q '"semantic_record_sha256":"edf0f26d0a5c9b6e525968a99d705328fd2194252bee0a89bbc6db28850e2b40"' "$tmp/domain.out"
grep -q '"foreign_receipt_sha256":"4f2d6db0155ec7698ff8ab2f80794cd482a36b72a7b1f1d95e126c0185891efc"' "$tmp/domain.out"
grep -q '"ingest_manifest_sha256":"bd51f2a04579af29b83118a3efe461e52629b2e83fe60eb794d148f2c046b2ac"' "$tmp/domain.out"

"$bin" protocol external:protocol-0304 \
  --evidence "$domain" --native-receipt "$native" >"$tmp/protocol.out"
grep -q '"owner":"ProtocolContract"' "$tmp/protocol.out"
grep -q '"domain_binding_sha256":"bbd60b773e56b7eed09803e3fed2d8b3f8a71b728ddbd36fc11770d7a388003a"' "$tmp/protocol.out"
grep -q '"native_registration_receipt_sha256":"eff00c3fbf56b2c3c4a0b15d551b8135602e6b9fc22726bf9d8c8ec39d914fe1"' "$tmp/protocol.out"

"$bin" domain external:protocol-0304 \
  --observation-receipts "$evidence" --ingest-dir "$ingest" --output "$domain" >/dev/null
[ "$(wc -l < "$domain" | tr -d ' ')" = 1 ]

if "$bin" protocol external:protocol-not-0304 \
  --evidence "$domain" --native-receipt "$native" >/dev/null 2>&1; then
  echo "experience binding: mismatched occurrence unexpectedly passed" >&2
  exit 1
fi

echo "experience binding: PASS"
