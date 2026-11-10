#!/usr/bin/env bash
set -euo pipefail

binary="$1"
root="$(mktemp -d)"
# One reader for every field a witness asserts on, so the witness and the
# organ cannot disagree about what the reply said.
lex=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)/cmd/BonfyreLex/bonfyre-lex
trap 'rm -rf "$root"' EXIT
database="$root/providers.db"

"$binary" record --db "$database" --provider codex --kind hard_capacity \
  --observed-at 2026-08-28T12:00:00+00:00 --reset-at 2026-08-29T12:00:00+00:00 >/dev/null
"$binary" record --db "$database" --provider codex --kind transient_failure \
  --observed-at 2026-08-28T13:00:00+00:00 >/dev/null
"$binary" status --db "$database" --provider codex --now 2026-08-28T14:00:00+00:00 \
  | grep -q '"status":"capacity_exhausted"'
"$binary" record --db "$database" --provider codex --kind manual_resume \
  --observed-at 2026-08-28T15:00:00+00:00 >/dev/null
"$binary" status --db "$database" --provider codex --now 2026-08-28T16:00:00+00:00 \
  | grep -q '"status":"ready"'
digest=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
created=$("$binary" materialize --db "$database" --provider openai --artifact-sid artifact:upper90:loss-2026 \
  --representation-digest "$digest" --external-ref file_abc --kind file --purpose retrieval \
  --scope private:upper90:loss --authority-ref authority:fixture --capabilities direct_input.file_search \
  --expires-at 2026-09-01T00:00:00+00:00)
printf '%s' "$created" | grep -q '"reused":false'
reused=$("$binary" materialize --db "$database" --provider openai --artifact-sid artifact:upper90:loss-2026 \
  --representation-digest "$digest" --external-ref file_abc --kind file --purpose retrieval \
  --scope private:upper90:loss --authority-ref authority:fixture)
printf '%s' "$reused" | grep -q '"reused":true'
"$binary" materializations --db "$database" --provider openai --now 2026-08-31T00:00:00+00:00 \
  | grep -q 'file_abc'
if "$binary" materialize --db "$database" --provider openai --artifact-sid artifact:upper90:loss-2026 \
  --representation-digest ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  --external-ref file_abc --kind file --purpose retrieval --scope private:upper90:loss --authority-ref authority:fixture; then
  echo 'provider native smoke: provider reference rebound to a different body'; exit 1
fi
receipt_id=$(printf '%s' "$created" | "$lex" field --kind json_number --key receipt_id)
"$binary" release --db "$database" --receipt-id "$receipt_id" --reason attention_cooling \
  | grep -q '"released":true'
if "$binary" materializations --db "$database" --provider openai --now 2026-08-31T00:00:00+00:00 | grep -q 'file_abc'; then
  echo 'provider native smoke: released materialization remained reusable'; exit 1
fi
# A BodyAvailability entry is a verified physical reuse projection, not a
# digest-shaped assertion.  It has to read the bytes back, refuses rebinding a
# derivation/representation to different bytes, notices later drift, and can
# be made stale without erasing the historical body or its receipt.
body="$root/immutable-body.html"
printf '<main>one frozen representation</main>' > "$body"
body_digest=$(shasum -a 256 "$body" | awk '{print $1}')
"$binary" body-record --db "$database" --derivation-key derivation:fixture:one \
  --representation-ref representation:static-html --body-file "$body" \
  --body-digest "$body_digest" --receipt-ref receipt:fixture:one \
  | grep -q '"availability":"CURRENT"'
"$binary" body-available --db "$database" --derivation-key derivation:fixture:one \
  --representation-ref representation:static-html | grep -q '"available":true'
printf '<main>different body</main>' > "$body"
different_digest=$(shasum -a 256 "$body" | awk '{print $1}')
if "$binary" body-record --db "$database" --derivation-key derivation:fixture:one \
  --representation-ref representation:static-html --body-file "$body" \
  --body-digest "$different_digest" --receipt-ref receipt:fixture:two >/dev/null; then
  echo 'provider native smoke: a current derivation was rebound to different bytes'; exit 1
fi
"$binary" body-available --db "$database" --derivation-key derivation:fixture:one \
  --representation-ref representation:static-html | grep -q '"available":false'
"$binary" body-invalidate --db "$database" --derivation-key derivation:fixture:one \
  --representation-ref representation:static-html --cause-ref fact-delta:fixture:changed \
  | grep -q '"availability":"STALE"'
"$binary" body-available --db "$database" --derivation-key derivation:fixture:one \
  --representation-ref representation:static-html | grep -q '"available":false'
# The provider's explicit local protected-floor policy is an admission input,
# not a way around admission.  A bounded rank against a complete fixture run
# receives and releases a grant before its deliberately unreachable loopback
# transport is attempted; no provider result or query receipt is created.
sqlite3 "$database" "INSERT INTO provider_embedding_run(run_id,provider,model,representation_id,input_digest,requested_count,stored_count,failed_count,dimensions,response_bytes,status,started_at,completed_at,detail) VALUES('embedding:fixture','fixture','fixture-embed','fixture-representation','fixture-input',1,1,0,4,16,'complete','2026-09-15T00:00:00+00:00','2026-09-15T00:00:01+00:00','fixture'); INSERT INTO provider_embedding_projection VALUES('embedding:fixture','fixture-subject','fixture-source','fixture-vector',4,zeroblob(16));"
"$binary" reconcile-embedding-deltas --db "$database" | grep -q '"provider_embedding_deltas_queued":1'
"$binary" emit-embedding-deltas --db "$database" --dir "$root/fact-deltas" | grep -q '"provider_embedding_deltas_dispatched":1'
[ "$(sqlite3 "$database" "SELECT count(*) FROM provider_fact_delta WHERE run_id='embedding:fixture' AND write_ref='ProviderEmbeddingRun:embedding:fixture' AND state='dispatched' AND dispatched_at IS NOT NULL;")" = 1 ]
grep -q '"schema":"bonfyre-fact-delta.v1"' "$root/fact-deltas"/*.json
grep -Fq '"writes":["ProviderEmbeddingRun:embedding:fixture"]' "$root/fact-deltas"/*.json
if MISTRAL_TEST_KEY=x "$binary" rank --db "$database" --run-id embedding:fixture --provider fixture --endpoint https://127.0.0.1:1 --model fixture-embed --api-key-env MISTRAL_TEST_KEY --representation-id fixture-representation --query bounded --admission-db "$root/admission.db" --admission-volume "$root" --admission-plane fixture-provider-rank --admission-estimated-bytes 16 --admission-protected-floor-bytes -1 --top-k 1 >/dev/null 2>&1; then
  echo 'provider native smoke: unreachable transport produced a provider rank'; exit 1
fi
[ "$(sqlite3 "$root/admission.db" "SELECT count(*) FROM resource_grants WHERE plane='fixture-provider-rank' AND released_at IS NOT NULL;")" = 1 ]
[ "$(sqlite3 "$database" "SELECT count(*) FROM provider_embedding_query WHERE run_id='embedding:fixture';")" = 0 ]
echo "provider native smoke: PASS"
