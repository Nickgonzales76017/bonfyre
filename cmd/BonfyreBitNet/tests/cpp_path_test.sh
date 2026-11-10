#!/bin/sh
# What this does not prove:
# - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
# The direct bitnet.cpp path must be reachable, not merely present.
#
# It was present and unreachable. The usage string offered two embedding
# sources -- an HTTP endpoint or the bitnet.cpp binary -- and main parsed only
# --endpoint, so bitnet_cpp_embeddings was compiled and never called. Two things
# followed. The documented offline path was documentation, and an unused static
# function under -Werror is a build failure waiting for the next edit to this
# file, which is how it was found.
#
# -Werror catches the unused function. Nothing caught that the flag was never
# parsed, so that is what this asserts: naming --bitnet-cpp must take the direct
# path, and the proof is that a bogus binary fails as an embedding failure
# rather than as a usage error.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

sqlite3 "$tmp/address.db" "CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT);CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);INSERT INTO candidate_advertisement VALUES('source:a','external_occurrence','ExternalEstate:a','ForeignTwin:admission','MaterialCompiler:external');INSERT INTO candidate_advertisement_term VALUES('source:a','domain','weather');"
printf '%s\n' '{"survivors":[{"identity":"source:a"}]}' > "$tmp/forward.json"
sqlite3 "$tmp/model.db" "CREATE TABLE model_representation_passport(representation_id TEXT,measurement_state TEXT,admission_state TEXT,admission_scope TEXT);INSERT INTO model_representation_passport VALUES('bitnet','measured','ADMIT','candidate_reorder');"

# Neither source named: still a usage error.
if "$bin" reorder --address-db "$tmp/address.db" --forward-result "$tmp/forward.json" \
   --model-db "$tmp/model.db" --representation bitnet --scope candidate_reorder \
   --query weather --out "$tmp/out.db" > "$tmp/none.out" 2>&1; then
  echo 'reorder ran with no embedding source' >&2; exit 1
fi
grep -q 'usage:' "$tmp/none.out" || { echo 'expected a usage error when no source is named' >&2; exit 1; }

# --bitnet-cpp alone satisfies the requirement that --endpoint used to hold, and
# the failure that follows must come from the embedding attempt, not the parser.
printf '#!/bin/sh\nexit 3\n' > "$tmp/fake-embed"
chmod +x "$tmp/fake-embed"
: > "$tmp/fake-model"
if "$bin" reorder --address-db "$tmp/address.db" --forward-result "$tmp/forward.json" \
   --model-db "$tmp/model.db" --representation bitnet --scope candidate_reorder \
   --bitnet-cpp "$tmp/fake-embed" --bitnet-model "$tmp/fake-model" \
   --admission-db "$tmp/admission.db" --admission-volume t --admission-plane model-bitnet \
   --admission-estimated-bytes 1024 --admission-free-bytes 1099511627776 \
   --query weather --out "$tmp/out.db" > "$tmp/cpp.out" 2>&1; then
  echo 'a binary that exits 3 produced a ranking' >&2; exit 1
fi
grep -q 'usage:' "$tmp/cpp.out" && { echo 'named --bitnet-cpp and still got a usage error; the direct path is unreachable' >&2; exit 1; }
grep -qi 'embedding' "$tmp/cpp.out" || { echo 'expected an embedding failure from the direct path' >&2; cat "$tmp/cpp.out" >&2; exit 1; }

echo 'BonfyreBitNet cpp path: PASS (direct embedding source reachable)'
