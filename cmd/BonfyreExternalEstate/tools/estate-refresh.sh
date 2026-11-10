#!/bin/sh
# Bring the live estate up to date and say what it is.
#
# Until this existed the estate was a fixture. Every witness built the index in
# a mktemp directory, asserted on it and deleted it, so the answer to "what does
# this institution currently know" was "run a test and read the assertions".
# 4,891 occurrences sat in a JSONL nobody had indexed since the last test run.
#
# This is the crank. It imports the corpus, asks each owner that can report to
# report, and prints the participation the estate can actually defend. It
# fetches nothing: every rung produced here is earned from what is already held,
# and the rungs that need the network are left at whatever observation has
# genuinely reached.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
estate=${ESTATE_DIR:-$HOME/.bonfyre/estate}
corpus=${CORPUS:-$root/estate/generated/external-estate-v4/observed-external-estate.jsonl}
plane="$root/cmd/BonfyreExternalEstate/bonfyre-external-estate"
facet="$root/cmd/BonfyreFacet/bonfyre-facet"
[ -x "$plane" ] || { echo "build cmd/BonfyreExternalEstate first" >&2; exit 1; }
[ -x "$facet" ] || { echo "build cmd/BonfyreFacet first" >&2; exit 1; }
mkdir -p "$estate"

cue export ./schemas/control -e surfacePresentationCellSet --out json > "$estate/cells.json"
"$plane" import --db "$estate/estate.db" --occurrences "$corpus" --cells "$estate/cells.json" \
  --bindings "$root/cmd/BonfyreExperienceMap/generated/external_bindings.jsonl" \
  --stages "$root/cmd/BonfyreExperienceMap/generated/stage_bindings.jsonl" \
  --chains "$root/cmd/BonfyreExperienceMap/generated/chains.jsonl" >/dev/null

# Each owner that can report, reports. Interpretation is rewritten rather than
# appended so a refresh does not double-count an occurrence.
rm -f "$estate/interpretation.jsonl"
"$facet" interpret --corpus "$corpus" --out "$estate/interpretation.jsonl" 2>/dev/null
"$plane" participation --db "$estate/estate.db" --out "$estate/plane-participation.jsonl" 2>/dev/null

# Observation evidence is whatever ForeignTwin has actually retained. It is not
# regenerated here, because regenerating it would mean fetching. Receipts are
# append-only logs and every line is read rather than the newest one: two
# separate observations of the same source are what earns `repeat_observed`,
# and keeping only the last would silently discard the rung.
observed="$root/cmd/BonfyreExperienceMap/generated/participation_evidence.jsonl"
sweep=${SWEEP_DIR:-$HOME/.bonfyre/foreign-sweep}
: > "$estate/observation.jsonl"
if [ -d "$sweep/receipts" ]; then
  find "$sweep/receipts" -name '*.json' -exec cat {} + >> "$estate/observation.jsonl" 2>/dev/null || true
fi
# The discovery sweep read the same occurrences at the paths they publish for
# agents rather than for people. A second observation of one source is what
# earns repeat_observed, so these belong in the same stream and not beside it.
disco=${DISCOVERY_DIR:-$HOME/.bonfyre/discovery}
if [ -d "$disco/receipts" ]; then
  find "$disco/receipts" -name '*.json' -exec cat {} + >> "$estate/observation.jsonl" 2>/dev/null || true
fi
# Whatever the ladder carried past observation: Ingest normalized it,
# DomainProfile bound it, ProtocolContract compiled over it. These are the rows
# that make the estate a chain rather than a set of separate reports.
chain="$estate/chain-evidence.jsonl"
contracts="$estate/contract-evidence.jsonl"
representations="$estate/representation-evidence.jsonl"
# Exact-owner queries are a distinct rung.  The BitNet shadow evaluator may
# emit them only after it calls the native ForeignTwin owner for every stored
# frontier member; a score or verifier reference alone never appears here.
exact_queries="$estate/bitnet-exact-owner-evidence.jsonl"
# What the served bytes conformed to, what was enacted under a published
# permission, and what the metered sources said they charge. All three are read
# from material the sweep already retained; none of them fetches.
conformance="$estate/conformance-evidence.jsonl"
enactments="$estate/enactment-evidence.jsonl"
quotes="$estate/quote-evidence.jsonl"
book="$estate/price-book.jsonl"
# What the services themselves declared they require, and which of it this
# estate holds. This is the demand side and it was missing entirely.
demand="$estate/demand-evidence.jsonl"
for f in "$chain" "$contracts" "$representations" "$exact_queries" "$conformance" "$enactments" "$quotes" "$book" "$demand"; do
  [ -f "$f" ] || : > "$f"
done
cat "$observed" "$estate/observation.jsonl" "$estate/interpretation.jsonl" \
    "$estate/plane-participation.jsonl" "$chain" "$contracts" "$representations" \
    "$exact_queries" "$conformance" "$enactments" "$quotes" "$book" "$demand" > "$estate/evidence.jsonl"

# Sign what was produced. Every row carries digests, which say the bytes did
# not move; a signature says who produced them. Without it the estate's claims
# are checkable by the estate and by nobody else.
signer="$root/cmd/BonfyreSign/bonfyre-sign"
identity=${IDENTITY_DIR:-$HOME/.bonfyre/identity}
if [ -x "$signer" ] && [ -f "$identity/institution.ed25519.secret" ]; then
  "$signer" sign --key-dir "$identity" --file "$estate/evidence.jsonl" > "$estate/attestation.json" 2>/dev/null || true
fi

bun cmd/BonfyreExperienceMap/tools/participation_report.mjs \
  --evidence "$estate/evidence.jsonl" > "$estate/report.json"
"$facet" density --corpus "$corpus" --evidence "$estate/evidence.jsonl" > "$estate/density.json"

bun cmd/BonfyreExternalEstate/tools/estate-refresh-summary.mjs "$estate/report.json" "$estate/density.json"
