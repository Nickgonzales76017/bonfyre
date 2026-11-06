#!/bin/sh
# Two readers of one evidence file must derive the same owner graph.
#
# They did not. A ForeignTwin receipt is the ExternalEstate -> ForeignTwin
# handoff and never says so -- the observer writes what it saw, not who handed
# it the occurrence. The participation reader synthesised that edge; the density
# reader read the file literally and did not. The same evidence therefore
# measured as four edges with a longest path of two in one reader and five edges
# with a longest path of three in the other, and the estate's own first hop was
# missing from every density report it had ever produced.
#
# This is the text-extractor failure in another costume: two implementations of
# one reading, disagreeing quietly, with nothing that would notice.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
corpus="$root/estate/generated/external-estate-v4/observed-external-estate.jsonl"

# One receipt, carrying no from_owner or to_owner, exactly as the observer
# writes them.
cat > "$tmp/evidence.jsonl" <<'JSON'
{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:ext-00001","observation_kind":"source_observed","body_path":"/dev/null","content_sha256":"0000000000000000000000000000000000000000000000000000000000000000"}
{"schema":"bonfyre-domain-binding.v1","occurrence_id":"external:ext-00001","axis":"semantic","owner":"DomainProfile","state":"profile_bound","from_owner":"ForeignTwin","to_owner":"DomainProfile","binding_validated":true}
JSON

"$bin" density --corpus "$corpus" --evidence "$tmp/evidence.jsonl" > "$tmp/density.json"
bun cmd/BonfyreExperienceMap/tools/participation_report.mjs --evidence "$tmp/evidence.jsonl" > "$tmp/report.json"

bun cmd/BonfyreFacet/tests/reader_agreement.mjs "$tmp/density.json" "$tmp/report.json"
