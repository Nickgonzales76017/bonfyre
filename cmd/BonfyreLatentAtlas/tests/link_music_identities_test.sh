#!/bin/sh
# A Wikidata recording's entity is an exact source assertion, not a claim that
# an audio file is identical to a composition/recording/release.  Keep the
# public item's wiki trail in the atlas as observed_as and reject loose IDs.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin="$root/cmd/BonfyreLatentAtlas/bonfyre-latent-atlas"
tool="$root/cmd/BonfyreLatentAtlas/tools/link_identities.mjs"
tmp=$(mktemp -d /tmp/bonfyre-link-music.XXXXXX)
trap 'rm -rf "$tmp"' EXIT
make -C "$root/cmd/BonfyreLatentAtlas" >/dev/null
sqlite3 "$tmp/cms.db" 'CREATE TABLE internet_source(source TEXT, publisher_ident TEXT, publisher_name TEXT, status TEXT);'
cat > "$tmp/items.jsonl" <<'EOF'
{"source":"wikidata-recorded-music","family":"music","key":"recording-1","kind":"audio","title":"The Internationale","body":"","ident":"Q49617","href":"https://en.wikipedia.org/wiki/The_Internationale","by":"Pierre De Geyter"}
{"source":"wikidata-recorded-music","family":"music","key":"recording-2","kind":"audio","title":"Bad identifier","body":"","ident":"not-a-qid","href":"https://en.wikipedia.org/wiki/Test","by":""}
{"source":"other","family":"music","key":"recording-3","kind":"audio","title":"Other","body":"","ident":"Q123","href":"https://en.wikipedia.org/wiki/Other","by":""}
EOF
bun "$tool" --items "$tmp/items.jsonl" --cms "$tmp/cms.db" --atlas "$bin" --db "$tmp/atlas.db" --only-music-identities > "$tmp/out.json"
grep -q '"scope":"exact_wikidata_recorded_music"' "$tmp/out.json"
sqlite3 "$tmp/atlas.db" "SELECT relation || '|' || src || '|' || dst || '|' || evidence FROM relation WHERE relation='observed_as';" > "$tmp/relations"
grep -q '^observed_as|o/wikidata-recorded-music/recording-1|qid:Q49617|linker: Wikidata recorded-music identifies this retained audio as Q49617; https://en.wikipedia.org/wiki/The_Internationale$' "$tmp/relations"
[ "$(wc -l < "$tmp/relations" | tr -d ' ')" = 1 ]
echo 'Wikidata recorded-music exact observation linkage: PASS'
