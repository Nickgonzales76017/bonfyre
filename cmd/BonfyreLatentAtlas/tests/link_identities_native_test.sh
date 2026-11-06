#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin="$root/cmd/BonfyreLatentAtlas/bonfyre-latent-atlas"
tool="$root/cmd/BonfyreLatentAtlas/tools/link_identities.mjs"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-link-native.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
[ "${BONFYRE_TEST_SKIP_MAKE:-0}" = 1 ] || make -C "$root/cmd/BonfyreLatentAtlas" >/dev/null
sqlite3 "$tmp/cms.db" "CREATE TABLE internet_source(source TEXT, publisher_ident TEXT, publisher_name TEXT, status TEXT); INSERT INTO internet_source VALUES('calendar','Q999','University of North Texas','published');"
cat > "$tmp/items.jsonl" <<'EOF'
{"source":"wikidata-places","family":"places","key":"dallas","kind":"place","title":"Dallas","body":"","ident":"Q16557"}
{"source":"wikidata-places","family":"places","key":"fort-worth","kind":"place","title":"Fort Worth","body":"","ident":"Q16558"}
{"source":"wikidata-places","family":"places","key":"garland","kind":"place","title":"Garland","body":"","ident":"Q71003"}
{"source":"nonlocal","family":"news","key":"a","kind":"text","title":"Garland","body":"An ambiguous name","ident":""}
{"source":"local-paper","family":"local-voices","key":"b","kind":"text","title":"Garland news","body":"","ident":""}
{"source":"nonlocal","family":"news","key":"c","kind":"text","title":"Garland, Texas","body":"","ident":""}
{"source":"calendar","family":"events","key":"d","kind":"event","title":"Dallas history","body":"A talk about Dallas County","where":"Fort Worth","ident":""}
{"source":"calendar","family":"events","key":"e","kind":"event","title":"University of North Texas lecture","body":"","where":"","ident":""}
{"source":"wikidata-public-domain-films","family":"film","key":"f","kind":"video","title":"The Great Show","body":"","by":"Jane Director","ident":"Q8123"}
{"source":"archive-feature-films","family":"film","key":"g","kind":"video","title":"Great Show","body":"Credits: Director","by":"Other Cast","ident":""}
{"source":"wikidata-recorded-music","family":"music","key":"h","kind":"audio","title":"Song","body":"","ident":"Q49617","href":"https://en.wikipedia.org/wiki/Song"}
EOF
bun "$tool" --items "$tmp/items.jsonl" --cms "$tmp/cms.db" --atlas "$bin" --db "$tmp/atlas.db" --out "$tmp/relations.jsonl" > "$tmp/summary.json"
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE evidence LIKE 'linker:%';")" = 7 ]
sqlite3 "$tmp/atlas.db" "SELECT relation || '|' || src || '|' || dst FROM relation ORDER BY relation,src,dst;" > "$tmp/actual"
cat > "$tmp/expected" <<'EOF'
happens_at|o/calendar/d|qid:Q16558
happens_at|o/calendar/e|qid:Q999
mentions|o/calendar/d|qid:Q16557
mentions|o/local-paper/b|qid:Q71003
mentions|o/nonlocal/c|qid:Q71003
observed_as|o/wikidata-recorded-music/h|qid:Q49617
same_subject_as|o/archive-feature-films/g|qid:Q8123
EOF
diff -u "$tmp/expected" "$tmp/actual"

# An identical tick has zero native Atlas owner calls and zero linker passes.
bun "$tool" --items "$tmp/items.jsonl" --cms "$tmp/cms.db" --atlas "$bin" --db "$tmp/atlas.db" > "$tmp/replay.json"
bun -e 'const r=await Bun.file(process.argv[1]).json(); if(r.owner_operations!==0 || r.source_cuts!==0 || r.avoided_source_compilations!==7) throw Error("unchanged corpus reran Atlas: "+JSON.stringify(r));' "$tmp/replay.json"

# A newsroom change must not erase six other source populations or authored
# human evidence, and the native linker cut must be exactly source-bound.
printf '%s\n' '{"relate":{"src":"o/nonlocal/c","relation":"observed_as","dst":"qid:Q42","evidence":"human verified"}}' > "$tmp/authored.jsonl"
"$bin" import --db "$tmp/atlas.db" --jsonl "$tmp/authored.jsonl" >/dev/null
sed 's/"title":"Garland, Texas"/"title":"Birds, Texas"/' "$tmp/items.jsonl" > "$tmp/changed.jsonl"
bun "$tool" --items "$tmp/changed.jsonl" --cms "$tmp/cms.db" --atlas "$bin" --db "$tmp/atlas.db" > "$tmp/one-source.json"
bun -e 'const r=await Bun.file(process.argv[1]).json(); if(r.owner_operations!==1 || r.source_cuts!==1 || r.avoided_source_compilations!==6) throw Error("changed source was not isolated: "+JSON.stringify(r));' "$tmp/one-source.json"
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE evidence LIKE 'linker:%';")" = 6 ]
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE src='o/nonlocal/c' AND evidence LIKE 'linker:%';")" = 0 ]
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE src='o/nonlocal/c' AND evidence='human verified';")" = 1 ]
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE src='o/calendar/e' AND evidence LIKE 'linker:%';")" = 1 ]

# Independent Atlas mutation invalidates the exact output generation even if
# the source corpus is byte-identical. No global DB fingerprint is involved.
sqlite3 "$tmp/atlas.db" "DELETE FROM relation WHERE src='o/calendar/e' AND evidence LIKE 'linker:%';"
bun "$tool" --items "$tmp/changed.jsonl" --cms "$tmp/cms.db" --atlas "$bin" --db "$tmp/atlas.db" > "$tmp/drift.json"
bun -e 'const r=await Bun.file(process.argv[1]).json(); if(r.source_cuts!==1 || r.avoided_source_compilations!==6) throw Error("native output drift was hidden: "+JSON.stringify(r));' "$tmp/drift.json"
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE src='o/calendar/e' AND evidence LIKE 'linker:%';")" = 1 ]

# A malformed transaction may have begun deleting the changed source's rows,
# but it must roll back BOTH the relations and the durable source generation.
sqlite3 "$tmp/atlas.db" "SELECT src,relation,dst,evidence FROM relation ORDER BY src,relation,dst;" > "$tmp/before-rollback"
sqlite3 "$tmp/atlas.db" "SELECT source,input_digest,context_digest,relation_digest FROM _linker_source_generation ORDER BY source;" > "$tmp/before-generation"
zeros=$(printf '%064d' 0)
printf 'nonlocal\t%s\t%s\t%s\n' "$zeros" "$zeros" "$zeros" > "$tmp/invalid-scopes.tsv"
printf '%s\n' '{"relate":{"src":"o/undeclared/item","relation":"mentions","dst":"qid:Q77","evidence":"linker: bad cut"}}' > "$tmp/invalid-relations.jsonl"
if "$bin" import --db "$tmp/atlas.db" --jsonl "$tmp/invalid-relations.jsonl" --linker-sources "$tmp/invalid-scopes.tsv" > /dev/null 2>&1; then
  echo 'FAIL: foreign relation escaped declared source scope' >&2; exit 1
fi
sqlite3 "$tmp/atlas.db" "SELECT src,relation,dst,evidence FROM relation ORDER BY src,relation,dst;" > "$tmp/after-rollback"
sqlite3 "$tmp/atlas.db" "SELECT source,input_digest,context_digest,relation_digest FROM _linker_source_generation ORDER BY source;" > "$tmp/after-generation"
diff -u "$tmp/before-rollback" "$tmp/after-rollback"
diff -u "$tmp/before-generation" "$tmp/after-generation"

# A later census retracts stale linker links, but never a person's evidence.
cat > "$tmp/manual.jsonl" <<'EOF'
{"relate":{"src":"o/person/manual","relation":"mentions","dst":"qid:Q16557","evidence":"a person said so"}}
EOF
"$bin" import --db "$tmp/atlas.db" --jsonl "$tmp/manual.jsonl" >/dev/null
cat > "$tmp/items.jsonl" <<'EOF'
{"source":"nonlocal","family":"news","key":"a","kind":"text","title":"Nothing local","body":"","ident":""}
EOF
bun "$tool" --items "$tmp/items.jsonl" --cms "$tmp/cms.db" --atlas "$bin" --db "$tmp/atlas.db" >/dev/null
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation;")" = 2 ]
[ "$(sqlite3 "$tmp/atlas.db" "SELECT count(*) FROM relation WHERE evidence IN ('a person said so','human verified');")" = 2 ]
echo 'Native exact identity linking and retraction: PASS'
