#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
bun -e '
import {writeFileSync} from "node:fs";
const rows=[
 {source:"s",key:"a",kind:"place",title:"A",lon:-96.8,lat:32.8,placed:true},
 {source:"s",key:"b",kind:"place",title:"B",lon:-96.801,lat:32.8,placed:true},
 {source:"s",key:"far",kind:"place",title:"Far",lon:-80,lat:30,placed:true},
 {source:"s",key:"missing",kind:"story",title:"Unplaced",placed:false}
];
writeFileSync(process.argv[1],rows.map(JSON.stringify).join("\n")+"\n");
' "$tmp/items.jsonl"

bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/items.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 >"$tmp/native.json"
bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
const base=process.argv[1], result=JSON.parse(readFileSync(base+"/native.json","utf8"));
if(result.placed!==3||result.pairs_kept!==2||result.backend!=="cpu") throw new Error(JSON.stringify(result));
const db=new Database(base+"/native.db");
const rows=db.query("SELECT address,near_address FROM neighbour ORDER BY address").all();
if(rows.length!==2||rows[0].address!=="o/s/a"||rows[0].near_address!=="o/s/b") throw new Error(JSON.stringify(rows));
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.py.archive" \
    --items "$tmp/items.jsonl" --db "$tmp/legacy.db" --backend cpu --max-km 4 >"$tmp/legacy.json"
  bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
const base=process.argv[1], load=name=>JSON.parse(readFileSync(base+"/"+name+".json","utf8"));
const native=load("native"),legacy=load("legacy");
for(const [field,value] of Object.entries(legacy)) {
 if(field==="seconds" || field==="kernel_ms") continue;
 if(JSON.stringify(native[field])!==JSON.stringify(value))
   throw new Error("legacy semantic report changed: "+field);
}
const rows=name=>new Database(base+"/"+name+".db").query("SELECT address,near_address,kind,metres,rank FROM neighbour ORDER BY address,rank").all();
if(JSON.stringify(rows("native"))!==JSON.stringify(rows("legacy"))) throw new Error("row parity");
' "$tmp"
fi

# Repeated observations do not spend the vGPU/CPU budget or rewrite neighbours.
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/items.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/replay.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==0 || r.kernel_invocations!==0 || r.rows_retained!==2)
  throw Error("unchanged spatial generation computed: "+JSON.stringify(r));
' "$tmp/replay.json"

# Independent archive writes, including last-read timestamps, must not dirty
# spatial material. Changing a source that has no placed items also does not.
sqlite3 "$tmp/native.db" "CREATE TABLE item (source TEXT, last_read TEXT); INSERT INTO item VALUES ('s','yesterday');"
sqlite3 "$tmp/native.db" "UPDATE item SET last_read='now';"
sed 's/"title":"Unplaced"/"title":"Different unplaced title"/' "$tmp/items.jsonl" > "$tmp/unplaced.jsonl"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/unplaced.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/independent.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==0) throw Error("archive or unplaced content invalidated spatial cut");
' "$tmp/independent.json"

# Add a disjoint spatial cluster; existing cluster rows retain their bytes.
sed '/"key":"missing"/i\
{"source":"s","key":"far2","kind":"place","title":"Far2","lon":-80.001,"lat":30,"placed":true}
' "$tmp/items.jsonl" > "$tmp/two-clusters.jsonl"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/two-clusters.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/expanded.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.pairs_kept!==4 || r.rows_updated!==2 || r.rows_retained!==2) throw Error(JSON.stringify(r));
' "$tmp/expanded.json"

# A transaction failure cannot retire existing relationships or checkpoint a
# new input generation. A retry with the same material must then succeed.
sed 's/"lon":-96.801/"lon":-96.803/' "$tmp/two-clusters.jsonl" > "$tmp/moved.jsonl"
sqlite3 "$tmp/native.db" "SELECT address,near_address,kind,metres,rank FROM neighbour ORDER BY address,near_address;" > "$tmp/before"
sqlite3 "$tmp/native.db" "SELECT name,input_digest,output_digest,rows FROM _scene_derived_generation;" > "$tmp/ledger-before"
sqlite3 "$tmp/native.db" "CREATE TRIGGER refuse_neighbour BEFORE INSERT ON neighbour BEGIN SELECT RAISE(ABORT,'injected failure'); END;"
if bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/moved.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 >"$tmp/failed.out" 2>&1; then
  echo 'FAIL: injected native spatial transaction committed' >&2; exit 1
fi
sqlite3 "$tmp/native.db" "DROP TRIGGER refuse_neighbour;"
sqlite3 "$tmp/native.db" "SELECT address,near_address,kind,metres,rank FROM neighbour ORDER BY address,near_address;" > "$tmp/after"
sqlite3 "$tmp/native.db" "SELECT name,input_digest,output_digest,rows FROM _scene_derived_generation;" > "$tmp/ledger-after"
diff -u "$tmp/before" "$tmp/after"
diff -u "$tmp/ledger-before" "$tmp/ledger-after"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/moved.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/repaired.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.rows_updated!==2 || r.rows_retained!==2 || r.owner_operations!==1)
  throw Error("unaffected cluster changed: "+JSON.stringify(r));
' "$tmp/repaired.json"

# A foreign owner changing just one output forces a repair of that output.
sqlite3 "$tmp/native.db" "UPDATE neighbour SET metres=999 WHERE address='o/s/a';"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/moved.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/drift.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.rows_updated!==1 || r.rows_retained!==3 || r.owner_operations!==1)
  throw Error("output drift reused: "+JSON.stringify(r));
' "$tmp/drift.json"

# Source retirement removes only its own and affected neighbours. Fewer than
# two remaining placed items retires the old derived table rather than leaving
# stale recommendations visible.
sed '/"key":"b"/d' "$tmp/moved.jsonl" > "$tmp/retired.jsonl"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/retired.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/retired.out"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.rows_removed!==2 || r.rows_retained!==2 || r.pairs_kept!==2) throw Error(JSON.stringify(r));
' "$tmp/retired.out"
[ "$(sqlite3 "$tmp/native.db" "SELECT count(*) FROM neighbour WHERE address='o/s/b' OR near_address='o/s/b';")" = 0 ]
printf '%s\n' '{"source":"s","key":"a","kind":"place","title":"A","lon":-96.8,"lat":32.8,"placed":true}' > "$tmp/one.jsonl"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs" \
  --items "$tmp/one.jsonl" --db "$tmp/native.db" --backend cpu --max-km 4 > "$tmp/one.out"
[ "$(sqlite3 "$tmp/native.db" "SELECT count(*) FROM neighbour;")" = 0 ]

echo 'compile_neighbourhood_native_test: PASS'
