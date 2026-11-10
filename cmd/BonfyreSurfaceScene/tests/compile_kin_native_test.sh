#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
bun -e '
import {Database} from "bun:sqlite";
const db=new Database(process.argv[1]);
db.exec("CREATE TABLE chart(chart_id TEXT,dims INTEGER); CREATE TABLE projection(chart_id TEXT,address TEXT,coordinate BLOB,updated_at TEXT)");
db.query("INSERT INTO chart VALUES(?,?)").run("words-256",2);
const add=db.query("INSERT INTO projection VALUES(?,?,?,?)");
for(const [address,x,y] of [["o/a",1,0],["o/b",0.99,0.01],["o/c",0,1]]) {
 const bytes=Buffer.alloc(8);bytes.writeFloatLE(x,0);bytes.writeFloatLE(y,4);
 add.run("words-256",address,bytes,"2026-09-29T00:00:00Z");
}
db.close();
' "$tmp/atlas.db"

bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 >"$tmp/native.json"
bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
const base=process.argv[1], report=JSON.parse(readFileSync(base+"/native.json","utf8"));
if(report.items!==3||report.dims!==2||report.pairs_kept!==2||report.backend!=="cpu") throw new Error(JSON.stringify(report));
const db=new Database(base+"/native.db");
const rows=db.query("SELECT address,kin_address FROM kin ORDER BY address").all();
if(rows.length!==2||rows[0].address!=="o/a"||rows[0].kin_address!=="o/b") throw new Error(JSON.stringify(rows));
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.py.archive" \
    --atlas "$tmp/atlas.db" --db "$tmp/legacy.db" --backend cpu --min 0.85 >"$tmp/legacy.json"
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
const rows=name=>new Database(base+"/"+name+".db").query("SELECT address,kin_address,chart,score,rank FROM kin ORDER BY address,rank").all();
if(JSON.stringify(rows("native"))!==JSON.stringify(rows("legacy"))) throw new Error("row parity");
' "$tmp"
fi

# Word chart's exact ordered vectors are the dependency. A second tick must
# neither launch the many-to-many kin kernel nor churn relationship rows.
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/replay.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==0 || r.kernel_invocations!==0 || r.rows_retained!==2)
  throw Error("unchanged chart recomputed: "+JSON.stringify(r));
' "$tmp/replay.json"

# A different owner may update unrelated tables; that is not a chart delta.
sqlite3 "$tmp/native.db" "CREATE TABLE archive_clock(last_read TEXT); INSERT INTO archive_clock VALUES ('today');"
sqlite3 "$tmp/native.db" "UPDATE archive_clock SET last_read='tomorrow';"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/clock.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==0) throw Error("unrelated archive owner invalidated kin");
' "$tmp/clock.json"

# An unrelated semantic vector can enter without rewriting the existing
# a/b pair, even though the source-specific kernel is correctly re-run.
bun -e '
import {Database} from "bun:sqlite";
const db=new Database(process.argv[1]);const b=Buffer.alloc(8);
b.writeFloatLE(0,0);b.writeFloatLE(-1,4);
db.query("INSERT INTO projection VALUES(?,?,?,?)").run("words-256","o/d",b,"2026-10-14T15:00:00Z");
db.close();
' "$tmp/atlas.db"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/new-independent.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==1 || r.pairs_kept!==2 || r.rows_updated!==0 || r.rows_retained!==2)
  throw Error("independent chart vector churned old kin: "+JSON.stringify(r));
' "$tmp/new-independent.json"

# Introducing a genuinely similar vector changes the a/c and b/c closure;
# the already valid a/b pair stays untouched with its original computed_at.
bun -e '
import {Database} from "bun:sqlite";
const db=new Database(process.argv[1]),b=Buffer.alloc(8);
b.writeFloatLE(0.98,0);b.writeFloatLE(0.02,4);
db.query("UPDATE projection SET coordinate=?,updated_at=? WHERE address=?").run(b,"2026-10-14T16:00:00Z","o/c");
db.close();
' "$tmp/atlas.db"
sqlite3 "$tmp/native.db" "SELECT address,kin_address,chart,score,rank,computed FROM kin ORDER BY address,kin_address;" > "$tmp/before"
sqlite3 "$tmp/native.db" "SELECT name,input_digest,output_digest,rows FROM _scene_derived_generation;" > "$tmp/gen-before"
sqlite3 "$tmp/native.db" "CREATE TRIGGER refuse_kin BEFORE INSERT ON kin BEGIN SELECT RAISE(ABORT,'injected kin failure'); END;"
if bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/failed" 2>&1; then
  echo 'FAIL: injected native kin transaction committed' >&2; exit 1
fi
sqlite3 "$tmp/native.db" "DROP TRIGGER refuse_kin;"
sqlite3 "$tmp/native.db" "SELECT address,kin_address,chart,score,rank,computed FROM kin ORDER BY address,kin_address;" > "$tmp/after"
sqlite3 "$tmp/native.db" "SELECT name,input_digest,output_digest,rows FROM _scene_derived_generation;" > "$tmp/gen-after"
diff -u "$tmp/before" "$tmp/after"
diff -u "$tmp/gen-before" "$tmp/gen-after"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/similar.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==1 || r.rows_updated!==4 || r.rows_retained!==2 ||
   r.pairs_kept!==6) throw Error("similar vector did not update its precise kin cut: "+JSON.stringify(r));
' "$tmp/similar.json"

# Readback catches an independent mutation to the current projected output.
sqlite3 "$tmp/native.db" "UPDATE kin SET score=0.01 WHERE address='o/a' AND kin_address='o/b';"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/drift.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==1 || r.rows_updated!==1 || r.rows_retained!==5)
  throw Error("foreign kin output mutation hidden: "+JSON.stringify(r));
' "$tmp/drift.json"

# A source retired from the selected chart does not leave ghosts in kin.
sqlite3 "$tmp/atlas.db" "DELETE FROM projection WHERE address='o/c';"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/retired.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.rows_removed!==4 || r.rows_retained!==2 || r.pairs_kept!==2) throw Error(JSON.stringify(r));
' "$tmp/retired.json"
sqlite3 "$tmp/atlas.db" "DELETE FROM projection WHERE address IN ('o/b','o/d');"
bun "$root/cmd/BonfyreSurfaceScene/tools/compile_kin.mjs" \
  --atlas "$tmp/atlas.db" --db "$tmp/native.db" --backend cpu --min 0.85 > "$tmp/one.json"
[ "$(sqlite3 "$tmp/native.db" "SELECT count(*) FROM kin;")" = 0 ]

echo 'compile_kin_native_test: PASS'
