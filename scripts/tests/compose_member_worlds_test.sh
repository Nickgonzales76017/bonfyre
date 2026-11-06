#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'compose_member_worlds_test: SKIP (CMS not built)'; exit 0; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
estate="$tmp/estate"
mkdir -p "$estate/foreign/receipts" "$tmp/schemas"
for type in publication_occurrence source_collection world world_fragment semantic_slot application_binding site_program site_presentation internet_source program_binding; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/"
done
"$cms" schema migrate --db "$estate/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
"$cms" entry create site_presentation '{"site_id":"person-alice","host":"alice.aurekai.life","subject_address":"person:alice","profile":"newsroom","territory":"life","navigation":[],"status":"published"}' --ns root --db "$estate/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
"$cms" entry create internet_source '{"family":"news","source":"source-one","label":"One","says":"Public reading","url":"https://example.test/feed","twin":"twin-one","state":"observed","extract":"rss:item","title_field":"title","meta_field":"pubDate","link_field":"link","body_regime":"retained-public","status":"published","tier":"warm","kind":"publication"}' --ns root --db "$estate/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1

bun -e '
import {Database} from "bun:sqlite";
import {writeFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1];
const archive=new Database(join(base,"items.db"));
archive.exec("CREATE TABLE item(source TEXT,key TEXT,kind TEXT,family TEXT,title TEXT,href TEXT,publisher TEXT,at INTEGER)");
archive.query("INSERT INTO item VALUES(?,?,?,?,?,?,?,?)").run("source-one","item-one","story","news","Retained public story","https://example.test/one","One",1789999200);
archive.close();
const matrix=new Database(join(base,"matrix.db"));
matrix.exec("CREATE TABLE matrix_subjects(subject_id TEXT,subject_kind TEXT); CREATE TABLE matrix_presences(subject_id TEXT,territory TEXT,scope TEXT,active INTEGER)");
matrix.query("INSERT INTO matrix_subjects VALUES(?,?)").run("person:alice","person");
matrix.query("INSERT INTO matrix_presences VALUES(?,?,?,?)").run("person:alice","life","public",1);
matrix.close();
writeFileSync(join(base,"foreign/receipts/source-one.json"),"{}\n");
' "$estate"

bun "$root/scripts/compose_member_worlds.mjs" --estate "$estate" --repo "$root" >"$tmp/dry.json"
bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], dry=JSON.parse(readFileSync(join(base,"dry.json"),"utf8"));
if(dry.members.length!==1||!dry.surfaces.length) throw new Error(JSON.stringify(dry));
const cms=new Database(join(base,"estate/cms.db"));
if(cms.query("SELECT COUNT(*) AS n FROM world").get().n!==0) throw new Error("dry run wrote world");
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then cp -R "$estate" "$tmp/legacy"; fi
bun "$root/scripts/compose_member_worlds.mjs" --estate "$estate" --repo "$root" --apply >"$tmp/native.json"
bun -e '
import {Database} from "bun:sqlite";
import {writeFileSync} from "node:fs";
const db=new Database(process.argv[1]);
const tables=["publication_occurrence","source_collection","world","world_fragment","semantic_slot","application_binding","site_program"];
writeFileSync(process.argv[2],JSON.stringify(Object.fromEntries(tables.map(table=>[table,db.query(`SELECT COUNT(*) AS n FROM ${table}`).get().n]))));
' "$estate/cms.db" "$tmp/first-counts.json"
bun "$root/scripts/compose_member_worlds.mjs" --estate "$estate" --repo "$root" --apply >"$tmp/repeat.json"
bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], result=JSON.parse(readFileSync(join(base,"native.json"),"utf8"));
if(result.read.occurrences!==1||result.collections!==2||result.members.length!==1||result.members[0].slots<5) throw new Error(JSON.stringify(result));
const cms=new Database(join(base,"estate/cms.db"));
const first=JSON.parse(readFileSync(join(base,"first-counts.json"),"utf8"));
const second=Object.fromEntries(Object.keys(first).map(table=>[table,cms.query(`SELECT COUNT(*) AS n FROM ${table}`).get().n]));
if(JSON.stringify(first)!==JSON.stringify(second)) throw new Error("repeat projection duplicated rows");
for(const [table,minimum] of [["publication_occurrence",1],["source_collection",2],["world",1],["world_fragment",1],["semantic_slot",5],["application_binding",5],["site_program",1]]) {
 const row=cms.query(`SELECT COUNT(*) AS n FROM ${table}`).get();
 if(row.n<minimum) throw new Error(table+" "+row.n);
}
const occurrence=cms.query("SELECT receipt_ref,assertion_state FROM publication_occurrence").get();
if(occurrence.receipt_ref!=="foreign/receipts/source-one.json"||occurrence.assertion_state!=="OBSERVED") throw new Error("receipt boundary");
const repeat=JSON.parse(readFileSync(join(base,"repeat.json"),"utf8"));
if(repeat.read.occurrences!==1||repeat.members[0].slots!==result.members[0].slots) throw new Error("repeat mismatch");
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/scripts/compose_member_worlds.py.archive" --estate "$tmp/legacy" --repo "$root" --apply >"$tmp/legacy.json"
  bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1];
const report=name=>JSON.parse(readFileSync(join(base,name+".json"),"utf8"));
const native=report("native"),legacy=report("legacy");
if(JSON.stringify(native)!==JSON.stringify(legacy)) throw new Error("report parity mismatch");
const counts=name=>{const db=new Database(join(base,name,"cms.db"));return Object.fromEntries(["publication_occurrence","source_collection","world","world_fragment","semantic_slot","application_binding","site_program"].map(table=>[table,db.query(`SELECT COUNT(*) AS n FROM ${table}`).get().n]));};
if(JSON.stringify(counts("estate"))!==JSON.stringify(counts("legacy"))) throw new Error("row-count parity mismatch");
' "$tmp"
fi
echo "compose_member_worlds_test: PASS"
