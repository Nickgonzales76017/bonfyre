#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/foreign/bodies" "$tmp/foreign/receipts"

bun -e '
import {Database} from "bun:sqlite";
import {mkdirSync,writeFileSync} from "node:fs";
import {join} from "node:path";
import {gzipSync} from "node:zlib";
const base=process.argv[1], foreign=join(base,"foreign");
const cms=new Database(join(base,"cms.db"));
cms.exec("CREATE TABLE internet_source(source TEXT,family TEXT,twin TEXT,status TEXT)");
const add=cms.query("INSERT INTO internet_source VALUES(?,?,?,?)");
for(const row of [["audio","news","a","published"],["film","news","v","published"],
 ["photo","news","i","published"],["calendar","news","c","published"],
 ["same","podcasts","s","published"],["missing","news","missing","published"],
 ["draft","news","draft","draft"]]) add.run(...row);
cms.close();
const frontier=new Database(join(base,"frontier.db"));
frontier.exec("CREATE TABLE channel(census_source TEXT)");
const channel=frontier.query("INSERT INTO channel VALUES(?)");
for(const source of ["audio","film","photo","calendar","same","missing"]) channel.run(source);
frontier.close();
const rss=part=>"<rss>"+Array(3).fill("<item>"+part+"</item>").join("")+"</rss>";
writeFileSync(join(foreign,"bodies","a.body"),rss("<enclosure type=\"audio/mpeg\"/>"));
writeFileSync(join(foreign,"bodies","v.body.gz"),gzipSync(rss("<media:group/>")));
writeFileSync(join(foreign,"bodies","i.body"),rss("<media:content type=\"image/jpeg\"/>"));
writeFileSync(join(foreign,"bodies","c.body"),"BEGIN:VCALENDAR\n"+"BEGIN:VEVENT\nEND:VEVENT\n".repeat(3));
writeFileSync(join(foreign,"bodies","s.body"),rss("<enclosure type=\"audio/mpeg\"/>"));
' "$tmp"

bun "$root/cmd/BonfyreSourceFrontier/tools/measure_carried_material.mjs" \
  --cms "$tmp/cms.db" --foreign "$tmp/foreign" --frontier "$tmp/frontier.db" \
  --floor 3 >"$tmp/dry-run.json"
bun -e '
import {Database} from "bun:sqlite";
const db=new Database(process.argv[1]);
if(db.query("PRAGMA table_info(internet_source)").all().some(row=>row.name==="carries")) throw new Error("dry-run mutated CMS");
' "$tmp/cms.db"

bun "$root/cmd/BonfyreSourceFrontier/tools/measure_carried_material.mjs" \
  --cms "$tmp/cms.db" --foreign "$tmp/foreign" --frontier "$tmp/frontier.db" \
  --floor 3 --apply --evidence "$tmp/evidence.jsonl" >"$tmp/result.json"

bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], result=JSON.parse(readFileSync(join(base,"result.json"),"utf8"));
if(result.measured!==6||result.no_retained_body!==1||result.carrying!==4||result.written!==4) throw new Error(JSON.stringify(result));
for(const material of ["podcasts","video","images","events"]) if(result.by_material[material]!==1) throw new Error(JSON.stringify(result.by_material));
const cms=new Database(join(base,"cms.db"));
for(const [source,expected] of [["audio","podcasts"],["film","video"],["photo","images"],["calendar","events"],["same",null]]) {
 const row=cms.query("SELECT carries FROM internet_source WHERE source=?").get(source);
 if((row.carries||null)!==expected) throw new Error(source+":"+row.carries);
}
const frontier=new Database(join(base,"frontier.db"));
if(frontier.query("SELECT carries FROM channel WHERE census_source=?").get("film").carries!=="video") throw new Error("frontier not updated");
const evidence=readFileSync(join(base,"evidence.jsonl"),"utf8").trim().split("\n").map(JSON.parse);
if(evidence.length!==4||!evidence.some(row=>row.body==="v.body.gz"&&row.carries.video===3)) throw new Error("evidence mismatch");
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/cmd/BonfyreSourceFrontier/tools/measure_carried_material.py.archive" \
    --cms "$tmp/cms.db" --foreign "$tmp/foreign" --frontier "$tmp/frontier.db" \
    --floor 3 --apply --evidence "$tmp/legacy-evidence.jsonl" >"$tmp/legacy-result.json"
  bun -e '
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1];
const load=name=>JSON.parse(readFileSync(join(base,name),"utf8"));
const native=load("result.json"), legacy=load("legacy-result.json");
delete native.at; delete legacy.at;
if(JSON.stringify(native)!==JSON.stringify(legacy)) throw new Error("legacy count parity failed");
const lines=name=>readFileSync(join(base,name),"utf8").trim().split("\n").map(JSON.parse);
const a=lines("evidence.jsonl"), b=lines("legacy-evidence.jsonl");
if(JSON.stringify(a)!==JSON.stringify(b)) throw new Error("legacy evidence parity failed");
' "$tmp"
fi

echo "measure_carried_material_test: PASS"
