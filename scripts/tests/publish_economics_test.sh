#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/estate"

bun -e '
import {Database} from "bun:sqlite";
import {join} from "node:path";
const base=process.argv[1];
const meter=new Database(join(base,"meter.db"));
meter.exec("CREATE TABLE rate(id INTEGER,provider TEXT,op TEXT,unit TEXT,amount REAL,currency TEXT,effective_at TEXT,evidence_ref TEXT); CREATE TABLE events(timestamp TEXT,cost REAL,provider TEXT,op TEXT,rate_id INTEGER,key_id TEXT,receipt_ref TEXT,physical_attempt_ref TEXT,realization_ref TEXT)");
meter.query("INSERT INTO rate VALUES(?,?,?,?,?,?,?,?)").run(1,"reader","fetch","request",0.25,"USD","2026-09-01","rate:1");
const add=meter.query("INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?)");
add.run("2026-09-28T10:00:00Z",0.25,"reader","fetch",1,"key:a","receipt:1","attempt:1","realization:1");
add.run("2026-09-29T10:00:00Z",null,"reader","fetch",null,"key:b","","","");
meter.close();
const cms=new Database(join(base,"cms.db"));
cms.exec("CREATE TABLE program_instance(instance_id TEXT,owner_subject TEXT,site_id TEXT,archetype TEXT,status TEXT); CREATE TABLE program_requirement(instance_id TEXT,state TEXT,status TEXT); CREATE TABLE funding_work_package(instance_id TEXT,status TEXT); CREATE TABLE site_presentation(host TEXT,site_id TEXT,subject_address TEXT,status TEXT)");
cms.query("INSERT INTO program_instance VALUES(?,?,?,?,?)").run("p1","institution:x","site:x","education","published");
cms.query("INSERT INTO program_requirement VALUES(?,?,?)").run("p1","open","published");
cms.query("INSERT INTO funding_work_package VALUES(?,?)").run("p1","published");
cms.query("INSERT INTO site_presentation VALUES(?,?,?,?)").run("x.aurekai.life","site:x","institution:x","published");
cms.close();
' "$tmp/estate"

bun "$root/scripts/publish_economics.mjs" --estate "$tmp/estate" --out "$tmp/native" >"$tmp/native.json"
bun -e '
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], report=JSON.parse(readFileSync(join(base,"native.json"),"utf8"));
if(report.pages!==2||report.metered_events!==2) throw new Error(JSON.stringify(report));
const estate=readFileSync(join(base,"native/economics.html"),"utf8");
const host=readFileSync(join(base,"native/hosts/x.aurekai.life/economics.html"),"utf8");
for(const term of ["unpriced","Evidence coverage","data-bf-series","Programs running here","not authority to spend"])
 if(!estate.includes(term)) throw new Error("missing "+term);
if(!host.includes("institution:x")) throw new Error("host subject missing");
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/scripts/publish_economics.py.archive" --estate "$tmp/estate" --out "$tmp/legacy" >"$tmp/legacy.json"
  diff -u "$tmp/legacy/economics.html" "$tmp/native/economics.html"
  diff -u "$tmp/legacy/hosts/x.aurekai.life/economics.html" "$tmp/native/hosts/x.aurekai.life/economics.html"
fi
echo "publish_economics_test: PASS"
