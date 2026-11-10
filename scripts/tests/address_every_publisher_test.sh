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
const archive=new Database(join(base,"items.db"));
archive.exec("CREATE TABLE item(publisher TEXT,source TEXT,family TEXT,kind TEXT,at INTEGER)");
const add=archive.query("INSERT INTO item VALUES(?,?,?,?,?)");
for(const row of [["Publisher & Co","source-a","news","story",100],
 ["Publisher & Co","source-a","news","story",101],
 ["Publisher & Co","source-b","events","event",102],
 ["Too Small","source-c","news","story",100]]) add.run(...row);
archive.close();
const cms=new Database(join(base,"cms.db"));
cms.exec("CREATE TABLE site_presentation(namespace TEXT,created_at TEXT,updated_at TEXT,published_at TEXT,status TEXT,site_id TEXT,host TEXT,subject_address TEXT,profile TEXT,territory TEXT,navigation TEXT)");
cms.close();
const fabric=new Database(join(base,"subject_fabric.db"));
fabric.exec("CREATE TABLE subject_reader(subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,material TEXT,protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT, UNIQUE(subject,scope,census_source,root_id,channel_id,material)); CREATE TABLE subject_bundle(subject TEXT,scope TEXT,bundle_digest TEXT,closure_digest TEXT,scene_digest TEXT,compiled_at TEXT,counts_json TEXT,payload_json TEXT, UNIQUE(subject,scope))");
fabric.close();
' "$tmp/estate"

bun "$root/scripts/address_every_publisher.mjs" --estate "$tmp/estate" --repo "$root" \
  --min-items 3 >"$tmp/dry.json"
bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], row=JSON.parse(readFileSync(join(base,"dry.json"),"utf8"));
if(row.candidates!==1||row.to_address!==1||row.sample[0]!=="publisher-co.aurekai.life") throw new Error(JSON.stringify(row));
const db=new Database(join(base,"estate/cms.db"));
if(db.query("SELECT COUNT(*) AS n FROM site_presentation").get().n!==0) throw new Error("dry run wrote CMS");
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  mkdir -p "$tmp/legacy"
  cp "$tmp/estate/items.db" "$tmp/legacy/items.db"
  cp "$tmp/estate/cms.db" "$tmp/legacy/cms.db"
  cp "$tmp/estate/subject_fabric.db" "$tmp/legacy/subject_fabric.db"
fi

bun "$root/scripts/address_every_publisher.mjs" --estate "$tmp/estate" --repo "$root" \
  --min-items 3 --apply >"$tmp/native.json"
bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], result=JSON.parse(readFileSync(join(base,"native.json"),"utf8"));
if(result.addressed!==1||result.refused!==0||result.readers!==2) throw new Error(JSON.stringify(result));
const cms=new Database(join(base,"estate/cms.db"));
const site=cms.query("SELECT host,subject_address FROM site_presentation").get();
if(site.host!=="publisher-co.aurekai.life"||site.subject_address!=="institution:publisher:publisher-co") throw new Error(JSON.stringify(site));
const fabric=new Database(join(base,"estate/subject_fabric.db"));
if(fabric.query("SELECT COUNT(*) AS n FROM subject_reader").get().n!==2) throw new Error("readers missing");
const bundle=fabric.query("SELECT * FROM subject_bundle").get();
if(!bundle||bundle.bundle_digest.length!==64||!JSON.parse(bundle.payload_json).cannot_infer) throw new Error("bundle invalid");
' "$tmp"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/scripts/address_every_publisher.py.archive" --estate "$tmp/legacy" --repo "$root" \
    --min-items 3 --apply >"$tmp/legacy.json"
  bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1];
const report=name=>JSON.parse(readFileSync(join(base,name+".json"),"utf8"));
if(JSON.stringify(report("native"))!==JSON.stringify(report("legacy"))) throw new Error("report parity");
const bundle=name=>new Database(join(base,name,"subject_fabric.db")).query("SELECT bundle_digest,closure_digest,counts_json,payload_json FROM subject_bundle").get();
if(JSON.stringify(bundle("estate"))!==JSON.stringify(bundle("legacy"))) throw new Error("bundle parity");
' "$tmp"
fi

echo "address_every_publisher_test: PASS"
