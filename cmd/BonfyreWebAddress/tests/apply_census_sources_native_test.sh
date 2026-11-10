#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tool="$root/cmd/BonfyreWebAddress/tools/apply_census_sources.mjs"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'apply_census_sources_native_test: SKIP (CMS not built)'; exit 0; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/schemas"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/schemas/"
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1

bun "$tool" --dry-run >"$tmp/native-dry.txt"
if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$root/cmd/BonfyreWebAddress/tools/apply_census_sources.py.archive" --dry-run >"$tmp/legacy-dry.txt"
  diff -u "$tmp/legacy-dry.txt" "$tmp/native-dry.txt"
fi

bun "$tool" --only wikipedia-most-read --cms "$cms" --db "$tmp/cms.db" \
  --schemas "$tmp/schemas" >"$tmp/first.txt"
bun "$tool" --only wikipedia-most-read --cms "$cms" --db "$tmp/cms.db" \
  --schemas "$tmp/schemas" >"$tmp/second.txt"
rg -q '^created wikipedia-most-read$' "$tmp/first.txt"
rg -q '^updated wikipedia-most-read$' "$tmp/second.txt"
bun -e '
import {Database} from "bun:sqlite";
const db=new Database(process.argv[1]);
const row=db.query("SELECT source,family,status FROM internet_source WHERE source=?").get("wikipedia-most-read");
if(row.source!=="wikipedia-most-read"||row.family!=="attention"||row.status!=="published") throw new Error(JSON.stringify(row));
if(db.query("SELECT COUNT(*) AS n FROM internet_source WHERE source=?").get("wikipedia-most-read").n!==1) throw new Error("duplicate source");
' "$tmp/cms.db"

bun -e '
import {readFileSync,writeFileSync} from "node:fs";
import {join} from "node:path";
const base=process.argv[1], script=process.argv[2];
const row={source:"fixture",family:"news",url:"https://fixture.test/body",twin:"fixture-twin"};
writeFileSync(join(base,"fixture.json"),JSON.stringify([row]));
globalThis.fetch=async url=>{
 if(url!==row.url) throw new Error("wrong acquisition address");
 return new Response("retained fixture",{status:200});
};
const {main}=await import(script);
if(await main(["--json",join(base,"fixture.json"),"--fetch-into",join(base,"foreign")])!==0) throw new Error("fetch refused");
const body=readFileSync(join(base,"foreign/bodies/fixture-twin.body"),"utf8");
const receipt=JSON.parse(readFileSync(join(base,"foreign/receipts/fixture-twin.json"),"utf8"));
if(body!=="retained fixture"||receipt.body_path!==join(base,"foreign/bodies/fixture-twin.body")) throw new Error("retained body/receipt mismatch");
' "$tmp" "$tool" >"$tmp/fetch.txt"
rg -q '^retained fixture-twin' "$tmp/fetch.txt"
echo 'apply_census_sources_native_test: PASS'
