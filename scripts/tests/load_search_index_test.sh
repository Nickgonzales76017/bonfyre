#!/bin/sh
# A repeat estate publication merges its address-keyed search projection.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

bun -e '
import {Database} from "bun:sqlite";
const db=new Database(process.argv[1]);
db.exec("CREATE TABLE item(source TEXT,key TEXT,kind TEXT,family TEXT,title TEXT,body TEXT,href TEXT,image TEXT,publisher TEXT,at INTEGER,first_read TEXT)");
db.query("INSERT INTO item VALUES(?,?,?,?,?,?,?,?,?,?,?)").run("podcast-source","episode-1","audio","podcasts","A public episode","summary","https://example.test/e/1","","Publisher",123,"2026-09-22T00:00:00Z");
db.close();
' "$tmp/items.db"

bun -e '
import {writeFileSync} from "node:fs";
let requests=0;
const server=Bun.serve({hostname:"127.0.0.1",port:0,async fetch(request){
 requests++;
 const rows=await request.json();
 const url=new URL(request.url);
 const good=url.pathname==="/rest/v1/estate_item" && url.searchParams.get("on_conflict")==="address" &&
   request.headers.get("Prefer")==="resolution=merge-duplicates,return=minimal" &&
   rows.length===1 && rows[0].address==="o/podcast-source/episode-1";
 const status=good?(requests===1?500:201):409;
 if(requests===2) setTimeout(()=>server.stop(),50);
 return new Response("",{status});
}});
writeFileSync(process.argv[1],String(server.port));
' "$tmp/port" &
server=$!
for _ in $(seq 1 50); do [ -s "$tmp/port" ] && break; sleep 0.05; done
[ -s "$tmp/port" ] || { kill "$server" 2>/dev/null || true; exit 1; }
port=$(sed -n '1p' "$tmp/port")

result=$(SUPABASE_PROJECT_REF=test SUPABASE_SERVICE_ROLE_KEY=test \
    bun "$root/scripts/load_search_index.mjs" --archive "$tmp/items.db" \
    --url "http://127.0.0.1:$port/rest/v1/estate_item")
wait "$server"
printf '%s' "$result" | grep -q '"loaded":1'
printf '%s' "$result" | grep -q '"refused":0'
bun -e '
const {flush}=await import(process.argv[1]);
let calls=0;
globalThis.fetch=async (_url,request)=>{
 calls++;
 const rows=JSON.parse(request.body);
 return {ok:!rows.some(row=>row.address==="poison"),status:409,arrayBuffer:async()=>new ArrayBuffer(0)};
};
const result={sent:0,failed:0};
await flush([{address:"a"},{address:"poison"},{address:"b"}],
 {url:"http://localhost/",key:"test",attempts:1},result);
if(result.sent!==2||result.failed!==1||calls<3) throw new Error(JSON.stringify({result,calls}));
' "$root/scripts/load_search_index.mjs" 2>"$tmp/refusal.log"
grep -q 'poison' "$tmp/refusal.log"
echo 'search index upsert: PASS'
