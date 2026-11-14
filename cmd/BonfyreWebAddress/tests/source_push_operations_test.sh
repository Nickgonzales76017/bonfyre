#!/bin/sh
# Native WebSub operator inspection with no foreign reads, effects, or secret leaks.
set -eu
web=$1
case "$web" in /*) ;; *) web="$(pwd)/$web";; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
[ -x "$auth" ] || exit 1
tmp=$(mktemp -d /tmp/bonfyre-source-push.XXXXXXXX)
pid=""
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ echo "SOURCE_PUSH_FAIL $*" >&2; tail -15 "$tmp/server.log" >&2 || true; exit 1; }
umask 077
export BONFYRE_WEBSUB_DB="$tmp/websub.db" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_MATRIX_DB="$tmp/matrix.db"
export BONFYRE_AUTH_BINARY="$auth"
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$auth" --db "$tmp/auth.db" signup --email owner@example.test --name Operator --password 'fixture-operator-password' >/dev/null
"$auth" --db "$tmp/auth.db" signup --email reader@example.test --name Reader --password 'fixture-operator-password' >/dev/null
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-operator-password' | jq -r .session.token)
reader=$("$auth" --db "$tmp/auth.db" login --email reader@example.test --password 'fixture-operator-password' | jq -r .session.token)
"$auth" --db "$tmp/auth.db" institution-grant --user-id 1 --institution bonfyre --role admin >/dev/null
"$auth" --db "$tmp/auth.db" institution-grant --user-id 2 --institution bonfyre --role read >/dev/null
sqlite3 "$BONFYRE_WEBSUB_DB" "
CREATE TABLE subscriptions(token TEXT PRIMARY KEY,topic TEXT,hub TEXT,secret TEXT,authority_receipt TEXT,state TEXT,lease_expires INTEGER,verified_at TEXT,last_delivery_at TEXT,deliveries INTEGER,rejected INTEGER);
CREATE TABLE source_dirty(token TEXT PRIMARY KEY,topic TEXT,frontier TEXT,state TEXT,dirty_at INTEGER,attempts INTEGER);
CREATE TABLE source_dirty_demand(topic TEXT,identity TEXT,first_requested_at INTEGER,last_requested_at INTEGER,PRIMARY KEY(topic,identity));
CREATE TABLE foreign_delta(token TEXT PRIMARY KEY,topic TEXT,observed_at INTEGER,attempts INTEGER,last_error TEXT);
INSERT INTO subscriptions VALUES('secret-token-1','https://news.example.test/private?credential=never-show','https://hub.example.test','secret-hmac-1','secret-authority','active',strftime('%s','now')+86400,'2026-11-04','2026-11-05',8,2);
INSERT INTO subscriptions VALUES('secret-token-2','https://expired.example.test/rss','https://hub.example.test','secret-hmac-2','','active',strftime('%s','now')-7200,'2026-10-04','',0,0);
INSERT INTO subscriptions VALUES('secret-token-3','https://pending.example.test/feed','https://hub.example.test','secret-hmac-3','','pending',0,'','',0,0);
INSERT INTO source_dirty VALUES('secret-token-1','https://news.example.test/private?credential=never-show','websub-delivery','pending',strftime('%s','now'),1);
INSERT INTO source_dirty_demand VALUES('https://news.example.test/private?credential=never-show','sensitive-material-identity-1',1700000000,1730000000);
INSERT INTO source_dirty_demand VALUES('https://news.example.test/private?credential=never-show','sensitive-material-identity-2',1700000100,1730000100);
INSERT INTO source_dirty_demand VALUES('https://unbound.example.test/feed?credential=never-show','sensitive-material-identity-3',1700000200,1730000200);
INSERT INTO source_dirty VALUES('material-unbound','https://unbound.example.test/feed?credential=never-show','MaterialCompiler:observation-demand','pending',strftime('%s','now'),3);
INSERT INTO foreign_delta VALUES('secret-token-1','https://news.example.test/private?credential=never-show',1730000900,2,'sensitive-handoff-error');
"
port=$((26000 + ($$ % 16000)))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --site-root "$root/site/matrix" --port "$port" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0;until curl -fsS "$base/healthz" >/dev/null 2>&1;do i=$((i+1)); [ "$i" -lt 100 ] || fail 'server boot';sleep .1;done
fetch(){
 if [ -n "$1" ];then
  curl -sS -o "$tmp/response" -w '%{http_code}' -X "$4" -H "Host: $3" -H 'Accept: text/html' -H "Authorization: Bearer $1" "$base$2"
 else
  curl -sS -o "$tmp/response" -w '%{http_code}' -X "$4" -H "Host: $3" -H 'Accept: text/html' "$base$2"
 fi
}
[ "$(fetch '' /sources/push aurekai.work GET)" = 403 ] || fail 'anonymous'
[ "$(fetch "$reader" /sources/push aurekai.work GET)" = 403 ] || fail 'read grant cannot inspect'
[ "$(fetch "$owner" /sources/push aurekai.life GET)" = 404 ] || fail 'apex isolation'
[ "$(fetch "$owner" /sources/push aurekai.work POST)" = 405 ] || fail 'no mutating operation'
[ "$(fetch "$owner" /sources/push aurekai.work GET)" = 200 ] || fail 'admin cannot inspect'
grep -q 'data-source-push-operations' "$tmp/response" || fail 'no inspector'
for domain in news.example.test expired.example.test pending.example.test;do grep -q "$domain" "$tmp/response" || fail "missing $domain";done
for forbidden in secret-token-1 secret-hmac-1 secret-authority never-show;do
 if grep -q "$forbidden" "$tmp/response";then fail "secret exposure $forbidden";fi
done
grep -q 'Stored receipt present (not revalidated here)' "$tmp/response" || fail 'authority receipt indicator'
grep -q 'data-source-material-demands' "$tmp/response" || fail 'material observation ledger missing'
grep -q 'data-source-fact-handoffs' "$tmp/response" || fail 'ForeignTwin FactDelta handoff ledger missing'
grep -q 'data-material-demand' "$tmp/response" || fail 'material demand absent'
grep -q 'data-fact-handoff' "$tmp/response" || fail 'FactDelta handoff absent'
grep -q 'unbound.example.test' "$tmp/response" || fail 'unsubscribed MaterialCompiler source was hidden'
grep -q 'Distinct requests' "$tmp/response" || fail 'coalesced demand counts missing'
grep -q 'Pending downstream notification' "$tmp/response" || fail 'notification state not explicit'
for confidential in sensitive-material-identity sensitive-handoff-error material-unbound;do
 if grep -q "$confidential" "$tmp/response";then fail "private demand or handoff identity exposed: $confidential";fi
done
grep -q 'No retained authority receipt' "$tmp/response" || fail 'unbound source'
for state in expired pending dirty rejected;do
 [ "$(fetch "$owner" "/sources/push?state=$state" aurekai.work GET)" = 200 ] || fail "$state filter"
 case "$state" in expired) yes=expired.example.test;no=news.example.test;;
 pending) yes=pending.example.test;no=expired.example.test;;
 dirty|rejected) yes=news.example.test;no=pending.example.test;;esac
 python3 -c 'import pathlib,sys; t=pathlib.Path(sys.argv[1]).read_text(); s=t.split("<section id=\"subscriptions\">",1)[1].split("</section>",1)[0]; pathlib.Path(sys.argv[2]).write_text(s)' "$tmp/response" "$tmp/subscriptions.html"
 grep -q "$yes" "$tmp/subscriptions.html" || fail "$state missed $yes"
 if grep -q "$no" "$tmp/subscriptions.html";then fail "$state leaked $no";fi
done
[ "$(fetch "$owner" '/sources/push?state=invalid' aurekai.work GET)" = 400 ] || fail 'invalid filter'
[ "$(fetch "$owner" '/sources/push?page=9999' aurekai.work GET)" = 400 ] || fail 'page bound'
[ "$(fetch "$owner" /sources aurekai.work GET)" = 200 ] || fail 'operator sources'
grep -q 'href="/sources/push"' "$tmp/response" || fail 'admin continuation'
[ "$(fetch "$reader" /sources aurekai.work GET)" = 200 ] || fail 'reader sources'
if grep -q 'href="/sources/push"' "$tmp/response";then fail 'admin continuation leak';fi
before=$(sqlite3 "$BONFYRE_WEBSUB_DB" "SELECT sum(deliveries),sum(rejected),sum(lease_expires) FROM subscriptions; SELECT count(*),sum(attempts) FROM source_dirty; SELECT count(*),sum(last_requested_at) FROM source_dirty_demand; SELECT count(*),sum(attempts) FROM foreign_delta")
[ "$(fetch "$owner" '/sources/push?q=news' aurekai.work GET)" = 200 ] || fail 'search'
after=$(sqlite3 "$BONFYRE_WEBSUB_DB" "SELECT sum(deliveries),sum(rejected),sum(lease_expires) FROM subscriptions; SELECT count(*),sum(attempts) FROM source_dirty; SELECT count(*),sum(last_requested_at) FROM source_dirty_demand; SELECT count(*),sum(attempts) FROM foreign_delta")
[ "$before" = "$after" ] || fail 'read mutated subscriptions'
[ "$(fetch "$owner" "/sources/push?q=unbound" aurekai.work GET)" = 200 ] || fail 'unbound source filter'
grep -q "unbound.example.test" "$tmp/response" || fail 'unbound material source missing after filter'
if grep -q "news.example.test" "$tmp/response";then fail 'unrelated source surfaced in filtered views';fi
BONFYRE_PUSH_BROWSER_BASE="$base" BONFYRE_PUSH_BROWSER_TOKEN="$owner" BONFYRE_PUSH_ROOT="$root" node <<'JS'
const assert=require('node:assert/strict'),path=require('node:path');
const {createRequire}=require('node:module');
const root=process.env.BONFYRE_PUSH_ROOT,base=process.env.BONFYRE_PUSH_BROWSER_BASE,token=process.env.BONFYRE_PUSH_BROWSER_TOKEN;
const puppeteer=createRequire(path.join(root,'tools/creator-ingest/package.json'))('puppeteer');
(async()=>{
 const browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking','--disable-extensions']});
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage(),errors=[];
 try{
  page.on('pageerror',e=>errors.push(e.message));
  page.setDefaultTimeout(10000);
  await page.setRequestInterception(true);
  page.on('request',async req=>{
   try{
    const url=new URL(req.url());
    if(url.origin!=='https://aurekai.work'||!['/sources/push','/sources/push/'].includes(url.pathname)){await req.respond({status:204,body:''});return;}
    const response=await new Promise((resolve,reject)=>{
      const endpoint=new URL(base);
      const relay=require("node:http").request({hostname:endpoint.hostname,port:endpoint.port,
        path:url.pathname+url.search,method:req.method(),
        headers:{Host:"aurekai.work",Authorization:"Bearer "+token,Accept:"text/html"}},res=>{
        const chunks=[];res.on("data",chunk=>chunks.push(chunk));
        res.on("end",()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks)}));
      });
      relay.on("error",reject);relay.end();
    });
    const headers={};
    for(const [k,v] of Object.entries(response.headers))
      if(typeof v==="string"&&!["content-length","transfer-encoding","connection","content-encoding","set-cookie"].includes(k))headers[k]=v;
    await req.respond({status:response.status,headers,body:response.body});
   }catch(e){errors.push('transport: '+e.message);await req.abort().catch(()=>{});}
  });
  const navigate=u=>page.goto('https://aurekai.work'+u,{waitUntil:'domcontentloaded'});
  const advance=fn=>Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),fn()]);
  await page.setViewport({width:390,height:844});
  const arrival=await navigate('/sources/push');
  const landing=await page.content();
  assert.equal(arrival?.status(),200,`Source push browser HTTP ${arrival?.status()}: ${landing.slice(0,350)}; errors=${errors.join(" / ")}`);
  assert.equal(await page.$$eval('[data-source-push-row]',n=>n.length),3);
  assert.equal(await page.$$eval('[data-material-demand]',n=>n.length),2);
  assert.equal(await page.$$eval('[data-fact-handoff]',n=>n.length),1);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'390px overflows');
  const copy=await page.$eval('[data-source-push-operations]',n=>n.textContent);
  for(const forbidden of ['secret-token-1','secret-hmac-1','sensitive-material-identity','sensitive-handoff-error','never-show'])
   assert(!copy.includes(forbidden),'private value in rendered source console');
  await page.click('[data-source-push-row] summary');
  assert.match(await page.$eval('[data-source-push-row] details',n=>n.textContent),/Rejected callbacks/);
  await page.select('select[name=state]','expired');
  await advance(()=>page.click('.spo-filters button[type=submit]'));
  assert.equal(await page.$$eval('[data-source-push-row]',n=>n.length),1);
  assert.equal(await page.$$eval('[data-material-demand]',n=>n.length),2,'independent demand panel incorrectly filtered by subscription state');
  await page.$eval('.spo-filters input[name=q]',n=>{n.value='unbound';});
  await page.select('select[name=state]','all');
  await advance(()=>page.click('.spo-filters button[type=submit]'));
  assert.equal(await page.$$eval('[data-source-push-row]',n=>n.length),0);
  assert.equal(await page.$$eval('[data-material-demand]',n=>n.length),1);
  assert.match(await page.$eval('[data-material-demand]',n=>n.textContent),/unbound.example.test/);
  assert.equal(await page.$$eval('[data-fact-handoff]',n=>n.length),0);
  await page.setViewport({width:1365,height:900});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'desktop overflows');
  assert.deepEqual(errors,[]);
  console.log('SOURCE_PUSH_BROWSER_PASS: 390px/desktop, real subscription filters, material demands, FactDelta handoffs, redaction');
 }finally{await page.close();await ctx.close();await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
JS
after_browser=$(sqlite3 "$BONFYRE_WEBSUB_DB" "SELECT sum(deliveries),sum(rejected),sum(lease_expires) FROM subscriptions; SELECT count(*),sum(attempts) FROM source_dirty; SELECT count(*),sum(last_requested_at) FROM source_dirty_demand; SELECT count(*),sum(attempts) FROM foreign_delta")
[ "$before" = "$after_browser" ] || fail 'Chromium inspection changed source carriers'
# Create 31 independent rows in every ledger; no carrier may page another.
python3 - "$BONFYRE_WEBSUB_DB" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
for n in range(31):
    name=f"bulk{n:02d}.example.test"
    topic=f"https://{name}/feed?private=never-show"
    db.execute("INSERT INTO subscriptions VALUES(?,?,?,?,?,'pending',0,'','',0,0)",(f"bulk-token-{n:02d}",topic,"https://bulk-hub.example.test","bulk-secret",""))
    db.execute("INSERT INTO source_dirty_demand VALUES(?,?,?,?)",(topic,f"bulk-private-identity-{n:02d}",1730000000+n,1730000000+n))
    db.execute("INSERT INTO foreign_delta VALUES(?,?,?,?,?)",(f"bulk-token-{n:02d}",topic,1730000000+n,0,"private-error"))
db.commit()
PY
[ "$(fetch "$owner" '/sources/push?state=all' aurekai.work GET)" = 200 ] || fail 'independent page 0'
[ "$(grep -o 'data-source-push-row ' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'subscription first page size'
[ "$(grep -o '<li data-material-demand>' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'demand first page size'
[ "$(grep -o '<li data-fact-handoff>' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'handoff first page size'
for anchor in subscriptions material-demands fact-handoffs;do grep -q "#$anchor" "$tmp/response" || fail "missing ledger page continuation $anchor";done
[ "$(fetch "$owner" '/sources/push?state=all&page=1&dpage=0&fpage=0' aurekai.work GET)" = 200 ] || fail 'subscription page 1'
[ "$(grep -o 'data-source-push-row ' "$tmp/response" | wc -l | tr -d ' ')" = 9 ] || fail 'subscription independent continuation'
[ "$(grep -o '<li data-material-demand>' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'demand moved when subscription paged'
[ "$(grep -o '<li data-fact-handoff>' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'FactDelta moved when subscription paged'
[ "$(fetch "$owner" '/sources/push?state=all&page=0&dpage=1&fpage=0' aurekai.work GET)" = 200 ] || fail 'demand page 1'
[ "$(grep -o '<li data-material-demand>' "$tmp/response" | wc -l | tr -d ' ')" = 8 ] || fail 'demand independent continuation'
[ "$(grep -o 'data-source-push-row ' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'subscription moved when demand paged'
[ "$(grep -o '<li data-fact-handoff>' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'FactDelta moved when demand paged'
[ "$(fetch "$owner" '/sources/push?state=all&page=0&dpage=0&fpage=1' aurekai.work GET)" = 200 ] || fail 'FactDelta page 1'
[ "$(grep -o '<li data-fact-handoff>' "$tmp/response" | wc -l | tr -d ' ')" = 7 ] || fail 'FactDelta independent continuation'
[ "$(grep -o 'data-source-push-row ' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'subscription moved when FactDelta paged'
[ "$(grep -o '<li data-material-demand>' "$tmp/response" | wc -l | tr -d ' ')" = 25 ] || fail 'demand moved when FactDelta paged'
for bad in 'dpage=-1' 'dpage=abc' 'fpage=9999' 'page=501' 'fpage=9z';do
 [ "$(fetch "$owner" "/sources/push?$bad" aurekai.work GET)" = 400 ] || fail "unbounded page accepted $bad"
done
if grep -Eq 'bulk-private-identity|bulk-token-|private-error|private=never-show' "$tmp/response";then fail 'pagination exposed secret';fi
# Exactly 26 rows is the lookahead boundary: a fetched 26th row must still offer More.
sqlite3 "$BONFYRE_WEBSUB_DB" "
DELETE FROM subscriptions WHERE token LIKE 'bulk-token-%' AND token >= 'bulk-token-23';
DELETE FROM source_dirty_demand WHERE topic LIKE 'https://bulk%' AND topic >= 'https://bulk24';
DELETE FROM foreign_delta WHERE token LIKE 'bulk-token-%' AND token >= 'bulk-token-25';
"
[ "$(fetch "$owner" '/sources/push?state=all' aurekai.work GET)" = 200 ] || fail '26-row first page'
for anchor in subscriptions material-demands fact-handoffs;do
 grep -q "#$anchor" "$tmp/response" || fail "exact 26th row lost continuation in $anchor"
done
[ "$(fetch "$owner" '/sources/push?state=all&page=1&dpage=1&fpage=1' aurekai.work GET)" = 200 ] || fail 'three independent final pages'
[ "$(grep -o 'data-source-push-row ' "$tmp/response" | wc -l | tr -d ' ')" = 1 ] || fail '26th subscription missing'
[ "$(grep -o '<li data-material-demand>' "$tmp/response" | wc -l | tr -d ' ')" = 1 ] || fail '26th demand missing'
[ "$(grep -o '<li data-fact-handoff>' "$tmp/response" | wc -l | tr -d ' ')" = 1 ] || fail '26th handoff missing'
grep -q 'page=1&amp;dpage=0&amp;fpage=1#material-demands' "$tmp/response" || fail 'demand previous link lost sibling positions'
grep -q 'page=1&amp;dpage=1&amp;fpage=0#fact-handoffs' "$tmp/response" || fail 'handoff previous link lost sibling positions'
sqlite3 "$BONFYRE_WEBSUB_DB" "DROP TABLE source_dirty_demand; DROP TABLE foreign_delta;"
[ "$(fetch "$owner" /sources/push aurekai.work GET)" = 200 ] || fail 'old WebSub carrier cannot open after optional ledgers are absent'
grep -q "material-demand ledger is not yet available" "$tmp/response" || fail 'missing-demand contract not disclosed'
grep -q "FactDelta handoff ledger is not yet available" "$tmp/response" || fail 'missing-handoff contract not disclosed'
"$auth" --db "$tmp/auth.db" institution-revoke --user-id 1 --institution bonfyre >/dev/null
[ "$(fetch "$owner" /sources/push aurekai.work GET)" = 403 ] || fail 'revocation ignored'
echo 'SOURCE_PUSH_PASS: active, pending, expired, dirty and rejected WebSub; scoped, revocable, secret-safe, no writes'
