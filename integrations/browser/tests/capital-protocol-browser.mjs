// A synthetic browser integration witness, not a lender, payment or policy engine.
// Reuses the retained site/matrix DuckDB/Arrow assets and existing Chrome owner.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { withChrome } from '../capture/chrome-session.mjs';
const root = process.env.BONFYRE_ROOT || resolve(import.meta.dir, '../../..');
const assets = join(root, 'site/matrix');
const scratch = mkdtempSync(join(tmpdir(), 'aurekai-capital-browser-'));
const profile = join(scratch, 'profile'); mkdirSync(profile);
const serialize = value => JSON.stringify(value, (_, v) => typeof v === 'bigint' ? v.toString() : v);
const assetNames = ['duckdb-browser.mjs', 'duckdb-browser-eh.worker.js', 'duckdb-eh.wasm', 'apache-arrow.mjs', 'flatbuffers.mjs', 'tslib.mjs'];
const manifest = assetNames.map(name => { const b = readFileSync(join(assets, name)); return { name, bytes: b.length, sha256: createHash('sha256').update(b).digest('hex') }; });
const html = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Aurekai capital workflow test</title><style>body{font:17px system-ui;max-width:70rem;margin:2rem auto;padding:1rem}button{padding:.75rem;margin:.4rem}pre{white-space:pre-wrap;overflow-wrap:anywhere}section{margin:1.5rem 0}</style>
<script type="importmap">{"imports":{"apache-arrow":"/assets/apache-arrow.mjs","flatbuffers":"/assets/flatbuffers.mjs","tslib":"/assets/tslib.mjs"}}</script>
<script type="module" src="/lab.mjs"></script></head><body><h1>Prepare once. Compare the next decision.</h1>
<p>Synthetic capital-workflow exercise. No bank connection, loan application or insurance cover is created.</p>
<section><button id="analyze" disabled>Compare cash and financing</button><button id="snapshot" disabled>Save a review snapshot</button><button id="correct" disabled>Correct the transport estimate</button></section>
<p id="state" role="status">Opening the retained browser engine...</p><pre id="result"></pre></body></html>`;
const browserModule = String.raw`
import * as duckdb from '/assets/duckdb-browser.mjs';
const text = v => JSON.stringify(v, (_, x) => typeof x === 'bigint' ? x.toString() : x, 2);
const state = document.querySelector('#state'), result = document.querySelector('#result');
const started = performance.now();
const worker = new Worker('/assets/duckdb-browser-eh.worker.js');
const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
window.lab = { ready: false, error: null };
async function hash(s) { return [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s)))].map(x => x.toString(16).padStart(2, '0')).join(''); }
try {
  await db.instantiate('/assets/duckdb-eh.wasm');
  await db.open({path:'opfs://capital-protocol-lab.duckdb',accessMode:duckdb.DuckDBAccessMode.READ_WRITE});
  const conn = await db.connect();
  const query = async sql => JSON.parse(text((await conn.query(sql)).toArray()));
  await conn.query("CREATE TABLE IF NOT EXISTS bank_events(provider VARCHAR, event_id BIGINT, amount_cents BIGINT, PRIMARY KEY(provider,event_id)); CREATE TABLE IF NOT EXISTS facts(field VARCHAR PRIMARY KEY, value VARCHAR, source_id VARCHAR, revision INTEGER); CREATE TABLE IF NOT EXISTS snapshots(id VARCHAR PRIMARY KEY, payload VARCHAR, sha256 VARCHAR)");
  await conn.query("INSERT INTO facts VALUES ('business_name','Example Fabrication LLC','synthetic:company',1),('equipment_cost_cents','1300000','synthetic:supplier-quote',1),('transport_cost_cents','120000','synthetic:transport-quote',1),('insurance_cost_cents','30000','synthetic:insurance-estimate',1),('bank_months','12','synthetic:bank-report',1),('requested_finance_cents','1000000','synthetic:funding-choice',1) ON CONFLICT DO NOTHING");
  const seed = async () => { const at=performance.now(); await conn.query("INSERT INTO bank_events SELECT 'fixture-provider',i,CASE WHEN i%2=0 THEN -1000 ELSE 1200 END FROM range(1,100001) t(i) ON CONFLICT DO NOTHING"); await conn.query('CHECKPOINT'); return {elapsed_ms:performance.now()-at,...(await query('SELECT count(*)::VARCHAR records, sum(amount_cents)::VARCHAR net_cents FROM bank_events'))[0]}; };
  const analyze = async () => {
    const at=performance.now();
    const rows=await query("WITH costs AS (SELECT max(CASE WHEN field='equipment_cost_cents' THEN value::BIGINT END) equipment, max(CASE WHEN field='transport_cost_cents' THEN value::BIGINT END) transport, max(CASE WHEN field='insurance_cost_cents' THEN value::BIGINT END) insurance, max(CASE WHEN field='requested_finance_cents' THEN value::BIGINT END) requested FROM facts), options(route,days,principal,apr_bps,origination_cents) AS (VALUES ('own_cash',30,0::BIGINT,0::BIGINT,0::BIGINT),('funded_30_days',30,(SELECT requested FROM costs),1800::BIGINT,10000::BIGINT),('funded_90_days',90,(SELECT requested FROM costs),1800::BIGINT,10000::BIGINT)), fees AS (SELECT *,((principal*apr_bps*days+3649999)//3650000)+origination_cents AS fee FROM options), events AS (SELECT route,1 ord,principal cents,0::BIGINT profit FROM fees UNION ALL SELECT route,2,-equipment,-equipment FROM fees CROSS JOIN costs UNION ALL SELECT route,3,-transport,-transport FROM fees CROSS JOIN costs UNION ALL SELECT route,4,-insurance,-insurance FROM fees CROSS JOIN costs UNION ALL SELECT route,5,2000000,2000000 FROM fees UNION ALL SELECT route,6,-principal-fee,-fee FROM fees), balances AS(SELECT *,sum(cents) OVER(PARTITION BY route ORDER BY ord ROWS UNBOUNDED PRECEDING) running FROM events) SELECT route, greatest(0,-min(running))::VARCHAR peak_own_cash_cents,sum(profit)::VARCHAR contribution_cents,sum(cents)::VARCHAR net_cash_cents FROM balances GROUP BY route ORDER BY route");
    const output={rows,elapsed_ms:performance.now()-at,basis:'synthetic; fees are illustrative, not a lender quote'}; result.textContent=text(output); return output;
  };
  const snapshot = async () => {
    const req=await fetch('/api/lender-profile').then(r=>r.json());
    const fields=await query('SELECT * FROM facts ORDER BY field');
    const wanted=fields.filter(f=>req.fields.includes(f.field));
    const missing=req.fields.filter(key=>!wanted.some(f=>f.field===key));
    if(missing.length)throw Error('Missing required source fields: '+missing.join(','));
    const amount=wanted.filter(f=>/_cost_cents$/.test(f.field)).reduce((sum,f)=>sum+BigInt(f.value),0n);
    const packet={schema:'synthetic.funding-review.v1',lender_profile:req.id,submission_state:'local_review_only',project_cost_cents:amount.toString(),requested_finance_cents:wanted.find(f=>f.field==='requested_finance_cents').value,facts:wanted,source_refs:[...new Set(wanted.map(f=>f.source_id))]};
    const payload=text(packet), digest=await hash(payload), stmt=await conn.prepare('INSERT INTO snapshots VALUES (?,?,?) ON CONFLICT DO NOTHING');
    await stmt.query(digest,payload,digest); await stmt.close(); await conn.query('CHECKPOINT');
    result.textContent=payload; state.textContent='Review snapshot saved locally. Nothing submitted.'; return {id:digest,packet};
  };
  const correct = async () => {await conn.query("UPDATE facts SET value='170000',revision=revision+1 WHERE field='transport_cost_cents'; CHECKPOINT");state.textContent='Current transport estimate changed; saved snapshots remain unchanged.';};
  const invariantChecks = async () => {
    const cents=(await query("SELECT (CAST('0.10' AS DECIMAL(18,2))+CAST('0.20' AS DECIMAL(18,2)))::VARCHAR amount"))[0];
    const unknown=(await query('SELECT (NULL::BIGINT-40000-10000)::VARCHAR contribution'))[0];
    const status=await query("SELECT status,status='settled' AS recognized_settlement FROM (VALUES('accepted'),('settled'),('reversed')) t(status)");
    const overlaps=await query("WITH assignments(asset,provider,active) AS(VALUES('asset-A','lender-A',true),('asset-A','lender-B',true),('asset-B','lender-A',true)) SELECT asset,count(*)::VARCHAR claims FROM assignments WHERE active GROUP BY asset HAVING count(*)>1");
    return {cents,unknown,status,overlaps};
  };
  window.lab={ready:true,query,seed,analyze,snapshot,correct,invariantChecks,db_version:await db.getVersion(),package_version:duckdb.PACKAGE_VERSION,init_ms:performance.now()-started,shutdown:async()=>{await conn.query('CHECKPOINT');await conn.close();await db.terminate();}};
  for(const [id,fn]of [['analyze',analyze],['snapshot',snapshot],['correct',correct]]){const b=document.getElementById(id);b.disabled=false;b.onclick=()=>fn().catch(e=>{state.textContent=e.message;});}
  state.textContent='Ready. Source references stay attached to each review version.';
}catch(error){window.lab.error=String(error.stack||error);state.textContent='The browser engine did not start: '+error.message;}
`;
const requestLog=[];
const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch(request){
  const url=new URL(request.url), path=url.pathname;requestLog.push({path,method:request.method});
  const headers={'X-Content-Type-Options':'nosniff','Cache-Control':'no-store','Content-Security-Policy':"default-src 'self'; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; worker-src 'self' blob:; connect-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:"};
  if(request.method!=='GET')return new Response('Method not allowed',{status:405,headers});
  if(path==='/')return new Response(html,{headers:{...headers,'content-type':'text/html;charset=utf-8'}});
  if(path==='/lab.mjs')return new Response(browserModule,{headers:{...headers,'content-type':'text/javascript'}});
  if(path==='/api/lender-profile')return Response.json({id:'synthetic-profile-A',fields:['business_name','equipment_cost_cents','transport_cost_cents','insurance_cost_cents','bank_months','requested_finance_cents'],live_lender:false},{headers});
  if(path==='/api/health')return Response.json({status:'ok',environment:'synthetic-lab',money_movement:false},{headers});
  const name=path.startsWith('/assets/')?path.slice(8):'';
  if(assetNames.includes(name))return new Response(readFileSync(join(assets,name)),{headers:{...headers,'content-type':name.endsWith('.wasm')?'application/wasm':'text/javascript'}});
  return new Response('Not found',{status:404,headers});
}});
const origin=`http://127.0.0.1:${server.port}`;
const report={schema:'aurekai.capital-browser-witness.v1',observed_at:new Date().toISOString(),origin,synthetic:true,external_financial_effects:0,asset_manifest:manifest,checks:[],browser_runs:[]};
const check=(name,ok)=>{assert.ok(ok,name);report.checks.push({name,passed:true});};
async function visit(second=false){
 return withChrome(async({open,events})=>{
  const tab=await open();await tab.go(origin);
  const read=async expression=>{const r=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value;};
  for(let i=0;i<300;i++){const state=await read('({ready:window.lab?.ready,error:window.lab?.error})');if(state.error)throw Error(state.error);if(state.ready)break;await Bun.sleep(100);}
  check(second?'second browser engine ready':'first browser engine ready',await read('Boolean(window.lab?.ready)'));
  const info=await read('({engine:lab.db_version,package:lab.package_version,init_ms:lab.init_ms})');report.browser_runs.push(info);
  if(second){
   const saved=await read("lab.query('SELECT count(*)::VARCHAR n FROM bank_events')");check('100000 rows survive a new browser process',saved[0].n==='100000');
   const snaps=await read("lab.query('SELECT count(*)::VARCHAR n FROM snapshots')");check('two review snapshots survive restart',snaps[0].n==='2');
   const now=await read("lab.query(\"SELECT value,revision FROM facts WHERE field='transport_cost_cents'\")");check('corrected source version survives restart',now[0].value==='170000'&&now[0].revision===2);
  }else{
   report.first_seed=await read('lab.seed()');check('100000 source events inserted',report.first_seed.records==='100000');check('cash total matches exact fixture',report.first_seed.net_cents==='10000000');
   report.replay_seed=await read('lab.seed()');check('replay creates no extra events',report.replay_seed.records==='100000'&&report.replay_seed.net_cents==='10000000');
   report.analysis=await read('lab.analyze()');const by=Object.fromEntries(report.analysis.rows.map(r=>[r.route,r]));
   check('own-funded contribution',by.own_cash.contribution_cents==='550000');check('own-funded peak cash',by.own_cash.peak_own_cash_cents==='1450000');
   check('financing changes peak own cash',by.funded_30_days.peak_own_cash_cents==='450000');check('30-day funding cost included',by.funded_30_days.contribution_cents==='525205');
   check('90-day funding cost included',by.funded_90_days.contribution_cents==='495616');check('borrowing is not revenue',report.analysis.rows.every(r=>r.contribution_cents===r.net_cash_cents));
   report.invariants=await read('lab.invariantChecks()');check('exact decimal arithmetic',report.invariants.cents.amount==='0.30');check('unknown proceeds remain unknown',report.invariants.unknown.contribution===null);
   check('acceptance and reversal are not settlement',report.invariants.status.every(r=>r.recognized_settlement===(r.status==='settled')));
   check('overlapping collateral detected',report.invariants.overlaps.length===1&&report.invariants.overlaps[0].asset==='asset-A');
   report.first_snapshot=await read('lab.snapshot()');check('application fields reused from source',report.first_snapshot.packet.facts.length===6&&report.first_snapshot.packet.source_refs.length===6);
   check('project cost and requested debt are distinct',report.first_snapshot.packet.project_cost_cents==='1450000'&&report.first_snapshot.packet.requested_finance_cents==='1000000');
   await read('lab.correct()');report.second_snapshot=await read('lab.snapshot()');check('correction makes new version',report.second_snapshot.id!==report.first_snapshot.id);
   check('cost correction does not silently request more debt',report.second_snapshot.packet.project_cost_cents==='1500000'&&report.second_snapshot.packet.requested_finance_cents==='1000000');
   report.corrected_analysis=await read('lab.analyze()');const corrected=Object.fromEntries(report.corrected_analysis.rows.map(r=>[r.route,r]));
   check('cash analysis consumes the corrected source',corrected.own_cash.contribution_cents==='500000'&&corrected.funded_30_days.peak_own_cash_cents==='500000');
   check('financing analysis retains the same funding choice',corrected.funded_30_days.contribution_cents==='475205');
   const before=await read(`lab.query("SELECT payload FROM snapshots WHERE id='${report.first_snapshot.id}'")`);check('previous lender review snapshot immutable',JSON.parse(before[0].payload).project_cost_cents==='1450000');
   const again=await read('lab.snapshot()');check('identical snapshot reuses its identity',again.id===report.second_snapshot.id);
   const counts=await read("lab.query('SELECT count(*)::VARCHAR n FROM snapshots')");check('snapshot replay stays deduplicated',counts[0].n==='2');
   for(const width of[390,1440]){await tab.page('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:width<721});check('no horizontal overflow at '+width,await read('document.documentElement.scrollWidth<=innerWidth'));}
  }
  const remote=events.filter(e=>e.method==='Network.requestWillBeSent').map(e=>e.params.request.url).filter(u=>/^https?:/.test(u)&&new URL(u).origin!==origin);
  check('no remote HTTP observed in browser run '+report.browser_runs.length,remote.length===0);
  await read('lab.shutdown()');
 },{profile,timeoutMs:90000});
}
try{
 const health=await fetch(origin+'/api/health');check('real loopback health response',health.status===200&&(await health.json()).money_movement===false);
 const bad=await fetch(origin+'/api/lender-profile',{method:'POST'});check('unavailable submission refused',bad.status===405);
 const missing=await fetch(origin+'/api/make-payment');check('undeclared payment route absent',missing.status===404);
 await visit();await visit(true);
 check('no actual application POST from browser',requestLog.filter(r=>r.method==='POST').length===1);
 report.status='passed';report.request_log=requestLog;
}catch(error){report.status='failed';report.error=String(error.stack||error);process.exitCode=1;}
finally{server.stop(true);rmSync(scratch,{recursive:true,force:true});}
const out=process.argv.indexOf('--out');if(out!==-1){const file=resolve(process.argv[out+1]||'');if(!file.startsWith(join(root,'packs')+'/'))throw Error('Write evidence under packs');writeFileSync(file,serialize(report)+'\n',{flag:'wx',mode:0o600});}
console.log(serialize(report));
