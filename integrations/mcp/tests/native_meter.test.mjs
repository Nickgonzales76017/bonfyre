import {afterAll,afterEach,beforeAll,test} from 'bun:test';
import {Database} from 'bun:sqlite';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {existsSync,mkdtempSync,rmSync,symlinkSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createNativeMeter,nativeMeterFromEnvironment,runNativeMeter} from '../native_meter.mjs';
import {encode,decode} from '../../browser/workspace-msgpack.mjs';

const roots=[];
let binary,build;
const source=fileURLToPath(new URL('../../../',import.meta.url));
const at='2026-10-21T12:00:00Z';
const input=()=>({key:'institution:studio',operation:'model-generation',provider:'native-moe',receiptRef:'sha256:'+'a'.repeat(64),
  attemptRef:'fabric:attempt:test-one',realizationRef:'sha256:'+'b'.repeat(64),at,bytes:1024,durationMs:1500});
const quote=()=>({provider:'native-moe',operation:'model-generation',at});
const reply=value=>({exitCode:0,stdout:encode([1,true,value])});
const retained=overrides=>({state:'recorded',eventId:1,key:'institution:studio',operation:'model-generation',provider:'native-moe',
  attemptRef:'fabric:attempt:test-one',receiptRef:'sha256:'+'a'.repeat(64),realizationRef:'sha256:'+'b'.repeat(64),at,
  outputBytes:1024,durationMs:1500,pricing:{state:'unknown'},...overrides});
function fixture(){const root=mkdtempSync(join(tmpdir(),'native-meter-'));roots.push(root);const db=join(root,'meter.db');return {root,db,meter:createNativeMeter({binary,db})};}
beforeAll(()=>{
  if(process.env.METER_TEST_BINARY){binary=resolve(process.env.METER_TEST_BINARY);return;}
  build=mkdtempSync(join(tmpdir(),'native-meter-build-'));binary=join(build,'bonfyre-meter');
  const lib=join(source,'lib/libbonfyre');
  const flags=value=>(value||'').split(/\s+/).filter(Boolean);
  const compiled=spawnSync(process.env.CC||'cc',['-O1','-g','-std=c11','-D_POSIX_C_SOURCE=200809L','-D_DEFAULT_SOURCE','-DBONFYRE_SHA256_PORTABLE',
    '-I'+join(lib,'include'),'-I'+join(source,'cmd/BonfyreLex/src'),...flags(process.env.METER_TEST_CFLAGS),
    join(source,'cmd/BonfyreMeter/src/main.c'),join(lib,'src/bf_common.c'),join(lib,'src/bf_sha256.c'),join(lib,'src/bf_sqlite.c'),join(lib,'bf_msgpack.c'),join(lib,'bf_csv.c'),
    ...flags(process.env.METER_TEST_LDFLAGS||'-lsqlite3'),'-o',binary],{encoding:'utf8',timeout:60000});
  assert.equal(compiled.status,0,compiled.stderr);
});
afterEach(()=>{for(const root of roots.splice(0))rmSync(root,{recursive:true,force:true});});
afterAll(()=>{if(build)rmSync(build,{recursive:true,force:true});});

test('native quote observes absent pricing without creating a store or inventing zero',async()=>{
  const f=fixture();assert.deepEqual(await f.meter.quote(quote()),{state:'unknown'});assert.equal(existsSync(f.db),false);
});
test('native record retains exact measured attempt, unpriced state and replay identity',async()=>{
  const f=fixture(),value=input(),first=await f.meter.record(value),again=await f.meter.record(value);
  assert.deepEqual(first,retained());assert.deepEqual(again,first);
  const db=new Database(f.db,{readonly:true});try{const row=db.query('SELECT COUNT(*) AS n,SUM(bytes) AS bytes,MAX(cost) AS cost,MAX(rate_id) AS rate FROM events').get();assert.deepEqual(row,{n:1,bytes:1024,cost:null,rate:null});}finally{db.close();}
});
test('native changed account, bytes, receipt and timestamp cannot reuse a physical attempt',async()=>{
  const f=fixture();await f.meter.record(input());
  for(const changes of [{key:'user:2'},{bytes:1025},{receiptRef:'sha256:'+'c'.repeat(64)},{at:'2026-10-21T12:00:01Z'}])
    await assert.rejects(()=>f.meter.record({...input(),...changes}),error=>error.code==='meter_refused'&&error.recordingMayHaveSucceeded===true);
  const db=new Database(f.db,{readonly:true});try{assert.equal(db.query('SELECT COUNT(*) AS n FROM events').get().n,1);}finally{db.close();}
});
test('native admitted unit rate and usage cost remain distinct and preserve event-time rate',async()=>{
  const f=fixture();
  const admit=(amount,effective,evidence)=>{
    const result=spawnSync(binary,['admit-rate','--db',f.db,'--provider','native-moe','--op','model-generation','--unit','second',
      '--amount',String(amount),'--currency','USD','--effective-at',effective,'--evidence',evidence],{encoding:'utf8',timeout:5000});assert.equal(result.status,0,result.stderr);
  };
  admit(0.2,'2026-10-01T00:00:00Z','fixture-rate:october');
  admit(0.4,'2026-11-01T00:00:00Z','fixture-rate:november');
  const price={state:'priced',rateId:1,amount:0.2,currency:'USD',unit:'second',effectiveAt:'2026-10-01T00:00:00Z',evidenceRef:'fixture-rate:october'};
  assert.deepEqual(await f.meter.quote(quote()),price);
  const first=await f.meter.record(input());assert.deepEqual({...first.pricing,cost:undefined},{...price,cost:undefined});assert.ok(Math.abs(first.pricing.cost-0.3)<1e-12);
  admit(0.8,'2026-10-20T00:00:00Z','fixture-rate:late-admission');
  assert.deepEqual(await f.meter.record(input()),first,'replay cannot reprice an observed attempt');
  assert.equal((await f.meter.quote({...quote(),at:'2026-09-30T00:00:00Z'})).state,'unknown');
});
test('native readonly quote refuses damaged store, symlink, unknown flags and invalid timestamps',async()=>{
  const f=fixture();writeFileSync(f.db,'not sqlite');await assert.rejects(()=>f.meter.quote(quote()),{code:'meter_unavailable'});
  const link=join(f.root,'linked.db');symlinkSync(f.db,link);await assert.rejects(()=>createNativeMeter({binary,db:link}).quote(quote()),{code:'meter_unavailable'});
  for(const args of [
    ['quote-packet','--db',f.db,'--provider','native-moe','--op','model-generation','--at','2026-02-30T00:00:00Z'],
    ['quote-packet','--db',f.db,'--provider','native-moe','--op','model-generation','--at',at,'--host','example.invalid']
  ]){const result=await runNativeMeter([binary,...args]);assert.equal(result.exitCode,64);assert.deepEqual(decode(result.stdout).slice(0,2),[1,false]);}
});
test('quote rejects mismatched owner observations and unknown pricing shapes',async()=>{
  for(const row of [
    {...quote(),provider:'another-provider',pricing:{state:'unknown'}},
    {...quote(),pricing:{state:'unknown',amount:0}},
    {...quote(),pricing:{state:'priced',rateId:1,amount:0,currency:'USD',unit:'token',effectiveAt:at,evidenceRef:'fixture:rate'}},
    {...quote(),pricing:{state:'priced',rateId:1,amount:0,currency:'USD',unit:'call',effectiveAt:at,evidenceRef:'/private/rate'}}
  ])await assert.rejects(()=>createNativeMeter({binary,db:'/unused/meter.db',run:async()=>reply(row)}).quote(quote()),{code:'meter_unavailable'});
});
test('record readback must bind every physical-attempt field and never parse diagnostics',async()=>{
  const rows=[retained({key:'user:2'}),retained({attemptRef:'other:attempt'}),retained({receiptRef:'other:receipt'}),retained({realizationRef:'other:bytes'}),
    retained({outputBytes:1025}),retained({durationMs:0}),retained({at:'2026-10-21T12:00:01Z'}),retained({pricing:{state:'unknown',cost:0}})];
  for(const row of rows)await assert.rejects(()=>createNativeMeter({binary,db:'/unused/meter.db',run:async()=>reply(row)}).record(input()),error=>error.code==='meter_unavailable'&&error.recordingMayHaveSucceeded);
  await assert.rejects(()=>createNativeMeter({binary,db:'/unused/meter.db',run:async()=>({exitCode:0,stdout:'[meter] recorded'})}).record(input()),{code:'meter_unavailable'});
});
test('replies require matching native exit status, valid raw packet and bounded input',async()=>{
  for(const result of [{exitCode:1,stdout:encode([1,true,retained()])},{exitCode:0,stdout:encode([1,false,{code:'failed'}])},{exitCode:0,stdout:new Uint8Array([1,2,3])}])
    await assert.rejects(()=>createNativeMeter({binary,db:'/unused/meter.db',run:async()=>result}).record(input()),{code:'meter_unavailable'});
  let calls=0;const meter=createNativeMeter({binary,db:'/unused/meter.db',run:async()=>{calls++;return reply(retained());}});
  for(const changes of [{bytes:-1},{bytes:67108865},{durationMs:3600001},{at:'2026-02-30T00:00:00Z'},{receiptRef:'/private/output'},{host:'example.invalid'}])
    await assert.rejects(()=>meter.record({...input(),...changes}),TypeError);
  assert.equal(calls,0);
});
test('unconfigured helper stays unavailable and never invents a private parallel meter',async()=>{
  const meter=nativeMeterFromEnvironment({},{root:undefined,home:'/private/fabric'});assert.equal(meter.configured,false);
  await assert.rejects(()=>meter.quote(quote()),{code:'meter_unavailable'});
  const f=fixture(),seen=[];
  const configured=nativeMeterFromEnvironment({BONFYRE_METER_BINARY:binary,BONFYRE_METER_DB:f.db},{root:'/ignored',run:async argv=>{seen.push(argv);return reply({...quote(),pricing:{state:'unknown'}});}});
  await configured.quote(quote());assert.equal(seen[0][3],f.db);assert.equal(seen[0][0],binary);
});
