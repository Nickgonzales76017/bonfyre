import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createOperatorStore} from '../operator_store.mjs';
import {createOperatorDaemon} from '../operator_daemon.mjs';

function fixture(fn){
 const home=mkdtempSync(join(tmpdir(),'operator-recovery-'));
 let now=1800000000;const stores=[];
 const open=()=>{const store=createOperatorStore({home,now:()=>now});stores.push(store);return store;};
 try{return fn({open,advance:seconds=>{now+=seconds},clock:()=>now});}
 finally{for(const store of stores)store.close();rmSync(home,{recursive:true,force:true});}
}
const goal=(store,key)=>store.addGoal({title:key,details:'Test-only',source:'fixture',dedupe_key:key});

test('terminal provider failure quarantines every new claim without touching unrelated goals',()=>fixture(({open})=>{
 const store=open(),first=goal(store,'exhausted'),other=goal(store,'other');
 const claimed=store.claimGoal({goalId:first.id});
 // Only the transport that made the request names the provider failure.
 const finished=store.failRun({run_id:claimed.run.id,error:'operator HTTP 429: credit_balance_exhausted',
  provider_failure:{provider:'openai',cause:'credit_balance_exhausted'}});
 expect(finished.goal.state).toBe('blocked');
 expect(store.providerStatus('openai')).toMatchObject({state:'quarantined',cause:'credit_balance_exhausted',admitted:false,retry_at:0});
 expect(store.getGoal(other.id).state).toBe('queued');
 const second=open();
 expect(second.providerStatus('openai').state).toBe('quarantined');
 expect(second.getGoal(other.id).state).toBe('queued');
 expect(goal(second,'exhausted').state).toBe('blocked');
 expect(second.getGoal(first.id).state).toBe('blocked');
}));

test('transient cooldown never self-authorizes reentry or weakens terminal quarantine',()=>fixture(({open,advance})=>{
 const store=open();store.quarantineProvider('openai','rate_limited',300);
 expect(store.providerStatus('openai').state).toBe('quarantined');
 advance(301);expect(store.providerStatus('openai')).toMatchObject({state:'recheck_required',admitted:false});
 store.quarantineProvider('openai','insufficient_quota');
 store.quarantineProvider('openai','rate_limited',300);
 expect(store.providerStatus('openai')).toMatchObject({state:'quarantined',cause:'insufficient_quota',retry_at:0});
 expect(()=>store.quarantineProvider('openai','unknown')).toThrow('classification');
}));

test('daily cycle and tool-call budgets close admission at the atomic store owner',()=>fixture(({open,clock})=>{
 const store=open();goal(store,'first');goal(store,'second');
 const policy={daily_cycle_budget:1,daily_tool_call_budget:1,max_cycles_per_hour:20};
 const claimed=store.claimGoal({policy});expect(claimed).not.toBeNull();
 expect(store.claimGoal({policy})).toBeNull();
 expect(store.budgetOk(policy).okay).toBe(false);
 const seq=store.recordAction({run_id:claimed.run.id,tool:'read',args:{},state:'started',result:null,started_at:clock()});
 store.completeAction({seq,state:'passed',result:{okay:true}});
 expect(store.budgetOk({daily_cycle_budget:10,daily_tool_call_budget:1}).okay).toBe(false);
 expect(store.budgetOk({daily_cycle_budget:10,daily_tool_call_budget:2}).okay).toBe(true);
}));

test('explicit model admission and gateway preflight refuse claims until healthy',async()=>{
 const home=mkdtempSync(join(tmpdir(),'operator-gateway-'));let now=1800000000,catalogCalls=0,paidCalls=0,allow=false;
 const store=createOperatorStore({home,now:()=>now});
 const policy={autonomous:true,mode:'power',max_cycles_per_hour:6,
  allow_provider_requests:false,approved_provider_models:['fixture-only'],gateway_backoff_seconds:900,
  cycle_timeout_seconds:60,max_rounds_per_cycle:2,max_tool_calls_per_cycle:4};
 const config={model:'fixture-only',paused:()=>false,loadPolicy:()=>policy};
 const tools={async catalog(){catalogCalls++;if(!allow)throw Error('fixture gateway outage');
  return {tools:[{name:'operator_finish',parameters:{type:'object',
   properties:{disposition:{type:'string'},summary:{type:'string'}},required:['disposition','summary']}}],catalog:{}};}};
 const transport={async events(cursor){return {events:[],next_after:cursor};},
  async response(){paidCalls++;return {id:'fixture-response',output:[{type:'function_call',
   name:'operator_finish',call_id:'finish',arguments:JSON.stringify({disposition:'resolved',summary:'safe fixture'})}],
   usage:{input_tokens:1,output_tokens:1}};},
  setDeadline(){},redact:String};
 try{
  const added=goal(store,'gateway');
  const daemon=createOperatorDaemon({config,store,tools,transport,now:()=>now});
  expect((await daemon.once()).state).toBe('provider_not_admitted');
  expect(catalogCalls).toBe(0);expect(paidCalls).toBe(0);
  policy.allow_provider_requests=true;
  expect((await daemon.once()).state).toBe('mcp_quarantined');
  expect(store.getGoal(added.id)).toMatchObject({state:'queued',attempts:0});
  expect((await daemon.once()).state).toBe('mcp_quarantined');
  expect(catalogCalls).toBe(1);expect(paidCalls).toBe(0);
  allow=true;now+=901;
  const result=await daemon.once();
  expect(result.state).toBe('completed');
  expect(store.getGoal(added.id)).toMatchObject({state:'resolved',attempts:1});
  expect(catalogCalls).toBe(2);expect(paidCalls).toBe(1);
 }finally{store.close();rmSync(home,{recursive:true,force:true});}
});

test('legacy terminal 429 receipts quarantine an uninitialized provider-health table',()=>fixture(({open})=>{
 const store=open(),g=goal(store,'legacy-quota'),run=store.claimGoal({goalId:g.id}).run;
 store.failRun({run_id:run.id,error:'RateLimitError: insufficient_quota'});
 const db=new Database(store.path);db.exec('DELETE FROM operator_provider_health');db.close();
 expect(store.providerStatus('openai')).toMatchObject({state:'quarantined',cause:'historical_terminal_quota',admitted:false});
}));

test('legacy terminal evidence survives unverified health, supersession and transient quarantine',()=>fixture(({open,advance})=>{
  const store=open(),g=goal(store,'legacy-row'),run=store.claimGoal({goalId:g.id}).run;
  store.failRun({run_id:run.id,error:'RateLimitError: credit_balance_exhausted'});
  const db=new Database(store.path);
  try{
    expect(db.query("SELECT state FROM operator_provider_health WHERE provider='openai'").get().state).toBe('unverified');
    db.query("UPDATE goals SET state='superseded' WHERE id=?").run(g.id);
  }finally{db.close();}
  expect(store.providerStatus('openai')).toMatchObject({state:'quarantined',cause:'historical_terminal_quota',admitted:false});
  store.quarantineProvider('openai','rate_limited',300);
  expect(store.providerStatus('openai')).toMatchObject({state:'quarantined',cause:'historical_terminal_quota',retry_at:0});
  advance(301);
  expect(store.providerStatus('openai')).toMatchObject({state:'quarantined',admitted:false});
  const db2=new Database(store.path);
  try{expect(db2.query("SELECT state FROM operator_provider_health WHERE provider='openai'").get().state).toBe('unverified');}
  finally{db2.close();}
}));
