import {beforeAll,afterAll,test,expect} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {decode} from '../../../integrations/browser/workspace-msgpack.mjs';

const release='351a6d4cdca6aab2741060b40d4dbb5d4707dccb3dcc3fcbd3214736d907f913';
let priorRelease,f,binding,store,schemas;

beforeAll(async()=>{
 priorRelease=process.env.BONFYRE_WORKSPACE_RELEASE;
 process.env.BONFYRE_WORKSPACE_RELEASE=release;
 f=await workspaceFixture({extraPrefixes:['procurement-approval:']});
 schemas=join(f.home,'schemas');
 for(const type of ['governance_decision','assent_signature'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 f.authsql.exec(`CREATE TABLE IF NOT EXISTS authority_edges(
  edge_id TEXT PRIMARY KEY,
  actor TEXT NOT NULL,
  permission TEXT NOT NULL,
  subject TEXT NOT NULL,
  purpose TEXT NOT NULL DEFAULT '',
  scope TEXT NOT NULL DEFAULT '',
  effective_from TEXT,
  expires_at TEXT,
  delegable INTEGER NOT NULL DEFAULT 0,
  revoked INTEGER NOT NULL DEFAULT 0,
  evidence TEXT NOT NULL DEFAULT '',
  originating_authority TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
 )`);
 binding=await f.install('rooms');
 store=f.store(binding);
},60000);

afterAll(async()=>{
 await f?.close();
 if(priorRelease===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=priorRelease;
});

function approval(id,{known=true}={}){
 return {
  schema:'bonfyre.procurement.approval-request.v1',
  id,
  status:'pending',
  requested_at:'2026-10-23T16:00:00Z',
  requester:'user:'+f.owner.id,
  reason:'Fixture approval review',
  source:{allocation_id:'allocation-'+id,revision:1,digest:'a'.repeat(64)},
  currency:'USD',
  known_merchandise_cents:22000,
  known_total_cents:known?22000:null,
  fees_attribution:known?'full_quote':'unallocated',
  selected_quotes:[],
  authority:{
   owner:'AuthorityGraph',
   subject:'procurement-allocation:allocation-'+id,
   required_permissions:['approve','spend'],
   decision:'pending',
   decided_by:null,
   decision_ref:null
  },
  approved:false,
  spend_authorized:false,
  quote_accepted:false,
  commitment_opened:false,
  order_created:false,
  payment_presented:false,
  does_not_establish:'Fixture keeps every procurement effect false.',
  digest:'b'.repeat(64)
 };
}
async function status(key,user=f.owner){
 const response=await f.raw('/apps/'+binding.id+'/_approval_status?'+new URLSearchParams({key}),user);
 const bytes=new Uint8Array(await response.arrayBuffer());
 return {status:response.status,value:response.status===200?decode(bytes):null,text:response.status===200?'':new TextDecoder().decode(bytes)};
}
function cms(type,value){return f.add(type,value);}
function edge({id,actor,permission,subject,purpose='procurement',effective='2026-10-23T00:00:00Z',expires='2026-10-24T00:00:00Z',revoked=0}){
 f.authsql.query('INSERT INTO authority_edges(edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,delegable,revoked,evidence,originating_authority,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)')
  .run(id,actor,permission,subject,purpose,'self',effective,expires,0,revoked,'fixture:authority','fixture-root','2026-10-23T00:00:00Z');
}

test('approval reconciliation keeps decision, assent and authority non-collapsible',async()=>{
 const key='procurement-approval:approval-fixture-main',saved=await store.saveLocal(key,approval('approval-fixture-main'),0);
 expect(saved.revision).toBe(1);
 let read=await status(key);
 expect(read.status).toBe(200);
 expect(read.value[0]).toBe(1);
 const bodySha=read.value[1].body_sha256;
 expect(bodySha).toMatch(/^[a-f0-9]{64}$/);
 expect(read.value[1]).toMatchObject({state:'pending',record_key:key,revision:1,body_sha256:bodySha,assent_present:false,approve_authority:false,spend_authority:false,eligible_for_order_intent:false});
 expect((await status(key,f.other)).status).toBe(403);

 const decider='user:'+f.owner.id,decidedAt='2026-10-23T17:00:00Z';
 cms('governance_decision',{
  decision_id:'decision-fixture-main',
  proposal_id:key,
  outcome:'approved',
  decided_by:decider,
  backing:'human_approval',
  evidence_ref:bodySha,
  receipt_ref:'fixture:decision-main',
  decided_at:decidedAt,
  cannot_infer:'A governance decision does not execute procurement.',
  status:'published'
 });
 read=await status(key);
 expect(read.value[1]).toMatchObject({state:'decision_unverified',outcome:'approved',decided_by:decider,assent_present:false,approve_authority:false,spend_authority:false,eligible_for_order_intent:false});

 cms('assent_signature',{
  signature_id:'assent-fixture-main',
  about:key,
  digest:bodySha,
  assented_by:decider,
  assented_at:decidedAt,
  statement:'I assent to these exact approval-request bytes.',
  cannot_infer:'Human assent does not execute procurement or create payment authority.',
  status:'published'
 });
 read=await status(key);
 expect(read.value[1]).toMatchObject({state:'decision_unverified',assent_present:true,approve_authority:false,spend_authority:false,eligible_for_order_intent:false});

 edge({id:'edge-approve-main',actor:decider,permission:'approve',subject:'procurement-allocation:allocation-approval-fixture-main'});
 read=await status(key);
 expect(read.value[1]).toMatchObject({state:'decision_unverified',assent_present:true,approve_authority:true,spend_authority:false,eligible_for_order_intent:false});

 edge({id:'edge-spend-main',actor:decider,permission:'spend',subject:'procurement-allocation:allocation-approval-fixture-main'});
 read=await status(key);
 expect(read.value[1]).toMatchObject({state:'eligible',assent_present:true,approve_authority:true,spend_authority:true,eligible_for_order_intent:true});
 expect(read.value[1].cannot_infer).toContain('not quote acceptance');
});

test('complete authority cannot authorize an order intent while the exact spend amount is unresolved',async()=>{
 const id='approval-fixture-amount',key='procurement-approval:'+id;await store.saveLocal(key,approval(id,{known:false}),0);
 const pending=await status(key),bodySha=pending.value[1].body_sha256,decider='user:'+f.owner.id,at='2026-10-23T17:00:00Z';
 cms('governance_decision',{decision_id:'decision-fixture-amount',proposal_id:key,outcome:'approved',decided_by:decider,backing:'human_approval',evidence_ref:bodySha,receipt_ref:'fixture:decision-amount',decided_at:at,cannot_infer:'Approval does not resolve an unknown amount.',status:'published'});
 cms('assent_signature',{signature_id:'assent-fixture-amount',about:key,digest:bodySha,assented_by:decider,assented_at:at,statement:'I assent to these exact approval bytes.',cannot_infer:'Assent does not invent an amount.',status:'published'});
 const subject='procurement-allocation:allocation-'+id;
 edge({id:'edge-amount-approve',actor:decider,permission:'approve',subject});edge({id:'edge-amount-spend',actor:decider,permission:'spend',subject});
 const read=await status(key);
 expect(read.value[1]).toMatchObject({state:'amount_unresolved',assent_present:true,approve_authority:true,spend_authority:true,amount_known:false,known_total_cents:null,eligible_for_order_intent:false});
 expect(read.value[1].cannot_infer).toContain('exact fully attributed spend total');
});

test('purpose mismatch and revoked or expired grants cannot establish eligibility',async()=>{
 const key='procurement-approval:approval-fixture-purpose';await store.saveLocal(key,approval('approval-fixture-purpose'),0);
 const pending=await status(key),bodySha=pending.value[1].body_sha256,decider='user:'+f.owner.id,at='2026-10-23T17:00:00Z';
 cms('governance_decision',{decision_id:'decision-fixture-purpose',proposal_id:key,outcome:'approved',decided_by:decider,backing:'human_approval',evidence_ref:bodySha,receipt_ref:'fixture:decision-purpose',decided_at:at,cannot_infer:'No effect.',status:'published'});
 cms('assent_signature',{signature_id:'assent-fixture-purpose',about:key,digest:bodySha,assented_by:decider,assented_at:at,statement:'Exact bytes.',cannot_infer:'Human assent does not execute procurement.',status:'published'});
 const subject='procurement-allocation:allocation-approval-fixture-purpose';
 edge({id:'edge-purpose-approve',actor:decider,permission:'approve',subject,purpose:'unrelated'});
 edge({id:'edge-purpose-spend',actor:decider,permission:'spend',subject,purpose:'procurement',revoked:1});
 edge({id:'edge-purpose-expired',actor:decider,permission:'spend',subject,purpose:'procurement',expires:'2026-10-23T16:00:00Z'});
 const read=await status(key);
 expect(read.value[1]).toMatchObject({state:'decision_unverified',assent_present:true,approve_authority:false,spend_authority:false,eligible_for_order_intent:false});
});

test('rejected decision remains rejected and duplicate exact decisions refuse reconciliation',async()=>{
 const rejectedKey='procurement-approval:approval-fixture-rejected';await store.saveLocal(rejectedKey,approval('approval-fixture-rejected'),0);
 const rejectedSha=(await status(rejectedKey)).value[1].body_sha256;
 cms('governance_decision',{decision_id:'decision-fixture-rejected',proposal_id:rejectedKey,outcome:'rejected',decided_by:'user:'+f.owner.id,backing:'human_approval',evidence_ref:rejectedSha,receipt_ref:'fixture:rejected',decided_at:'2026-10-23T17:00:00Z',cannot_infer:'Rejected is not execution.',status:'published'});
 expect((await status(rejectedKey)).value[1]).toMatchObject({state:'rejected',eligible_for_order_intent:false});

 const duplicateKey='procurement-approval:approval-fixture-duplicate';await store.saveLocal(duplicateKey,approval('approval-fixture-duplicate'),0);
 const duplicateSha=(await status(duplicateKey)).value[1].body_sha256;
 for(const n of [1,2])cms('governance_decision',{decision_id:'decision-fixture-duplicate-'+n,proposal_id:duplicateKey,outcome:'approved',decided_by:'user:'+f.owner.id,backing:'human_approval',evidence_ref:duplicateSha,receipt_ref:'fixture:duplicate-'+n,decided_at:'2026-10-23T17:00:00Z',cannot_infer:'Duplicate decisions require reconciliation.',status:'published'});
 const duplicate=await status(duplicateKey);
 expect(duplicate.status).toBe(409);
 expect(duplicate.text).toContain('More than one decision');
});
