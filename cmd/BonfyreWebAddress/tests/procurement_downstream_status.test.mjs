import {beforeAll,afterAll,test,expect} from 'bun:test';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {createHash} from 'node:crypto';
import {workspaceFixture} from './workspace_fixture.mjs';
import {decode} from '../../../integrations/browser/workspace-msgpack.mjs';

const release='351a6d4cdca6aab2741060b40d4dbb5d4707dccb3dcc3fcbd3214736d907f913';
let priorRelease,f,binding,store,commitments,commerce;

function prepareStores({home}){
 const cpath=join(home,'commitments.db'),xpath=join(home,'commerce.db');
 const c=new Database(cpath),x=new Database(xpath);
 c.exec(`CREATE TABLE commitments(
   commitment_id TEXT PRIMARY KEY,committer TEXT NOT NULL,beneficiary TEXT NOT NULL,
   resource TEXT NOT NULL,conditions TEXT NOT NULL DEFAULT '',expires_at TEXT NOT NULL DEFAULT '',
   authority TEXT NOT NULL DEFAULT '',origin TEXT NOT NULL DEFAULT '',enactment TEXT NOT NULL DEFAULT '');
 CREATE TABLE commitment_events(
   seq INTEGER PRIMARY KEY AUTOINCREMENT,commitment_id TEXT NOT NULL,state TEXT NOT NULL,
   actor TEXT NOT NULL DEFAULT '',detail TEXT NOT NULL DEFAULT '',at TEXT NOT NULL DEFAULT '');
 CREATE INDEX commitment_events_by_id ON commitment_events(commitment_id,seq);`);
 x.exec("CREATE TABLE commerce_progress(exchange TEXT NOT NULL,stage TEXT NOT NULL,owner TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '',at TEXT NOT NULL DEFAULT '',PRIMARY KEY(exchange,stage));");
 c.close();x.close();
}

beforeAll(async()=>{
 priorRelease=process.env.BONFYRE_WORKSPACE_RELEASE;
 process.env.BONFYRE_WORKSPACE_RELEASE=release;
 f=await workspaceFixture({extraPrefixes:['procurement-order-intent:'],beforeServe:prepareStores});
 binding=await f.install('rooms');store=f.store(binding);
 commitments=new Database(join(f.home,'commitments.db'));
 commerce=new Database(join(f.home,'commerce.db'));
},60000);

afterAll(async()=>{
 commitments?.close();commerce?.close();await f?.close();
 if(priorRelease===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=priorRelease;
});

function orderIntent(id){
 return {
  schema:'bonfyre.procurement.order-intent.v1',
  id,
  status:'prepared',
  prepared_at:'2026-10-23T18:00:00Z',
  source:{allocation_id:'allocation-'+id,allocation_revision:1,allocation_digest:'a'.repeat(64),approval_id:'approval-'+id,approval_revision:1,approval_digest:'b'.repeat(64),approval_body_sha256:'c'.repeat(64),decision_id:'decision-'+id},
  authority_reconciliation:{owner:'AuthorityGraph',state:'eligible',outcome:'approved',decided_by:'user:'+f.owner.id,decided_at:'2026-10-23T17:30:00Z',assent_present:true,approve_authority:true,spend_authority:true},
  currency:'USD',known_merchandise_cents:22000,known_total_cents:null,fees_attribution:'unallocated',
  selected_quotes:[],allocations:[],
  commitment_handoff:{owner:'CommitmentGraph',status:'unmaterialized',proposed_resource:'procurement-order-intent:'+id,requires_quote_acceptance:true,requires_supplier_counterparty_terms:true},
  commerce_chain:{owner:'CommerceChain',exchange_candidate:'procurement:allocation-'+id,status:'unmaterialized',next_required_stage:'product'},
  authority_eligible:true,
  quote_accepted:false,reservation_created:false,cart_created:false,commitment_opened:false,order_created:false,purchase_order_issued:false,supplier_accepted:false,payable_created:false,payment_presented:false,settled:false,fulfilled:false,shipped:false,received:false,
  does_not_establish:'Fixture order intent is not an order, commitment, payment, shipment or receipt.',
  digest:createHash('sha256').update(id).digest('hex')
 };
}

async function downstream(key,user=f.owner){
 const response=await f.raw('/apps/'+binding.id+'/_procurement_downstream?'+new URLSearchParams({key}),user);
 const bytes=new Uint8Array(await response.arrayBuffer());
 return {status:response.status,value:response.status===200?decode(bytes):null,text:response.status===200?'':new TextDecoder().decode(bytes)};
}
function counts(){
 return {
  commitments:commitments.query('SELECT count(*) AS n FROM commitments').get().n,
  events:commitments.query('SELECT count(*) AS n FROM commitment_events').get().n,
  commerce:commerce.query('SELECT count(*) AS n FROM commerce_progress').get().n
 };
}

test('downstream reconciliation is read-only and distinguishes unmaterialized from observed owner state',async()=>{
 const id='order-fixture-main',key='procurement-order-intent:'+id,saved=await store.saveLocal(key,orderIntent(id),0);
 expect(saved.revision).toBe(1);
 const before=counts();
 let read=await downstream(key);
 expect(read.status).toBe(200);
 expect(read.value[0]).toBe(1);
 expect(read.value[1]).toMatchObject({
  state:'unmaterialized',record_key:key,revision:1,
  commitment_resource:key,commitment_found:false,commitment_state:'',commitment_delivered:false,
  commerce_exchange:'procurement:allocation-'+id,commerce_furthest:'',established:[]
 });
 expect(read.value[1].outstanding).toHaveLength(15);
 expect(read.value[1].outstanding[0]).toMatchObject({stage:'product',owner:'ObjectContract / catalogue'});
 expect(counts()).toEqual(before);
 expect((await downstream(key,f.other)).status).toBe(403);

 commitments.query('INSERT INTO commitments(commitment_id,committer,beneficiary,resource,conditions,authority,origin,enactment) VALUES(?,?,?,?,?,?,?,?)')
  .run(key,'supplier:fixture','buyer:fixture',key,'Fixture-only external counterparty terms','fixture:counterparty','fixture:supplier','fixture:quote-accepted');
 commitments.query('INSERT INTO commitment_events(commitment_id,state,actor,detail,at) VALUES(?,?,?,?,?)')
  .run(key,'open','supplier:fixture','Fixture downstream owner opened this commitment outside the reconciliation route.','2026-10-23T18:10:00Z');
 for(const [stage,owner] of [['product','ObjectContract / catalogue'],['listing','merchant or marketplace ForeignTwin']])
  commerce.query('INSERT INTO commerce_progress(exchange,stage,owner,detail,at) VALUES(?,?,?,?,?)')
   .run('procurement:allocation-'+id,stage,owner,'Fixture downstream evidence','2026-10-23T18:11:00Z');

 const materialized=counts();
 read=await downstream(key);
 expect(read.status).toBe(200);
 expect(read.value[1]).toMatchObject({
  state:'downstream_observed',commitment_found:true,commitment_state:'open',commitment_actor:'supplier:fixture',
  commitment_delivered:false,commerce_furthest:'listing'
 });
 expect(read.value[1].established.map(row=>row.stage)).toEqual(['product','listing']);
 expect(read.value[1].outstanding[0]).toMatchObject({stage:'inventory',owner:'merchant system of record'});
 expect(read.value[1].cannot_infer).toContain('does not establish quote acceptance');
 expect(counts()).toEqual(materialized);
});

test('protocol provenance recovers every exact commitment for one order-intent digest',async()=>{
 const id='order-fixture-protocol',key='procurement-order-intent:'+id,intent=orderIntent(id);await store.saveLocal(key,intent,0);
 const origin='procurement-order:'+intent.digest,exchange='procurement:exact-protocol-fixture';
 for(const [suffix,actor,resource] of [['delivery','deliverer:fixture','capability:fixture'],['payment','buyer:fixture','money:USD:22000']]){
  const commitmentId=exchange+':'+suffix;
  commitments.query('INSERT INTO commitments(commitment_id,committer,beneficiary,resource,conditions,authority,origin,enactment) VALUES(?,?,?,?,?,?,?,?)').run(commitmentId,actor,'buyer:fixture',resource,suffix==='delivery'?'delivery':'acceptance','fixture:authority',origin,exchange);
  commitments.query('INSERT INTO commitment_events(commitment_id,state,actor,detail,at) VALUES(?,?,?,?,?)').run(commitmentId,'open',actor,'ProtocolContract fixture opening','2026-10-23T18:40:00Z');
 }
 const before=counts(),read=await downstream(key);expect(read.status).toBe(200);
 expect(read.value[1]).toMatchObject({state:'downstream_observed',commitment_found:true,commitment_state:'open',commitment_actor:'multiple',protocol_commitment_count:2});
 expect(read.value[1].protocol_commitments.map(row=>row.commitment_id)).toEqual([exchange+':delivery',exchange+':payment']);
 expect(read.value[1].protocol_commitments.every(row=>row.enactment===exchange&&row.state==='open')).toBe(true);
 expect(counts()).toEqual(before);
});

test('a discharged commitment is reported as discharged without inventing later commerce stages',async()=>{
 const id='order-fixture-discharged',key='procurement-order-intent:'+id;
 await store.saveLocal(key,orderIntent(id),0);
 commitments.query('INSERT INTO commitments(commitment_id,committer,beneficiary,resource,conditions,authority,origin,enactment) VALUES(?,?,?,?,?,?,?,?)')
  .run(key,'supplier:fixture','buyer:fixture',key,'Fixture terms','fixture','fixture:supplier','fixture:quote');
 commitments.query('INSERT INTO commitment_events(commitment_id,state,actor,detail,at) VALUES(?,?,?,?,?)').run(key,'open','supplier:fixture','Open','2026-10-23T18:20:00Z');
 commitments.query('INSERT INTO commitment_events(commitment_id,state,actor,detail,at) VALUES(?,?,?,?,?)').run(key,'discharged','buyer:fixture','Explicit fixture discharge','2026-10-23T18:30:00Z');
 const before=counts(),read=await downstream(key);
 expect(read.status).toBe(200);
 expect(read.value[1]).toMatchObject({commitment_state:'discharged',commitment_delivered:true,commerce_furthest:''});
 expect(read.value[1].established).toEqual([]);
 expect(read.value[1].outstanding).toHaveLength(15);
 expect(counts()).toEqual(before);
});
