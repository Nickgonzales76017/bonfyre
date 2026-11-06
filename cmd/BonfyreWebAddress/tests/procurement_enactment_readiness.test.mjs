import {beforeAll,afterAll,test,expect} from 'bun:test';
import {copyFileSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {decode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {createProcurementKernel} from '../../../integrations/browser/procurement-wasm.mjs';

const release='351a6d4cdca6aab2741060b40d4dbb5d4707dccb3dcc3fcbd3214736d907f913';
const H='a'.repeat(64);
let priorRelease,f,binding,store,kernel,schemas;

beforeAll(async()=>{
 priorRelease=process.env.BONFYRE_WORKSPACE_RELEASE;process.env.BONFYRE_WORKSPACE_RELEASE=release;
 const wasm=readFileSync(join(root,'cmd/BonfyreProcurement/wasm/procurement.wasm'));
 kernel=await createProcurementKernel(wasm,{random:{getRandomValues(view){view.fill(7);return view;}}});
 f=await workspaceFixture({
  extraPrefixes:['procurement-allocation:','procurement-approval:','procurement-order-intent:'],
  beforeServe:async({env})=>{env.BONFYRE_PROCUREMENT_BINARY=join(root,'cmd/BonfyreProcurement/bonfyre-procurement');}
 });
 schemas=join(f.home,'schemas');
 for(const type of ['governance_decision','assent_signature'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 f.authsql.exec(`CREATE TABLE IF NOT EXISTS authority_edges(
  edge_id TEXT PRIMARY KEY,actor TEXT NOT NULL,permission TEXT NOT NULL,subject TEXT NOT NULL,purpose TEXT NOT NULL DEFAULT '',
  scope TEXT NOT NULL DEFAULT '',effective_from TEXT,expires_at TEXT,delegable INTEGER NOT NULL DEFAULT 0,revoked INTEGER NOT NULL DEFAULT 0,
  evidence TEXT NOT NULL DEFAULT '',originating_authority TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL)`);
 binding=await f.install('rooms');store=f.store(binding);
},60000);

afterAll(async()=>{await f?.close();if(priorRelease===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=priorRelease;});

const purchase={schema:'service-site.room-purchase-list.v1',id:'purchase-readiness',state:'draft',currency:'USD',sha256:H,commitment:null,
 source:{key:'fixture-plan:readiness',revision:1,sha256:H},
 lines:[{product_id:'product-1',variant_id:'variant-1',label:'Chair',option:'Walnut',source_url:'https://merchant.example/items/1',source_sha256:H,observed_at:'2026-10-23T12:00:00Z',unit_cents:1000,quantity:2,subtotal_cents:2000}],
 subtotal_cents:2000};

async function approvalStatus(key){
 const response=await f.raw('/apps/'+binding.id+'/_approval_status?'+new URLSearchParams({key}));
 const bytes=new Uint8Array(await response.arrayBuffer());return {status:response.status,value:response.status===200?decode(bytes):null};
}
async function readiness(key,user=f.owner){
 const response=await f.raw('/apps/'+binding.id+'/_procurement_enactment_readiness?'+new URLSearchParams({key}),user);
 const bytes=new Uint8Array(await response.arrayBuffer());return {status:response.status,value:response.status===200?decode(bytes):null,text:response.status===200?'':new TextDecoder().decode(bytes)};
}
function edge(id,actor,permission,subject){
 f.authsql.query('INSERT INTO authority_edges(edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,delegable,revoked,evidence,originating_authority,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)')
  .run(id,actor,permission,subject,'procurement','self','2026-10-23T00:00:00Z','2026-10-25T00:00:00Z',0,0,'fixture:authority','fixture-root','2026-10-23T00:00:00Z');
}

test('enactment readiness regenerates exact native lineage and goes stale when an upstream revision changes',async()=>{
 const rfq=kernel.prepareRFQ({purchase_list:purchase,purchase_list_revision:1,id:'rfq-readiness',requested_at:'2026-10-23T13:00:00Z',supplier:{id:'supplier-one',name:'Supplier One'},selected:null});
 const line=rfq.lines[0];
 const quote=kernel.observeQuote({rfq,observation:{id:'quote-readiness',supplier:{id:'supplier-one',name:'Supplier One'},supplier_quote_id:'SQ-R',observed_at:'2026-10-23T14:00:00Z',quoted_at:null,valid_until:'2026-10-30T00:00:00Z',currency:'USD',source_ref:'file:quote.pdf',freight_cents:0,tax_cents:0,other_fees_cents:0,lines:[{id:'quote-line-r',rfq_line_id:line.line_id,quantity:2,unit_cents:900,line_total_cents:null,substitution:null,availability:'In stock',lead_time:'5 days'}]}});
 const comparison=kernel.compareQuotes({rfq,quotes:[quote],expected_rfq_digest:rfq.digest,expected_quotes:[{id:quote.id,digest:quote.digest}]},{asOf:'2026-10-24T00:00:00Z'});
 const allocation=kernel.allocateQuotes({rfq,quotes:[quote],comparison,id:'allocation-readiness',prepared_at:'2026-10-24T00:00:00Z',policy:'lowest_known_merchandise'});
 const allocationKey='procurement-allocation:'+allocation.id,allocationSaved=await store.saveLocal(allocationKey,allocation,0);expect(allocationSaved.revision).toBe(1);

 const approval=kernel.prepareApprovalRequest({allocation,allocation_revision:allocationSaved.revision,id:'approval-readiness',requested_at:'2026-10-24T00:01:00Z',requester:'user:'+f.owner.id,reason:'Fixture exact approval'});
 const approvalKey='procurement-approval:'+approval.id,approvalSaved=await store.saveLocal(approvalKey,approval,0);expect(approvalSaved.revision).toBe(1);
 let status=await approvalStatus(approvalKey);expect(status.status).toBe(200);const bodySha=status.value[1].body_sha256;

 const decider='user:'+f.owner.id,decidedAt='2026-10-24T00:02:00Z';
 f.add('governance_decision',{decision_id:'decision-readiness',proposal_id:approvalKey,outcome:'approved',decided_by:decider,backing:'human_approval',evidence_ref:bodySha,receipt_ref:'fixture:decision-readiness',decided_at:decidedAt,cannot_infer:'Approval is not execution.',status:'published'});
 f.add('assent_signature',{signature_id:'assent-readiness',about:approvalKey,digest:bodySha,assented_by:decider,assented_at:decidedAt,statement:'I assent to these exact approval bytes.',cannot_infer:'Assent is not execution.',status:'published'});
 edge('edge-readiness-approve',decider,'approve',approval.authority.subject);edge('edge-readiness-spend',decider,'spend',approval.authority.subject);
 status=await approvalStatus(approvalKey);expect(status.value[1]).toMatchObject({state:'eligible',eligible_for_order_intent:true});

 const intent=kernel.prepareOrderIntent({allocation,allocation_revision:allocationSaved.revision,approval,approval_revision:approvalSaved.revision,authority_status:status.value[1],id:'order-intent-readiness',prepared_at:'2026-10-24T00:03:00Z'});
 const orderKey='procurement-order-intent:'+intent.id,orderSaved=await store.saveLocal(orderKey,intent,0);expect(orderSaved.revision).toBe(1);

 let check=await readiness(orderKey);expect(check.status).toBe(200);expect(check.value[0]).toBe(1);
 expect(check.value[1]).toMatchObject({state:'ready_for_supplier_acceptance',ready:true,record_key:orderKey,revision:1,order_digest:intent.digest,native_lineage_attested:true,approval_eligible:true,decision_id:'decision-readiness',supplier_acceptance_present:false});
 expect((await readiness(orderKey,f.other)).status).toBe(403);

 await store.saveLocal(allocationKey,{...allocation,fixture_revision_marker:'changed-after-order'},1);
 check=await readiness(orderKey);expect(check.status).toBe(200);expect(check.value[1]).toMatchObject({state:'lineage_changed',ready:false});
},60000);
