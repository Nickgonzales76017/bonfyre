import {beforeAll,afterAll,test,expect} from 'bun:test';
import {copyFileSync,readFileSync,mkdirSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {decode,encode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {createProcurementKernel} from '../../../integrations/browser/procurement-wasm.mjs';

const release='351a6d4cdca6aab2741060b40d4dbb5d4707dccb3dcc3fcbd3214736d907f913',H='a'.repeat(64);
let priorRelease,f,binding,store,kernel,schemas,commitmentDb,sign,seller,keyDir;

beforeAll(async()=>{
 priorRelease=process.env.BONFYRE_WORKSPACE_RELEASE;process.env.BONFYRE_WORKSPACE_RELEASE=release;
 kernel=await createProcurementKernel(readFileSync(join(root,'cmd/BonfyreProcurement/wasm/procurement.wasm')),{random:{getRandomValues(view){view.fill(9);return view;}}});
 f=await workspaceFixture({extraPrefixes:['procurement-allocation:','procurement-approval:','procurement-order-intent:'],beforeServe:async({env,home})=>{
  commitmentDb=join(home,'commitment.db');env.BONFYRE_COMMITMENT_DB=commitmentDb;env.BONFYRE_PROCUREMENT_BINARY=join(root,'cmd/BonfyreProcurement/bonfyre-procurement');
 }});
 schemas=join(f.home,'schemas');for(const type of ['governance_decision','assent_signature'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 f.authsql.exec(`CREATE TABLE IF NOT EXISTS authority_edges(edge_id TEXT PRIMARY KEY,actor TEXT NOT NULL,permission TEXT NOT NULL,subject TEXT NOT NULL,purpose TEXT NOT NULL DEFAULT '',scope TEXT NOT NULL DEFAULT '',effective_from TEXT,expires_at TEXT,delegable INTEGER NOT NULL DEFAULT 0,revoked INTEGER NOT NULL DEFAULT 0,evidence TEXT NOT NULL DEFAULT '',originating_authority TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL)`);
 binding=await f.install('rooms');store=f.store(binding);f.run('/usr/bin/make',['-s','-C',join(root,'cmd/BonfyreSign'),'bonfyre-sign']);
 sign=join(root,'cmd/BonfyreSign/bonfyre-sign');keyDir=join(f.home,'supplier-key');mkdirSync(keyDir,{recursive:true});f.run(sign,['keygen','--key-dir',keyDir]);
 seller='ed25519:'+readFileSync(join(keyDir,'institution.ed25519.public'),'utf8').trim();
},60000);
afterAll(async()=>{await f?.close();if(priorRelease===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=priorRelease;});

const purchase={schema:'service-site.room-purchase-list.v1',id:'purchase-enact',state:'draft',currency:'USD',sha256:H,commitment:null,source:{key:'fixture-plan:enact',revision:1,sha256:H},lines:[{product_id:'product-1',variant_id:'variant-1',label:'Chair',option:'Walnut',source_url:'https://merchant.example/items/1',source_sha256:H,observed_at:'2026-10-23T12:00:00Z',unit_cents:1000,quantity:2,subtotal_cents:2000}],subtotal_cents:2000};
function edge(id,actor,permission,subject){f.authsql.query('INSERT INTO authority_edges(edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,delegable,revoked,evidence,originating_authority,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(id,actor,permission,subject,'procurement','self','2026-10-23T00:00:00Z','2026-10-25T00:00:00Z',0,0,'fixture:authority','fixture-root','2026-10-23T00:00:00Z');}
async function approvalStatus(key){const response=await f.raw('/apps/'+binding.id+'/_approval_status?'+new URLSearchParams({key}));return decode(new Uint8Array(await response.arrayBuffer()))[1];}
async function post(key,payload,user=f.owner,headers={}){const response=await f.raw('/apps/'+binding.id+'/_procurement_enactment?'+new URLSearchParams({key}),user,{method:'POST',headers:{'Content-Type':'application/msgpack',...headers},body:Buffer.from(encode(payload))});const bytes=new Uint8Array(await response.arrayBuffer());return{status:response.status,value:response.status===200?decode(bytes):null,text:response.status===200?'':new TextDecoder().decode(bytes)};}

test('signed cryptographic supplier acceptance enacts exact ProtocolContract commitments once',async()=>{
 const supplier={id:seller,name:'Supplier One'},rfq=kernel.prepareRFQ({purchase_list:purchase,purchase_list_revision:1,id:'rfq-enact',requested_at:'2026-10-23T13:00:00Z',supplier,selected:null}),line=rfq.lines[0];
 const quote=kernel.observeQuote({rfq,observation:{id:'quote-enact',supplier,supplier_quote_id:'SQ-E',observed_at:'2026-10-23T14:00:00Z',quoted_at:null,valid_until:'2026-10-30T00:00:00Z',currency:'USD',source_ref:'file:quote.pdf',freight_cents:0,tax_cents:0,other_fees_cents:0,lines:[{id:'quote-line-e',rfq_line_id:line.line_id,quantity:2,unit_cents:900,line_total_cents:null,substitution:null,availability:'In stock',lead_time:'5 days'}]}});
 const comparison=kernel.compareQuotes({rfq,quotes:[quote],expected_rfq_digest:rfq.digest,expected_quotes:[{id:quote.id,digest:quote.digest}]},{asOf:'2026-10-24T00:00:00Z'}),allocation=kernel.allocateQuotes({rfq,quotes:[quote],comparison,id:'allocation-enact',prepared_at:'2026-10-24T00:00:00Z',policy:'lowest_known_merchandise'});
 const allocationKey='procurement-allocation:'+allocation.id,allocationSaved=await store.saveLocal(allocationKey,allocation,0),approval=kernel.prepareApprovalRequest({allocation,allocation_revision:allocationSaved.revision,id:'approval-enact',requested_at:'2026-10-24T00:01:00Z',requester:'user:'+f.owner.id,reason:'Fixture exact approval'});
 const approvalKey='procurement-approval:'+approval.id,approvalSaved=await store.saveLocal(approvalKey,approval,0);let status=await approvalStatus(approvalKey),bodySha=status.body_sha256,decider='user:'+f.owner.id,decidedAt='2026-10-24T00:02:00Z';
 f.add('governance_decision',{decision_id:'decision-enact',proposal_id:approvalKey,outcome:'approved',decided_by:decider,backing:'human_approval',evidence_ref:bodySha,receipt_ref:'fixture:decision-enact',decided_at:decidedAt,cannot_infer:'Approval is not execution.',status:'published'});f.add('assent_signature',{signature_id:'assent-enact',about:approvalKey,digest:bodySha,assented_by:decider,assented_at:decidedAt,statement:'I assent to these exact approval bytes.',cannot_infer:'Assent is not execution.',status:'published'});
 edge('edge-enact-approve',decider,'approve',approval.authority.subject);edge('edge-enact-spend',decider,'spend',approval.authority.subject);status=await approvalStatus(approvalKey);expect(status).toMatchObject({state:'eligible',eligible_for_order_intent:true});
 const intent=kernel.prepareOrderIntent({allocation,allocation_revision:allocationSaved.revision,approval,approval_revision:approvalSaved.revision,authority_status:status,id:'order-intent-enact',prepared_at:'2026-10-24T00:03:00Z'}),orderKey='procurement-order-intent:'+intent.id,orderSaved=await store.saveLocal(orderKey,intent,0);
 const buyer='user:'+f.owner.id,settlement='settlement:fixture',deliverer='deliverer:fixture',capability='procurement-capability:exact-chair',preflight=kernel.prepareProtocolPreflight({order_intent:intent,id:'preflight-enact',prepared_at:'2026-10-24T00:03:30Z',buyer_actor:buyer,lot_actors:[{quote_id:quote.id,seller_actor:seller,settlement_actor:settlement,deliverer_actor:deliverer,capability_ref:capability}]}),contract=preflight.contracts[0];
 expect(contract.supplier_actor_binding_verified).toBe(true);for(const [id,actor,permission] of [['buyer',buyer,'request:capability'],['seller',seller,'quote:capability'],['settlement',settlement,'accept:quote'],['deliverer',deliverer,'deliver:capability']])edge('edge-enact-'+id,actor,permission,contract.exchange_id);
 const acceptedAt='2026-10-24T00:04:00Z',statement=['bonfyre.procurement.supplier-acceptance.v1','exchange='+contract.exchange_id,'order_intent_digest='+intent.digest,'quote_id='+quote.id,'quote_digest='+quote.digest,'supplier_id='+seller,'capability='+capability,'buyer='+buyer,'seller='+seller,'settlement='+settlement,'deliverer='+deliverer,'amount_cents='+contract.quoted_amount_cents,'currency='+contract.currency,'accepted_at='+acceptedAt,''].join('\n'),acceptance=join(f.home,'supplier-acceptance.txt');
 writeFileSync(acceptance,statement);const signature=JSON.parse(f.run(sign,['sign','--key-dir',keyDir,'--file',acceptance])).signature,payload=[1,orderSaved.revision,intent.digest,quote.id,contract.exchange_id,buyer,seller,settlement,deliverer,capability,signature,acceptedAt];
 let result=await post(orderKey,payload);expect(result.status).toBe(200);expect(result.value[0]).toBe(1);expect(result.value[1]).toMatchObject({state:'enacted',record_key:orderKey,revision:1,order_digest:intent.digest,quote_id:quote.id,exchange:contract.exchange_id,supplier_id:seller,supplier_identity_binding_verified:true,supplier_acceptance_verified:true,protocol_enacted:true,commitment_opened:true,replayed:false,accepted_at:acceptedAt});
 let db=new Database(commitmentDb,{readonly:true});try{expect(db.query('select count(*) as n from commitments').get().n).toBe(2);expect(db.query("select count(*) as n from commitment_events where state='open'").get().n).toBe(2);}finally{db.close();}
 result=await post(orderKey,payload);expect(result.status).toBe(200);expect(result.value[1]).toMatchObject({state:'replayed',replayed:true});db=new Database(commitmentDb,{readonly:true});try{expect(db.query('select count(*) as n from commitments').get().n).toBe(2);}finally{db.close();}
 expect((await post(orderKey,[payload[0],2,...payload.slice(2)])).status).toBe(409);const bad=[...payload];bad[10]='0'.repeat(128);expect((await post(orderKey,bad)).status).toBe(409);
 expect((await post(orderKey,payload,f.reader)).status).toBe(403);expect((await post(orderKey,payload,f.other)).status).toBe(403);expect((await post(orderKey,payload,f.owner,{Origin:'https://foreign.example'})).status).toBe(403);
},60000);
