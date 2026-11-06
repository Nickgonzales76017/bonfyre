import {test,expect,beforeAll,afterAll} from 'bun:test';
import {createRequire} from 'node:module';
import {Database} from 'bun:sqlite';
import {mkdirSync,copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {materializeRoomProcurement,roomProcurementBase} from '../materialize_room_procurement.mjs';
import {preparePurchaseList,savePurchaseList,purchaseListStatus,purchaseListPrefix} from '../../components/BonfyreServiceSite/room-procurement.mjs';
import {openDestinationStore} from '../../integrations/browser/workspace-store.mjs';
import {decode} from '../../integrations/browser/workspace-msgpack.mjs';
const packageRoot=process.env.BONFYRE_ROOM_BASE_ROOT,retained=packageRoot?test:test.skip;
let f,binding,roomsBinding,browser,pack,files,cfg,commitments,commerce;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
function prepareDownstreamStores({home}){
 const commitments=new Database(join(home,'commitments.db')),commerce=new Database(join(home,'commerce.db'));
 commitments.exec("CREATE TABLE commitments(commitment_id TEXT PRIMARY KEY,committer TEXT NOT NULL,beneficiary TEXT NOT NULL,resource TEXT NOT NULL,conditions TEXT NOT NULL DEFAULT '',expires_at TEXT NOT NULL DEFAULT '',authority TEXT NOT NULL DEFAULT '',origin TEXT NOT NULL DEFAULT '',enactment TEXT NOT NULL DEFAULT '');CREATE TABLE commitment_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,commitment_id TEXT NOT NULL,state TEXT NOT NULL,actor TEXT NOT NULL DEFAULT '',detail TEXT NOT NULL DEFAULT '',at TEXT NOT NULL DEFAULT '');CREATE INDEX commitment_events_by_id ON commitment_events(commitment_id,seq);");
 commerce.exec("CREATE TABLE commerce_progress(exchange TEXT NOT NULL,stage TEXT NOT NULL,owner TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '',at TEXT NOT NULL DEFAULT '',PRIMARY KEY(exchange,stage));");
 commitments.close();commerce.close();
}
beforeAll(async()=>{
 if(!packageRoot)return;pack=await materializeRoomProcurement({root:packageRoot});
 console.log(JSON.stringify({schema:'bonfyre.procurement.materialization.v1',release:pack.id,base:pack.base,files:pack.files,modules:pack.modules}));files=await loadWorkspaceFiles(packageRoot,pack.id);
 cfg=JSON.parse(files.get('procurement/index.html').toString().match(/<script[^>]*data-purchase-config[^>]*>([\s\S]*?)<\/script>/)[1]);
 process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;f=await workspaceFixture({extraPrefixes:[purchaseListPrefix,'procurement-rfq:','procurement-quote:','procurement-comparison:','procurement-allocation:','procurement-approval:','procurement-order-intent:'],beforeServe:prepareDownstreamStores});
 const schemas=join(f.home,'schemas');for(const type of ['governance_decision','assent_signature'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 f.authsql.exec("CREATE TABLE IF NOT EXISTS authority_edges(edge_id TEXT PRIMARY KEY,actor TEXT NOT NULL,permission TEXT NOT NULL,subject TEXT NOT NULL,purpose TEXT NOT NULL DEFAULT '',scope TEXT NOT NULL DEFAULT '',effective_from TEXT,expires_at TEXT,delegable INTEGER NOT NULL DEFAULT 0,revoked INTEGER NOT NULL DEFAULT 0,evidence TEXT NOT NULL DEFAULT '',originating_authority TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL)");
 binding=await f.install('catalogue','institution:workspace-team');roomsBinding=await f.install('rooms','institution:workspace-team');commitments=new Database(join(f.home,'commitments.db'));commerce=new Database(join(f.home,'commerce.db'));
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();commitments?.close();commerce?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;});
async function seed(id,count=40){
 const catalogue=cfg.catalogues[0],plan={schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id,...structuredClone(catalogue.defaults),title:'Private showroom '+id,notes:'Private fixture note',width:60,depth:40,items:Array.from({length:count},(_,i)=>({key:'item-'+i,product:catalogue.products[0].id,x:(i%10)*3,y:Math.floor(i/10)*3,rotation:0}))};
 const key='fixture-plan:planner-rooms:'+id,row=await f.store(binding).saveLocal(key,{plan,receipt:null},0);return{row,key,catalogue};
}
async function open(width=1440,user=f.owner,b=binding,suffix=''){
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);await page.setViewport({width,height:1000});page.on('pageerror',e=>errors.push(e.message));const[name,...v]=user.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 await page.goto(f.base+'/apps/'+b.id+'/procurement/'+suffix,{waitUntil:'networkidle0'});return{context,page,errors};
}
async function approvalStatus(key,user=f.owner){
 const response=await f.raw('/apps/'+binding.id+'/_approval_status?'+new URLSearchParams({key}),user),bytes=new Uint8Array(await response.arrayBuffer());
 return {status:response.status,value:response.status===200?decode(bytes):null,text:response.status===200?'':new TextDecoder().decode(bytes)};
}
function authorityEdge({id,actor,permission,subject}){
 f.authsql.query('INSERT INTO authority_edges(edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,delegable,revoked,evidence,originating_authority,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)')
  .run(id,actor,permission,subject,'procurement','self','2026-10-23T00:00:00Z','2026-10-25T00:00:00Z',0,0,'fixture:authority','fixture-root','2026-10-23T00:00:00Z');
}
retained('retained room and catalogue modules are preserved; new purchase list has a declared view',async()=>{
 const before=await loadWorkspaceFiles(packageRoot,roomProcurementBase);for(const[n,b]of before)if(!pack.changed.includes(n))expect(files.get(n).equals(b)).toBe(true);
 expect(files.get('CONTINUATIONS.tsv').toString()).toContain('purchase-list\tRoom purchase list\tprocurement/\tprocurement-draft:');
 expect(files.get('CONTINUATIONS.tsv').toString()).toContain('supplier-request\tSupplier request\tprocurement/\tprocurement-rfq:');
 expect(files.get('CONTINUATIONS.tsv').toString()).toContain('supplier-quote\tSupplier quote observation\tprocurement/\tprocurement-quote:');
 expect(files.get('CONTINUATIONS.tsv').toString()).toContain('quote-comparison\tQuote comparison\tprocurement/\tprocurement-comparison:');
 expect(files.get('CONTINUATIONS.tsv').toString()).toContain('allocation-plan\tAllocation plan\tprocurement/\tprocurement-allocation:');expect(files.get('CONTINUATIONS.tsv').toString()).toContain('approval-request\tApproval request\tprocurement/\tprocurement-approval:');expect(files.get('CONTINUATIONS.tsv').toString()).toContain('order-intent\tOrder intent proposal\tprocurement/\tprocurement-order-intent:');
 expect(files.get('product-room/index.html').toString()).toContain('Purchase lists');expect(files.size-before.size).toBe(pack.modules+1);
 expect([...files.keys()].some(name=>name.endsWith('/procurement.wasm'))).toBe(true);
});
for(const width of [1440,390])retained('forty-item purchase list saves, reopens across installed editors and closes at '+width,async()=>{
 const {key,row}=await seed('procurement-ui-'+width),{context,page,errors}=await open(width);
 try{
  await page.waitForSelector('[data-purchase-lists][data-ready=true]');await page.select('[data-ui=room]',key);await page.click('[data-ui=prepare]');await page.waitForSelector('[data-ui=review] tbody tr');
  expect(await page.$eval('[data-ui=review] tbody tr td:nth-child(2)',n=>n.textContent)).toBe('40');await page.click('[data-ui=save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Purchase list saved'));
  const listKey=await page.$eval('[data-ui=saved]',n=>n.value),saved=(await f.store(binding).readLocal(listKey))[0];expect(saved.revision).toBe(1);expect(saved.value.source.revision).toBe(row.revision);expect((await f.store(binding).readLocal(key))[0]).toEqual(row);
  expect(new URL(page.url()).searchParams.get('list')).toBe(listKey);await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('[data-purchase-lists][data-ready=true]');expect(await page.$eval('[data-ui=review]',n=>n.textContent)).toContain('Saved list version 1');
  await page.goto(f.base+'/apps/'+roomsBinding.id+'/procurement/?'+new URLSearchParams({list:listKey}),{waitUntil:'networkidle0'});await page.waitForSelector('[data-purchase-lists][data-ready=true]');expect(await page.$eval('[data-ui=review]',n=>n.textContent)).toContain('Saved list version 1');
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  if(process.env.PROCUREMENT_PROOF){mkdirSync(process.env.PROCUREMENT_PROOF,{recursive:true});await page.screenshot({path:join(process.env.PROCUREMENT_PROOF,'list-'+width+'.png'),fullPage:true});}
  await page.evaluate(()=>{window.heldChoice=document.querySelector('[data-ui=room]');dispatchEvent(new Event('bonfyre:account-changed'));});await page.waitForSelector('[data-purchase-lists][data-closed=true]');expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);expect(await page.evaluate(()=>heldChoice.options.length)).toBe(0);expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
retained('native supplier requests, split quotes, comparison, allocation and pending approval request stay in the same procurement product',async()=>{
 const {key}=await seed('procurement-native-001',2),{context,page,errors}=await open(1440);
 try{
  await page.waitForSelector('[data-purchase-lists][data-ready=true][data-native-procurement-ready=true]');
  await page.select('[data-ui=room]',key);await page.click('[data-ui=prepare]');await page.waitForSelector('[data-ui=review] tbody tr');await page.click('[data-ui=save]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Purchase list saved'));
  const listKey=await page.$eval('[data-ui=saved]',n=>n.value);
  await page.click('[data-ui=native-rfq-prepare]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=native-rfq-review]')?.textContent.includes('Supplier request draft'));
  await page.click('[data-ui=native-rfq-save]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Supplier request saved'));
  const rfqKey=await page.$eval('[data-ui=native-rfq-saved]',n=>n.value),rfq=(await f.store(binding).readLocal(rfqKey))[0];
  expect(rfq.value.source.purchase_list_id).toBe(listKey.slice('procurement-draft:'.length));expect(rfq.value.source.revision).toBe(1);expect(rfq.value.supplier).toBeNull();
  expect(rfq.value.supplier_contacted).toBe(false);expect(rfq.value.purchase_approved).toBe(false);expect(rfq.value.order_committed).toBe(false);

  await page.type('[data-ui=native-quote-supplier-name]','Supplier One');await page.type('[data-ui=native-source-ref]','fixture:supplier-quote-001');
  for(const field of ['native-freight','native-tax','native-fees'])await page.type('[data-ui='+field+']','0');
  await page.$$eval('[data-ui=native-quote-lines] fieldset',rows=>rows.forEach(row=>{const q=row.querySelector('[data-field=quantity]'),u=row.querySelector('[data-field=unit]');q.value='1';u.value='11000';q.dispatchEvent(new Event('input',{bubbles:true}));u.dispatchEvent(new Event('input',{bubbles:true}));}));
  await page.click('[data-ui=native-quote-save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Quote observation saved'));
  const firstQuoteKey=await page.$eval('[data-ui=native-quote-saved]',n=>n.value),firstQuote=(await f.store(binding).readLocal(firstQuoteKey))[0];
  expect(firstQuote.value.supplier.name).toBe('Supplier One');expect(firstQuote.value.quote_accepted).toBe(false);

  await page.$eval('[data-ui=native-quote-supplier-name]',n=>n.value='Supplier Two');await page.$eval('[data-ui=native-source-ref]',n=>n.value='fixture:supplier-quote-002');
  await page.$$eval('[data-ui=native-quote-lines] [data-field=unit]',inputs=>inputs.forEach(input=>{input.value='10000';input.dispatchEvent(new Event('input',{bubbles:true}));}));
  await page.click('[data-ui=native-quote-save]');await page.waitForFunction(first=>document.querySelector('[data-ui=native-quote-saved]').value!==first,{},firstQuoteKey);
  const secondQuoteKey=await page.$eval('[data-ui=native-quote-saved]',n=>n.value),secondQuote=(await f.store(binding).readLocal(secondQuoteKey))[0];
  expect(secondQuote.value.supplier.name).toBe('Supplier Two');expect(secondQuote.value.quote_accepted).toBe(false);expect(secondQuote.value.reserved).toBe(false);expect(secondQuote.value.order_authorized).toBe(false);

  await page.click('[data-ui=native-compare-save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Quote comparison saved'));
  const comparisonKey=await page.$eval('[data-ui=native-comparison-saved]',n=>n.value),comparison=(await f.store(binding).readLocal(comparisonKey))[0];
  expect(comparison.value.status).toBe('current');expect(comparison.value.approved).toBe(false);expect(comparison.value.committed).toBe(false);expect(comparison.value.ordered).toBe(false);
  expect(await page.$eval('[data-ui=native-comparison-review]',n=>n.textContent)).toContain('Current status: current');

  await page.click('[data-ui=native-allocation-prepare]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Allocation plan prepared'));
  expect(await page.$eval('[data-ui=native-allocation-review]',n=>n.textContent)).toContain('CommerceChain: unmaterialized');
  await page.click('[data-ui=native-allocation-save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Allocation plan saved'));
  const allocationKey=await page.$eval('[data-ui=native-allocation-saved]',n=>n.value),allocation=(await f.store(binding).readLocal(allocationKey))[0];
  expect(allocation.value.schema).toBe('bonfyre.procurement.allocation-plan.v1');expect(allocation.value.allocations).toHaveLength(2);expect(allocation.value.selected_quotes).toHaveLength(2);
  expect(allocation.value.known_merchandise_cents).toBe(21000);expect(allocation.value.known_total_cents).toBe(21000);expect(allocation.value.fees_attribution).toBe('full_quote');expect(allocation.value.order_lots).toHaveLength(2);expect(allocation.value.order_lots.every(row=>row.full_quote_consumed&&row.known_total_cents!==null)).toBe(true);
  expect(allocation.value.approval_required).toBe(true);expect(allocation.value.approved).toBe(false);expect(allocation.value.cart_created).toBe(false);expect(allocation.value.commitment_opened).toBe(false);expect(allocation.value.order_created).toBe(false);
  expect(allocation.value.commerce_chain).toMatchObject({owner:'CommerceChain',status:'unmaterialized',next_required_stage:'product'});
  expect((await f.store(binding,f.reader).readLocal(allocationKey))[0]).toEqual(allocation);await expect(f.store(binding,f.other).readLocal(allocationKey)).rejects.toThrow();

  await page.type('[data-ui=native-approval-requester]','user:19');await page.type('[data-ui=native-approval-reason]','Review exact room allocation');
  await page.click('[data-ui=native-approval-prepare]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Approval request prepared'));
  expect(await page.$eval('[data-ui=native-approval-review]',n=>n.textContent)).toContain('Authority owner: AuthorityGraph');
  await page.click('[data-ui=native-approval-save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Approval request saved'));
  const approvalKey=await page.$eval('[data-ui=native-approval-saved]',n=>n.value),approval=(await f.store(binding).readLocal(approvalKey))[0];
  expect(approval.value.status).toBe('pending');expect(approval.value.source).toEqual({allocation_id:allocation.value.id,revision:allocation.revision,digest:allocation.value.digest});
  expect(approval.value.authority).toMatchObject({owner:'AuthorityGraph',required_permissions:['approve','spend'],decision:'pending',decided_by:null,decision_ref:null});
  expect(approval.value.known_total_cents).toBe(21000);expect(approval.value.fees_attribution).toBe('full_quote');expect(approval.value.approved).toBe(false);expect(approval.value.spend_authorized).toBe(false);expect(approval.value.quote_accepted).toBe(false);expect(approval.value.commitment_opened).toBe(false);expect(approval.value.order_created).toBe(false);expect(approval.value.payment_presented).toBe(false);
  expect((await f.store(binding,f.reader).readLocal(approvalKey))[0]).toEqual(approval);await expect(f.store(binding,f.other).readLocal(approvalKey)).rejects.toThrow();

  expect(await page.$eval('[data-ui=native-approval-reconcile]',n=>n.disabled)).toBe(false);
  expect(await page.evaluate(async key=>(await fetch('../_approval_status?'+new URLSearchParams({key}),{credentials:'same-origin',cache:'no-store'})).status,approvalKey)).toBe(200);
  await page.click('[data-ui=native-approval-reconcile]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=native-authority-status]')?.textContent.includes('State: pending')||document.querySelector('[data-ui=error]')?.textContent);
  expect(await page.$eval('[data-ui=error]',n=>n.textContent)).toBe('');
  let reconciliation=await approvalStatus(approvalKey);expect(reconciliation.status).toBe(200);const bodySha=reconciliation.value[1].body_sha256;expect(bodySha).toMatch(/^[a-f0-9]{64}$/);expect(reconciliation.value[1]).toMatchObject({state:'pending',eligible_for_order_intent:false});
  const decider='user:'+f.owner.id,decidedAt='2026-10-23T17:00:00Z';
  f.add('governance_decision',{decision_id:'decision-procurement-native-001',proposal_id:approvalKey,outcome:'approved',decided_by:decider,backing:'human_approval',evidence_ref:bodySha,receipt_ref:'fixture:decision-procurement-native-001',decided_at:decidedAt,cannot_infer:'Approval does not execute procurement.',status:'published'});
  f.add('assent_signature',{signature_id:'assent-procurement-native-001',about:approvalKey,digest:bodySha,assented_by:decider,assented_at:decidedAt,statement:'I assent to these exact approval-request bytes.',cannot_infer:'Human assent does not execute procurement.',status:'published'});
  authorityEdge({id:'edge-procurement-native-approve',actor:decider,permission:'approve',subject:approval.value.authority.subject});
  authorityEdge({id:'edge-procurement-native-spend',actor:decider,permission:'spend',subject:approval.value.authority.subject});
  reconciliation=await approvalStatus(approvalKey);expect(reconciliation.value[1]).toMatchObject({state:'eligible',assent_present:true,approve_authority:true,spend_authority:true,amount_known:true,known_total_cents:21000,eligible_for_order_intent:true});
  await page.click('[data-ui=native-approval-reconcile]');await page.waitForFunction(()=>document.querySelector('[data-ui=native-authority-status]')?.textContent.includes('State: eligible'));
  expect(await page.$eval('[data-ui=native-authority-status]',n=>n.textContent)).toContain('State: eligible');
  await page.click('[data-ui=native-order-prepare]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Order intent prepared'));
  expect(await page.$eval('[data-ui=native-order-review]',n=>n.textContent)).toContain('CommitmentGraph handoff: unmaterialized');
  await page.click('[data-ui=native-order-save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Order intent saved'));
  const orderIntentKey=await page.$eval('[data-ui=native-order-saved]',n=>n.value),orderIntent=(await f.store(binding).readLocal(orderIntentKey))[0];
  expect(orderIntent.value.schema).toBe('bonfyre.procurement.order-intent.v1');expect(orderIntent.value.authority_eligible).toBe(true);expect(orderIntent.value.known_total_cents).toBe(21000);expect(orderIntent.value.fees_attribution).toBe('full_quote');expect(orderIntent.value.source).toMatchObject({allocation_id:allocation.value.id,allocation_revision:allocation.revision,approval_id:approval.value.id,approval_revision:approval.revision,decision_id:'decision-procurement-native-001'});
  expect(orderIntent.value.commitment_handoff).toMatchObject({owner:'CommitmentGraph',status:'unmaterialized',requires_quote_acceptance:true,requires_supplier_counterparty_terms:true});
  for(const field of ['quote_accepted','reservation_created','cart_created','commitment_opened','order_created','purchase_order_issued','supplier_accepted','payable_created','payment_presented','settled','fulfilled','shipped','received'])expect(orderIntent.value[field]).toBe(false);
  expect((await f.store(binding,f.reader).readLocal(orderIntentKey))[0]).toEqual(orderIntent);await expect(f.store(binding,f.other).readLocal(orderIntentKey)).rejects.toThrow();

  expect(await page.$eval('[data-ui=native-protocol-prepare]',n=>n.disabled)).toBe(false);
  await page.type('[data-ui=native-protocol-buyer]','buyer:browser-fixture');
  await page.$$eval('[data-ui=native-protocol-lots] fieldset',fields=>fields.forEach((field,index)=>{
   const set=(name,value)=>{const input=field.querySelector('[data-field="'+name+'"]');input.value=value;input.dispatchEvent(new Event('input',{bubbles:true}));};
   set('seller','seller:browser-'+(index+1));set('settlement','settlement:browser');set('deliverer','deliverer:browser-'+(index+1));set('capability','procurement-capability:browser-'+(index+1));
  }));
  await page.click('[data-ui=native-protocol-prepare]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Effect-free ProtocolContract preflight prepared'));
  const protocolText=await page.$eval('[data-ui=native-protocol-review]',n=>n.textContent);
  expect(protocolText).toContain('Protocol owner: ProtocolContract');expect(protocolText).toContain('commitment owner: CommitmentGraph');expect(protocolText).toContain('effect free: true');
  expect(await page.$$eval('[data-ui=native-protocol-review] > fieldset',nodes=>nodes.length)).toBe(2);
  expect(protocolText).toContain('Contract: quote-accept · authority_preflight_required');expect(protocolText).toContain('Supplier actor binding verified: false');expect(protocolText).toContain('supplier acceptance present: false');expect(protocolText).toContain('protocol enacted: false');expect(protocolText).toContain('commitment opened: false');
  expect(protocolText).toContain('buyer → request:capability');expect(protocolText).toContain('seller → quote:capability');expect(protocolText).toContain('settlement → accept:quote');expect(protocolText).toContain('deliverer → deliver:capability');
  expect(await page.$eval('[data-ui=native-supplier-acceptance]',n=>n.hidden)).toBe(false);expect(await page.$eval('[data-ui=native-enactment-contract]',n=>n.options.length)).toBe(1);expect(await page.$eval('[data-ui=native-enactment-submit]',n=>n.disabled)).toBe(true);expect(await page.$eval('[data-ui=native-enactment-result]',n=>n.textContent)).toContain('No preflight lot binds a retained supplier ID');

  expect(await page.$eval('[data-ui=native-order-reconcile]',n=>n.disabled)).toBe(false);
  await page.click('[data-ui=native-order-reconcile]');await page.waitForFunction(()=>document.querySelector('[data-ui=native-downstream-status]')?.textContent.includes('State: unmaterialized'));
  expect(await page.$eval('[data-ui=native-downstream-status]',n=>n.textContent)).toContain('CommerceChain furthest stage: none established');

  const commitmentId=orderIntent.value.commitment_handoff.proposed_resource,exchange=orderIntent.value.commerce_chain.exchange_candidate;
  commitments.query('INSERT INTO commitments(commitment_id,committer,beneficiary,resource,conditions,authority,origin,enactment) VALUES(?,?,?,?,?,?,?,?)').run(commitmentId,'supplier:browser-fixture','buyer:browser-fixture',commitmentId,'Fixture downstream terms','fixture:counterparty','fixture:supplier','fixture:quote-accepted');
  commitments.query('INSERT INTO commitment_events(commitment_id,state,actor,detail,at) VALUES(?,?,?,?,?)').run(commitmentId,'open','supplier:browser-fixture','Fixture downstream commitment','2026-10-23T18:10:00Z');
  for(const [stage,owner] of [['product','ObjectContract / catalogue'],['listing','merchant or marketplace ForeignTwin']])commerce.query('INSERT INTO commerce_progress(exchange,stage,owner,detail,at) VALUES(?,?,?,?,?)').run(exchange,stage,owner,'Fixture downstream evidence','2026-10-23T18:11:00Z');
  await page.click('[data-ui=native-order-reconcile]');await page.waitForFunction(()=>document.querySelector('[data-ui=native-downstream-status]')?.textContent.includes('State: downstream_observed'));
  const downstreamText=await page.$eval('[data-ui=native-downstream-status]',n=>n.textContent);expect(downstreamText).toContain('CommitmentGraph: open');expect(downstreamText).toContain('CommerceChain furthest stage: listing');expect(downstreamText).toContain('inventory · owner merchant system of record');

  await page.goto(f.base+'/apps/'+binding.id+'/procurement/?'+new URLSearchParams({orderIntent:orderIntentKey}),{waitUntil:'networkidle0'});
  await page.waitForFunction(()=>document.querySelector('[data-ui=native-order-review]')?.textContent.includes('Order intent proposal'));
  const allocationText=await page.$eval('[data-ui=native-allocation-review]',n=>n.textContent);expect(allocationText).toContain('Supplier One');expect(allocationText).toContain('Supplier Two');expect(allocationText).toContain('Current source status: current');const approvalText=await page.$eval('[data-ui=native-approval-review]',n=>n.textContent);expect(approvalText).toContain('decision: pending');expect(approvalText).toContain('source allocation: current');
  await page.click('[data-ui=native-order-reconcile]');await page.waitForFunction(()=>document.querySelector('[data-ui=native-downstream-status]')?.textContent.includes('State: downstream_observed'));expect(await page.$eval('[data-ui=native-downstream-status]',n=>n.textContent)).toContain('CommerceChain furthest stage: listing');
  expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);

retained('atomic source revision check refuses a stale preview without saving a purchase list',async()=>{
 const {row,key,catalogue}=await seed('procurement-stale-001',2),value=await preparePurchaseList({row,catalogue}),store=f.store(binding);
 await store.saveLocal(key,{...row.value,plan:{...row.value.plan,notes:'New layout review'}},1);
 await expect(savePurchaseList(store,value,catalogue)).rejects.toThrow();expect(await store.readLocal(purchaseListPrefix+value.id)).toEqual([]);expect((await purchaseListStatus(value,catalogue,(await store.readLocal(key))[0])).current).toBe(false);
});
retained('reader sees the same list but cannot save; unrelated account cannot read it',async()=>{
 const {row,catalogue}=await seed('procurement-role-001',2),value=await preparePurchaseList({row,catalogue});const saved=await savePurchaseList(f.store(binding),value,catalogue);
 expect((await f.store(binding,f.reader).readLocal(saved.key))[0]).toEqual(saved);await expect(savePurchaseList(f.store(binding,f.reader),value,catalogue,1)).rejects.toThrow();await expect(f.store(binding,f.other).readLocal(saved.key)).rejects.toThrow();expect((await f.store(binding).readLocal(saved.key))[0].revision).toBe(1);
});
retained('native lost-ack replay retains exactly one purchase list version',async()=>{
 const {row,catalogue}=await seed('procurement-replay-001',2),value=await preparePurchaseList({row,catalogue});let drop=true;
 const store=openDestinationStore(binding.endpoint,{origin:f.base,request:async(url,options)=>{const response=await f.raw(url,f.owner,options);if(drop&&new URL(url,f.base).searchParams.get('operation')==='write'&&response.ok){drop=false;throw Error('fixture: dropped committed acknowledgement');}return response;}});
 await expect(savePurchaseList(store,value,catalogue)).rejects.toThrow();expect((await f.store(binding).readLocal(purchaseListPrefix+value.id))[0].revision).toBe(1);
 const result=await savePurchaseList(store,value,catalogue);expect(result.revision).toBe(1);expect((await f.store(binding).readLocal(result.key))[0].revision).toBe(1);
});
retained('changed source displays stale status; refresh retains a new list version and exact lineage',async()=>{
 const {row,key,catalogue}=await seed('procurement-refresh-001',2),value=await preparePurchaseList({row,catalogue}),saved=await savePurchaseList(f.store(binding),value,catalogue);
 await f.store(binding).saveLocal(key,{...row.value,plan:{...row.value.plan,title:'Private updated room',items:[row.value.plan.items[0]]}},1);
 const {context,page,errors}=await open(390,f.owner,binding,'?'+new URLSearchParams({list:saved.key}));
 try{
  await page.waitForSelector('[data-purchase-lists][data-ready=true]');expect(await page.$eval('[data-ui=status]',n=>n.textContent)).toContain('changed');expect(await page.$eval('[data-ui=save]',n=>n.disabled)).toBe(true);
  await page.click('[data-ui=refresh]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Refreshed'));await page.click('[data-ui=save]');await page.waitForFunction(()=>document.querySelector('[data-ui=status]').textContent.startsWith('Purchase list saved'));
  const updated=(await f.store(binding).readLocal(saved.key))[0];expect(updated.revision).toBe(2);expect(updated.value.source.revision).toBe(2);expect(updated.value.lines[0].quantity).toBe(1);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
