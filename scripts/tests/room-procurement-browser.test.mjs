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
function prepareDownstreamStores({home,env}){
 env.BONFYRE_PROCUREMENT_BINARY=join(root,'cmd/BonfyreProcurement/bonfyre-procurement');
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
afterAll(async()=>{await browser?.close();commitments?.close();commerce?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;},30000);
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
async function savedList(id,count){
 const {row,key,catalogue}=await seed(id,count),value=await preparePurchaseList({row,catalogue}),saved=await savePurchaseList(f.store(binding),value,catalogue);return{row,key,saved};
}
const go=(page,action)=>Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),action()]);
for(const width of [1440,390])retained('forty-item purchase list is saved, listed in full on the native buying page and fits at '+width,async()=>{
 const {saved}=await savedList('procurement-ui-'+width,40),{context,page,errors}=await open(width,f.owner,binding,'?'+new URLSearchParams({list:saved.key}));
 try{
  expect(await page.$$eval('table.pf-items tbody tr td:nth-child(3)',cells=>cells.map(c=>+c.textContent))).toEqual([40]);expect(await page.$eval('h1',n=>n.textContent)).toContain('Private showroom procurement-ui-'+width);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  await page.reload({waitUntil:'networkidle0'});expect(await page.$$eval('table.pf-items tbody tr td:nth-child(3)',cells=>cells.map(c=>+c.textContent))).toEqual([40]);
  await page.goto(f.base+'/apps/'+roomsBinding.id+'/procurement/?'+new URLSearchParams({list:saved.key}),{waitUntil:'networkidle0'});expect(await page.$$eval('table.pf-items tbody tr td:nth-child(3)',cells=>cells.map(c=>+c.textContent))).toEqual([40]);
  if(process.env.PROCUREMENT_PROOF){mkdirSync(process.env.PROCUREMENT_PROOF,{recursive:true});await page.screenshot({path:join(process.env.PROCUREMENT_PROOF,'list-'+width+'.png'),fullPage:true});}
  expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
retained('native buying flow: quote request, two quotes in one table, pick the lowest, send for approval; nothing is ordered',async()=>{
 const {saved}=await savedList('procurement-native-001',2),{context,page,errors}=await open(1440,f.owner,binding,'?'+new URLSearchParams({list:saved.key}));
 const quoteFor=async(name,price)=>{
  await page.type('input[name=supplier]',name);await page.type('input[name=source]','fixture:'+name);
  for(const input of await page.$$('input[name^=price]'))await input.type(price);
  for(const field of ['delivery','tax','fees'])await page.type('input[name='+field+']','0');
  await go(page,()=>page.click('button[form=pf-add]'));
 };
 try{
  await go(page,()=>page.click('form.ak-actions button'));
  const rfq=await f.store(binding).readLocal('procurement-rfq:');expect(rfq.length).toBe(1);expect(rfq[0].value.supplier).toBeNull();expect(rfq[0].value.supplier_contacted).toBe(false);expect(rfq[0].value.order_committed).toBe(false);
  await quoteFor('Supplier One','110.00');await quoteFor('Supplier Two','100.00');
  const quotes=await f.store(binding).readLocal('procurement-quote:');expect(quotes.length).toBe(2);for(const q of quotes){expect(q.value.quote_accepted).toBe(false);expect(q.value.order_authorized).toBe(false);}
  expect(await page.$eval('th.is-best span',n=>n.textContent)).toBe('Supplier Two');expect(await page.$eval('input[name=pick]:checked',n=>n.value)).toBe(quotes.find(q=>q.value.supplier.name==='Supplier Two').key);
  expect(await page.$eval('input[name=confirm]',n=>n.required)).toBe(true);
  await page.click('input[name=confirm]');await go(page,()=>page.click('button[form=pf-send]'));
  expect(await page.$eval('.ak-alert',n=>n.textContent)).toContain('Sent for approval');
  const approvals=await f.store(binding).readLocal('procurement-approval:');expect(approvals.length).toBe(1);const approval=approvals[0].value;
  expect(approval.status).toBe('pending');expect(approval.authority).toMatchObject({owner:'AuthorityGraph',decision:'pending',decided_by:null});
  for(const field of ['approved','spend_authorized','quote_accepted','commitment_opened','order_created','payment_presented'])expect(approval[field]).toBe(false);
  expect((await f.store(binding,f.reader).readLocal(approvals[0].key))[0]).toEqual(approvals[0]);await expect(f.store(binding,f.other).readLocal(approvals[0].key)).rejects.toThrow();
  expect(await page.$eval('.pf-ledger',n=>n.textContent)).toContain('Nothing has been ordered or paid');
  expect(errors).toEqual([]);
 }finally{await context.close();}
},90000);

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
retained('changed source: the native page refuses a quote request from a stale list and says so',async()=>{
 const {row,key,saved}=await savedList('procurement-refresh-001',2);
 await f.store(binding).saveLocal(key,{...row.value,plan:{...row.value.plan,title:'Private updated room',items:[row.value.plan.items[0]]}},1);
 const {context,page,errors}=await open(390,f.owner,binding,'?'+new URLSearchParams({list:saved.key}));
 try{
  await go(page,()=>page.click('form.ak-actions button'));expect(await page.$eval('.ak-alert',n=>n.textContent)).toContain('The room changed after this list was saved');
  expect((await f.store(binding).readLocal('procurement-rfq:')).filter(r=>r.value.source.purchase_list_id===saved.value.id)).toEqual([]);expect((await f.store(binding).readLocal(saved.key))[0].revision).toBe(1);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
