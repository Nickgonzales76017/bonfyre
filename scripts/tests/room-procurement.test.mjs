import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {preparePurchaseList,verifyPurchaseList,purchaseListStatus,savePurchaseList} from '../../components/BonfyreServiceSite/room-procurement.mjs';
import {portfolio,projectWorkspaceSetups} from '../publish_experience_setups.mjs';
const repo=resolve(fileURLToPath(new URL('../..',import.meta.url)));
const product={id:'table',product_id:'TABLE-1',variant_id:'OAK-1',label:'Display table',option:'Oak',source_url:'https://fixture.example/products/table',source_sha256:'b'.repeat(64),observed_at:'2026-10-17T10:00:00Z',price_cents:12345,dimensions:{width:24,depth:24},questions:[]};
const catalogue={digest:'a'.repeat(64),currency:'USD',products:[product]};
function row(count=40){return{key:'fixture-plan:rooms:room-test-0001',revision:2,value:{plan:{schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id:'room-test-0001',title:'Fictional showroom',width:60,depth:40,budget:1000000,deadline:'',notes:'',items:Array.from({length:count},(_,i)=>({key:'placement-'+i,product:'table',x:(i%10)*3,y:Math.floor(i/10)*3,rotation:0}))},receipt:null}};}
const prepare=(r=row(),c=catalogue)=>preparePurchaseList({row:r,catalogue:c,id:'purchase-test-001',createdAt:'2026-10-23T00:00:00Z'});
test('forty placements group by exact option and retain exact source revision and cents',async()=>{
 const r=row(),v=await prepare(r);expect(v.lines).toHaveLength(1);expect(v.lines[0].quantity).toBe(40);expect(v.lines[0].placements).toHaveLength(40);expect(v.subtotal_cents).toBe(493800);expect(v.source.value).toEqual(r.value);expect(v.source.revision).toBe(2);expect(v.commitment).toBeNull();expect(v.state).toBe('draft');expect(await verifyPurchaseList(v,catalogue)).toEqual(v);expect((await purchaseListStatus(v,catalogue,r)).current).toBe(true);
});
test('sixty-four quantities remain exact; excessive quantities and unsafe prices fail',async()=>{
 expect((await prepare(row(64))).subtotal_cents).toBe(790080);await expect(prepare(row(65))).rejects.toThrow();await expect(prepare(row(),{...catalogue,products:[{...product,price_cents:Number.MAX_SAFE_INTEGER}]})).rejects.toThrow('precision');await expect(prepare(row(),{...catalogue,currency:'EUR'})).rejects.toThrow();
});
test('a shared title never merges distinct options and conflicting observations are refused',async()=>{
 const c=structuredClone(catalogue);c.products.push({...product,id:'table-two',variant_id:'OAK-2'});const r=row(2);r.value.plan.items[1].product='table-two';expect((await prepare(r,c)).lines).toHaveLength(2);
 c.products[1].variant_id=product.variant_id;c.products[1].source_sha256='c'.repeat(64);await expect(prepare(r,c)).rejects.toThrow('conflicting');
 c.products[1].id='table';await expect(prepare(r,c)).rejects.toThrow('ambiguous');
});
test('malformed identity, missing option, mismatched source and tampered derived amounts fail',async()=>{
 const r=row();await expect(prepare({...r,key:r.key+'other'})).rejects.toThrow();await expect(prepare(r,{...catalogue,products:[{...product,variant_id:''}]})).rejects.toThrow();await expect(prepare(r,{...catalogue,products:[{...product,source_url:'javascript:alert(1)'}]})).rejects.toThrow();
 const v=await prepare();for(const change of [x=>x.lines[0].quantity++,x=>x.subtotal_cents++,x=>x.source.revision++,x=>x.commitment={},x=>x.state='approved',x=>x.source.value.plan.notes='changed',x=>x.extra='undeclared']){
  const bad=structuredClone(v);change(bad);await expect(verifyPurchaseList(bad,catalogue)).rejects.toThrow();
 }
});
test('source changes, disappearance and same-version content mismatch make a saved list stale',async()=>{
 const r=row(),v=await prepare(r);expect((await purchaseListStatus(v,catalogue,null)).current).toBe(false);expect((await purchaseListStatus(v,catalogue,{...r,revision:3})).current).toBe(false);r.value.plan.notes='changed without version';expect((await purchaseListStatus(v,catalogue,r)).current).toBe(false);
});
test('preparation snapshots mutable source before asynchronous hashing',async()=>{
 const r=row(),c=structuredClone(catalogue),future=prepare(r,c);r.value.plan.title='later mutation';c.products[0].price_cents=1;const v=await future;expect(v.title).toBe('Fictional showroom');expect(v.lines[0].unit_cents).toBe(12345);expect((await verifyPurchaseList(v,catalogue)).source.value.plan.title).toBe('Fictional showroom');
});
test('save verifies exact native acknowledgement and cannot accept altered readback',async()=>{
 const v=await prepare();let effects;const store={saveLocalBatch:async rows=>{effects=rows;return[{key:'procurement-draft:'+v.id,revision:1,value:{...v,title:'Changed'}}];}};
 await expect(savePurchaseList(store,v,catalogue)).rejects.toThrow('confirmed');expect(effects[0]).toEqual({key:v.source.key,expectedRevision:2,checkOnly:true});expect(effects[1].expectedRevision).toBe(0);
});

test('workspace projection admits every procurement continuation including allocation',()=>{
 const rows=portfolio(readFileSync(join(repo,'cmd/BonfyreExperienceStage/experience-maturities.yaff'),'utf8'));
 const records=projectWorkspaceSetups(rows,'f'.repeat(64)),packs=['experience.retail-procurement.workspace','experience.multi-merchant-catalogue.workspace'];
 for(const pack of packs){
  const fields=records.filter(row=>row.type==='pack_demand'&&row.value.pack===pack&&row.value.demand_kind==='workspace-record').map(row=>row.value.fields);
  for(const prefix of ['procurement-draft:','procurement-rfq:','procurement-quote:','procurement-comparison:','procurement-allocation:','procurement-approval:','procurement-order-intent:'])expect(fields).toContain(prefix);
 }
});
