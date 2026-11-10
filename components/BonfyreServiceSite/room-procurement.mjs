import {assessPlan,validatePlanRecordKey} from './planner-model.mjs';
import {releaseDigest} from './catalogue-release.mjs';
export const purchaseListPrefix='procurement-draft:';
const fail=message=>{throw Error(message);},hash=v=>/^[a-f0-9]{64}$/.test(v??''),id=v=>typeof v==='string'&&/^[A-Za-z0-9-]{8,80}$/.test(v);
const cents=v=>{if(!Number.isSafeInteger(v)||v<0)fail('The purchase list exceeds supported price precision.');return v;};
const text=(v,n)=>typeof v==='string'&&v.length>0&&v.length<=n&&!/[\x00-\x1f\x7f]/.test(v);
function sourceRows(row,catalogue){
 if(!row||!Number.isSafeInteger(row.revision)||row.revision<1||row.revision>=2147483647)fail('Choose a current saved room version.');
 validatePlanRecordKey(row.key,row.value?.plan);
 if(!hash(catalogue?.digest)||catalogue.currency!=='USD'||!Array.isArray(catalogue.products)||new Set(catalogue.products.map(p=>p.id)).size!==catalogue.products.length)fail('The retained room collection is unavailable or ambiguous.');
 const assessment=assessPlan(row.value.plan,catalogue),groups=new Map();
 for(const placement of assessment.lines){
  const p=placement.product;
  if(!text(p.product_id,128)||!text(p.variant_id,128)||!hash(p.source_sha256)||!text(p.source_url,4000)||!text(p.label,1000)||!text(p.option,1000)||!Number.isFinite(Date.parse(p.observed_at)))fail('A product is missing its exact option or source observation.');
  const url=new URL(p.source_url);if(url.protocol!=='https:'||url.username||url.password)fail('A product source address is not supported.');
  const identity=JSON.stringify([p.source_url,p.product_id,p.variant_id]),unit=cents(p.price_cents),prior=groups.get(identity);
  if(prior&&(prior.source_sha256!==p.source_sha256||prior.unit_cents!==unit||prior.observed_at!==p.observed_at))fail('The same option has conflicting source observations.');
  if(!prior)groups.set(identity,{product_id:p.product_id,variant_id:p.variant_id,label:p.label,option:p.option,source_url:p.source_url,source_sha256:p.source_sha256,observed_at:p.observed_at,unit_cents:unit,quantity:0,placements:[],subtotal_cents:0});
  const line=groups.get(identity);line.quantity++;line.placements.push(placement.key);line.subtotal_cents=cents(Number(BigInt(line.quantity)*BigInt(unit)));
 }
 const lines=[...groups.entries()].sort(([a],[b])=>a<b?-1:a>b?1:0).map(([,v])=>({...v,placements:v.placements.sort()}));
 return {lines,subtotal_cents:cents(Number(lines.reduce((s,l)=>s+BigInt(l.subtotal_cents),0n))),issues:assessment.issues,questions:assessment.questions};
}
export async function preparePurchaseList({row,catalogue,id:identity=crypto.randomUUID(),createdAt=new Date().toISOString()}){
 if(!id(identity)||typeof createdAt!=='string'||createdAt.length>64||!Number.isFinite(Date.parse(createdAt)))fail('Invalid purchase list identity.');
 // Snapshot before hashing can yield; mutable caller objects never enter a retained list.
 row=structuredClone(row);catalogue=structuredClone(catalogue);
 const derived=sourceRows(row,catalogue),value={schema:'service-site.room-purchase-list.v1',id:identity,state:'draft',created_at:createdAt,title:row.value.plan.title,
  currency:'USD',catalogue:catalogue.digest,source:{key:row.key,revision:row.revision,sha256:await releaseDigest(row.value),value:row.value},...derived,
  exclusions:['Current supplier quote','Availability','Delivery','Tax','Purchase authorization'],commitment:null};
 value.sha256=await releaseDigest(value);
 if(new TextEncoder().encode(JSON.stringify(value)).length>220*1024)fail('This purchase list exceeds 220 KiB.');
 return value;
}
export async function verifyPurchaseList(value,catalogue){
 value=structuredClone(value);
 if(!value||value.schema!=='service-site.room-purchase-list.v1'||!hash(value.sha256)||value.state!=='draft'||value.commitment!==null)fail('Invalid saved purchase list.');
 const expected=await preparePurchaseList({row:value.source,catalogue,id:value.id,createdAt:value.created_at});
 if(await releaseDigest(value)!==await releaseDigest(expected))fail('The purchase list differs from its saved room and retained product observations.');
 return value;
}
export async function purchaseListStatus(value,catalogue,current){
 current=structuredClone(current);value=await verifyPurchaseList(value,catalogue);
 if(!current||current.key!==value.source.key||current.revision!==value.source.revision||await releaseDigest(current.value)!==value.source.sha256)return {current:false,message:'The source room changed or is unavailable. Refresh this list before continuing.'};
 return {current:true,message:'Matches saved room version '+current.revision+'. Confirm supplier prices and availability before requesting a quote.'};
}
export async function savePurchaseList(store,value,catalogue,expectedRevision=0){
 value=await verifyPurchaseList(value,catalogue);
 if(!Number.isSafeInteger(expectedRevision)||expectedRevision<0||expectedRevision>=2147483646)fail('Invalid saved purchase list version.');
 const key=purchaseListPrefix+value.id,rows=await store.saveLocalBatch([{key:value.source.key,expectedRevision:value.source.revision,checkOnly:true},{key,value,expectedRevision}]);
 const saved=rows.find(r=>r.key===key);
 if(!saved||saved.revision!==expectedRevision+1||await releaseDigest(saved.value)!==await releaseDigest(value))fail('The purchase list save could not be confirmed. Reload saved work before trying again.');
 return saved;
}
