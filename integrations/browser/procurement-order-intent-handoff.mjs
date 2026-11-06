import {openDestinationStore} from './workspace-store.mjs';
import {decode} from './workspace-msgpack.mjs';
export const procurementOrderIntentPrefix='procurement-order-intent:';
export const downstreamFalseFlags=Object.freeze(['quote_accepted','reservation_created','cart_created','commitment_opened','order_created','purchase_order_issued','supplier_accepted','payable_created','payment_presented','settled','fulfilled','shipped','received']);
const text=(v,max=512)=>typeof v==='string'&&v.length>0&&v.length<=max&&!/[\x00-\x1f\x7f]/.test(v);
const safeInt=v=>Number.isSafeInteger(v)&&v>=0;
export function verifyProcurementOrderIntentRow(row,key){
 if(!text(key)||!key.startsWith(procurementOrderIntentPrefix))throw Error('Choose an exact procurement order-intent record.');
 if(!row||row.key!==key||!Number.isSafeInteger(row.revision)||row.revision<1)throw Error('The saved order-intent version is unavailable.');
 const v=row.value;
 if(!v||v.schema!=='bonfyre.procurement.order-intent.v1'||!text(v.id,128)||key!==procurementOrderIntentPrefix+v.id)throw Error('This record is not a procurement order intent.');
 if(v.status!=='prepared'||v.authority_eligible!==true||!text(v.currency,3)||!safeInt(v.known_total_cents))throw Error('The order intent is not a bounded approved preparation record.');
 if(!v.source||!text(v.source.allocation_id,128)||!safeInt(v.source.allocation_revision)||v.source.allocation_revision<1||!text(v.source.approval_id,128)||!safeInt(v.source.approval_revision)||v.source.approval_revision<1)throw Error('The order intent has incomplete source lineage.');
 if(!Array.isArray(v.order_lots)||!v.order_lots.length||v.order_lots.length>64)throw Error('The order intent has an invalid supplier-lot set.');
 for(const flag of downstreamFalseFlags)if(v[flag]!==false)throw Error('The order intent already claims an effect that this read-only handoff cannot establish.');
 return row;
}
const el=(tag,value)=>{const n=document.createElement(tag);if(value!==undefined)n.textContent=value;return n;};
const line=(host,label,value)=>host.append(el('p',label+': '+String(value??'—')));
export async function readDownstreamStatus(key,revision,{signal}={}){
 const url=new URL('../../_procurement_downstream',location.href);url.searchParams.set('key',key);
 const response=await fetch(url,{credentials:'same-origin',cache:'no-store',redirect:'error',signal}),bytes=new Uint8Array(await response.arrayBuffer());
 if(bytes.byteLength>16384)throw Error('Downstream owner status exceeded its response bound.');
 if(!response.ok)throw Error(new TextDecoder().decode(bytes)||'Downstream owner status is unavailable.');
 const envelope=decode(bytes),status=Array.isArray(envelope)&&envelope.length===2&&envelope[0]===1?envelope[1]:null;
 if(!status||status.record_key!==key||status.revision!==revision)throw Error('Downstream owner status does not bind this exact order-intent version.');
 return status;
}
export async function mountProcurementOrderIntentHandoff(root){
 if(!root)throw Error('Procurement handoff host is missing.');
 const life=new AbortController(),clear=()=>{life.abort();root.replaceChildren(el('h1','Reopen this downstream review'),el('p','The account or page context changed; retained procurement details were cleared.'));};
 addEventListener('bonfyre:account-changed',clear,{signal:life.signal});addEventListener('pagehide',clear,{signal:life.signal});
 const key=new URL(location.href).searchParams.get('intent');if(!key?.startsWith(procurementOrderIntentPrefix))throw Error('Open this view from a saved procurement order intent.');
 const endpoint=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content;if(!endpoint)throw Error('No native workspace binding is available.');
 const store=openDestinationStore(endpoint,{signal:life.signal}),rows=await store.readLocal(key),row=verifyProcurementOrderIntentRow(rows.find(x=>x.key===key),key),v=row.value;if(life.signal.aborted)return;
 root.replaceChildren(el('p','Procurement / downstream review'),el('h1','Order intent handoff'));line(root,'Saved record',row.key);line(root,'Revision',row.revision);line(root,'Prepared status',v.status);line(root,'Exact approved spend',v.known_total_cents+' '+v.currency+' cents');
 const source=el('section');source.append(el('h2','Exact source lineage'));line(source,'Allocation',v.source.allocation_id+' · revision '+v.source.allocation_revision);line(source,'Approval',v.source.approval_id+' · revision '+v.source.approval_revision);line(source,'Decision',v.source.decision_id||'—');root.append(source);
 const lots=el('section');lots.append(el('h2','Supplier lots'));const list=el('ul');for(const lot of v.order_lots)list.append(el('li',(lot.supplier?.name||lot.supplier?.id||lot.quote_id||'Supplier lot')+' · '+(lot.known_total_cents??'unknown')+' cents'));lots.append(list);root.append(lots);
 const boundaries=el('section');boundaries.append(el('h2','Effects remain separate'));for(const flag of downstreamFalseFlags)line(boundaries,flag,false);root.append(boundaries);
 const statusHost=el('section');statusHost.append(el('h2','Native owner readback'),el('p','Checking CommitmentGraph and CommerceChain without writing…'));root.append(statusHost);
 try{const s=await readDownstreamStatus(key,row.revision,{signal:life.signal});if(life.signal.aborted)return;statusHost.replaceChildren(el('h2','Native owner readback'));line(statusHost,'State',s.state);line(statusHost,'CommitmentGraph',s.commitment_found?s.commitment_state:'not materialized');line(statusHost,'CommerceChain furthest stage',s.commerce_furthest||'none established');statusHost.append(el('p',s.cannot_infer||'No later effect is inferred.'));}catch(error){if(!life.signal.aborted)statusHost.replaceChildren(el('h2','Native owner readback'),el('p',error.message));}
 root.dataset.ready='true';return{key,row,close:clear};
}
if(typeof document!=='undefined'){const root=document.querySelector('[data-procurement-order-intent-handoff]');if(root)mountProcurementOrderIntentHandoff(root).catch(error=>{root.dataset.failed='true';root.replaceChildren(el('h1','Order intent handoff could not open'),el('p',error.message));});}
