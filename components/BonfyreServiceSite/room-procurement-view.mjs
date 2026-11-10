import {purchaseListPrefix,preparePurchaseList,verifyPurchaseList,purchaseListStatus,savePurchaseList} from './room-procurement.mjs';
import {createWorkspaceLifetime,clearWorkspaceHost,openWorkspaceStore} from './workspace-lifetime.mjs';
import {money} from './planner-model.mjs';
const root=document.querySelector('[data-purchase-lists]'),cfg=JSON.parse(document.querySelector('[data-purchase-config]').textContent),life=createWorkspaceLifetime();
const ui=name=>root.querySelector('[data-ui="'+name+'"]'),el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
let store,rooms=[],saved=[],value=null,revision=0,busy=false,sourceCurrent=false;
const catalogue=v=>{const hits=cfg.catalogues.filter(c=>c.digest===v);if(hits.length!==1)throw Error('The saved room collection is unavailable.');return hits[0];};
function controls(){if(life.closed)return;ui('prepare').disabled=busy||!ui('room').value;ui('save').disabled=busy||!value||!sourceCurrent;ui('refresh').disabled=busy||!value;ui('export').disabled=busy||!value||!sourceCurrent;ui('room').disabled=busy;ui('saved').disabled=busy;root.setAttribute('aria-busy',String(busy));}
function message(text,error=false){if(life.closed)return;ui(error?'error':'status').textContent=text;}
async function run(fn){if(busy||life.closed)return;busy=true;message('',true);controls();try{await fn();}catch(e){message(e.message,true);}finally{busy=false;controls();}}
function choices(){
 if(life.closed)return;
 for(const [name,rows,label]of [['room',rooms,r=>r.value.plan.title+' · version '+r.revision],['saved',saved,r=>r.value.title+' · list version '+r.revision]]){
  const select=ui(name),old=select.value;select.replaceChildren(new Option(name==='room'?'Choose a saved room':'Choose a saved purchase list',''));
  for(const row of rows)select.add(new Option(label(row),row.key));if(rows.some(r=>r.key===old))select.value=old;
 }
}
async function readRooms(){const rows=await store.readLocal('fixture-plan:');life.assert();rooms=rows.filter(r=>!r.key.endsWith(':latest')&&r.value?.plan).sort((a,b)=>a.key.localeCompare(b.key));}
async function readSaved(){const rows=await store.readLocal(purchaseListPrefix);life.assert();saved=rows.filter(r=>r.value?.schema==='service-site.room-purchase-list.v1').sort((a,b)=>a.key.localeCompare(b.key));}
async function render(){
 if(life.closed||!value)return;
 const checked=await verifyPurchaseList(value,catalogue(value.catalogue));life.assert();value=checked;
 const status=await purchaseListStatus(value,catalogue(value.catalogue),rooms.find(r=>r.key===value.source.key));life.assert();sourceCurrent=status.current;
 const host=ui('review');host.replaceChildren();host.hidden=false;
 host.append(el('h2',value.title),el('p',revision?'Saved list version '+revision:'Unsaved purchase list preview'));
 const trace=el('p','From saved room version '+value.source.revision+' · '+value.lines.reduce((n,l)=>n+l.quantity,0)+' placements');trace.className='rp-version';host.append(trace);
 const back=el('a','Open source room');const entry=cfg.entries[value.catalogue];if(entry){const url=new URL('../'+entry+'/',location.href);url.searchParams.set('project',value.source.value.plan.id);back.href=url;host.append(back);}
 const table=el('table'),head=el('tr');for(const title of ['Product and option','Quantity','Listed unit price','Listed subtotal'])head.append(el('th',title));const thead=el('thead');thead.append(head);table.append(thead);const body=el('tbody');
 for(const line of value.lines){const tr=el('tr'),name=el('td');name.append(el('strong',line.label),el('small',line.option),el('small','Product '+line.product_id+' · option '+line.variant_id));const a=el('a','Source observed '+line.observed_at.slice(0,10));a.href=line.source_url;a.target='_blank';a.rel='noopener noreferrer';name.append(a);tr.append(name,el('td',String(line.quantity)),el('td',money(line.unit_cents)),el('td',money(line.subtotal_cents)));body.append(tr);}
 for(const tr of body.children)for(const [i,label]of ['Product and option','Quantity','Listed unit price','Listed subtotal'].entries())tr.children[i].dataset.label=label;
 table.append(body);const wrap=el('div');wrap.className='rp-table';wrap.tabIndex=0;wrap.setAttribute('role','region');wrap.setAttribute('aria-label','Purchase list products');wrap.append(table);host.append(wrap);
 const total=el('p','Listed subtotal '+money(value.subtotal_cents));total.className='rp-total';host.append(total,el('p','Prices come from the room’s retained product observations. Availability, delivery and tax need supplier confirmation.'));
 if(value.issues.length){host.append(el('h3','Layout and budget questions'));const ul=el('ul');for(const issue of value.issues)ul.append(el('li',issue));host.append(ul);}
 host.append(el('h3','Before requesting a quote'));const questions=el('ul');for(const question of value.questions)questions.append(el('li',question));host.append(questions);
 message(status.message);controls();
}
function listen(name,event,fn){ui(name).addEventListener(event,()=>run(fn),{signal:life.signal});}
listen('prepare','click',async()=>{await readRooms();life.assert();const row=rooms.find(r=>r.key===ui('room').value);if(!row)throw Error('This room is no longer available. Reload saved work.');value=await preparePurchaseList({row,catalogue:catalogue(row.value.plan.catalogue)});life.assert();revision=0;ui('saved').value='';await render();});
listen('save','click',async()=>{const result=await savePurchaseList(store,value,catalogue(value.catalogue),revision);life.assert();value=result.value;revision=result.revision;history.replaceState(null,'','?'+new URLSearchParams({list:result.key}));await readSaved();choices();ui('saved').value=result.key;await render();message('Purchase list saved as version '+revision+'. No order or supplier message was sent.');});
listen('saved','change',async()=>{const key=ui('saved').value;if(!key)return;const rows=await store.readLocal(key);life.assert();const row=rows.find(r=>r.key===key);if(!row)throw Error('This saved list is no longer available.');const next=await verifyPurchaseList(row.value,catalogue(row.value.catalogue));life.assert();value=next;revision=row.revision;await readRooms();ui('room').value=value.source.key;await render();history.replaceState(null,'','?'+new URLSearchParams({list:key}));});
listen('room','change',async()=>controls());
listen('refresh','click',async()=>{await readRooms();life.assert();const row=rooms.find(r=>r.key===value.source.key);if(!row)throw Error('The source room is no longer available. The retained purchase list is unchanged.');value=await preparePurchaseList({row,catalogue:catalogue(row.value.plan.catalogue),id:value.id,createdAt:value.created_at});life.assert();await render();message('Refreshed preview. Save to retain this version.');});
listen('export','click',async()=>{await readRooms();const status=await purchaseListStatus(value,catalogue(value.catalogue),rooms.find(r=>r.key===value.source.key));life.assert();if(!status.current){await render();throw Error(status.message);}const url=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)+'\n'],{type:'application/json'})),a=el('a');a.href=url;a.download='purchase-list-'+value.id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);message('Purchase list downloaded with its source snapshot and listed prices.');});
life.onClose(()=>{rooms=[];saved=[];value=null;store=null;clearWorkspaceHost(root);});
run(async()=>{store=await openWorkspaceStore(life);await readRooms();await readSaved();life.assert();choices();const key=new URL(location.href).searchParams.get('list');if(key){const row=saved.find(r=>r.key===key);if(!row)throw Error('This saved purchase list is unavailable.');value=await verifyPurchaseList(row.value,catalogue(row.value.catalogue));life.assert();revision=row.revision;ui('saved').value=key;ui('room').value=value.source.key;await render();}root.dataset.ready='true';if(!rooms.length)message('Save a room plan to prepare its purchase list.');});
