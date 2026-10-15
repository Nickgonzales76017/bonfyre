import {createCatalogueRoomDialog} from './catalogue-product-scene.mjs';
import {workspaceSelection} from './workspace-navigation.mjs';
import {catalogueViews,draftRows,deskView} from './catalogue-desk.mjs';
import {inventoryWorkspace} from './catalogue-grid.mjs';
import { preparationFields } from './catalogue-preparation.mjs';
import { readLocal, saveLocal } from './store.mjs';
import { baseline, draftKey, review, makeDraft, restoreDraft } from './catalogue-model.mjs';
import { reviewRequest, reviewHistoryPrefix, reviewVersion, localReviewReceipt, storedReviewReceipt } from './catalogue-review.mjs';
const root = document.querySelector('[data-catalogue-workspace]');
const config = JSON.parse(root.querySelector('[data-catalogue-config]').textContent);
const $ = name => root.querySelector(`[data-ui="${name}"]`);
const el = (tag, text, cls) => { const n = document.createElement(tag); if (text != null) n.textContent = text; if (cls) n.className = cls; return n; };
let product = null, original = null, edit = null, lastSaved = '', revision = 0;
let loadSerial = 0, saving = false, blocked = false, changedElsewhere = false, storageError = '';
let records = new Map(), cache = new Map(), store = config.stores[0].id, currentView = 'edit';
let receiptHistory = new Map(), versions = new Map();
let workspaceView='products',deskQuery='',currentAddress=location.href;
const panels=['edit','options','preparation','review'];
const unkeptReceipts = new Set();
const dirty = () => (!!edit && JSON.stringify(edit) !== lastSaved)||inventory.dirty();
const roomDialog=createCatalogueRoomDialog({routes:config.roomRoutes??[],canLeave:()=>!dirty()||confirm('Continue to the room without saving the catalogue edits?'),onError:text=>message(text,true)});
const inventory=inventoryWorkspace($('inventory'),{config,fetchProduct,inspectProduct:(source,variantId)=>roomDialog.open(source,variantId),records:()=>records,openProduct:productId=>openWorkspace(store,'products',{productId}),saved:async()=>{await getRecords();renderList();renderWorkspace();}});
const readableDate = value => new Date(value).toLocaleDateString('en-US', {month:'short',day:'numeric',year:'numeric'});
function message(text, bad = false) { $('message').textContent = text; $('message').classList.toggle('bad', bad); $('message').setAttribute('role', bad ? 'alert' : 'status'); }
async function getRecords() {
 try {
  const rows=(await Promise.all([readLocal('draft:catalogue:'),readLocal(reviewHistoryPrefix)])).flat();
  records=new Map(rows.filter(r=>typeof r.key==='string'&&r.key.startsWith('draft:catalogue:')).map(r=>[r.key,r]));
  for(const r of rows){if(typeof r.key!=='string'||!r.key.startsWith(reviewHistoryPrefix))continue;const version=r.key.slice(reviewHistoryPrefix.length),value=storedReviewReceipt(version,r.value);if(value)receiptHistory.set(version,value);}
  versions.clear();
  if(config.review)for(const [key,r]of records){try{versions.set(key,await reviewVersion(config.review.form,r.value));}catch{}}
  storageError='';
 }catch{storageError='Your browser could not open draft storage. Enable site storage and reload before saving.';message(storageError,true);}
}
function receivedFor(p){if(!p)return null;const version=versions.get(draftKey(p));return version?storedReviewReceipt(version,receiptHistory.get(version)):null;}
function showCurrentReceipt(){
 const status=$('submit-status'),receipt=receivedFor(product);
 if(!status||dirty()||blocked||changedElsewhere||!receipt)return false;
 status.dataset.receipt=receipt.reference;
 status.textContent='Sent for review. Reference: '+receipt.reference.split(':').at(-1).slice(0,12)+'. Receipt recorded '+readableDate(receipt.recorded_at)+'.';
 const unkept=unkeptReceipts.has(receipt.version);
 if(unkept)status.textContent+=' This browser could not save the receipt. Keep this reference before leaving.';
 status.setAttribute('role',unkept?'alert':'status');
 $('submit').disabled=true;return true;
}

function description(html) { const t = document.createElement('template'); t.innerHTML = html || ''; t.content.querySelectorAll('script,style,noscript,iframe,object').forEach(n => n.remove()); t.content.querySelectorAll('br').forEach(n => n.replaceWith(document.createTextNode('\n'))); t.content.querySelectorAll('p,li,div,h1,h2,h3').forEach(n => n.append(document.createTextNode('\n'))); return t.content.textContent.replace(/\n[\t ]+/g,'\n').replace(/\n{3,}/g,'\n\n').trim(); }
function renderList() {
 const query=$('search').value.trim().toLowerCase(),kind=$('filter').value;
 const visible=config.entries.filter(x=>x.store===store&&(!query||x.title.toLowerCase().includes(query)||x.id.includes(query))).filter(x=>kind==='all'||kind==='goods'&&x.kind==='goods'||kind==='services'&&x.kind==='service'||kind==='missing'&&x.missing_skus>0||kind==='drafts'&&records.has(draftKey(x))||kind==='sent'&&!!receivedFor(x));
 $('list').replaceChildren();
 for(const x of visible){const b=el('button',null,'cw-product');b.type='button';b.dataset.productId=x.id;b.setAttribute('aria-pressed',String(product?.id===x.id&&product?.store===x.store));if(x.image){const image=el('img');image.src=x.image;image.alt='';image.loading='lazy';image.referrerPolicy='no-referrer';image.onerror=()=>{image.remove();b.prepend(el('span','◇','cw-photo-fallback'));};b.append(image);}else b.append(el('span','◇','cw-photo-fallback'));const w=el('span',null,'cw-product-words');w.append(el('strong',x.title),el('small',x.kind==='service'?'Service':`${x.variant_count} options`));if(receivedFor(x))w.append(el('span','Sent for review','cw-tag'));else if(records.has(draftKey(x)))w.append(el('span','Draft saved','cw-tag'));else if(x.missing_skus)w.append(el('span','SKU review','cw-tag cw-quiet'));b.append(w);b.onclick=()=>selectProduct(x);$('list').append(b);}
 $('count').textContent=`${visible.length} ${visible.length===1?'product':'products'}`;const saved=config.entries.filter(x=>x.store===store&&records.has(draftKey(x))).length;$('saved-count').textContent=`${saved} saved ${saved===1?'draft':'drafts'}`;if(!visible.length)$('list').append(el('p','No products match. Try another search or filter.','cw-empty'));
}
function renderWorkspace(){
 const rows=draftRows(config,records,store,receivedFor);
 $('draft-total').textContent=rows.length;$('sent-total').textContent=rows.filter(r=>r.receipt).length;
 root.querySelectorAll('.cw-workspace-nav [data-workspace-view]').forEach(b=>{if(b.dataset.workspaceView===workspaceView)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');});
 $('product-layout').hidden=workspaceView!=='products';$('desk').hidden=!['drafts','sent'].includes(workspaceView);$('inventory').hidden=workspaceView!=='inventory';
 if(['drafts','sent'].includes(workspaceView))$('desk').innerHTML=deskView(rows,workspaceView,deskQuery);
}
function writeLocation(replace=false){
 const url=new URL(location.href);url.searchParams.set('store',store);url.searchParams.set('workspace',workspaceView);
 if(workspaceView==='products'&&product){url.searchParams.set('product',product.id);url.searchParams.set('panel',currentView);}else{url.searchParams.delete('product');url.searchParams.delete('panel');}
 if(url.href!==location.href)history[replace?'replaceState':'pushState'](null,'',url);currentAddress=url.href;
}
async function openWorkspace(nextStore,nextView,{replace=false,productId=null,panel='edit'}={}){
 if(saving||inventory.busy()||dirty()&&!confirm('Leave your unsaved catalogue changes?'))return false;
 inventory.close();
 const selected=workspaceSelection({workspace:nextStore,view:nextView},config.stores,catalogueViews);
 ++loadSerial;store=selected.workspace.id;workspaceView=selected.view.id;deskQuery='';$('store').value=store;
 product=null;edit=null;original=null;lastSaved='';$('editor').hidden=true;$('placeholder').hidden=false;
 $('search').value='';$('filter').value='all';root.classList.remove('cw-open');
 await getRecords();renderList();renderWorkspace();
 if(workspaceView==='products'){
   const next=config.entries.find(x=>x.store===store&&x.id===productId)||config.entries.find(x=>x.store===store&&x.kind!=='service')||config.entries.find(x=>x.store===store);
   if(next)await selectProduct(next,{ask:false,panel,replace});else writeLocation(replace);
 }else {writeLocation(replace);if(workspaceView==='inventory')await inventory.activate(store);}
 return true;
}

async function fetchProduct(x){const key=x.chunk;if(!cache.has(key)){const promise=(async()=>{const r=await fetch(new URL(key,new URL(config.data,location.origin)),{cache:'no-cache'});if(!r.ok)throw Error('The product details could not be loaded. Try again.');if(!globalThis.DecompressionStream)throw Error('This browser cannot open the catalogue. Use a current Chrome, Safari or Firefox.');const packed=await r.arrayBuffer();if(packed.byteLength>1_000_000)throw Error('The product file is larger than expected.');const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',packed)),b=>b.toString(16).padStart(2,'0')).join('');if(digest!==x.chunk_sha256)throw Error('The product details could not be verified. Reload and try again.');const reader=new Blob([packed]).stream().pipeThrough(new DecompressionStream('gzip')).getReader(),chunks=[];let bytes=0;for(;;){const q=await reader.read();if(q.done)break;bytes+=q.value.length;if(bytes>4_000_000){await reader.cancel();throw Error('The product file is larger than expected.');}chunks.push(q.value);}const text=await new Blob(chunks).text();const data=JSON.parse(text);if(!Array.isArray(data))throw Error('Product details are unreadable.');return data;})();cache.set(key,promise);promise.catch(()=>cache.delete(key));}const p=(await cache.get(key)).find(p=>p.id===x.id&&p.store===x.store);if(!p||p.source_sha256!==config.stores.find(s=>s.id===x.store).sha256)throw Error('The catalogue changed. Reload before editing.');return p;}
async function selectProduct(x,{ask=true,panel='edit',replace=false}={}){if(saving)return;if(ask&&dirty()&&!confirm('Leave your unsaved changes to this product?'))return;const serial=++loadSerial;$('editor').setAttribute('aria-busy','true');$('save').disabled=true;$('review').disabled=true;message('Opening product…');try{const p=await fetchProduct(x);if(serial!==loadSerial)return;product=p;original=baseline(p,description(p.description_html));edit=structuredClone(original);const record=records.get(draftKey(p));revision=record?.revision??0;blocked=false;changedElsewhere=false;if(record){try{edit=restoreDraft(p,original,record.value);}catch(e){blocked=true;message(e.message,true);}}lastSaved=JSON.stringify(edit);workspaceView='products';currentView=panels.includes(panel)?panel:'edit';renderEditor();renderList();renderWorkspace();root.classList.add('cw-open');$('editor').hidden=false;$('placeholder').hidden=true;writeLocation(replace);if(!blocked)message(storageError||(record?`Draft saved ${readableDate(record.value.saved_at)} on this browser.`:'Edit a field, review the changes, then save your draft.'),!!storageError);}catch(e){if(serial===loadSerial){message(e.message,true);if(!product){$('placeholder').hidden=false;$('placeholder').textContent=e.message;$('placeholder').setAttribute('role','alert');root.classList.add('cw-open');}updateStatus();}}finally{if(serial===loadSerial)$('editor').setAttribute('aria-busy','false');}}
function renderEditor(){
 $('load-sample').hidden=!product.prepared_sample;
 $('load-sample').onclick=()=>{if(saving||blocked||changedElsewhere)return;if(dirty()&&!confirm('Replace unsaved edits with the prepared sample?'))return;edit=restoreDraft(product,original,product.prepared_sample);renderEditor();message('Prepared sample loaded. Review and save any changes you want to keep.');};
 if($('submit-status')){$('submit-status').textContent='Save your draft before sending it.';$('submit-status').removeAttribute('data-receipt');$('submit-status').setAttribute('role','status');}
 $('name').textContent=product.title;$('type').textContent=product.kind==='service'?'Service':'Product';$('source-link').href=product.source_url;$('source-date').textContent=`Source captured ${readableDate(product.observed_at)}`;$('photo').replaceChildren();if(product.image){const image=el('img');image.src=product.image;image.alt=product.title;image.referrerPolicy='no-referrer';$('photo').append(image);}$('original-title').textContent=product.title;$('original-description').textContent=original.description||'No description published.';
 for(const field of ['title','description','tags',...preparationFields.map(p=>p.field)]){$(field).value=edit[field];$(field).oninput=()=>{edit[field]=$(field).value;clearReceipt();updateStatus();};}
 $('variants').replaceChildren();for(const[i,v]of product.variants.entries()){const row=el('div',null,'cw-variant');row.append(el('span',v.label,'cw-option'));for(const field of ['sku','price']){const label=el('label');label.append(el('span',field==='sku'?'SKU':'Listed price','cw-mobile-label'));const input=el('input');input.value=edit.variants[i][field];input.dataset.field=`${field}:${v.id}`;input.setAttribute('aria-label',`${v.label} ${field==='sku'?'SKU':'price'}`);if(field==='price')input.inputMode='decimal';else input.placeholder='Not published';input.oninput=()=>{edit.variants[i][field]=input.value;clearReceipt();updateStatus();};label.append(input);row.append(label);}row.append(el('span',v.available===true?'Available':v.available===false?'Unavailable':'Not stated','cw-availability'));$('variants').append(row);}
 const missing=product.variants.filter(v=>!v.sku?.trim()).length;$('sku-note').textContent=missing?`${missing} ${missing===1?'option has':'options have'} no SKU in the public listing. Add one only when you know it.`:'';switchView(currentView,{route:false});updateStatus();
}
function updateStatus(){if(!product||!edit)return;const r=review(product,original,edit);root.querySelectorAll('[aria-invalid]').forEach(n=>n.removeAttribute('aria-invalid'));for(const e of r.errors){const node=['title','description','tags',...preparationFields.map(p=>p.field)].includes(e.field)?$(e.field):[...root.querySelectorAll('[data-field]')].find(n=>n.dataset.field===e.field);node?.setAttribute('aria-invalid','true');}$('errors').textContent=r.errors.map(e=>e.message).join(' ');$('errors').hidden=!r.errors.length;$('change-count').textContent=r.errors.length?'Check highlighted fields':`${r.changes.length} ${r.changes.length===1?'change':'changes'}`;$('draft-state').textContent=dirty()?'Unsaved changes':revision?'Draft saved':'No changes yet';$('save').disabled=saving||blocked||changedElsewhere||!!storageError||r.errors.length>0||!r.changes.length||!dirty();$('review').disabled=saving||blocked||r.errors.length>0||!r.changes.length;$('reset').disabled=saving||blocked||!dirty();if($('submit'))$('submit').disabled=saving||blocked||changedElsewhere||!!storageError||dirty()||!revision||!!r.errors.length||!r.changes.length;if(currentView==='review')drawReview(r);showCurrentReceipt();}
function drawReview(r){root.classList.toggle('cw-ready-review',!!config.review&&!dirty()&&revision>0);if($('submit-status')&&!dirty()&&revision&&$('submit-status').textContent.startsWith('Save'))$('submit-status').textContent='Ready to send this saved draft.';$('changes').replaceChildren();for(const c of r.changes){const box=el('section',null,'cw-change');box.append(el('h3',c.label));const grid=el('div',null,'cw-diff');for(const[label,text]of[[c.field.startsWith('prep:')?'Previous proposal':'Original',c.before],['Your draft',c.after]]){const side=el('div');side.append(el('small',label),el('p',text||'(empty)'));grid.append(side);}box.append(grid);$('changes').append(box);}if(!r.changes.length)$('changes').append(el('p','Make an edit to compare it with the original.'));showCurrentReceipt();}
function clearReceipt(){const status=$('submit-status');if(status){status.removeAttribute('data-receipt');status.textContent='Save your changes before sending this version.';status.setAttribute('role','status');}}
function switchView(view,{route=true}={}){
 currentView=panels.includes(view)?view:'edit';root.classList.remove('cw-ready-review');
 for(const id of panels){$(id+'-view').hidden=id!==currentView;$(id+'-tab').setAttribute('aria-selected',String(id===currentView));}
 if(currentView==='review'&&product)drawReview(review(product,original,edit));
 if(route&&product)writeLocation();
}
for(const id of panels)$(id+'-tab').onclick=()=>switchView(id);
$('review').onclick=()=>{switchView('review');$('review-view').scrollIntoView({block:'start'});};

$('reset').onclick=()=>{if(dirty()&&!confirm('Reset these unsaved edits to your last saved version?'))return;edit=JSON.parse(lastSaved);renderEditor();message('Unsaved edits reset.');};
$('save').onclick=async()=>{if(saving||blocked||changedElsewhere||storageError)return;try{const savedEdit=structuredClone(edit),draft=makeDraft(product,original,savedEdit);saving=true;updateStatus();const record=await saveLocal(draftKey(product),draft,revision);revision=record.revision;records.set(record.key,record);if(config.review)versions.set(record.key,await reviewVersion(config.review.form,draft));lastSaved=JSON.stringify(savedEdit);renderList();renderWorkspace();message('Draft saved. Your store is unchanged.');}catch(e){changedElsewhere=/another tab/.test(e.message);message(e.message,true);}finally{saving=false;updateStatus();}};
if(config.review)$('submit-form').onsubmit=async event=>{
 event.preventDefault();if(saving||$('submit').disabled)return;
 const status=$('submit-status'),selected=product,selectedRevision=revision;
 const name=$('sender-name').value,email=$('sender-email').value;
 saving=true;updateStatus();root.querySelectorAll('input,textarea,select').forEach(n=>n.disabled=true);
 try{
  const current=(await readLocal(draftKey(selected))).find(r=>r.key===draftKey(selected));
  if(!current||current.revision!==selectedRevision){changedElsewhere=true;throw Error('This draft changed in another tab. Reload the saved draft before sending.');}
  const version=await reviewVersion(config.review.form,current.value),key=reviewHistoryPrefix+version;
  versions.set(draftKey(selected),version);
  const kept=(await readLocal(key)).find(r=>r.key===key),earlier=storedReviewReceipt(version,kept?.value)||storedReviewReceipt(version,receiptHistory.get(version));
  if(earlier){receiptHistory.set(version,earlier);return;}
  const body=reviewRequest(config.review.form,name,email,current.value,config.review.page);
  status.textContent='Sending your saved draft…';status.removeAttribute('data-receipt');
  const response=await fetch('/api/lead/'+encodeURIComponent(config.review.form),{method:'POST',headers:{'content-type':'application/json'},body,signal:AbortSignal.timeout(20000)});
  const result=await response.json().catch(()=>({ok:false,why:'No receipt was returned. Your saved draft is safe to retry.'}));
  if(!response.ok||!result.ok)throw Error(response.status===429?'Too many requests right now. Your draft is saved; please try again later.':result.why||'No receipt was returned. Your saved draft is safe to retry.');
  const receipt=localReviewReceipt(version,result.receipt);
  receiptHistory.set(version,receipt);
  try{await saveLocal(key,receipt,kept?.revision??0);unkeptReceipts.delete(version);}catch{
   // The server already received the draft. A failed local write must not be called a failed send.
   let persisted=null;try{persisted=storedReviewReceipt(version,(await readLocal(key)).find(r=>r.key===key)?.value);}catch{}
   if(persisted&&persisted.reference===receipt.reference){receiptHistory.set(version,persisted);unkeptReceipts.delete(version);}else unkeptReceipts.add(version);
  }
 }catch(error){status.textContent=error.name==='TimeoutError'||error instanceof TypeError?'No receipt was received. Your draft is still saved; you can retry safely.':error.message;status.setAttribute('role','alert');}
 finally{saving=false;root.querySelectorAll('input,textarea,select').forEach(n=>n.disabled=false);renderList();renderWorkspace();updateStatus();}
};
$('reload-draft').onclick=async()=>{if(!product||saving)return;if(dirty()&&!confirm('Reload the saved draft and replace your unsaved edits?'))return;await getRecords();await selectProduct(config.entries.find(x=>x.id===product.id&&x.store===product.store),{ask:false});};
$('back').onclick=()=>{root.classList.remove('cw-open');$('search').focus();};$('search').oninput=renderList;$('filter').onchange=renderList;
$('store').onchange=async()=>{const next=$('store').value;$('store').value=store;await openWorkspace(next,workspaceView);};
root.addEventListener('click',async event=>{
 const button=event.target.closest('[data-workspace-view],[data-desk-product],[data-desk-refresh]');if(!button)return;event.preventDefault();
 if(button.dataset.workspaceView){await openWorkspace(store,button.dataset.workspaceView);return;}
 if(button.dataset.deskProduct){await openWorkspace(store,'products',{productId:button.dataset.deskProduct,panel:'review'});return;}
 if(button.hasAttribute('data-desk-refresh')){await getRecords();renderWorkspace();}
});
root.addEventListener('submit',event=>{if(!event.target.matches('[data-desk-search]'))return;event.preventDefault();deskQuery=new FormData(event.target).get('query').trim();renderWorkspace();});
addEventListener('popstate',async()=>{
 const requested=new URL(location.href);history.replaceState(null,'',currentAddress);
 if(workspaceView==='products'&&requested.searchParams.get('workspace')==='products'&&requested.searchParams.get('store')===store&&requested.searchParams.get('product')===product?.id){switchView(requested.searchParams.get('panel'),{route:false});writeLocation(true);return;}
 await openWorkspace(requested.searchParams.get('store'),requested.searchParams.get('workspace'),{replace:true,productId:requested.searchParams.get('product'),panel:requested.searchParams.get('panel')});
});
addEventListener('beforeunload',e=>{if(dirty()){e.preventDefault();e.returnValue='';}});
const params=new URL(location.href).searchParams;
await openWorkspace(params.get('store'),params.get('workspace'),{replace:true,productId:params.get('product'),panel:params.get('panel')});
