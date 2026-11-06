// Browser-side candidate/evidence storage; no institutional state, payments or renewal scheduler.
const DB='aurekai-browser-workbench-v1';
let destinationPromise;
function destinationStore(){
  const path=globalThis.document?.querySelector('meta[name="bonfyre-workspace-store"]')?.content;
  if(!path)return null;
  if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(path))throw Error('Invalid workspace storage binding.');
  destinationPromise??=import('/apps/workspace-store.mjs').then(m=>m.openDestinationStore(path));
  return destinationPromise;
}
function openDB(){return new Promise((resolve,reject)=>{const r=indexedDB.open(DB,1);r.onupgradeneeded=()=>r.result.createObjectStore('records',{keyPath:'key'});r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error)})}
// Source removal and draft admission serialize on the same records store.
export async function deleteUnreferencedSource(key,expectedRevision){
  if(await destinationStore())throw Error('Source removal is not connected to this workspace. The source was kept.');
  if(typeof key!=='string'||!key.startsWith('observation:')||!Number.isInteger(expectedRevision)||expectedRevision<1)throw Error('Choose a retained source version.');
  const db=await openDB();return new Promise((resolve,reject)=>{
    const tx=db.transaction('records','readwrite'),records=tx.objectStore('records');let error;
    const stop=message=>{error=Error(message);tx.abort();};
    const request=records.get(key);request.onsuccess=()=>{
      if(request.result?.revision!==expectedRevision){stop('This source changed in another tab. Reload before removing it.');return;}
      const scan=records.openCursor(IDBKeyRange.bound('draft:','draft:\uffff'));
      scan.onsuccess=()=>{const cursor=scan.result;if(!cursor){records.delete(key);return;}
        if(cursor.value.value?.browser_evidence?.some(ref=>ref.id===key)){stop('This source is attached to a saved program. Remove that reference and save the program before removing the source.');return;}cursor.continue();};
    };
    tx.oncomplete=()=>{db.close();resolve();};tx.onabort=tx.onerror=()=>{db.close();reject(error??tx.error??Error('The source could not be removed.'));};
  });
}

export async function saveLocal(key,value,expectedRevision=null){
  // Capture the reviewed body before destination import or IndexedDB opening yields.
  value=structuredClone(value);
  const destination=await destinationStore();
  if(destination)return destination.saveLocal(key,value,expectedRevision);
  const db=await openDB();return new Promise((resolve,reject)=>{
    const tx=db.transaction('records','readwrite'),store=tx.objectStore('records');let result,error;
    const get=store.get(key);get.onsuccess=()=>{
      const old=get.result,revision=old?.revision??0;
      if(expectedRevision!==null&&expectedRevision!==revision){error=Error('Draft changed in another tab; reload before overwriting');tx.abort();return;}
      result={key,revision:revision+1,value};
      try{store.put(result);}catch(cause){error=cause;tx.abort();}
    };
    tx.oncomplete=()=>{db.close();resolve(result)};tx.onabort=()=>{db.close();reject(error||tx.error||Error('Storage transaction aborted'))};tx.onerror=()=>{};
  });
}
export async function readLocal(prefix=''){
  const destination=await destinationStore();
  if(destination)return destination.readLocal(prefix);
  if(typeof prefix!=='string')throw Error('Record prefix must be text.');
  const db=await openDB();return new Promise((resolve,reject)=>{
    const tx=db.transaction('records','readonly'),rows=[];
    const r=tx.objectStore('records').openCursor(prefix?IDBKeyRange.lowerBound(prefix):undefined);
    r.onsuccess=()=>{const cursor=r.result;if(!cursor||typeof cursor.key!=='string'||!cursor.key.startsWith(prefix))return;rows.push(cursor.value);cursor.continue();};
    tx.oncomplete=()=>{db.close();resolve(rows);};tx.onabort=()=>{db.close();reject(tx.error||Error('Storage read aborted'));};tx.onerror=()=>{};
  });
}
// One revision check and transaction for a user's multi-record edit.
// A stale row aborts the entire batch; no partial catalogue save is reported.
export async function saveLocalBatch(changes){
  // Include record identity, expected revision and check-only mode in the snapshot.
  changes=structuredClone(changes);
  const destination=await destinationStore();
  if(destination)return destination.saveLocalBatch(changes);
  if(!Array.isArray(changes)||!changes.length||changes.length>1000)throw Error('Choose between 1 and 1000 drafts to save.');
  const keys=new Set();
  for(const c of changes){if(typeof c.key!=='string'||!c.key||keys.has(c.key)||!Number.isInteger(c.expectedRevision)||c.expectedRevision<0||c.checkOnly!==undefined&&typeof c.checkOnly!=='boolean')throw Error('Invalid draft batch.');keys.add(c.key);}
  const db=await openDB();return new Promise((resolve,reject)=>{
    const tx=db.transaction('records','readwrite'),records=tx.objectStore('records'),results=[];let error;
    for(const c of changes){const request=records.get(c.key);request.onsuccess=()=>{
      if(error)return;
      const revision=request.result?.revision??0;
      if(revision!==c.expectedRevision){error=Error('A draft changed in another tab. None of these changes were saved. Reload before overwriting.');tx.abort();return;}
      if(c.checkOnly){if(!request.result){error=Error('A selected source was removed. None of these changes were saved.');tx.abort();return;}results.push(request.result);return;}
      const result={key:c.key,revision:revision+1,value:c.value};
      try{records.put(result);results.push(result);}catch(cause){error=cause;tx.abort();}
    };}
    tx.oncomplete=()=>{db.close();resolve(results)};tx.onabort=()=>{db.close();reject(error||tx.error||Error('None of these changes were saved.'))};tx.onerror=()=>{};
  });
}
export async function deleteLocal(key){
  const destination=await destinationStore();
  if(destination)return destination.deleteLocal(key);
  const db=await openDB();return new Promise((resolve,reject)=>{const tx=db.transaction('records','readwrite');tx.objectStore('records').delete(key);tx.oncomplete=()=>{db.close();resolve()};tx.onabort=()=>{db.close();reject(tx.error)}})
}

// A user-selected transfer keeps the original browser records unchanged.
// The recipient receives nothing until the source window confirms the selection.
function offerBrowserDraftTransfer(){
  if(!globalThis.document||document.querySelector('meta[name="bonfyre-workspace-store"]')||!window.opener)return;
  const query=new URL(location.href).searchParams,nonce=query.get('aurekai-transfer'),recipient=query.get('aurekai-recipient');
  if(!/^[a-f0-9-]{36}$/.test(nonce??'')||!/^https:\/\/aurekai\.(?:work|life|live|shop)$/.test(recipient??'')&&!/^http:\/\/127\.0\.0\.1:[0-9]+$/.test(recipient??''))return;
  const family=query.get('aurekai-family')??'spaces';if(!['spaces','creator-audio'].includes(family))return;
  const prefixes=family==='creator-audio'?['draft:creator:']:['draft:catalogue:','fixture-plan:','product-model:'];
  const allowed=key=>typeof key==='string'&&prefixes.some(prefix=>key.startsWith(prefix));
  const make=(tag,text)=>{const node=document.createElement(tag);if(text)node.textContent=text;return node;};
  const dialog=make('dialog');dialog.setAttribute('aria-label','Choose browser drafts to bring into Aurekai');dialog.style.cssText='max-width:660px;width:calc(100% - 48px);max-height:85vh;overflow:auto;padding:24px;border:1px solid;border-radius:12px;color:#17201c;background:#fafbf8;font:16px system-ui';
  const heading=make('h2','Bring existing browser work'),explanation=make('p','Choose the drafts and model files to review at '+recipient+'. Originals stay in this browser. Nothing is sent until you confirm below.'),status=make('p'),list=make('div'),open=make('button','Show my saved drafts'),send=make('button','Send selected work for review'),cancel=make('button','Cancel');
  for(const button of [open,send,cancel]){button.type='button';button.style.cssText='min-height:44px;padding:10px 16px;margin:8px 8px 0 0;font:inherit';}send.hidden=true;
  status.setAttribute('role','status');dialog.append(heading,explanation,open,list,status,send,cancel);document.body.append(dialog);dialog.showModal();
  let rows=[],busy=false;
  cancel.onclick=()=>{if(!busy){dialog.close();dialog.remove();}};
  open.onclick=async()=>{
    if(busy)return;busy=true;open.disabled=true;status.textContent='Reading the saved work on this browser…';
    try{
      rows=(await readLocal()).filter(row=>allowed(row.key));
      if(rows.length>200)throw Error('This browser has more than 200 eligible records. Open a narrower working browser profile before transferring.');
      list.replaceChildren();
      for(const row of rows){const label=make('label'),checkbox=make('input');checkbox.type='checkbox';checkbox.value=row.key;label.style.cssText='display:block;padding:10px 0;border-bottom:1px solid #d6ded7';
        const title=row.value?.plan?.title??row.value?.title??row.value?.files?.map(file=>file.name).join(', ')??row.value?.binding?.productId??row.key;
        label.append(checkbox,document.createTextNode(' '+String(title).slice(0,200)));label.append(make('small',' · '+(row.key.startsWith('draft:creator:')?'Audio session':row.key.startsWith('product-model:')?'3D model record':row.key.startsWith('fixture-plan:')?'Room plan':'Product draft')));list.append(label);
      }
      status.textContent=rows.length?(family==='creator-audio'?'Selected sessions include their recordings and edits. Recording permissions need a new review in the destination.':'Select any required 3D model records with your room plan. Historical review references are not approvals.'):'No eligible saved work was found in this browser.';send.hidden=!rows.length;open.hidden=true;
    }catch(error){status.textContent=error.message;open.disabled=false;}finally{busy=false;}
  };
  send.onclick=()=>{
    if(busy)return;
    try{
      const selected=new Set([...list.querySelectorAll('input:checked')].map(input=>input.value)),chosen=rows.filter(row=>selected.has(row.key));
      if(!chosen.length||chosen.length>100)throw Error('Choose between one and 100 records to review.');
      let bytes=0,nodes=0;const visit=(value,depth=0)=>{if(depth>32||++nodes>100000)throw Error('This selection is too large. Choose fewer drafts.');if(value instanceof Blob)bytes+=value.size;else if(value instanceof Uint8Array||value instanceof ArrayBuffer)bytes+=value.byteLength;else if(typeof value==='string')bytes+=new TextEncoder().encode(value).length;else if(value&&typeof value==='object')for(const key of Object.keys(value)){bytes+=key.length;visit(value[key],depth+1);}if(bytes>32*1024*1024)throw Error('Choose less than 32 MiB of draft and model data at once.');};
      for(const row of chosen)visit(row.value);
      window.opener.postMessage({type:'bonfyre.workspace.transfer.v1',nonce,rows:chosen.map(({key,value,revision})=>({key,value,revision}))},recipient);
      send.disabled=true;status.textContent='Sent for review. Return to Aurekai to choose what to adopt. These originals have not changed.';
    }catch(error){status.textContent=error.message;}
  };
}
if(globalThis.document)queueMicrotask(offerBrowserDraftTransfer);
