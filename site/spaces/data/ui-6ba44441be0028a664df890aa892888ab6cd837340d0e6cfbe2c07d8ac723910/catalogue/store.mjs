// Browser-side candidate/evidence storage; no institutional state, payments or renewal scheduler.
const DB='aurekai-browser-workbench-v1';
function openDB(){return new Promise((resolve,reject)=>{const r=indexedDB.open(DB,1);r.onupgradeneeded=()=>r.result.createObjectStore('records',{keyPath:'key'});r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error)})}
export async function saveLocal(key,value,expectedRevision=null){
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
  if(!Array.isArray(changes)||!changes.length||changes.length>1000)throw Error('Choose between 1 and 1000 drafts to save.');
  const keys=new Set();
  for(const c of changes){if(typeof c.key!=='string'||!c.key||keys.has(c.key)||!Number.isInteger(c.expectedRevision)||c.expectedRevision<0)throw Error('Invalid draft batch.');keys.add(c.key);}
  const db=await openDB();return new Promise((resolve,reject)=>{
    const tx=db.transaction('records','readwrite'),records=tx.objectStore('records'),results=[];let error;
    for(const c of changes){const request=records.get(c.key);request.onsuccess=()=>{
      if(error)return;
      const revision=request.result?.revision??0;
      if(revision!==c.expectedRevision){error=Error('A draft changed in another tab. None of these changes were saved. Reload before overwriting.');tx.abort();return;}
      const result={key:c.key,revision:revision+1,value:c.value};
      try{records.put(result);results.push(result);}catch(cause){error=cause;tx.abort();}
    };}
    tx.oncomplete=()=>{db.close();resolve(results)};tx.onabort=()=>{db.close();reject(error||tx.error||Error('None of these changes were saved.'))};tx.onerror=()=>{};
  });
}
export async function deleteLocal(key){const db=await openDB();return new Promise((resolve,reject)=>{const tx=db.transaction('records','readwrite');tx.objectStore('records').delete(key);tx.oncomplete=()=>{db.close();resolve()};tx.onabort=()=>{db.close();reject(tx.error)}})}
