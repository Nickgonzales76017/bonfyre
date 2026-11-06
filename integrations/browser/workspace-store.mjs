import {encode,decode,maxPacket} from './workspace-msgpack.mjs';
import {observeWorkspaceRecords} from './workspace-continuation.mjs';
import {prepareRecord,restoreRecord,blobTransport,digest} from './workspace-value.mjs';
const integer=n=>Number.isSafeInteger(n)&&n>=0&&n<2147483647;
export function openDestinationStore(endpoint,{request=globalThis.fetch.bind(globalThis),origin=globalThis.location?.origin,signal}={}){
  if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint)||!origin)throw Error('Invalid native workspace binding.');
  const observed=new Map(),pending=new Map();let queue=Promise.resolve();
  const assertOpen=()=>{if(signal?.aborted)throw Object.assign(Error('This workspace session closed. Reload before reading or saving.'),{code:'workspace_closed'});};
  signal?.addEventListener('abort',()=>{observed.clear();pending.clear();},{once:true});
  async function exchange(operation,packet=null,parameters={}){
    assertOpen();const url=new URL(endpoint,origin);url.searchParams.set('operation',operation);
    for(const [key,value]of Object.entries(parameters))url.searchParams.set(key,String(value));
    const response=await request(url,{...(signal?{signal}:{}),method:packet===null?'GET':'POST',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/msgpack',...(packet===null?{}:{'Content-Type':'application/msgpack'})},body:packet===null?undefined:encode(packet)});
    assertOpen();if(!response.ok){const message=(await response.text()).slice(0,2000);throw Object.assign(Error(message||'The native workspace could not confirm this operation.'),{status:response.status});}
    if(response.headers.get('content-type')?.split(';')[0]!=='application/msgpack')throw Error('The native workspace returned an unexpected response.');
    const reader=response.body.getReader(),pieces=[];let bytes=0;
    for(;;){const part=await reader.read();if(signal?.aborted){await reader.cancel();assertOpen();}if(part.done)break;bytes+=part.value.length;if(bytes>maxPacket){await reader.cancel();throw Error('The native response exceeds its bound.');}pieces.push(part.value);}
    const body=new Uint8Array(bytes);let at=0;for(const p of pieces){body.set(p,at);at+=p.length;}return decode(body);
  }
  const blobs=blobTransport(exchange);
  function packet(value){
    if(!Array.isArray(value)||value.length!==5||value[0]!==1||!integer(value[1])||typeof value[2]!=='boolean'||typeof value[3]!=='string'||!Array.isArray(value[4])||value[4].length>1000)throw Error('Invalid native record response.');
    const keys=new Set();for(const row of value[4]){
      if(!Array.isArray(row)||row.length!==3||typeof row[0]!=='string'||!row[0]||keys.has(row[0])||!integer(row[1])||row[1]<1||row[2]!==null&&!(row[2] instanceof Uint8Array))throw Error('Invalid native draft identity or revision.');keys.add(row[0]);
    }
    return value;
  }
  async function restore(rows){return Promise.all(rows.filter(r=>r[2]!==null).map(async([key,revision,body])=>({key,revision,value:await restoreRecord(body,blobs.get)})));}
  async function exact(key){const answer=packet(await exchange('get',null,{prefix:key}));if(answer[2]||answer[4].length>1||answer[4].some(r=>r[0]!==key))throw Error('The native record lookup changed identity.');const row=answer[4][0];assertOpen();observed.set(key,row?{revision:row[1],deleted:row[2]===null}:{revision:0,deleted:false});return row;}
  async function readLocal(prefix=''){
    if(typeof prefix!=='string'||new TextEncoder().encode(prefix).length>=512)throw Error('Use a bounded record prefix.');
    let rows=[],cursor='',epoch=-1;
    for(let page=0;page<80;page++){
      const answer=packet(await exchange('read',null,{prefix,cursor,epoch}));
      if(epoch!==-1&&answer[1]!==epoch)throw Error('These drafts changed while reading. Refresh the workspace.');epoch=answer[1];
      for(const row of answer[4]){if(!row[0].startsWith(prefix)||row[0]<=cursor||row[2]===null)throw Error('The native record page is inconsistent.');cursor=row[0];rows.push(row);}
      if(answer[3]!==cursor&&answer[4].length||answer[2]&&!answer[4].length)throw Error('The native record cursor is invalid.');
      if(!answer[2]){
        const restored=await restore(rows);assertOpen();
        for(const row of rows)observed.set(row[0],{revision:row[1],deleted:false});observeWorkspaceRecords(endpoint,restored,{prefix,signal});return restored;
      }
    }
    throw Error('The draft collection exceeds this editor’s read limit. No partial collection was opened.');
  }
  // Metadata pages never hydrate files. A selected original is read with
  // readVersion; this prevents a library visit from downloading every recording.
  async function readMetadata(prefix='',{cursor='',epoch=-1}={}){
    if(typeof prefix!=='string'||new TextEncoder().encode(prefix).length>=512||typeof cursor!=='string'||cursor.length>=512||!Number.isSafeInteger(epoch)||epoch< -1)throw Error('Use a bounded native metadata page.');
    const answer=packet(await exchange('read',null,{prefix,cursor,epoch}));
    if(epoch!==-1&&answer[1]!==epoch)throw Error('The library changed while reading. Reload its first page.');
    let previous=cursor;const rows=answer[4].map(([key,revision,body])=>{
      if(!key.startsWith(prefix)||key<=previous||body===null)throw Error('The native metadata cursor changed.');previous=key;
      const p=decode(body,{maxBytes:256*1024});
      if(!Array.isArray(p)||p.length!==3||![2,3,4].includes(p[0])||!Array.isArray(p[2])||p[2].length>64)throw Error('Unsupported saved material metadata.');
      return {key,revision,value:p[1],fileCount:p[2].length};
    });
    if(rows.length&&answer[3]!==previous||answer[2]&&!rows.length)throw Error('The native metadata page is incomplete.');
    return {rows,cursor:answer[3],epoch:answer[1],more:answer[2]};
  }
  // Capture the reviewed values and operation modes before asynchronous queueing.
  // prepareRecord already snapshots scalars/bytes and retains immutable Blob references.
  function prepareChanges(changes){
    if(!Array.isArray(changes)||!changes.length||changes.length>1000)throw Error('Choose between one and 1000 drafts.');
    const keys=new Set(),prepared=changes.map(c=>{
      if(!c||typeof c.key!=='string'||!c.key||keys.has(c.key)||!integer(c.expectedRevision)||c.checkOnly!==undefined&&typeof c.checkOnly!=='boolean'||c.remove!==undefined&&typeof c.remove!=='boolean'||c.checkOnly&&c.remove)throw Error('Invalid or duplicated draft change.');
      keys.add(c.key);return {key:c.key,revision:c.expectedRevision,mode:c.checkOnly?1:c.remove?2:0,value:c.checkOnly||c.remove?null:prepareRecord(c.value)};
    });
    return prepared;
  }
  async function batch(prepared){
    // Check the complete packet before retaining any file body.
    encode([1,'00000000-0000-0000-0000-000000000000',prepared.map(c=>[c.key,c.revision,c.mode,c.value?new Uint8Array(c.value.bytes):null])]);
    const rows=[];for(const c of prepared)rows.push([c.key,c.revision,c.mode,c.value?await c.value.encodeWith(blobs.put):null]);
    const fingerprint=await digest(encode(rows));assertOpen();let requestId=pending.get(fingerprint);
    if(!requestId){requestId=crypto.randomUUID();pending.set(fingerprint,requestId);if(pending.size>32)pending.delete(pending.keys().next().value);}
    let answer;
    try{answer=packet(await exchange('write',[1,requestId,rows]));}catch(error){if(error.status&&error.status<500)pending.delete(fingerprint);throw error;}
    if(answer[2]||answer[4].length!==rows.length)throw Error('The native save receipt is incomplete.');
    for(let n=0;n<rows.length;n++){
      const [key,revision,mode,body]=rows[n],kept=answer[4][n];
      if(kept[0]!==key||kept[1]!==revision+(mode===1?0:1)||mode===2&&kept[2]!==null||mode===0&&(kept[2]?.length!==body.length||body.some((b,i)=>b!==kept[2][i])))throw Error('The native save receipt does not match the requested draft.');
    }
    const restored=await restore(answer[4]);assertOpen();
    for(const [key,revision,body]of answer[4])observed.set(key,{revision,deleted:body===null});
    pending.delete(fingerprint);observeWorkspaceRecords(endpoint,restored,{signal,removed:answer[4].filter(row=>row[2]===null).map(row=>row[0])});return restored;
  }
  function serial(fn){const next=queue.then(()=>{assertOpen();return fn();});queue=next.catch(()=>{});return next;}
  function schedule(changes,work){
    try{assertOpen();const prepared=prepareChanges(changes);return serial(()=>work(prepared));}
    catch(error){return Promise.reject(error);}
  }
  return {
    readLocal,readMetadata,
    saveLocalBatch:changes=>schedule(changes,batch),
    saveLocal:(key,value,expectedRevision=null)=>schedule([{key,value,expectedRevision:expectedRevision===null?0:expectedRevision}],async prepared=>{
      if(expectedRevision===null){
        if(!observed.has(key))await exact(key);
        const current=observed.get(key);
        if(current.deleted)throw Error('This draft was removed. Review the retained version before restoring it.');
        prepared[0].revision=current.revision;
      }
      return (await batch(prepared))[0];
    }),
    deleteLocal:key=>serial(async()=>{if(!observed.has(key))await exact(key);const current=observed.get(key);if(current.revision===0||current.deleted)return;await batch(prepareChanges([{key,expectedRevision:current.revision,remove:true}]));}),
    readVersion:async key=>{const row=await exact(key),result=row?{key,revision:row[1],deleted:row[2]===null,value:row[2]===null?null:await restoreRecord(row[2],blobs.get)}:null;assertOpen();return result;}
  };
}
