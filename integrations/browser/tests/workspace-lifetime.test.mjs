import {test,expect} from 'bun:test';
import {openDestinationStore} from '../workspace-store.mjs';
import {encode,decode} from '../workspace-msgpack.mjs';
import {prepareRecord} from '../workspace-value.mjs';
const endpoint='/apps/ws-'+'a'.repeat(40)+'/_records',origin='https://workspace.example.test';
const gate=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
const reply=packet=>new Response(encode(packet),{headers:{'content-type':'application/msgpack'}});
test('closure refuses every entry point before dispatch including a queued deletion',async()=>{
 const controller=new AbortController();let sent=0;
 const store=openDestinationStore(endpoint,{origin,signal:controller.signal,request:()=>{sent++;throw Error('must not dispatch');}});controller.abort();
 for(const work of [()=>store.readLocal(),()=>store.readMetadata(),()=>store.readVersion('k'),()=>store.saveLocal('k',{},0),()=>store.saveLocalBatch([{key:'k',value:{},expectedRevision:0}]),()=>store.deleteLocal('k')])await expect(work()).rejects.toThrow('closed');
 expect(sent).toBe(0);
});
test('a committed first write has no retry, and a second queued write never dispatches after abort',async()=>{
 const controller=new AbortController(),held=gate(),started=gate();let writes=0;
 const store=openDestinationStore(endpoint,{origin,signal:controller.signal,request:async(_,options)=>{
  writes++;const [,id,rows]=decode(options.body);started.resolve();await held.promise;return reply([1,1,false,'',rows.map(([k,r,m,b])=>[k,r+1,b])]);
 }});
 const first=store.saveLocal('draft:first',{private:'first'},0),second=store.saveLocal('draft:second',{private:'second'},0);
 const settled=Promise.allSettled([first,second]);await started.promise;controller.abort();held.resolve();const results=await settled;
 expect(results.map(r=>r.status)).toEqual(['rejected','rejected']);expect(results.every(r=>r.reason.code==='workspace_closed')).toBe(true);expect(writes).toBe(1);
});
test('a response body delayed past account closure is cancelled and cannot restore private records',async()=>{
 const controller=new AbortController(),held=gate(),started=gate();let cancelled=0;
 const body=await prepareRecord({title:'Private draft title'}).encodeWith(()=>{throw Error('No blobs');}),packet=encode([1,1,false,'draft:one',[['draft:one',1,body]]]);
 const store=openDestinationStore(endpoint,{origin,signal:controller.signal,request:async()=>({ok:true,headers:new Headers({'content-type':'application/msgpack'}),body:{getReader:()=>({async read(){started.resolve();await held.promise;return{done:false,value:packet};},async cancel(){cancelled++;}})}})});
 const pending=store.readLocal();await started.promise;controller.abort();held.resolve();await expect(pending).rejects.toThrow('closed');expect(cancelled).toBe(1);
});
test('closure during asset preparation prevents blob and record writes',async()=>{
 const controller=new AbortController(),started=gate(),held=gate();let sent=0;
 // Blob hashing yields before its upload. The immutable file body must not become a new account request.
 class HeldBlob extends Blob{async arrayBuffer(){started.resolve();await held.promise;return super.arrayBuffer();}}
 const store=openDestinationStore(endpoint,{origin,signal:controller.signal,request:async()=>{sent++;throw Error('Must not dispatch');}});
 const saving=store.saveLocal('draft:file',{files:[new HeldBlob(['private bytes'])]},0);
 await started.promise;controller.abort();held.resolve();await expect(saving).rejects.toThrow('closed');expect(sent).toBe(0);
});
