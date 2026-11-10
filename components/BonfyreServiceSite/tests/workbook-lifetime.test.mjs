import {test,expect} from 'bun:test';
import {createWorkbookLifetime,openWorkbookStore,guardWorkbookStore} from '../workbook-lifetime.mjs';
test('account and page lifetimes close once, abort work, remove listeners and run every cleanup',()=>{
 const events=new EventTarget(),life=createWorkbookLifetime(events);let n=0;
 life.onClose(()=>{n++;throw Error('fixture cleanup error');});life.onClose(()=>n++);
 events.dispatchEvent(new Event('bonfyre:account-changed'));events.dispatchEvent(new Event('pagehide'));life.close();
 expect(life.closed).toBe(true);expect(life.signal.aborted).toBe(true);expect(n).toBe(2);expect(()=>life.assert()).toThrow('closed');
 let late=false;life.onClose(()=>{late=true;});expect(late).toBe(true);
});
test('a delayed native module cannot create a store after account invalidation',async()=>{
 const life=createWorkbookLifetime(new EventTarget());let release,created=0;
 const module=new Promise(resolve=>release=resolve),pending=openWorkbookStore(life,{endpoint:'/apps/ws-'+ 'a'.repeat(40)+'/_records',loadNative:()=>module});
 life.close();release({openDestinationStore(){created++;}});await expect(pending).rejects.toThrow('closed');expect(created).toBe(0);
});
test('queued native transport and late responses are refused after the lifetime ends',async()=>{
 const life=createWorkbookLifetime(new EventTarget());let release,sent=0,options;
 const gate=new Promise(resolve=>release=resolve),native={openDestinationStore(endpoint,{request}){return{async readLocal(){await gate;return request(endpoint,{});},async saveLocal(){return request(endpoint,{method:'POST'});}};}};
 const store=await openWorkbookStore(life,{endpoint:'/apps/ws-'+ 'a'.repeat(40)+'/_records',loadNative:async()=>native,request:async(u,o)=>{sent++;options=o;return[];}});
 const pending=store.readLocal();life.close();release();await expect(pending).rejects.toThrow('closed');expect(sent).toBe(0);
 const next=createWorkbookLifetime(new EventTarget());let reply;
 const slow=await openWorkbookStore(next,{endpoint:'/apps/ws-'+ 'b'.repeat(40)+'/_records',loadNative:async()=>native,request:async(u,o)=>{sent++;options=o;return await new Promise(resolve=>reply=resolve);}});
 const saving=slow.saveLocal('k',{},0);next.close();reply([]);await expect(saving).rejects.toThrow('closed');expect(sent).toBe(1);expect(options.signal.aborted).toBe(true);
});
test('an already dispatched fallback save is never retried or reported as current after closure',async()=>{
 const life=createWorkbookLifetime(new EventTarget());let reply,calls=0;
 const store=guardWorkbookStore({saveLocal(){calls++;return new Promise(resolve=>reply=resolve);}},life),pending=store.saveLocal('key',{},0);
 life.close();reply({revision:1});await expect(pending).rejects.toThrow('closed');expect(calls).toBe(1);await expect(store.saveLocal('key',{},0)).rejects.toThrow('closed');expect(calls).toBe(1);
});
