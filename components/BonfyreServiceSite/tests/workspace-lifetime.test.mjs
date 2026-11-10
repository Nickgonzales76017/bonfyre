import {test,expect} from 'bun:test';
import {createWorkspaceLifetime,openWorkspaceStore,guardWorkspaceStore} from '../workspace-lifetime.mjs';
test('document closure is permanent, aborts work, removes listeners and runs every cleanup once',()=>{
 const events=new EventTarget(),life=createWorkspaceLifetime(events);let calls=0;
 life.onClose(()=>{calls++;throw Error('cleanup fixture');});life.onClose(()=>calls++);
 events.dispatchEvent(new Event('bonfyre:account-changed'));events.dispatchEvent(new Event('pagehide'));life.close();
 expect(life.closed).toBe(true);expect(life.signal.aborted).toBe(true);expect(calls).toBe(2);expect(()=>life.assert()).toThrow('closed');
 life.onClose(()=>calls++);expect(calls).toBe(3);
});
test('late native module resolution never creates a closed account store',async()=>{
 const life=createWorkspaceLifetime(new EventTarget());let resolve,calls=0;
 const pending=openWorkspaceStore(life,{endpoint:'/apps/ws-'+'a'.repeat(40)+'/_records',loadNative:()=>new Promise(r=>resolve=r)});
 life.close();resolve({openDestinationStore(){calls++;}});
 await expect(pending).rejects.toThrow('closed');expect(calls).toBe(0);
});
test('a fallback save dispatched before closure is neither retried nor returned as current',async()=>{
 const life=createWorkspaceLifetime(new EventTarget());let reply,calls=0;
 const store=guardWorkspaceStore({saveLocalBatch(){calls++;return new Promise(r=>reply=r);}},life);
 const saving=store.saveLocalBatch([{key:'draft:one',value:{private:'notes'}}]);life.close();reply([{revision:1}]);
 await expect(saving).rejects.toThrow('closed');await expect(store.saveLocalBatch([])).rejects.toThrow('closed');expect(calls).toBe(1);
});
test('native lifetime passes its signal to both transport and carrier',async()=>{
 const life=createWorkspaceLifetime(new EventTarget());let carrier,transport;
 const store=await openWorkspaceStore(life,{endpoint:'/apps/ws-'+'a'.repeat(40)+'/_records',loadNative:async()=>({openDestinationStore(endpoint,options){carrier=options.signal;return{readLocal:()=>options.request(endpoint,{})};}}),request:async(_,options)=>{transport=options.signal;return [];}});
 expect(await store.readLocal()).toEqual([]);expect(carrier).toBe(life.signal);expect(transport).toBe(life.signal);
 life.close();await expect(store.readLocal()).rejects.toThrow('closed');
});
