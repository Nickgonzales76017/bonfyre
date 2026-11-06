import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createNativeSession} from '../extension/native-session.mjs';
const event=()=>{const listeners=[];return{addListener:fn=>listeners.push(fn),emit:value=>listeners.forEach(fn=>fn(value))};};
function runtime(handler){const ports=[],api={ports,lastError:null,connectNative(){const p={onMessage:event(),onDisconnect:event(),disconnect(){p.onDisconnect.emit();},postMessage(m){handler(p,m,ports.length);}};ports.push(p);return p;}};return api;}
test('400 requests rotate spent sessions without duplicating native work',async()=>{
  const executed=[],counts=new WeakMap();const r=runtime((p,m)=>queueMicrotask(()=>{
    const n=counts.get(p)??0;counts.set(p,n+1);
    if(n>=64)p.onMessage.emit({id:m.id,ok:false,refused:'session_spent'});
    else{executed.push(m.index);p.onMessage.emit({id:m.id,ok:true});}
  }));const c=createNativeSession(r);
  for(let i=0;i<400;i++)assert.equal((await c.ask('enroll_record',{index:i})).ok,true);
  assert.equal(r.ports.length,7);assert.deepEqual(executed,Array.from({length:400},(_,i)=>i));c.close();
});
test('disconnect after dispatch never replays a mutating request',async()=>{
  const dispatched=[];const r=runtime((p,m,n)=>{dispatched.push(m.operation);queueMicrotask(()=>n===1?p.onDisconnect.emit():p.onMessage.emit({id:m.id,ok:true}));});
  const c=createNativeSession(r);assert.equal((await c.ask('enroll_seal')).refused,'disconnected');
  assert.deepEqual(dispatched,['enroll_seal']);assert.equal((await c.ask('enroll_status')).ok,true);
  assert.deepEqual(dispatched,['enroll_seal','enroll_status']);c.close();
});
test('timeout ends a stuck session and reports uncertain outcome without replay',async()=>{
  let calls=0;const r=runtime(()=>calls++),c=createNativeSession(r,{timeoutMs:10});
  assert.equal((await c.ask('enroll_record')).refused,'uncertain');assert.equal(calls,1);c.close();
});
test('concurrent callers are serialized through one host port',async()=>{
  let inFlight=0,max=0;const r=runtime((p,m)=>{inFlight++;max=Math.max(max,inFlight);setTimeout(()=>{inFlight--;p.onMessage.emit({id:m.id,ok:true});},1);});
  const c=createNativeSession(r);await Promise.all(Array.from({length:20},()=>c.ask('enroll_status')));assert.equal(max,1);c.close();
});
test('a second explicit refusal is returned without an infinite reconnect loop',async()=>{
  let calls=0;const r=runtime((p,m)=>{calls++;queueMicrotask(()=>p.onMessage.emit({id:m.id,ok:false,refused:'session_spent'}));});
  const c=createNativeSession(r);assert.equal((await c.ask('enroll_status')).refused,'session_spent');assert.equal(calls,2);c.close();
});
