import {afterEach,expect,test} from 'bun:test';
import {createHash} from 'node:crypto';
import {mkdirSync,mkdtempSync,rmSync,symlinkSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {createFabricStore} from '../fabric_store.mjs';
import {createFabricModelWork} from '../fabric_model.mjs';

const roots=[],models=[],stores=[];
const a={actor:'user:1',subject:'institution:studio',scope:'institution'};
const b={actor:'user:2',subject:'institution:studio',scope:'institution'};
const c={actor:'user:1',subject:'institution:elsewhere',scope:'institution'};
const hash=value=>createHash('sha256').update(value).digest('hex');
let seq=0;
function fixture() {
  const root=mkdtempSync(join(tmpdir(),'fabric-model-'));roots.push(root);const projectRoot=join(root,'project');mkdirSync(projectRoot);
  let at=Date.parse('2026-10-21T12:00:00Z'),allowed=true,stopped=false,pricing={state:'unknown'},meterFailure=false;
  let model={id:'moe:small.gguf',label:'Small local model',catalogId:'small',provider:'bonfyre-moe',execution:'local',available:true,
    maxTokens:512,maxTimeoutMs:120000,checkpoint:'small.gguf',witness:'a'.repeat(64)};
  const queue=[],running=new Set(),calls={generate:0,meter:[],quote:[],inspect:0},home=join(root,'owner');
  const store=createFabricStore({home,projectRoot,now:()=>at/1000});stores.push(store);
  const native={async capabilities(){return [model];},async inspect(id){calls.inspect++;if(id!==model.id)throw Error('model unavailable');return {...model};},
    async generate(request,options){calls.generate++;return generate(request,options);}};
  let generate=async(request,{beforeDispatch,onAttempt})=>{await beforeDispatch();await onAttempt({leaseId:'1',pid:100,grantId:'1',checkpoint:'small.gguf'});
    return {text:'Verified local output ✓',digest:hash('Verified local output ✓'),bytes:Buffer.byteLength('Verified local output ✓'),durationMs:125,usage:null};};
  const meter={async quote(input){calls.quote.push(input);return {...pricing};},async record(input){calls.meter.push({...input});if(meterFailure)throw Error('meter reply lost');
    return {...input,state:'recorded',eventId:1,outputBytes:input.bytes,pricing:{...pricing,...pricing.state==='priced'?{cost:0.25}:{}}};}};
  const execution=id=>({owned:running.has(id),queued:queue.includes(id)});
  const deps={store,native,meter,now:()=>at,authorizeAccount:async()=>allowed,admit:()=>{if(stopped)throw Error('STOP');},enqueue:id=>queue.push(id),execution};
  const make=()=>{const value=createFabricModelWork(deps);models.push(value);return value;};let service=make();
  return {root,store,native,meter,calls,queue,running,execution,get service(){return service;},
    setTime:value=>{at=value;},advance:value=>{at+=value;},allow:value=>{allowed=value;},stop:value=>{stopped=value;},
    price:value=>{pricing=value;},setModel:value=>{model={...model,...value};},failMeter:value=>{meterFailure=value;},
    generate:value=>{generate=value;},async restart(){await service.close();queue.length=0;running.clear();service=make();},
    async run(id){queue.splice(queue.indexOf(id),1);running.add(id);try{return await service.execute(id);}finally{running.delete(id);}},
    async review(binding=a,extra={}){return service.dispatch(binding,'model-review',{expectedBinding:binding,requestId:'review-'+(++seq),
      modelId:model.id,prompt:'Summarize this local example.',maxTokens:64,timeoutMs:10000,...extra});},
    async submit(review,binding=a,extra={}){return service.dispatch(binding,'model-submit',{expectedBinding:binding,requestId:'submit-'+(++seq),reviewId:review.review.id,...extra});},
    read(id,binding=a){return service.dispatch(binding,'model-read',undefined,{taskId:id});}};
}
afterEach(async()=>{for(const model of models.splice(0))await model.close();for(const store of stores.splice(0))store.close();for(const root of roots.splice(0))rmSync(root,{recursive:true,force:true});});

test('capabilities expose only native inventory without model paths or execution effects',async()=>{
  const f=fixture(),value=await f.service.dispatch(a,'model-capabilities');
  expect(value).toMatchObject({binding:a,models:[{id:'moe:small.gguf',catalogId:'small',execution:'local'}],limits:{maxPromptBytes:32768}});
  expect(value.models[0]).not.toHaveProperty('checkpoint');expect(value.models[0]).not.toHaveProperty('witness');expect(f.calls.generate).toBe(0);
});
test('review survives process restart and exact replay preserves its original expiration and binding',async()=>{
  const f=fixture(),r=await f.review(a,{requestId:'review-durable'});f.advance(1000);await f.restart();
  const same=await f.review(a,{requestId:'review-durable'});expect(same).toEqual(r);
  await expect(f.review(a,{requestId:'review-durable',prompt:'changed'})).rejects.toMatchObject({code:'conflict'});
  expect(f.calls.generate).toBe(0);expect(f.store.summary().tasks).toEqual({});
});
test('wrong expected account binding is refused before owner reads or effects',async()=>{
  const f=fixture();await expect(f.review(a,{expectedBinding:b})).rejects.toMatchObject({code:'conflict'});
  expect(f.calls.inspect).toBe(0);expect(f.calls.generate).toBe(0);
  const r=await f.review();await expect(f.submit(r,b)).rejects.toMatchObject({code:'not_found'});
  await expect(f.submit(r,c)).rejects.toMatchObject({code:'not_found'});expect(f.queue).toEqual([]);
});
test('review bounds reject NUL, extra authority and excessive UTF-8 bytes',async()=>{
  const f=fixture();for(const extra of [{prompt:'\0'},{prompt:'😀'.repeat(8193)},{maxTokens:513},{timeoutMs:120001},{apply_to_project:true}])
    await expect(f.review(a,extra)).rejects.toMatchObject({code:'invalid'});
  expect(f.calls.inspect).toBe(0);
});
test('native binding, reviewed rate and review expiration are checked before new scoped work',async()=>{
  const f=fixture(),r=await f.review();f.setModel({witness:'b'.repeat(64)});
  await expect(f.submit(r)).rejects.toMatchObject({code:'conflict'});f.setModel({witness:'a'.repeat(64)});
  f.price({state:'priced',rateId:1,amount:0.25,currency:'USD',unit:'call',effectiveAt:'2026-10-01T00:00:00Z',evidenceRef:'receipt:rate'});
  await expect(f.submit(r)).rejects.toMatchObject({code:'conflict'});f.price({state:'unknown'});f.advance(300001);
  await expect(f.submit(r)).rejects.toMatchObject({code:'conflict'});expect(f.queue).toEqual([]);
});
test('one reviewed one-call delegation cannot create a second task under a new submission identity',async()=>{
  const f=fixture(),r=await f.review(),results=await Promise.allSettled([f.submit(r,a,{requestId:'submit-one'}),f.submit(r,a,{requestId:'submit-two'})]);
  expect(results.filter(x=>x.status==='fulfilled')).toHaveLength(1);expect(results.find(x=>x.status==='rejected').reason.code).toBe('conflict');
  expect(f.queue).toHaveLength(1);expect(f.calls.generate).toBe(0);
});
test('lost acknowledgement recovery after restart and expiry returns the same uncertain task without replay',async()=>{
  const f=fixture(),r=await f.review(),s=await f.submit(r,a,{requestId:'submit-recovery'});f.advance(600000);await f.restart();
  const recovered=await f.submit(r,a,{requestId:'submit-recovery'});
  expect(recovered).toMatchObject({taskId:s.taskId,replayed:true,phase:'uncertain',reconciliationRequired:true});
  expect(f.queue).toEqual([]);expect(f.calls.generate).toBe(0);
  await expect(f.submit({...r,review:{...r.review,id:'f'.repeat(48)}},a,{requestId:'submit-recovery'})).rejects.toMatchObject({code:'conflict'});
});
test('native completed generation produces scoped output and records measured bytes once with a frozen attempt',async()=>{
  const f=fixture(),r=await f.review(),s=await f.submit(r);await f.run(s.taskId);
  const value=await f.read(s.taskId);expect(value).toMatchObject({state:'completed',phase:'completed',reconciliationRequired:false,
    output:{text:'Verified local output ✓',digest:hash('Verified local output ✓'),bytes:25},usage:{state:'recorded',durationMs:125,outputBytes:25,pricing:{state:'unknown'}}});
  expect(value.usage).not.toHaveProperty('cost');expect(value.usage).not.toHaveProperty('key');expect(value.usage).not.toHaveProperty('receiptRef');
  expect(f.calls.generate).toBe(1);expect(f.calls.meter).toHaveLength(1);
  expect(f.calls.meter[0]).toMatchObject({key:'institution:studio',operation:'model-generation',provider:'bonfyre-moe',bytes:25,durationMs:125,at:'2026-10-21T12:00:00Z'});
  const activity=f.store.activityRead(a,{execution:f.execution});expect(activity.outputs.some(x=>x.kind==='model-output')).toBe(true);
  await f.service.execute(s.taskId);expect(f.calls.generate).toBe(1);
});
test('revocation or STOP after queuing blocks the native request before a physical attempt',async()=>{
  for(const mode of ['revoked','stopped']){
    const f=fixture(),s=await f.submit(await f.review());if(mode==='revoked')f.allow(false);else f.stop(true);
    await f.run(s.taskId);expect(f.store.task(s.taskId).state).toBe('blocked');expect(f.store.readReceipt(s.taskId,'model-attempt')).toBeNull();expect(f.calls.meter).toHaveLength(0);
  }
});
test('a rate admitted while queued is checked again at the actual dispatch boundary',async()=>{
  const f=fixture(),s=await f.submit(await f.review());f.price({state:'priced',rateId:1,amount:1,currency:'USD',unit:'call',effectiveAt:'2026-10-01T00:00:00Z',evidenceRef:'rate:new'});
  await f.run(s.taskId);expect(f.store.task(s.taskId).state).toBe('blocked');expect(f.store.readReceipt(s.taskId,'model-attempt')).toBeNull();expect(f.calls.meter).toHaveLength(0);
});
test('meter failure retains output and exact retry after restart never calls the model again',async()=>{
  const f=fixture(),s=await f.submit(await f.review());f.failMeter(true);await f.run(s.taskId);
  expect(await f.read(s.taskId)).toMatchObject({state:'uncertain',output:{text:'Verified local output ✓'},usage:{state:'pending'},reconciliationAllowed:true});
  const observed={...f.calls.meter[0]};await f.restart();f.failMeter(false);
  const done=await f.service.dispatch(a,'model-reconcile',{expectedBinding:a,requestId:'reconcile-once',taskId:s.taskId});
  expect(done).toMatchObject({state:'completed',phase:'completed',usage:{state:'recorded'},reconciliationAllowed:false});
  expect(f.calls.meter).toEqual([observed,observed]);expect(f.calls.generate).toBe(1);
  await f.service.dispatch(a,'model-reconcile',{expectedBinding:a,requestId:'reconcile-once',taskId:s.taskId});
  expect(f.calls.meter).toHaveLength(2);expect(f.calls.generate).toBe(1);
});
test('meter lost ack can also recover from the original submission identity after review expiry',async()=>{
  const f=fixture(),r=await f.review(),s=await f.submit(r,a,{requestId:'submit-meter-recovery'});f.failMeter(true);await f.run(s.taskId);
  f.advance(600000);await f.restart();f.failMeter(false);
  const recovered=await f.submit(r,a,{requestId:'submit-meter-recovery'});expect(recovered).toMatchObject({taskId:s.taskId,state:'completed',replayed:true});expect(f.calls.generate).toBe(1);
});
test('restart after a retained output closes an orphan running row with pending or already recorded usage without inference replay',async()=>{
  for(const recorded of [false,true]){
    const f=fixture(),r=await f.review(),s=await f.submit(r,a,{requestId:'submit-crash-boundary'});f.failMeter(!recorded);await f.run(s.taskId);
    // Model result/usage files precede the final state update. Reproduce that
    // durable crash frontier directly; the restarted owner has no executor.
    const db=new Database(join(f.store.home,'fabric.sqlite3'));
    db.query("UPDATE tasks SET state='running',completed_at=NULL,error='' WHERE id=?").run(s.taskId);db.close();
    await f.restart();f.failMeter(false);
    expect(await f.read(s.taskId)).toMatchObject({state:'running',phase:'uncertain',reconciliationAllowed:true,
      output:{text:'Verified local output ✓'},usage:{state:recorded?'recorded':'pending'}});
    const meterCalls=f.calls.meter.length;
    const done=await f.service.dispatch(a,'model-reconcile',{expectedBinding:a,requestId:'reconcile-crash-boundary',taskId:s.taskId});
    expect(done).toMatchObject({state:'completed',phase:'completed',reconciliationAllowed:false,usage:{state:'recorded'}});
    expect(f.calls.generate).toBe(1);expect(f.calls.meter).toHaveLength(meterCalls+(recorded?0:1));
    expect((await f.submit(r,a,{requestId:'submit-crash-boundary'})).taskId).toBe(s.taskId);
    expect(f.calls.generate).toBe(1);expect(f.queue).toEqual([]);
  }
});
test('original submission recovery closes an orphan with a retained Meter receipt only after its live witness is gone',async()=>{
  const f=fixture(),r=await f.review(),s=await f.submit(r,a,{requestId:'submit-recorded-crash'});await f.run(s.taskId);
  const db=new Database(join(f.store.home,'fabric.sqlite3'));
  db.query("UPDATE tasks SET state='running',completed_at=NULL,error='' WHERE id=?").run(s.taskId);db.close();await f.restart();
  f.running.add(s.taskId);
  expect((await f.read(s.taskId)).reconciliationAllowed).toBe(false);
  await expect(f.service.dispatch(a,'model-reconcile',{expectedBinding:a,requestId:'reconcile-live-refusal',taskId:s.taskId})).rejects.toMatchObject({code:'conflict'});
  expect(f.store.task(s.taskId).state).toBe('running');f.running.clear();f.advance(600000);
  const recovered=await f.submit(r,a,{requestId:'submit-recorded-crash'});
  expect(recovered).toMatchObject({taskId:s.taskId,state:'completed',phase:'completed',replayed:true,reconciliationRequired:false});
  expect(f.calls.generate).toBe(1);expect(f.calls.meter).toHaveLength(1);expect(f.queue).toEqual([]);
});
test('usage reconciliation cannot reopen an unknown physical outcome or change its measurements',async()=>{
  const f=fixture(),s=await f.submit(await f.review());
  f.generate(async(request,{beforeDispatch,onAttempt})=>{await beforeDispatch();await onAttempt({pid:9});throw Error('response lost');});
  await f.run(s.taskId);expect(await f.read(s.taskId)).toMatchObject({phase:'uncertain',output:null,reconciliationAllowed:false,usage:{state:'pending',receiptId:null,durationMs:null,outputBytes:null}});
  await expect(f.service.dispatch(a,'model-reconcile',{expectedBinding:a,requestId:'reconcile-unknown',taskId:s.taskId})).rejects.toMatchObject({code:'conflict'});
  expect(f.calls.generate).toBe(1);expect(f.calls.meter).toHaveLength(0);
});
test('queued cancellation prevents inference and initiating actor ownership is retained',async()=>{
  const f=fixture(),s=await f.submit(await f.review());
  expect((await f.read(s.taskId,b)).cancellationAllowed).toBe(false);
  await expect(f.service.dispatch(b,'model-cancel',{expectedBinding:b,requestId:'cancel-other',taskId:s.taskId})).rejects.toMatchObject({code:'not_found'});
  await f.service.dispatch(a,'model-cancel',{expectedBinding:a,requestId:'cancel-own',taskId:s.taskId});await f.run(s.taskId);
  expect(f.calls.generate).toBe(0);expect(await f.read(s.taskId)).toMatchObject({state:'cancelled',phase:'cancelled'});
});
test('cancelling a started request joins this owner and preserves uncertain native completion',async()=>{
  const f=fixture(),s=await f.submit(await f.review());let began;const started=new Promise(resolve=>{began=resolve;});
  f.generate(async(request,{beforeDispatch,onAttempt,signal})=>{await beforeDispatch();await onAttempt({pid:9});began();
    await new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(Error('cancelled')),{once:true}));});
  const task=f.run(s.taskId);await started;
  expect(await f.read(s.taskId)).toMatchObject({state:'running',phase:'active'});
  const stopped=await f.service.dispatch(a,'model-cancel',{expectedBinding:a,requestId:'cancel-inflight',taskId:s.taskId});await task;
  expect(stopped).toMatchObject({state:'cancelled',phase:'uncertain',reconciliationRequired:true});expect(f.calls.meter).toHaveLength(0);
  await f.service.execute(s.taskId);expect(f.calls.generate).toBe(1);
});
test('changed admitted prompt blocks execution; historical unscoped tasks stay outside customer model activity',async()=>{
  const f=fixture(),s=await f.submit(await f.review());f.store.updateTask(s.taskId,{prompt:'changed'});await f.run(s.taskId);
  expect(f.store.task(s.taskId).state).toBe('blocked');expect(f.calls.generate).toBe(0);
  const legacy=f.store.createTask({kind:'model',title:'operator work',prompt:'unowned'});expect(f.service.ownsTask(legacy)).toBe(false);
  await expect(f.read(legacy)).rejects.toMatchObject({code:'not_found'});
});
test('output descriptor comes only from the scoped integrity-checked native receipt',async()=>{
  const f=fixture(),s=await f.submit(await f.review());await f.run(s.taskId);const detail=await f.read(s.taskId);
  const descriptor=await f.service.outputArtifact(a,s.taskId,detail.output.id);expect(descriptor).toMatchObject({digest:detail.output.digest,bytes:detail.output.bytes});
  expect(detail.localTransferAllowed).toBe(true);expect((await f.read(s.taskId,b)).localTransferAllowed).toBe(false);
  await expect(f.service.outputArtifact(c,s.taskId,detail.output.id)).rejects.toMatchObject({code:'not_found'});
  await expect(f.service.outputArtifact(a,s.taskId,'f'.repeat(24))).rejects.toMatchObject({code:'not_found'});
  writeFileSync(descriptor.path,'changed');await expect(f.read(s.taskId)).rejects.toThrow('integrity');
});
test('public prices retain an admitted unit rate and actual metered cost without leaking native coordinates',async()=>{
  const f=fixture();f.price({state:'priced',rateId:12,amount:0.25,currency:'USD',unit:'call',effectiveAt:'2026-10-01T00:00:00Z',evidenceRef:'rate:observed'});
  const review=await f.review();expect(review.review.pricing).toMatchObject({rateId:'12',amount:0.25});
  const s=await f.submit(review);await f.run(s.taskId);const detail=await f.read(s.taskId);
  expect(detail.usage).toMatchObject({state:'recorded',cost:0.25,pricing:{state:'priced',rateId:'12',amount:0.25}});
  expect(Object.keys(detail.usage).sort()).toEqual(['attemptRef','cost','durationMs','outputBytes','pricing','receiptId','state']);
});
test('native reconciliation refuses a changed usage payload and preserves a cancelled task',async()=>{
  const f=fixture(),s=await f.submit(await f.review());f.failMeter(true);await f.run(s.taskId);
  const attempt=f.store.readReceipt(s.taskId,'model-attempt').value,physical=f.calls.meter[0];
  f.store.saveReceipt(s.taskId,'model-usage',{...physical,eventId:1,state:'recorded',outputBytes:physical.bytes+1,pricing:{state:'unknown'}},'msgpack');
  expect(()=>f.store.completeModelUsage(s.taskId,attempt.attemptRef)).toThrow('does not resolve');expect(f.store.task(s.taskId).state).toBe('uncertain');
  f.store.saveReceipt(s.taskId,'model-usage',{...physical,eventId:1,state:'recorded',outputBytes:physical.bytes,pricing:{state:'unknown'}},'msgpack');
  expect(()=>f.store.completeModelUsage(s.taskId,'another-attempt')).toThrow('does not resolve');
  expect(f.store.completeModelUsage(s.taskId,attempt.attemptRef).state).toBe('completed');
  const second=await f.submit(await f.review());await f.service.cancel(second.taskId);
  expect(()=>f.store.completeModelUsage(second.taskId,attempt.attemptRef)).toThrow('does not resolve');expect(f.store.task(second.taskId).state).toBe('cancelled');
});
test('matching missing physical coordinates are never sufficient native reconciliation evidence',async()=>{
  const f=fixture(),s=await f.submit(await f.review());f.failMeter(true);await f.run(s.taskId);
  const attempt=f.store.readReceipt(s.taskId,'model-attempt').value,physical=f.calls.meter[0];
  const incomplete={...attempt};delete incomplete.at;delete incomplete.realizationRef;
  const usage={...physical,eventId:1,state:'recorded',outputBytes:physical.bytes,pricing:{state:'unknown'}};delete usage.at;delete usage.realizationRef;
  f.store.saveReceipt(s.taskId,'model-attempt',incomplete,'msgpack');f.store.saveReceipt(s.taskId,'model-usage',usage,'msgpack');
  expect(()=>f.store.completeModelUsage(s.taskId,attempt.attemptRef)).toThrow('does not resolve');expect(f.store.task(s.taskId).state).toBe('uncertain');
});
test('review file owner refuses a symlink and never follows it into another directory',async()=>{
  const f=fixture(),foreign=join(f.root,'foreign');mkdirSync(foreign);mkdirSync(f.store.home,{recursive:true});
  symlinkSync(foreign,join(f.store.home,'model-intents'));
  await expect(f.review()).rejects.toMatchObject({code:'unavailable'});expect(f.calls.generate).toBe(0);
});
