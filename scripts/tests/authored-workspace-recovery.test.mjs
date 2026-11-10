import {test,expect} from 'bun:test';
import {createAuthoredWorkspace,addInstrument,connectInstruments,blankInstrument,validateAuthoredWorkspace,productionPrefix} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
import {publicProductionDesign,instantiatePublicDesign} from '../../cmd/BonfyreSurfaceScene/runtime/authored-publication.mjs';
import {recoverySchema,inspectRecoveryMaterial,previewRecoveryWorkflow,commitRecoveryWorkflow,reconcileRecoveryWorkflow,commitRecoveryBatch} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery.mjs';
import {prepareRecord,digest} from '../../integrations/browser/workspace-value.mjs';
import {encode,decode} from '../../integrations/browser/workspace-msgpack.mjs';

const digestValue=async value=>digest(await prepareRecord(value).encodeWith(async blob=>digest(await blob.arrayBuffer())));
const target={context:'self',id:'ws-'+'a'.repeat(40),release:'b'.repeat(64),entry:'instruments/production/'};
// This fixture exercises the compiler's mandatory validator boundary. The live
// caller supplies the retained domain owners, tested by the native/UI witness.
const validateInstrument=async(kind,value)=>Object.hasOwn({research:1,product:1,traces:1,schema:1},kind)&&value.kind===kind&&typeof value.title==='string'&&value.title!=='invalid parked child';
const options={digestValue,validateInstrument};
function input({key='review-follow-up',kinds=['research','product','traces'],sourceLine=42}={}){
  let production=createAuthoredWorkspace('Review the existing project');production.description='Editable recovered configuration; no provider action has run.';
  for(const kind of kinds)production=addInstrument(production,kind,{role:kind==='research'?'source':'working'});
  if(kinds.includes('research')&&kinds.includes('product'))production=connectInstruments(production,production.panels.find(p=>p.kind==='research').id,production.panels.find(p=>p.kind==='product').id);
  const records=production.panels.map(p=>({key:p.recordKey,value:blankInstrument(p.kind,p.label)}));
  return {recovery_key:key,source:{conversation_id:'session-2026-04-03',message_ids:['session:session-2026-04-03:line:'+sourceLine],source_sha256:'c'.repeat(64)},production,records};
}
const envelope=(workflows=[input()])=>[1,recoverySchema,{archive_sha256:'d'.repeat(64),workflows}];
function memoryStore(){
  const data=new Map();let writes=0;
  return {data,get writes(){return writes;},async readVersion(key){return structuredClone(data.get(key)??null);},async saveLocalBatch(changes){
    writes++;for(const c of changes)if((data.get(c.key)?.revision??0)!==c.expectedRevision)throw Error('Native revision conflict');
    const rows=changes.map(c=>({key:c.key,revision:1,deleted:false,value:structuredClone(c.value)}));for(const row of rows)data.set(row.key,row);return structuredClone(rows);
  }};
}
async function fixture(raw=envelope()){
  const material=await inspectRecoveryMaterial(raw,options),workflow=material.workflows[0],targetStore=memoryStore();
  const opts={...options,target,targetStore,verifyTarget:async()=>target};
  const plan=await previewRecoveryWorkflow(workflow,opts);return {material,workflow,targetStore,opts,plan};
}
test('MessagePack recovery creates a complete native composition and preserves exact private lineage',async()=>{
  const raw=envelope(),before=structuredClone(raw),f=await fixture(decode(encode(raw))),result=await commitRecoveryWorkflow(f.plan,f.opts);
  expect(raw).toEqual(before);expect(result.state).toBe('created');expect(result.records).toHaveLength(4);expect(f.targetStore.writes).toBe(1);
  const p=result.production.value;expect(result.production.key).toBe(productionPrefix+p.id);expect(p.panels.every(p=>p.revision===1)).toBe(true);expect(p.connections).toHaveLength(1);expect(p.connections[0].from).toBe(p.panels[0].id);
  expect(p.recoveryOrigin).toEqual({schema:'bonfyre.authored-workspace-recovery-origin.v1',archive_sha256:'d'.repeat(64),recovery_key:'review-follow-up',conversation_id:'session-2026-04-03',message_ids:['session:session-2026-04-03:line:42'],source_sha256:'c'.repeat(64)});
  expect(validateAuthoredWorkspace(p,{saved:true})).toBe(p);expect(result.records.slice(0,3).map(r=>r.value.kind)).toEqual(['research','product','traces']);
});
test('source identity is stable across file rereads and unrelated UUIDs; distinct workflows sharing one message remain distinct',async()=>{
  const a=await inspectRecoveryMaterial(envelope([input()]),options),b=await inspectRecoveryMaterial(envelope([input()]),options);
  expect(a.workflows[0].productionKey).toBe(b.workflows[0].productionKey);expect(a.workflows[0].changes).toEqual(b.workflows[0].changes);
  const both=await inspectRecoveryMaterial(envelope([input(),input({key:'second-workflow'})]),options);expect(both.workflows[0].productionKey).not.toBe(both.workflows[1].productionKey);
  await expect(inspectRecoveryMaterial(envelope([input(),input()]),options)).rejects.toThrow('repeats');
});
test('changed archive/content hashes keep the same native identity and become a review conflict after recovery',async()=>{
  const f=await fixture();await commitRecoveryWorkflow(f.plan,f.opts);const changed=envelope();changed[2].archive_sha256='e'.repeat(64);changed[2].workflows[0].source.source_sha256='f'.repeat(64);
  const next=(await inspectRecoveryMaterial(changed,options)).workflows[0];expect(next.productionKey).toBe(f.workflow.productionKey);
  const preview=await previewRecoveryWorkflow(next,f.opts);expect(preview.state).toBe('conflict');await expect(commitRecoveryWorkflow(preview,f.opts)).rejects.toThrow('changed');expect(f.targetStore.writes).toBe(1);
});
test('source message anchors are sorted and retained without manufacturing a provider identity',async()=>{
  const raw=envelope();raw[2].workflows[0].source.message_ids=['session:session-2026-04-03:line:99','session:session-2026-04-03:line:42'];
  const one=await inspectRecoveryMaterial(raw,options);raw[2].workflows[0].source.message_ids.reverse();const two=await inspectRecoveryMaterial(raw,options);
  expect(one.workflows[0].productionKey).toBe(two.workflows[0].productionKey);expect(one.workflows[0].production.recoveryOrigin.message_ids).toEqual(['session:session-2026-04-03:line:42','session:session-2026-04-03:line:99']);
});
test('every parked child reaches the required validator and a failure prevents any write',async()=>{
  const raw=envelope(),visited=[];raw[2].workflows[0].records[2].value.title='invalid parked child';
  await expect(inspectRecoveryMaterial(raw,{digestValue,validateInstrument:async(kind,value)=>{visited.push(kind);return validateInstrument(kind,value);}})).rejects.toThrow('native domain validator');expect(visited).toEqual(['research','product','traces']);
  await expect(inspectRecoveryMaterial(envelope(),{digestValue})).rejects.toThrow('native domain validator');
  const f=await fixture();await expect(commitRecoveryWorkflow(f.plan,{...f.opts,validateInstrument:async()=>false})).rejects.toThrow('native domain validator');expect(f.targetStore.writes).toBe(0);
});
test('extra authority fields, saved revisions, fabricated adoption origins, orphaned and repeated child records are refused',async()=>{
  for(const mutate of [w=>w.production.actor='user:1',w=>w.production.panels[0].revision=1,w=>w.records[0].value.grants=['write'],w=>w.records[1].value.stories.push({id:'claim',origin:{record:'another-account'}}),w=>w.records.pop(),w=>w.records[1].key=w.records[0].key,w=>w.source.messages='raw transcript',w=>w.source.message_ids=['duplicate','duplicate']]){
    const raw=envelope();mutate(raw[2].workflows[0]);await expect(inspectRecoveryMaterial(raw,options)).rejects.toThrow();
  }
});
test('archive sources cannot manufacture native publication provenance',async()=>{
  const raw=envelope([input({kinds:['research']})]);raw[2].workflows[0].records[0].value.sources.push({id:'claimed-source',publication:{receipt:'invented-native-receipt'}});
  await expect(inspectRecoveryMaterial(raw,options)).rejects.toThrow('publication receipts');
});
test('native composition validation rejects malformed retained recovery origin and public design strips valid provenance',async()=>{
  const f=await fixture(),p=f.workflow.production,design=publicProductionDesign(p),publicBytes=new TextDecoder().decode(encode(design));
  expect(publicBytes).not.toContain('session-2026-04-03');expect(publicBytes).not.toContain('recoveryOrigin');expect(publicBytes).not.toContain('dddddddddddd');expect(publicBytes).not.toContain('source_sha256');
  const reused=instantiatePublicDesign(design);expect(reused.recoveryOrigin).toBeUndefined();expect(reused.panels.every(p=>p.revision===0)).toBe(true);
  const invalid=structuredClone(p);invalid.recoveryOrigin.actor='user:1';expect(()=>validateAuthoredWorkspace(invalid)).toThrow('provenance');
});
test('changed target, values, root, summaries and forged delete/check-only modes fail before writing',async()=>{
  for(const mutate of [p=>p.target.context='institution:elsewhere',p=>p.workflow.changes[0].value.title='Edited',p=>p.workflow.production.title='Changed',p=>p.summary.title='Changed',p=>p.workflow.changes[0].remove=true,p=>p.workflow.changes[0].checkOnly=true,p=>p.workflow.changes[0].expectedRevision=1]){
    const f=await fixture();mutate(f.plan);await expect(commitRecoveryWorkflow(f.plan,f.opts)).rejects.toThrow();expect(f.targetStore.writes).toBe(0);
  }
});
test('native admission is refreshed for preview, creation and already retained recovery',async()=>{
  const f=await fixture();await expect(previewRecoveryWorkflow(f.workflow,{...f.opts,verifyTarget:async()=>false})).rejects.toThrow('admission changed');
  await expect(commitRecoveryWorkflow(f.plan,{...f.opts,verifyTarget:async()=>({...target,release:'f'.repeat(64)})})).rejects.toThrow('admission changed');expect(f.targetStore.writes).toBe(0);
  await commitRecoveryWorkflow(f.plan,f.opts);await expect(commitRecoveryWorkflow(f.plan,{...f.opts,verifyTarget:async()=>false})).rejects.toThrow('admission changed');expect(f.targetStore.writes).toBe(1);
});
test('a release change during save or retained readback prevents success and preserves the same work for reconciliation',async()=>{
  for(const mode of ['created','lost-acknowledgement','retained']){
    const f=await fixture();let currentTarget=target,admissions=0;const opts={...f.opts,verifyTarget:async()=>{admissions++;return currentTarget;}};
    if(mode==='retained'){
      await commitRecoveryWorkflow(f.plan,f.opts);const read=f.targetStore.readVersion;f.targetStore.readVersion=async key=>{const row=await read(key);currentTarget={...target,release:'f'.repeat(64)};return row;};
    }else{
      const save=f.targetStore.saveLocalBatch;f.targetStore.saveLocalBatch=async changes=>{const rows=await save(changes);currentTarget={...target,release:'f'.repeat(64)};if(mode==='lost-acknowledgement')throw Error('Lost native acknowledgement');return rows;};
    }
    let failure;try{await commitRecoveryWorkflow(f.plan,opts);}catch(error){failure=error;}
    expect(failure?.code).toBe('recovery_uncertain');expect(failure?.message).toContain('work is retained');expect(admissions).toBe(2);expect(f.targetStore.writes).toBe(1);expect(f.targetStore.data.size).toBe(4);
    const reconciled=await commitRecoveryWorkflow(f.plan,f.opts);expect(reconciled.state).toBe('retained');expect(f.targetStore.writes).toBe(1);
  }
});
test('lost acknowledgement is reconciled against every current record and rereading the file never duplicates work',async()=>{
  const f=await fixture(),save=f.targetStore.saveLocalBatch;f.targetStore.saveLocalBatch=async changes=>{await save(changes);throw Error('Lost acknowledgement');};
  const recovered=await commitRecoveryWorkflow(f.plan,f.opts);expect(recovered.state).toBe('recovered');expect(f.targetStore.writes).toBe(1);
  const again=(await inspectRecoveryMaterial(envelope(),options)).workflows[0],plan=await previewRecoveryWorkflow(again,f.opts);expect(plan.state).toBe('retained');expect((await commitRecoveryWorkflow(plan,f.opts)).state).toBe('retained');expect(f.targetStore.writes).toBe(1);
});
test('partial, removed, edited and later revision records refuse resume without restoring or overwriting',async()=>{
  for(const mode of ['partial','deleted','edited','revision']){
    const f=await fixture();await commitRecoveryWorkflow(f.plan,f.opts);const first=f.workflow.changes[0].key;
    if(mode==='partial')f.targetStore.data.delete(first);else if(mode==='deleted')f.targetStore.data.set(first,{key:first,revision:2,deleted:true,value:null});else if(mode==='edited')f.targetStore.data.get(first).value.title='User edited work';else f.targetStore.data.get(first).revision=2;
    const current=await reconcileRecoveryWorkflow(f.workflow,f.opts);expect(current.state).toBe('conflict');expect(current.conflicts.length).toBeGreaterThan(0);await expect(commitRecoveryWorkflow(f.plan,f.opts)).rejects.toThrow('changed');expect(f.targetStore.writes).toBe(1);
  }
});
test('an unreadable destination never becomes empty and never permits a write',async()=>{
  const f=await fixture();f.targetStore.readVersion=async()=>{throw Error('Read unavailable');};await expect(previewRecoveryWorkflow(f.workflow,f.opts)).rejects.toThrow('Read unavailable');await expect(commitRecoveryWorkflow(f.plan,f.opts)).rejects.toThrow('Read unavailable');expect(f.targetStore.writes).toBe(0);
});
test('a previously retained review never silently recreates a now-missing closure',async()=>{
  const f=await fixture();await commitRecoveryWorkflow(f.plan,f.opts);const retained=await previewRecoveryWorkflow(f.workflow,f.opts);expect(retained.state).toBe('retained');f.targetStore.data.clear();
  await expect(commitRecoveryWorkflow(retained,f.opts)).rejects.toThrow('previously retained');expect(f.targetStore.writes).toBe(1);
});
test('transport uncertainty with failed readback stops; a deliberate retry uses the same identities',async()=>{
  const f=await fixture(),save=f.targetStore.saveLocalBatch,read=f.targetStore.readVersion;
  f.targetStore.saveLocalBatch=async changes=>{await save(changes);f.targetStore.readVersion=async()=>{throw Error('Offline');};throw Error('Lost reply');};
  await expect(commitRecoveryWorkflow(f.plan,f.opts)).rejects.toThrow('outcome is unconfirmed');expect(f.targetStore.writes).toBe(1);f.targetStore.readVersion=read;
  expect((await commitRecoveryWorkflow(f.plan,f.opts)).state).toBe('retained');expect(f.targetStore.writes).toBe(1);
});
test('a failure before native commit does not trigger an automatic retry',async()=>{
  const f=await fixture(),save=f.targetStore.saveLocalBatch;let attempts=0;f.targetStore.saveLocalBatch=async()=>{attempts++;throw Error('Offline before send');};await expect(commitRecoveryWorkflow(f.plan,f.opts)).rejects.toThrow('not confirmed');expect(attempts).toBe(1);expect(f.targetStore.data.size).toBe(0);
  f.targetStore.saveLocalBatch=save;expect((await commitRecoveryWorkflow(f.plan,f.opts)).state).toBe('created');expect(f.targetStore.writes).toBe(1);
});
test('a retained native receipt cannot hide current changes made after its original commit',async()=>{
  const f=await fixture(),save=f.targetStore.saveLocalBatch;f.targetStore.saveLocalBatch=async changes=>{const rows=await save(changes);f.targetStore.data.get(changes[1].key).revision=2;return rows;};await expect(commitRecoveryWorkflow(f.plan,f.opts)).rejects.toThrow('differs');expect(f.targetStore.writes).toBe(1);
});
test('caller mutations while admission or validation yields cannot replace reviewed bytes',async()=>{
  const f=await fixture(),title=f.workflow.changes[0].value.title;
  const saved=await commitRecoveryWorkflow(f.plan,{...f.opts,verifyTarget:async()=>{f.plan.workflow.changes[0].value.title='Concurrent caller edit';return true;},validateInstrument:async(kind,value)=>{value.title='Callback mutation';return true;}});
  expect(saved.records[0].value.title).toBe(title);
});
test('SQLite material is preserved byte-completely and tampered binary review bytes are refused',async()=>{
  const raw=envelope([input({kinds:['schema']})]);raw[2].workflows[0].records[0].value.bytes=new Uint8Array([83,81,76,105,116,101]);const f=await fixture(decode(encode(raw))),result=await commitRecoveryWorkflow(f.plan,f.opts);expect(result.records[0].value.bytes).toEqual(raw[2].workflows[0].records[0].value.bytes);
  const other=await fixture(raw);other.plan.workflow.changes[0].value.bytes[0]=0;await expect(commitRecoveryWorkflow(other.plan,other.opts)).rejects.toThrow('changed');expect(other.targetStore.writes).toBe(0);
});
test('plain-data, native record and whole atomic batch bounds are enforced before any effects',async()=>{
  const getter=envelope();let calls=0;Object.defineProperty(getter[2].workflows[0].records[0].value,'secret',{enumerable:true,get(){calls++;return 'x';}});await expect(inspectRecoveryMaterial(getter,options)).rejects.toThrow('ordinary data');expect(calls).toBe(0);
  const cycle=envelope();cycle[2].cycle=cycle;await expect(inspectRecoveryMaterial(cycle,options)).rejects.toThrow('acyclic');
  const record=envelope();record[2].workflows[0].records[0].value.title='x'.repeat(256*1024);await expect(inspectRecoveryMaterial(record,options)).rejects.toThrow('byte limit');
  const batch=envelope();for(const r of batch[2].workflows[0].records)r.value.title='x'.repeat(200000);await expect(inspectRecoveryMaterial(batch,options)).rejects.toThrow('atomic batch limit');
});
test('bounded sequential batches stop at uncertainty and resume from exact existing closures',async()=>{
  const material=await inspectRecoveryMaterial(envelope([input({key:'one'}),input({key:'two'}),input({key:'three'})]),options),targetStore=memoryStore(),opts={...options,target,targetStore,verifyTarget:async()=>true},plans=[];
  for(const w of material.workflows)plans.push(await previewRecoveryWorkflow(w,opts));const save=targetStore.saveLocalBatch;let attempts=0;targetStore.saveLocalBatch=async changes=>{if(++attempts===2)throw Error('Connection unavailable');return save(changes);};
  const stopped=await commitRecoveryBatch(plans,{...opts,limit:3});expect(stopped.state).toBe('stopped');expect(stopped.results).toHaveLength(1);expect(stopped.nextIndex).toBe(1);expect(attempts).toBe(2);expect(targetStore.data.size).toBe(4);
  targetStore.saveLocalBatch=save;const resumed=await commitRecoveryBatch(plans,{...opts,limit:2});expect(resumed.state).toBe('paused');expect(resumed.results.map(r=>r.state)).toEqual(['retained','created']);expect(targetStore.writes).toBe(2);
  const done=await commitRecoveryBatch(plans.slice(2),{...opts,limit:2});expect(done.state).toBe('complete');expect(targetStore.writes).toBe(3);
});
test('all selected plans validate before the first batch write and invalid limits refuse immediately',async()=>{
  const f=await fixture(),bad=structuredClone(f.plan);bad.target.id='ws-'+'f'.repeat(40);
  await expect(commitRecoveryBatch([f.plan,bad],{...f.opts,limit:2})).rejects.toThrow('changed');expect(f.targetStore.writes).toBe(0);
  for(const limit of [0,26,1.5])await expect(commitRecoveryBatch([f.plan],{...f.opts,limit})).rejects.toThrow('one to twenty-five');expect(f.targetStore.writes).toBe(0);
});
