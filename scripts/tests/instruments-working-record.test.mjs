import test from 'node:test';
import assert from 'node:assert/strict';
import {WorkingRecord,readExactRecord,checkRecordKey} from '../../components/BonfyreServiceSite/instruments/working-record.mjs';
import {preparedOpportunity,adoptOpportunity} from '../../components/BonfyreServiceSite/instruments/prepared-opportunity.mjs';
import {sourceRecord,excerpt,roadmapCandidate} from '../../components/BonfyreServiceSite/instruments/research.mjs';
const defer=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
function memoryStore(){
  const records=new Map(),calls=[];let gate=null;
  return {records,calls,set gate(value){gate=value;},async readLocal(prefix){return structuredClone([...records.values()].filter(r=>r.key.startsWith(prefix)));},async saveLocalBatch(changes){
    calls.push(structuredClone(changes));if(gate)await gate.promise;
    for(const c of changes)if((records.get(c.key)?.revision??0)!==c.expectedRevision)throw Error('stale transaction');
    const result=changes.map(c=>({key:c.key,revision:c.expectedRevision+1,value:structuredClone(c.value)}));
    for(const row of result)records.set(row.key,row);return structuredClone(result);
  }};
}
function record(store,kind='product',name='a',options={}){
 let value={kind,title:name,edits:0},fail=false;
 const work=new WorkingRecord({kind,store,key:`draft:instruments:${kind}:${name}`,capture:()=>value,load:async v=>{if(fail)throw Error('invalid domain object');value=v;},...options});
 return {work,get value(){return value;},set fail(v){fail=v;},edit(){value.edits++;work.changed();}};
}
test('captures before asynchronous storage and preserves subsequent working edits',async()=>{
 const store=memoryStore(),a=record(store),gate=defer();store.gate=gate;a.edit();const promise=a.work.save();a.edit();gate.resolve();await promise;
 assert.equal(store.records.get(a.work.key).value.edits,1);assert.equal(a.value.edits,2);assert.equal(a.work.dirty,true);assert.equal(a.work.revision,1);
});
test('joint save is one transaction across independent records',async()=>{
 const store=memoryStore(),a=record(store,'research'),b=record(store,'product');a.edit();b.edit();const rows=await WorkingRecord.saveTogether([a.work,b.work]);
 assert.equal(store.calls.length,1);assert.equal(store.calls[0].length,2);assert.equal(rows.length,2);assert.equal(a.work.dirty,false);assert.equal(b.work.dirty,false);
});
test('stale member aborts entire joint save and retains both dirty drafts',async()=>{
 const store=memoryStore(),a=record(store,'research'),b=record(store,'product');await WorkingRecord.saveTogether([a.work,b.work]);a.edit();b.edit();store.records.get(b.work.key).revision++;
 await assert.rejects(WorkingRecord.saveTogether([a.work,b.work]),/stale/);assert.equal(store.records.get(a.work.key).revision,1);assert.equal(store.records.get(a.work.key).value.edits,0);assert(a.work.dirty&&b.work.dirty);assert(!a.work.busy&&!b.work.busy);
});
test('failed capture of a second record releases the first without saving',async()=>{
 const store=memoryStore(),a=record(store),b=record(store,'research','b',{capture:()=>{throw Error('capture failed');}});
 await assert.rejects(WorkingRecord.saveTogether([a.work,b.work]),/capture/);assert.equal(store.calls.length,0);assert.equal(a.work.busy,false);
});
test('cross-destination records cannot pretend a joint transaction',async()=>{
 const a=record(memoryStore()),b=record(memoryStore(),'research');await assert.rejects(WorkingRecord.saveTogether([a.work,b.work]),/same existing destination/);
});
test('duplicated objects or keys are refused before storage',async()=>{
 const store=memoryStore(),a=record(store),b=record(store);await assert.rejects(WorkingRecord.saveTogether([a.work,a.work]));await assert.rejects(WorkingRecord.saveTogether([a.work,b.work]));assert.equal(store.calls.length,0);
});
test('joint save tracks the dirty epoch of each pane independently',async()=>{
 const store=memoryStore(),a=record(store,'research'),b=record(store),gate=defer();store.gate=gate;a.edit();b.edit();const p=WorkingRecord.saveTogether([a.work,b.work]);b.edit();gate.resolve();await p;assert(!a.work.dirty);assert(b.work.dirty);
});
test('a blocked storage operation cannot be repeated or closed concurrently',async()=>{
 const store=memoryStore(),a=record(store),gate=defer();store.gate=gate;const p=a.work.save();await assert.rejects(a.work.save(),/first/);assert.throws(()=>a.work.dispose(),/before closing/);gate.resolve();await p;
});
test('storage rejection is never automatically retried',async()=>{
 let calls=0;const store={readLocal:async()=>[],saveLocalBatch:async()=>{calls++;throw Error('unavailable');}},a=record(store);a.edit();await assert.rejects(a.work.save(),/unavailable/);assert.equal(calls,1);assert(a.work.dirty);assert(!a.work.busy);
});
test('presentation callback failure does not mask a confirmed save',async()=>{
 const store=memoryStore(),a=record(store,'product','a',{onChange:()=>{throw Error('view failed');}});a.edit();await a.work.save();assert.equal(a.work.revision,1);assert(!a.work.dirty);
});
test('exact reads never select a matching-prefix neighbour',async()=>{
 const store=memoryStore();store.records.set('draft:instruments:product:ab',{key:'draft:instruments:product:ab',revision:1,value:{kind:'product'}});await assert.rejects(readExactRecord(store,'product','draft:instruments:product:a'),/No example/);
});
test('continuation refuses a stale requested revision rather than silently opening current',async()=>{
 const store=memoryStore(),a=record(store);await a.work.save();await a.work.save();await assert.rejects(readExactRecord(store,'product',a.work.key,1),/revision 1.*revision 2/);
});
test('malformed and wrong-instrument keys are refused',()=>{
 for(const key of ['draft:instruments:research:a','draft:instruments:product:','draft:instruments:product:../x','http://example.test/x'])assert.throws(()=>checkRecordKey('product',key));
});
test('failed domain load preserves current working identity and dirty state',async()=>{
 const store=memoryStore(),a=record(store),b=record(store,'product','b');await b.work.save();a.edit();a.fail=true;await assert.rejects(a.work.open(b.work.key),/invalid domain/);assert.equal(a.work.key,'draft:instruments:product:a');assert.equal(a.work.revision,0);assert(a.work.dirty);assert(!a.work.loading);
});
test('valid reopen adopts exact value and revision, not a default sample',async()=>{
 const store=memoryStore(),a=record(store),b=record(store,'product','b');b.edit();await b.work.save();await a.work.open(b.work.key,1);assert.equal(a.value.title,'b');assert.equal(a.value.edits,1);assert.equal(a.work.revision,1);
});
test('file bytes survive save coordination without JSON serialization',async()=>{
 const store=memoryStore(),data={kind:'schema',bytes:new Uint8Array([0,1,255]),blob:new Blob(['source'])},a=record(store,'schema','a',{capture:()=>data});await a.work.save();const v=store.records.get(a.work.key).value;assert(v.bytes instanceof Uint8Array);assert.deepEqual([...v.bytes],[0,1,255]);assert.equal(await v.blob.text(),'source');
});
test('wrong-kind capture fails before any save',async()=>{
 const store=memoryStore(),a=record(store,'product','a',{capture:()=>({kind:'research'})});await assert.rejects(a.work.save(),/kind/);assert.equal(store.calls.length,0);
});
test('source and incoming candidate save atomically with exact source revision',async()=>{
 const store=memoryStore(),a=record(store,'research');a.edit();const row=await a.work.handoff({id:'research-f1',title:'Candidate'});assert.equal(store.calls.length,1);assert.equal(row.value.sourceRecord,a.work.key);assert.equal(row.value.sourceRevision,1);assert.equal(a.work.revision,1);
});
test('duplicate incoming candidate refuses without advancing source revision',async()=>{
 const store=memoryStore(),a=record(store,'research');await a.work.handoff({id:'research-f1'});a.edit();await assert.rejects(a.work.handoff({id:'research-f1'}),/stale/);assert.equal(a.work.revision,1);assert.equal(store.records.get(a.work.key).revision,1);assert(a.work.dirty);
});
test('non-research record cannot offer a research candidate',async()=>{
 const store=memoryStore(),a=record(store);await assert.rejects(a.work.handoff({id:'research-f1'}));assert.equal(store.calls.length,0);
});
async function researchFixture(){
 const store=memoryStore(),source=await sourceRecord({id:'own-input',title:'Field observation',body:'The observed return rate was twelve percent. The follow-up sample differed.'});
 const ref={...excerpt(source,0,43),relation:'supports'},finding={id:'f1',statement:'The initial sample needs follow-up.',theme:'Follow-up',evidence:[ref]},project={kind:'research',title:'Unfamiliar review',sources:[source],findings:[finding]};
 const candidate=roadmapCandidate(project,finding),key='draft:instruments:incoming:'+candidate.id,sourceKey='draft:instruments:research:source';
 store.records.set(sourceKey,{key:sourceKey,revision:1,value:project});store.records.set(key,{key,revision:1,value:{candidate,sourceRecord:sourceKey,sourceRevision:1}});
 return {store,key,sourceKey,source,project,candidate};
}
test('prepared candidate verifies exact retained passages and leaves estimates uninferred',async()=>{
 const f=await researchFixture(),p=await preparedOpportunity(f.store,f.key);assert.equal(p.candidate.reach,0);assert.equal(p.candidate.confidence,0);assert.equal(p.candidate.evidence[0].quote,f.source.body.slice(0,43));assert(!p.sourceAdvanced);
});
test('unrelated newer source-review revision does not invalidate identical consumed text',async()=>{
 const f=await researchFixture();f.store.records.get(f.sourceKey).revision++;const p=await preparedOpportunity(f.store,f.key);assert(p.sourceAdvanced);assert.equal(p.row.value.sourceRevision,1);
});
test('changed passage or corrupted source refuses preparation',async()=>{
 const f=await researchFixture();f.source.body='Changed source.';await assert.rejects(preparedOpportunity(f.store,f.key),/digest/);
});
test('changed source with a recomputed digest still refuses old passage references',async()=>{
 const f=await researchFixture();f.project.sources[0]=await sourceRecord({...f.source,body:'A corrected observation differs.'});await assert.rejects(preparedOpportunity(f.store,f.key),/passage changed/);
});
test('adoption preserves existing stories and carries the original source coordinates',async()=>{
 const f=await researchFixture(),p=await preparedOpportunity(f.store,f.key),plan={kind:'product',title:'Plan',releases:[],stories:[]},next=adoptOpportunity(plan,p);assert.equal(plan.stories.length,0);assert.equal(next.stories.length,1);assert.equal(next.stories[0].origin.record,f.sourceKey);assert.equal(next.stories[0].origin.revision,1);assert.equal(next.selected,p.candidate.id);assert.throws(()=>adoptOpportunity(next,p),/already/);
});
test('invalid receiving requirements do not partially mutate the plan',async()=>{
 const f=await researchFixture(),p=await preparedOpportunity(f.store,f.key),plan={kind:'product',title:'Plan',releases:[],stories:[]};p.candidate.effort=-1;assert.throws(()=>adoptOpportunity(plan,p));assert.deepEqual(plan.stories,[]);
});
