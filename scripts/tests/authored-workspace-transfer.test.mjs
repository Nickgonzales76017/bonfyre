import {test,expect} from 'bun:test';
import {createAuthoredWorkspace,addInstrument,connectInstruments,productionPrefix,blankInstrument} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
import {prepareProductionTransfer,commitProductionTransfer} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace-transfer.mjs';
import {prepareRecord,digest} from '../../integrations/browser/workspace-value.mjs';
const digestValue=async value=>digest(await prepareRecord(value).encodeWith(async blob=>digest(await blob.arrayBuffer())));
const sourceEndpoint='/apps/ws-'+ 'a'.repeat(40)+'/_records',sourceRelease='b'.repeat(64),target={context:'institution:fixture-team',id:'ws-'+'c'.repeat(40),release:'d'.repeat(64),entry:'instruments/production/'};
const validateResearchEvidence=async (source,candidate)=>{
  for(const ref of candidate.evidence){const s=source.sources.find(s=>s.id===ref.sourceId);if(!s||await digest(new TextEncoder().encode(s.body))!==s.sha256||ref.sourceSha256!==s.sha256||s.body.slice(ref.start,ref.end)!==ref.quote)return false;}return true;
};
function memoryStore(rows=[]){
  const data=new Map(rows.map(row=>[row.key,structuredClone(row)]));let writes=0;
  return {data,get writes(){return writes;},async readVersion(key){return structuredClone(data.get(key)??null);},async saveLocalBatch(changes){
    writes++;for(const c of changes)if((data.get(c.key)?.revision??0)!==c.expectedRevision)throw Error('Native revision conflict.');
    const saved=changes.map(c=>({key:c.key,revision:c.expectedRevision+1,value:structuredClone(c.value)}));for(const r of saved)data.set(r.key,r);return structuredClone(saved);
  }};
}
async function fixture({binary=false}={}){
  let production=addInstrument(createAuthoredWorkspace('Field evidence into delivery planning'),'research',{role:'source'});production=addInstrument(production,'product',{role:'output'});production=connectInstruments(production,production.panels[0].id,production.panels[1].id);if(binary)production=addInstrument(production,'schema');
  production.layout='stack';production.appearance='night';production.panels.forEach(p=>p.revision=3);production.grants=['never-copy'];
  const [research,product,schema]=production.panels,body='An actual retained passage.';
  const source={id:'source-1',title:'Source report',body,sha256:await digest(new TextEncoder().encode(body)),url:'https://example.test/report'};
  const evidence={id:'excerpt-1',sourceId:source.id,sourceSha256:source.sha256,start:0,end:body.length,quote:body};
  const candidate={id:'research-finding-1',title:'Reviewed opportunity',epic:'Research',reach:0,impact:0,confidence:0,effort:1,release:'Unscheduled',requires:[],criteria:[],evidence:[evidence],researchTitle:'Source review'};
  const incomingKey='draft:instruments:incoming:'+candidate.id;
  const incoming={key:incomingKey,revision:1,value:{candidate,sourceRecord:research.recordKey,sourceRevision:2}};
  const researchValue={...blankInstrument('research'),sources:[source],findings:[{id:'finding-1',statement:candidate.title,decision:'include',evidence:[evidence]},{id:'other-finding',statement:'A separate retained observation',decision:'exclude',evidence:[evidence]}],selectedFinding:'finding-1'};
  const adopted={...structuredClone(candidate),reach:120,impact:2,confidence:70,effort:4,origin:{record:research.recordKey,revision:2,incoming:incomingKey,incomingRevision:1},grants:['not-authority']};
  const manual={...structuredClone(candidate),id:'manual-1',title:'Follow-through',evidence:[],requires:[candidate.id]};
  const productValue={...blankInstrument('product'),stories:[adopted,manual],selected:adopted.id};
  const root={key:productionPrefix+production.id,revision:5,value:production},rows=[root,{key:research.recordKey,revision:3,value:researchValue},{key:product.recordKey,revision:3,value:productValue},incoming];
  if(schema)rows.push({key:schema.recordKey,revision:3,value:{...blankInstrument('schema'),bytes:new Uint8Array([83,81,76,105,116,101]).buffer}});
  const sourceStore=memoryStore(rows),targetStore=memoryStore();
  const prepare=extras=>prepareProductionTransfer({sourceEndpoint,sourceRelease,productionKey:root.key,productionRevision:root.revision,sourceStore,target,digestValue,validateResearchEvidence,...extras});
  const commit=(plan,extras)=>commitProductionTransfer(plan,{sourceStore,targetStore,verifyTarget:async()=>true,digestValue,...extras});
  return {production,research,product,schema,root,sourceStore,targetStore,prepare,commit,incomingKey};
}
test('a populated production preserves authoring and remaps every adopted origin and dependency',async()=>{
  const f=await fixture(),before=structuredClone([...f.sourceStore.data]),plan=await f.prepare(),saved=await f.commit(plan);
  expect(saved.revision).toBe(1);expect(saved.key).not.toBe(f.root.key);expect(saved.value.layout).toBe('stack');expect(saved.value.appearance).toBe('night');
  expect(saved.value.panels.map(p=>p.role)).toEqual(['source','output']);expect(saved.value.connections[0].from).toBe(saved.value.panels[0].id);
  const research=f.targetStore.data.get(saved.value.panels[0].recordKey),product=f.targetStore.data.get(saved.value.panels[1].recordKey),story=product.value.stories[0];
  const incoming=f.targetStore.data.get(story.origin.incoming);expect(incoming.value.candidate.id).toBe(story.id);expect(story.origin.record).toBe(research.key);expect(story.origin.revision).toBe(1);expect(story.origin.incomingRevision).toBe(1);expect(incoming.value.sourceRevision).toBe(1);expect(incoming.value.sourceRecord).toBe(research.key);
  expect(story.reach).toBe(120);expect(story.evidence[0].quote).toBe('An actual retained passage.');expect(product.value.selected).toBe(story.id);expect(product.value.stories[1].requires).toEqual([story.id]);
  expect(saved.value.grants).toBeUndefined();expect(story.grants).toBeUndefined();expect(saved.value.transferFrom.endpoint).toBe(sourceEndpoint);expect(saved.value.transferFrom.records.find(r=>r.key===f.research.recordKey).revision).toBe(3);
  expect([...f.sourceStore.data]).toEqual(before);expect(f.sourceStore.writes).toBe(0);expect(f.targetStore.writes).toBe(1);
});
test('missing bound research, incoming revision and changed evidence refuse preparation',async()=>{
  for(const mutate of [f=>{f.root.value.panels.shift();f.root.value.connections=[];},f=>f.sourceStore.data.get(f.incomingKey).revision++,f=>{f.sourceStore.data.get(f.research.recordKey).value.sources[0].body='Altered';}]){
    const f=await fixture();mutate(f);if(f.root.value.panels.length===1)f.sourceStore.data.set(f.root.key,structuredClone(f.root));await expect(f.prepare()).rejects.toThrow();expect(f.targetStore.writes).toBe(0);
  }
});
test('all exact saved sources are checked again before any destination submission',async()=>{
  for(const kind of ['root','bound','incoming']){const f=await fixture(),plan=await f.prepare(),key=kind==='root'?f.root.key:kind==='bound'?f.product.recordKey:f.incomingKey;f.sourceStore.data.get(key).revision++;await expect(f.commit(plan)).rejects.toThrow('source revision changed');expect(f.targetStore.writes).toBe(0);}
});
test('preview edits to content, target or summaries cannot silently change the copied bytes',async()=>{
  for(const mutate of [p=>p.changes[0].value.title='Edited preview',p=>p.target.context='self',p=>p.summary.recordCount++,p=>p.production.title='Different title']){const f=await fixture(),plan=await f.prepare();mutate(plan);await expect(f.commit(plan)).rejects.toThrow('reviewed copy changed');expect(f.targetStore.writes).toBe(0);}
});
test('a revoked destination refuses the complete native batch',async()=>{
  const f=await fixture(),plan=await f.prepare();await expect(f.commit(plan,{verifyTarget:async()=>false})).rejects.toThrow('write admission changed');expect(f.targetStore.writes).toBe(0);
});
test('target partial state, deleted records and conflicting content are reconciled without another write',async()=>{
  for(const mode of ['partial','deleted','different','revision']){const f=await fixture(),plan=await f.prepare();for(const c of (mode==='partial'?plan.changes.slice(0,1):plan.changes))f.targetStore.data.set(c.key,{key:c.key,revision:1,value:structuredClone(c.value)});const first=f.targetStore.data.get(plan.changes[0].key);if(mode==='deleted')first.deleted=true;if(mode==='different')first.value.title='Other content';if(mode==='revision')first.revision=2;await expect(f.commit(plan)).rejects.toThrow();expect(f.targetStore.writes).toBe(0);}
});
test('a lost acknowledgement accepts complete exact readback and later retry never writes again',async()=>{
  const f=await fixture(),plan=await f.prepare(),save=f.targetStore.saveLocalBatch;f.targetStore.saveLocalBatch=async changes=>{await save(changes);throw Error('Connection lost after commit.');};
  const row=await f.commit(plan);expect(row.key).toBe(plan.productionKey);expect(f.targetStore.writes).toBe(1);
  f.sourceStore.data.get(f.root.key).revision++;const retried=await f.commit(plan);expect(retried).toEqual(row);expect(f.targetStore.writes).toBe(1);
});
test('a failed write with no retained target keeps the same reviewed plan usable',async()=>{
  const f=await fixture(),plan=await f.prepare(),save=f.targetStore.saveLocalBatch;f.targetStore.saveLocalBatch=async()=>{throw Error('Offline before commit.');};await expect(f.commit(plan)).rejects.toThrow('Offline');expect(f.targetStore.data.size).toBe(0);f.targetStore.saveLocalBatch=save;expect((await f.commit(plan)).key).toBe(plan.productionKey);
});
test('SQLite bytes survive transfer and byte mutations fail preview validation',async()=>{
  const f=await fixture({binary:true}),plan=await f.prepare(),saved=await f.commit(plan),schema=f.targetStore.data.get(saved.value.panels[2].recordKey);expect(new Uint8Array(schema.value.bytes)).toEqual(new Uint8Array([83,81,76,105,116,101]));
  const other=await fixture({binary:true}),bad=await other.prepare();new Uint8Array(bad.changes[2].value.bytes)[0]=0;await expect(other.commit(bad)).rejects.toThrow('reviewed copy changed');expect(other.targetStore.writes).toBe(0);
});
test('caller mutations while admission awaits cannot replace the captured reviewed material',async()=>{
  const f=await fixture(),plan=await f.prepare();const original=plan.changes[0].value.title;const row=await f.commit(plan,{verifyTarget:async()=>{plan.changes[0].value.title='Concurrent mutation';return true;}});expect(f.targetStore.data.get(row.value.panels[0].recordKey).value.title).toBe(original);
});
test('a new branch in the same destination creates new records without altering the source closure',async()=>{
  const f=await fixture(),before=structuredClone([...f.sourceStore.data]),plan=await f.prepare({target:{...target,id:'ws-'+'a'.repeat(40),release:sourceRelease}});const saved=await f.commit(plan,{targetStore:f.sourceStore});expect(saved.key).not.toBe(f.root.key);for(const [key,row]of before)expect(f.sourceStore.data.get(key)).toEqual(row);expect(f.sourceStore.data.size).toBe(before.length+plan.changes.length);
});
test('the complete value and batch bounds reject oversized production material before writes',async()=>{
  const f=await fixture();f.sourceStore.data.get(f.research.recordKey).value.title='x'.repeat(256*1024);await expect(f.prepare()).rejects.toThrow('native value limit');expect(f.targetStore.writes).toBe(0);
  const g=await fixture({binary:true});g.sourceStore.data.get(g.research.recordKey).value.title='x'.repeat(200000);g.sourceStore.data.get(g.product.recordKey).value.title='x'.repeat(200000);g.sourceStore.data.get(g.schema.recordKey).value.acceptedSQL=['x'.repeat(200000)];await expect(g.prepare()).rejects.toThrow('native batch limit');expect(g.targetStore.writes).toBe(0);
});

test('copied findings retain roadmap candidate identity through repeated destination transfers',async()=>{
  const f=await fixture(),first=await f.prepare(),saved=await f.commit(first);
  function verify(store,row){
    const research=store.data.get(row.value.panels[0].recordKey).value,product=store.data.get(row.value.panels[1].recordKey).value,finding=research.findings.find(f=>f.id===research.selectedFinding),story=product.stories.find(s=>s.origin);
    expect(story.id).toBe('research-'+finding.id);expect(story.origin.incoming).toBe('draft:instruments:incoming:research-'+finding.id);expect(research.findings).toHaveLength(2);expect(research.findings[1].statement).toBe('A separate retained observation');expect(research.findings[1].decision).toBe('exclude');expect(finding.evidence[0].quote).toBe('An actual retained passage.');return finding.id;
  }
  const firstFinding=verify(f.targetStore,saved),secondStore=memoryStore();
  const second=await prepareProductionTransfer({sourceEndpoint:'/apps/'+target.id+'/_records',sourceRelease:target.release,productionKey:saved.key,productionRevision:saved.revision,sourceStore:f.targetStore,target:{...target,id:'ws-'+'e'.repeat(40)},digestValue,validateResearchEvidence});
  const again=await commitProductionTransfer(second,{sourceStore:f.targetStore,targetStore:secondStore,verifyTarget:async()=>true,digestValue});expect(verify(secondStore,again)).not.toBe(firstFinding);expect(f.sourceStore.data.get(f.research.recordKey).value.selectedFinding).toBe('finding-1');
});
test('an incoming candidate without its original research finding cannot create an orphaned repeat offer',async()=>{
  const f=await fixture(),research=f.sourceStore.data.get(f.research.recordKey).value;research.findings=[];research.selectedFinding=null;await expect(f.prepare()).rejects.toThrow('original research finding');expect(f.targetStore.writes).toBe(0);
});
