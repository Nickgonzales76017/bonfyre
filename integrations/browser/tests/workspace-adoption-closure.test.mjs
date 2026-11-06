import {test,expect} from 'bun:test';
import {prepareWorkspaceAdoption,adoptionValue,adoptionDependencies} from '../workspace-adoption.mjs';
import {prepareAdoptionReview,planAdoptionSelection} from '../workspace-adoption-review.mjs';
const scope='a'.repeat(64),digest='b'.repeat(64),bodyKey=`product-model:${scope}:body:${digest}`;
const binding=(id='lamp / café')=>({key:`product-model:${scope}:binding:${encodeURIComponent(id)}`,revision:3,value:{scope,productId:id,digest,unit:'metres',variant:null,name:'model.gltf'}});
const body=()=>({key:bodyKey,revision:1,value:{files:[{name:'model.gltf',blob:new Blob(['{"asset":{"version":"2.0"}}'],{type:'model/gltf+json'})}]}});
const readNothing=async()=>null;
const current=row=>({...structuredClone(row),deleted:false});
const review=rows=>prepareWorkspaceAdoption(rows,{readVersion:readNothing});
test('a selected model binding brings its exact offered body in the same batch',async()=>{
  const root=binding(),b=body(),rows=await review([root,b]),plan=planAdoptionSelection(rows,[root.key]);
  expect(plan.blockers).toEqual([]);expect(plan.writes).toBe(2);expect(plan.changes.map(c=>c.key)).toEqual([bodyKey,root.key]);expect(plan.required[0].for).toEqual([root.key]);
});
test('two product bindings share one body without copying another catalogue',async()=>{
  const a=binding('A'),b=binding('B'),rows=await review([a,b,body()]),plan=planAdoptionSelection(rows,[a.key,b.key]);
  expect(plan.writes).toBe(3);expect(plan.required).toHaveLength(1);expect(plan.required[0].for).toEqual([a.key,b.key]);
});
test('a missing body produces an exact blocker and no partial write plan',async()=>{
  const root=binding(),rows=await review([root]),plan=planAdoptionSelection(rows,[root.key]);
  expect(plan.blockers.join(' ')).toContain(bodyKey);expect(plan.changes).toEqual([]);expect(plan.writes).toBe(0);
});
test('an existing exact body is read once and bound as a check-only dependency',async()=>{
  const root=binding(),b=current(body()),read=[];
  const rows=await prepareWorkspaceAdoption([root],{readVersion:async key=>{read.push(key);return key===bodyKey?b:null;}});
  const plan=planAdoptionSelection(rows,[root.key]);expect(read).toEqual([root.key,bodyKey]);expect(plan.writes).toBe(1);expect(plan.checks).toBe(1);expect(plan.changes[0]).toEqual({key:bodyKey,expectedRevision:1,checkOnly:true});
});
test('different bytes under a content-addressed body cannot be adopted over the original',async()=>{
  const root=binding(),existing=body();existing.value.files[0].blob=new Blob(['different']);
  const rows=await prepareWorkspaceAdoption([root,body()],{readVersion:async key=>key===bodyKey?current(existing):null});
  expect(rows.find(r=>r.id===bodyKey).selectable).toBe(false);expect(planAdoptionSelection(rows,[root.key]).blockers.length).toBe(1);
  expect(()=>planAdoptionSelection(rows,[root.key,bodyKey])).toThrow();
});
test('deleted dependencies are not restored merely by selecting a parent',async()=>{
  const root=binding(),rows=await prepareWorkspaceAdoption([root,body()],{readVersion:async key=>key===bodyKey?{key,revision:2,deleted:true,value:null}:null});
  expect(rows.find(r=>r.id===bodyKey).initiallySelected).toBe(false);expect(planAdoptionSelection(rows,[root.key]).changes).toEqual([]);
  expect(planAdoptionSelection(rows,[root.key,bodyKey]).writes).toBe(2);
});
test('all source bodies snapshot before the first native read yields',async()=>{
  const input=[binding(),body()];let release;const gate=new Promise(r=>release=r);
  const pending=prepareWorkspaceAdoption(input,{readVersion:async()=>{await gate;return null;}});
  input[0].value.name='later';input[1].value.files[0].blob=new Blob(['later']);release();
  const plan=planAdoptionSelection(await pending,[input[0].key]);expect(plan.changes[1].value.name).toBe('model.gltf');expect(await plan.changes[0].value.files[0].blob.text()).toBe('{"asset":{"version":"2.0"}}');
});
test('mutating display rows or an earlier plan cannot alter the reviewed commit basis',async()=>{
  const root=binding(),rows=await review([root,body()]);rows[0].data.after.name='forged';rows[0].data.row.key='wrong';rows[0].data.current={revision:80};
  const first=planAdoptionSelection(rows,[root.key]);first.changes[1].value.name='changed output';
  const next=planAdoptionSelection(rows,[root.key]);expect(next.changes[1].value.name).toBe('model.gltf');expect(next.changes[1].expectedRevision).toBe(0);
});
test('same flags cannot hide a destination conflict',async()=>{
  const row={key:'draft:catalogue:one',revision:2,value:{title:'proposed'}};
  const rows=await prepareAdoptionReview([{row,current:{key:row.key,revision:4,deleted:false,value:{title:'current'}},same:true}],{candidate:adoptionValue});
  expect(rows[0].status).toBe('Conflicting draft');expect(rows[0].initiallySelected).toBe(false);expect(planAdoptionSelection(rows,[row.key]).changes[0].expectedRevision).toBe(4);
});
test('candidate transformation runs at review and is not rerun at commit',async()=>{
  let calls=0;const row={key:'draft:catalogue:one',revision:1,value:{title:'source'}};
  const rows=await prepareAdoptionReview([{row,current:null}],{candidate:r=>({...r.value,stamp:++calls})});
  planAdoptionSelection(rows,[row.key]);planAdoptionSelection(rows,[row.key]);expect(calls).toBe(1);
});
test('native adoption still resets recording permission without losing original audio or edits',async()=>{
  const row={key:'draft:creator:session',revision:7,value:{permission:'licensed',files:[{name:'voice.wav',bytes:new Uint8Array([1,2,3]).buffer}],clips:[{id:'take',gain:0.5}]}};
  const rows=await prepareWorkspaceAdoption([row],{family:'creator-audio',readVersion:readNothing});const v=planAdoptionSelection(rows,[row.key]).changes[0].value;
  expect(v.permission).toBe('unreviewed');expect(v.sourcePermission).toBe('licensed');expect(v.clips).toEqual(row.value.clips);expect([...new Uint8Array(v.files[0].bytes)]).toEqual([1,2,3]);expect(row.value.permission).toBe('licensed');
});
test('room identity survives while the source submission receipt does not become destination approval',async()=>{
  const row={key:'fixture-plan:room',revision:1,value:{plan:{id:'room',items:[{key:'part',product:'SKU',x:1.5}]},receipt:{reference:'source-only'}}};
  const v=planAdoptionSelection(await review([row]),[row.key]).changes[0].value;
  expect(v.plan).toEqual(row.value.plan);expect(v.receipt).toBeNull();expect(row.value.receipt.reference).toBe('source-only');
});
test('wrong account families and mismatched model identity refuse before reads',async()=>{
  let reads=0;const readVersion=async()=>{reads++;return null;};
  await expect(prepareWorkspaceAdoption([{key:'credential:private',revision:1,value:{}}],{readVersion})).rejects.toThrow();
  const row=binding();row.value.productId='different';await expect(prepareWorkspaceAdoption([row],{readVersion})).rejects.toThrow();expect(reads).toBe(0);
});
test('model shape, duplicate source identities and invented review handles are refused',async()=>{
  const root=binding(),bad=body();bad.value.files=[];
  await expect(review([root,bad])).rejects.toThrow();await expect(review([root,root])).rejects.toThrow();
  const rows=await review([root,body()]);expect(()=>planAdoptionSelection([...rows],[root.key])).toThrow();expect(()=>planAdoptionSelection(rows,[root.key,root.key])).toThrow();
});
test('dependency cycles retain blockers rather than producing a partial batch',async()=>{
  const rows=await prepareAdoptionReview(['A','B'].map(key=>({row:{key,revision:1,value:{}},current:null})),{candidate:r=>r.value,dependencies:r=>[r.key==='A'?'B':'A']});
  const plan=planAdoptionSelection(rows,['A']);expect(plan.blockers.join(' ')).toContain('cycle');expect(plan.changes).toEqual([]);
});
test('aborted review produces no committable handle',async()=>{
  const abort=new AbortController();abort.abort();await expect(prepareWorkspaceAdoption([binding()],{readVersion:readNothing,signal:abort.signal})).rejects.toThrow();
});
test('oversized and multiply referenced payloads refuse before native reads',async()=>{
  let reads=0;const readVersion=async()=>{reads++;return null;},bytes=new Uint8Array(17*1024*1024);
  await expect(prepareWorkspaceAdoption([{key:'draft:catalogue:big',revision:1,value:{a:bytes,b:bytes}}],{readVersion})).rejects.toThrow('retained-byte');expect(reads).toBe(0);
});
test('native baseline reads stop at the aggregate retained-byte budget',async()=>{
  let reads=0;const blob=new Blob([new Uint8Array(24*1024*1024)]);
  const rows=Array.from({length:4},(_,i)=>({key:'draft:catalogue:large-'+i,revision:1,value:{title:'candidate'}}));
  await expect(prepareWorkspaceAdoption(rows,{readVersion:async key=>{reads++;return{key,revision:1,deleted:false,value:{blob}};}})).rejects.toThrow('retained-byte');expect(reads).toBe(3);
});
test('shared memory and computed properties cannot cross the review snapshot',async()=>{
  let accessed=0;const value={};Object.defineProperty(value,'computed',{enumerable:true,get(){accessed++;return 'not reviewed';}});
  await expect(review([{key:'draft:catalogue:getter',revision:1,value}])).rejects.toThrow('Computed properties');expect(accessed).toBe(0);
  await expect(review([{key:'draft:catalogue:shared',revision:1,value:{bytes:new Uint8Array(new SharedArrayBuffer(8))}}])).rejects.toThrow('Shared mutable memory');
});
