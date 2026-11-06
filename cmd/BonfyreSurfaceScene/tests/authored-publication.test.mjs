import {test,expect} from 'bun:test';
import {createAuthoredWorkspace,addInstrument,connectInstruments} from '../runtime/authored-workspace.mjs';
import {publicProductionDesign,validatePublicDesign,instantiatePublicDesign,designFromPacket,publicationPayload,publicationRoute,reconcilePublication} from '../runtime/authored-publication.mjs';
function source(){let p=addInstrument(createAuthoredWorkspace('Workshop investigation'),'research',{role:'source'});p=addInstrument(p,'product',{role:'output'});return connectInstruments(p,p.panels[0].id,p.panels[1].id);}
test('design projection preserves the arrangement without copying private identities or material',()=>{
  const p=source();p.transferFrom={recordKey:'private-origin'};p.panels[0].localMaterial='not part of the design';const d=publicProductionDesign(p);
  expect(Object.keys(d)).toEqual(['schema','title','description','layout','appearance','panels','connections']);expect(Object.keys(d.panels[0])).toEqual(['kind','label','role']);expect(d.connections).toEqual([{from:0,to:1,operation:'research-opportunity'}]);
  expect(JSON.stringify(d)).not.toContain(p.id);expect(JSON.stringify(d)).not.toContain('private-origin');expect(JSON.stringify(d)).not.toContain('not part of the design');p.panels[0].label='Changed later';expect(d.panels[0].label).not.toBe(p.panels[0].label);
});
test('each reuse prepares independent blank record identities and retains the receiving operation',()=>{
  const d=publicProductionDesign(source()),a=instantiatePublicDesign(d),b=instantiatePublicDesign(d);
  expect(a.id).not.toBe(b.id);expect(a.panels.map(p=>p.revision)).toEqual([0,0]);expect(a.panels.every((p,i)=>p.recordKey!==b.panels[i].recordKey)).toBe(true);expect(a.connections[0]).toEqual({from:a.panels[0].id,to:a.panels[1].id,operation:'research-opportunity'});expect(a.panels[0].role).toBe('source');expect(a.panels[1].role).toBe('output');
});
test('unknown fields, unsupported operations and ambiguous handoffs are rejected',()=>{
  const d=publicProductionDesign(source());expect(()=>validatePublicDesign({...d,privateBody:'x'})).toThrow();expect(()=>validatePublicDesign({...d,panels:[{...d.panels[0],recordKey:'x'}]})).toThrow();expect(()=>validatePublicDesign({...d,connections:[...d.connections,...d.connections]})).toThrow();expect(()=>validatePublicDesign({...d,connections:[{from:1,to:0,operation:'research-opportunity'}]})).toThrow();expect(()=>validatePublicDesign({...d,layout:'execute-all'})).toThrow();expect(()=>validatePublicDesign({...d,title:'\u0000'})).toThrow();
});
test('public MessagePack tuple accepts only the supported design grammar',()=>{
  const d=publicProductionDesign(source()),packet=[1,d.title,d.description,d.layout,d.appearance,d.panels.map(p=>[p.kind,p.label,p.role]),d.connections.map(e=>[e.from,e.to,e.operation])];expect(designFromPacket(packet)).toEqual(d);expect(()=>designFromPacket([...packet,'extra'])).toThrow();expect(()=>designFromPacket([1])).toThrow();packet[5][0].push('extra');expect(()=>designFromPacket(packet)).toThrow();
});
test('CMS publication and withdrawal retain the exact reviewed aggregate baseline',async()=>{
  const p=source(),d=publicProductionDesign(p),route=await publicationRoute(p.id),baseline={schema:'bonfyre.page-snapshot.v1',id:0,digest:'a'.repeat(64),snapshot:{namespace:'actor-7',route,page:null,fragments:[]}};
  const next=publicationPayload(d,route,baseline);expect(next.operation).toBe('create');expect(next.expected_digest).toBe(baseline.digest);expect(next.page.status).toBe('published');expect(next.fragments[0].projection_contract.design).toEqual(d);
  const existing={...baseline,id:8,snapshot:{...baseline.snapshot,page:next.page,fragments:next.fragments}};const withdrawn=publicationPayload(d,route,existing,false);expect(withdrawn.operation).toBe('replace');expect(withdrawn.expected_id).toBe(8);expect(withdrawn.page.status).toBe('draft');expect(withdrawn.fragments[0].status).toBe('draft');
  expect(()=>publicationPayload(d,route,{...existing,digest:'bad'})).toThrow();expect(()=>publicationPayload(d,route,{...existing,snapshot:{...existing.snapshot,page:{machine_view:'different-page'}}})).toThrow();d.panels[0].label='edited';expect(next.fragments[0].projection_contract.design.panels[0].label).not.toBe('edited');
});
test('publication coordinates are deterministic for one production and distinct for another',async()=>{
  const a=source(),b=source();expect(await publicationRoute(a.id)).toBe(await publicationRoute(a.id));expect(await publicationRoute(a.id)).not.toBe(await publicationRoute(b.id));expect(await publicationRoute(a.id)).toMatch(/^\/design-[a-f0-9]{64}$/);
});
test('empty productions and oversized public fields do not masquerade as ready designs',()=>{
  expect(()=>publicProductionDesign(createAuthoredWorkspace())).toThrow();const d=publicProductionDesign(source());expect(()=>validatePublicDesign({...d,description:'x'.repeat(4001)})).toThrow();expect(()=>validatePublicDesign({...d,panels:[...d.panels,...d.panels,...d.panels]})).toThrow();
});

test('recovery requires the exact native page, fragments, namespace and reviewed identity',async()=>{
  const p=source(),route=await publicationRoute(p.id),baseline={schema:'bonfyre.page-snapshot.v1',id:0,digest:'a'.repeat(64),snapshot:{namespace:'actor-7',route,page:null,fragments:[]}};
  const payload=publicationPayload(publicProductionDesign(p),route,baseline),current={schema:baseline.schema,id:7,digest:'b'.repeat(64),snapshot:{namespace:'actor-7',route,page:{...payload.page,id:7,namespace:'actor-7'},fragments:payload.fragments.map((f,i)=>({...f,id:8+i,namespace:'actor-7',projection_contract:JSON.stringify(f.projection_contract)}))}};
  expect(reconcilePublication(payload,baseline,current)).toBe('committed');expect(reconcilePublication(payload,baseline,baseline)).toBe('unchanged');
  for(const alter of [s=>s.snapshot.page.title='Another editor',s=>s.snapshot.page.status='draft',s=>s.snapshot.page.redirect='https://example.test',s=>s.snapshot.fragments.push({...s.snapshot.fragments[0],id:9}),s=>s.snapshot.fragments[0].projection_contract='{}',s=>s.snapshot.fragments[0].namespace='actor-8']){
    const changed=structuredClone(current);alter(changed);expect(reconcilePublication(payload,baseline,changed)).toBe('conflict');
  }
  const other=structuredClone(current);other.snapshot.namespace='actor-8';expect(()=>reconcilePublication(payload,baseline,other)).toThrow('destination');
  const replacement=publicationPayload(publicProductionDesign(p),route,current),recreated=structuredClone(current);recreated.id=recreated.snapshot.page.id=9;recreated.digest='c'.repeat(64);expect(reconcilePublication(replacement,current,recreated)).toBe('conflict');
});
test('withdrawal recovery requires both page and every public fragment to be withdrawn',async()=>{
  const p=source(),route=await publicationRoute(p.id),empty={schema:'bonfyre.page-snapshot.v1',id:0,digest:'a'.repeat(64),snapshot:{namespace:'actor-7',route,page:null,fragments:[]}},published=publicationPayload(publicProductionDesign(p),route,empty);
  const baseline={...empty,id:7,digest:'b'.repeat(64),snapshot:{namespace:'actor-7',route,page:{...published.page,id:7,namespace:'actor-7'},fragments:published.fragments.map(f=>({...f,id:8,namespace:'actor-7'}))}};
  const payload=publicationPayload(publicProductionDesign(p),route,baseline,false),current=structuredClone(baseline);current.digest='c'.repeat(64);current.snapshot.page.status='draft';
  expect(reconcilePublication(payload,baseline,current)).toBe('conflict');current.snapshot.fragments[0].status='draft';expect(reconcilePublication(payload,baseline,current)).toBe('committed');
});
test('a missing readback digest or route cannot be mistaken for recovery',async()=>{
  const p=source(),route=await publicationRoute(p.id),baseline={schema:'bonfyre.page-snapshot.v1',id:0,digest:'a'.repeat(64),snapshot:{namespace:'actor-7',route,page:null,fragments:[]}},payload=publicationPayload(publicProductionDesign(p),route,baseline);
  expect(()=>reconcilePublication(payload,baseline,{...baseline,digest:null})).toThrow();expect(()=>reconcilePublication(payload,baseline,{...baseline,snapshot:{...baseline.snapshot,route:'/another'}})).toThrow();
});
