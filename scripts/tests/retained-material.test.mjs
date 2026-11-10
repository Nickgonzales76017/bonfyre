import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {homedir,tmpdir} from 'node:os';
import {localReferences,referenceDescriptor,materialSegments,requirementRange,textDigest,readMaterial,readContract,CONTRACT_PREFIX} from '../../integrations/browser/retained-material.mjs';
import {readWorkspaceFile,loadWorkspaceFiles} from '../workspace_material.mjs';
import {materializeReferenceRepair} from '../materialize_reference_repair.mjs';
import {baseRelease,historyFixture} from './retained-material-fixture.mjs';
let fixture;
beforeAll(async()=>{fixture=await historyFixture();},20000);
test('every local reference in all 98 imported studies resolves to complete existing native material',async()=>{
 let total=0,contracts=0,attachments=0,provenance=0,affected=0;const unique=new Set(),unresolved=[];
 for(const row of fixture.rows){let hits=0;for(const source of row.value.sources){
  const segments=materialSegments(source);expect(segments.filter(s=>s.kind==='text').every(s=>source.body.slice(s.start,s.end)===s.text)).toBe(true);
  const tokens=localReferences(source.body);for(const token of tokens){hits++;total++;const ref={...referenceDescriptor(source,token),recordKey:row.key,sourceId:source.id,sourceSha256:source.sha256};
   if(ref.kind==='provenance'){provenance++;expect(ref.label).toMatch(/^Historical release: /);continue;}
   let material;try{material=await readMaterial(ref,{store:fixture.store});}catch(error){unresolved.push({name:token.name,kind:source.history?.kind,matchedCoordinate:token.raw.slice(-180),record:row.key,source:source.id,reason:error.message});continue;}
   expect(material.body.length).toBeGreaterThan(0);expect(await textDigest(material.body)).toBe(material.sha256);
   if(ref.kind==='contract'){contracts++;const packet=fixture.packets.find(p=>p.contractHash===ref.digest);expect(material.body).toBe(packet.contracts??packet.shared);}
   else {attachments++;const coordinate=token.raw.slice(token.raw.indexOf('/.bonfyre/')+1),original=fixture.packets.flatMap(p=>p.sources).find(s=>s.label===coordinate);expect(original).toBeDefined();expect(material.body).toBe(original.text);expect(material.parentSha256).toBe(source.sha256);}
   unique.add(ref.key||ref.locator);
  }
 }if(hits)affected++;}
 console.log('REFERENCE_COVERAGE',{total,contracts,attachments,provenance,affected,distinct:unique.size,unresolved});expect(unresolved).toEqual([]);
 expect({total,contracts,attachments,provenance,affected,distinct:unique.size}).toEqual({total:1516,contracts:1412,attachments:102,provenance:2,affected:92,distinct:104});
},30000);
test('requirement-only presentation keeps all underlying text and original offsets intact',()=>{
 const source=fixture.rows.flatMap(r=>r.value.sources).find(s=>s.history?.kind==='setup-fleet');const range=requirementRange(source);expect(range.start).toBeGreaterThan(0);expect(range.end).toBeLessThan(source.body.length);const shown=source.body.slice(range.start,range.end);expect(shown).not.toContain('Current user-directed task:');expect(shown).not.toContain('Receiving allocation (');expect(shown.trim().length).toBeGreaterThan(20);
 const tokens=localReferences('Begin file:///Users/test/contracts/a.md. Then /home/test/original.yaml, C:\\Users\\tester\\note.md and sandbox:/mnt/data/example.txt.');expect(tokens).toHaveLength(4);expect(tokens.every(t=>!/[.,]$/.test(t.raw))).toBe(true);
 const plain={body:'Exact café 🧭 日本語 text',sha256:'test'};expect(materialSegments(plain)).toEqual([{kind:'text',start:0,end:plain.body.length,text:plain.body}]);
});
test('missing, repeated, altered and invalid contract parts cannot produce verified success',async()=>{
 const ref={kind:'contract',digest:fixture.packets[1].contractHash,key:CONTRACT_PREFIX+fixture.packets[1].contractHash.slice(0,32),label:'Shared contract'},original=await fixture.store.readLocal(ref.key);
 for(const mutate of [rows=>rows.slice(1),rows=>[...rows,rows[0]],rows=>{rows[1].value.sources[0].body+='changed';return rows;},rows=>{rows[0].value.sources[0].history.detailEnd++;return rows;},rows=>{rows[0].value.sources[0].history.detailSha256='f'.repeat(64);return rows;}])await expect(readContract(ref,{store:{readLocal:async()=>mutate(structuredClone(original))}})).rejects.toThrow();
 await expect(readContract({...ref,digest:'a'.repeat(64)},{store:fixture.store})).rejects.toThrow(/identity/);
 await expect(readContract(ref,{store:{readLocal:async()=>{throw Object.assign(Error('Current role revoked'),{status:403});}}})).rejects.toMatchObject({status:403});
 const abort=new AbortController();abort.abort();await expect(readContract(ref,{store:fixture.store,signal:abort.signal})).rejects.toThrow();
 expect((await readContract(ref,{store:{readLocal:async()=>[...original].reverse()}})).body).toBe(fixture.packets[1].shared);
});
test('embedded profile reads recheck the exact source and never use filesystem or network fallback',async()=>{
 const row=fixture.rows.find(r=>r.value.sources.some(s=>s.history?.kind==='partner-mechanism')),source=row.value.sources.find(s=>localReferences(s.body).some(t=>t.name.endsWith('.yaml'))),token=localReferences(source.body).find(t=>t.name.endsWith('.yaml'));
 const ref={...referenceDescriptor(source,token),recordKey:row.key,sourceId:source.id,sourceSha256:source.sha256};
 await expect(readMaterial({...ref,sourceSha256:'0'.repeat(64)},{store:fixture.store})).rejects.toThrow(/changed/);
 await expect(readMaterial({...ref,sourceId:'missing'},{store:fixture.store})).rejects.toThrow(/identity/);
 await expect(readMaterial({...ref,locator:'/Users/test/secrets.txt'},{store:fixture.store})).rejects.toThrow(/no attached/);
 await expect(readMaterial(ref,{store:{readVersion:async()=>null}})).rejects.toThrow(/unavailable/);
 const altered=structuredClone(row);altered.value.sources.find(s=>s.id===source.id).body='incomplete';await expect(readMaterial(ref,{store:{readVersion:async()=>altered}})).rejects.toThrow(/changed/);
});
test('the release patch changes only the declared reader seams and preserves all other package files',async()=>{
 const root=join(homedir(),'.bonfyre/workspace-packages'),candidate=await materializeReferenceRepair({sourceRelease:baseRelease,root}),before=await loadWorkspaceFiles(root,baseRelease),after=await loadWorkspaceFiles(root,candidate.id);
 expect([...before].filter(([path,body])=>!body.equals(after.get(path))).map(([path])=>path).sort()).toEqual(['SOURCE.tsv','bridge/interaction-collection.mjs','instruments/app.mjs','instruments/research-view.mjs']);
 expect([...after.keys()].filter(k=>!before.has(k)).sort()).toEqual(['bridge/retained-material-view.mjs','bridge/retained-material.css','bridge/retained-material.mjs']);
 expect(after.get('instruments/research-view.mjs').toString()).toContain('material.selection(box,current(),selection)');expect(after.get('instruments/app.mjs').toString()).toContain('sourceRecordKey:()=>record?.key');
 console.log('REFERENCE_REPAIR_CANDIDATE',candidate.id);
},30000);

test('library originals require the saved research reference, current access and exact UTF-8 bytes',async()=>{const body='\ufeffOriginal café 🧭 <script>inert</script>',hash=await textDigest(body),material={binding:'ws-'+'1'.repeat(40),key:'source-material:'+'2'.repeat(64),revision:3,sha256:hash,filename:'original.txt'},source={id:'material-'+ '2'.repeat(64),title:'Original',body,sha256:hash,material},parent={key:'draft:instruments:research:fixture',revision:2,value:{kind:'research',sources:[source]}},row={key:material.key,revision:5,value:{schema:'bonfyre.source-material.v1',kind:'file',state:'archived',id:'2'.repeat(64),contentSHA256:hash,original:new Blob([body])}},ref={kind:'material',material,recordKey:parent.key,sourceId:source.id,sourceSha256:hash,label:'Original'};let reads=0;const options={store:{readVersion:async key=>key===parent.key?structuredClone(parent):null},libraryStore:binding=>{expect(binding).toBe(material.binding);return{readVersion:async key=>{reads++;expect(key).toBe(material.key);return structuredClone(row);}}}};const result=await readMaterial(ref,options);expect(result.body).toBe(body);expect(result.material).toEqual({revision:3,currentRevision:5,state:'archived'});expect(result.parts[0].href).toBe('/apps/'+material.binding+'/sources/?record='+encodeURIComponent(material.key));for(const patch of [{binding:'https://foreign.example'},{key:'draft:instruments:research:other'},{sha256:'f'.repeat(64)},{revision:4},{filename:'changed.txt'}]){const before=reads;await expect(readMaterial({...ref,material:{...material,...patch}},options)).rejects.toThrow();expect(reads).toBe(before);}for(const mutate of [r=>{r.deleted=true;},r=>{r.revision=2;},r=>{r.value.contentSHA256='a'.repeat(64);},r=>{r.value.original=new Blob(['Changed']);},r=>{r.value.original=new Blob([new Uint8Array(2*1024*1024+1)]);}]){const changed=structuredClone(row);mutate(changed);await expect(readMaterial(ref,{...options,libraryStore:()=>({readVersion:async()=>changed})})).rejects.toThrow();}await expect(readMaterial(ref,{...options,libraryStore:()=>({readVersion:async()=>{throw Object.assign(Error('Access revoked'),{status:403});}})})).rejects.toMatchObject({status:403});const control=new AbortController();control.abort();await expect(readMaterial(ref,{...options,signal:control.signal})).rejects.toThrow();parent.value.sources[0].body+='changed';await expect(readMaterial(ref,options)).rejects.toThrow(/changed/);});
