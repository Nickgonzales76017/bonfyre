import {test,expect} from 'bun:test';
import {createAuthoredWorkspace,addInstrument,connectInstruments,prepareAuthoredBatch,validateAuthoredReceipt,validateAuthoredWorkspace} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
function fixture(){
  let production=addInstrument(createAuthoredWorkspace('Evidence into planning'),'research');
  production=addInstrument(production,'product');production=connectInstruments(production,production.panels[0].id,production.panels[1].id);
  const records=production.panels.map((p,i)=>({id:p.id,kind:p.kind,key:p.recordKey,revision:i+2}));
  const change={key:records[0].key,expectedRevision:records[0].revision,value:{kind:'research',title:'Reviewed source',sources:[],findings:[]}};
  return {production,records,change,prepare:(changes=[change],rows=records)=>prepareAuthoredBatch(production,4,rows,changes)};
}
function receipt(prepared){return prepared.changes.map(c=>({key:c.key,revision:c.expectedRevision+(c.checkOnly?0:1),value:c.checkOnly?{kind:prepared.production.panels.find(p=>p.recordKey===c.key).kind}:structuredClone(c.value)}));}
test('a production cannot delete a bound source or overwrite unbound work',()=>{
  const f=fixture();for(const mode of [{remove:true},{remove:false},{checkOnly:true},{checkOnly:false}])expect(()=>f.prepare([{...f.change,...mode}])).toThrow();
  for(const key of ['draft:instruments:product:other','draft:instruments:research:other','draft:catalogue:other','draft:instruments:composition:other'])expect(()=>f.prepare([{...f.change,key}])).toThrow();
  expect(()=>f.prepare([{...f.change,value:{kind:'product'}}])).toThrow();
});
test('each panel has exactly one current record with its reviewed identity',()=>{
  const f=fixture();for(const rows of [f.records.slice(0,1),[...f.records,f.records[0]],[f.records[0],f.records[0]],[f.records[0],{...f.records[1],key:f.records[0].key}],[{...f.records[0],key:'draft:instruments:research:substitution'},f.records[1]],[{...f.records[0],kind:'product'},f.records[1]]])expect(()=>f.prepare([],rows)).toThrow();
  const malformed=structuredClone(f.production);malformed.panels[0].recordKey='draft:instruments:research:';expect(()=>validateAuthoredWorkspace(malformed)).toThrow();
});
test('the WorkingRecord handoff packet retains the written source and new incoming record together',()=>{
  const f=fixture(),candidate={id:'opportunity-1',title:'Reviewed interpretation',evidence:[{sourceId:'s1',quote:'Exact source words',start:0,end:18}]};
  const incoming={key:'draft:instruments:incoming:'+candidate.id,expectedRevision:0,value:{candidate,sourceRecord:f.change.key,sourceRevision:f.change.expectedRevision+1}};
  const batch=f.prepare([f.change,incoming]);
  expect(batch.changes).toHaveLength(4);expect(batch.production.panels.map(p=>p.revision)).toEqual([3,3]);
  expect(batch.changes.find(c=>c.key===f.records[1].key)).toEqual({key:f.records[1].key,expectedRevision:3,checkOnly:true});
  expect(validateAuthoredReceipt(batch,receipt(batch)).value.title).toBe('Evidence into planning');
  candidate.title='Later interpretation';f.change.value.title='Later source';expect(batch.changes[1].value.candidate.title).toBe('Reviewed interpretation');expect(batch.changes[0].value.title).toBe('Reviewed source');
});
test('an incoming opportunity cannot replace existing work or claim an unwritten or unrelated source',()=>{
  const f=fixture(),incoming={key:'draft:instruments:incoming:candidate',expectedRevision:0,value:{candidate:{id:'candidate'},sourceRecord:f.change.key,sourceRevision:3}};
  for(const edit of [c=>c.expectedRevision=1,c=>c.key+='-other',c=>c.value.sourceRevision=2,c=>c.value.sourceRecord=f.records[1].key,c=>c.value.sourceRecord='draft:instruments:research:other',c=>c.value.extra='unsupported']){const bad=structuredClone(incoming);edit(bad);expect(()=>f.prepare([f.change,bad])).toThrow();}
  expect(()=>f.prepare([incoming])).toThrow();
  const unconnected=structuredClone(f.production);unconnected.connections=[];expect(()=>prepareAuthoredBatch(unconnected,4,f.records,[f.change,incoming])).toThrow();
});
test('native revision bounds are enforced before a save can be submitted',()=>{
  const f=fixture();for(const revision of [-1,2147483646,2147483647,Number.MAX_SAFE_INTEGER])expect(()=>prepareAuthoredBatch(f.production,revision,f.records,[f.change])).toThrow();
  const records=structuredClone(f.records);records[0].revision=2147483646;expect(()=>f.prepare([{...f.change,expectedRevision:2147483646}],records)).toThrow();
  expect(f.prepare([],records).production.panels[0].revision).toBe(2147483646);
});
test('receipt coordinates cannot hide changed content, deleted sources or an incomplete production',()=>{
  const f=fixture(),batch=f.prepare(),rows=receipt(batch);expect(validateAuthoredReceipt(batch,rows).revision).toBe(5);
  for(const mutate of [r=>r.pop(),r=>r[0].revision++,r=>r[0].value.title='Different saved source',r=>r.at(-1).value.panels[0].recordKey='draft:instruments:research:other',r=>r[1].value.kind='research',r=>r[1].deleted=true,r=>r[1]=r[0]]){const wrong=structuredClone(rows);mutate(wrong);expect(()=>validateAuthoredReceipt(batch,wrong)).toThrow();}
});
test('receipt verification compares SQLite material bytes without treating different bytes as a saved draft',()=>{
  const production=addInstrument(createAuthoredWorkspace('Database rehearsal'),'schema'),p=production.panels[0];
  for(const bytes of [new Uint8Array([83,81,76]),new Uint8Array([83,81,76]).buffer]){
    const batch=prepareAuthoredBatch(production,0,[{id:p.id,kind:p.kind,key:p.recordKey,revision:0}],[{key:p.recordKey,expectedRevision:0,value:{kind:'schema',bytes}}]);
    const rows=receipt(batch);expect(validateAuthoredReceipt(batch,rows).revision).toBe(1);
    (rows[0].value.bytes instanceof ArrayBuffer?new Uint8Array(rows[0].value.bytes):rows[0].value.bytes)[1]=0;expect(()=>validateAuthoredReceipt(batch,rows)).toThrow();
  }
});
