import {test,expect} from 'bun:test';
import {createProduction,productionDigest,productionStatus,previewProductionEdit,applyProductionEdit,
  reviewScripts,renderRequests,acceptTake,mixerSession,selectTake} from '../narration-production.mjs';
const source={href:'https://example.org/article',sha256:'a'.repeat(64)};
function production(){
  return createProduction({id:'edition-1',title:'Community edition',items:[
    {id:'article-1',title:'The library',source,segments:[{id:'library',text:'The library opens on Monday.'},{id:'hours',text:'Opening hours are nine to five.'}]}
  ],languages:[{id:'en',label:'English'},{id:'es',label:'Español'}]});
}
const wav=(frames=8000,rate=8000)=>{
  const bytes=new ArrayBuffer(44+frames*2),v=new DataView(bytes);
  const text=(at,s)=>[...s].forEach((c,i)=>v.setUint8(at+i,c.charCodeAt(0)));
  text(0,'RIFF');v.setUint32(4,bytes.byteLength-8,true);text(8,'WAVE');text(12,'fmt ');
  v.setUint32(16,16,true);v.setUint16(20,1,true);v.setUint16(22,1,true);v.setUint32(24,rate,true);
  v.setUint32(28,rate*2,true);v.setUint16(32,2,true);v.setUint16(34,16,true);text(36,'data');v.setUint32(40,frames*2,true);
  return bytes;
};
// The integration suite also uses the real WAV decoder; this isolates production arithmetic.
const decode=bytes=>({frames:new DataView(bytes).getUint32(40,true)/2,sample_rate:new DataView(bytes).getUint32(24,true)});
async function ready(p,ids=['library@en','hours@en']){
  p=await reviewScripts(p,ids);
  for(const request of await renderRequests(p,ids))p=await acceptTake(p,request,{id:'take-1',bytes:wav(),decode});
  return p;
}
test('language editions retain aligned source identities and never fabricate a translation',async()=>{
  const p=production(),rows=await productionStatus(p);
  expect(rows.map(r=>r.state)).toEqual(['needs-review','needs-script','needs-review','needs-script']);
  expect(p.cells.find(c=>c.id==='library@es').script).toBe('');
  await expect(renderRequests(p,['library@en'])).rejects.toThrow('Review');
  await expect(reviewScripts(p,['library@es'])).rejects.toThrow('empty');
});
test('default edits reveal the exact affected set while cell overrides and locks survive',async()=>{
  let p=production();
  p=await applyProductionEdit(p,await previewProductionEdit(p,{scope:'cells',ids:['library@en'],set:{rate:210}}));
  p=await applyProductionEdit(p,await previewProductionEdit(p,{scope:'cells',ids:['hours@en'],set:{locked:true}}));
  const preview=await previewProductionEdit(p,{scope:'defaults',set:{rate:150}});
  expect(preview.affected.filter(r=>r.regenerate).map(r=>r.id)).toEqual(['library@es','hours@es']);
  p=await applyProductionEdit(p,preview);
  expect((await productionStatus(p)).map(r=>r.settings.rate)).toEqual([210,150,180,150]);
  await expect(previewProductionEdit(p,{scope:'cells',ids:['hours@en'],set:{script:'changed'}})).rejects.toThrow('Unlock');
});
test('stale and altered batch previews cannot replace a newer production',async()=>{
  const p=production(),preview=await previewProductionEdit(p,{scope:'defaults',set:{rate:200}});
  const changed=await reviewScripts(p,['library@en']);
  await expect(applyProductionEdit(changed,preview)).rejects.toThrow('changed');
  const tampered=structuredClone(preview);tampered.edit.set.rate=250;
  await expect(applyProductionEdit(p,tampered)).rejects.toThrow('changed');
  expect(p.defaults.rate).toBe(180);
});
test('resetting an override restores the current inherited value',async()=>{
  let p=production();
  p=await applyProductionEdit(p,await previewProductionEdit(p,{scope:'cells',ids:['library@en'],set:{rate:220}}));
  p=await applyProductionEdit(p,await previewProductionEdit(p,{scope:'cells',ids:['library@en'],set:{},reset:['rate']}));
  expect((await productionStatus(p))[0].settings.rate).toBe(180);
  expect(p.cells[0].overrides).toEqual({});
});
test('mix changes reuse takes; script changes require review and selective regeneration',async()=>{
  let p=await ready(production());
  const preview=await previewProductionEdit(p,{scope:'cells',ids:['library@en'],set:{gain:.4,gapMs:750}});
  expect(preview.affected[0].regenerate).toBe(false);p=await applyProductionEdit(p,preview);
  expect((await renderRequests(p,['library@en']))[0].reusableTake).toBe('take-1');
  p=await applyProductionEdit(p,await previewProductionEdit(p,{scope:'cells',ids:['library@en'],set:{script:'The library opens on Tuesday.'}}));
  const rows=await productionStatus(p);
  expect(rows.find(r=>r.id==='library@en').state).toBe('needs-review');
  expect(rows.find(r=>r.id==='hours@en').state).toBe('ready');
  expect(p.cells[0].takes).toHaveLength(1);
  p=await reviewScripts(p,['library@en']);
  expect((await renderRequests(p,['library@en']))[0].reusableTake).toBeNull();
});
test('late takes cannot overwrite scripts edited while rendering',async()=>{
  let p=await reviewScripts(production(),['library@en']);
  const [request]=await renderRequests(p,['library@en']);
  p=await applyProductionEdit(p,await previewProductionEdit(p,{scope:'cells',ids:['library@en'],set:{script:'A different script.'}}));
  await expect(acceptTake(p,request,{id:'late',bytes:wav(),decode})).rejects.toThrow('changed');
  expect(p.cells[0].takes).toHaveLength(0);
});
test('real takes arrange sequentially with measured chapters and independent gap arithmetic',async()=>{
  const p=await ready(production()),session=await mixerSession(p,'en');
  expect(session.files).toHaveLength(2);
  expect(session.clips).toEqual([
    {at:0,from:0,frames:8000,gain:.8,muted:false},
    {at:10400,from:0,frames:8000,gain:.8,muted:false}
  ]);
  expect(session.measured.seconds).toBe(2.3);
  expect(session.production.chapters[1].source).toEqual(source);
  expect(session.permission).toBe('unreviewed');
  await expect(mixerSession(p,'en',{maxSeconds:2})).rejects.toThrow('duration');
});
test('partial editions require a deliberate choice and identify each omission',async()=>{
  const p=await ready(production(),['library@en']);
  await expect(mixerSession(p,'en')).rejects.toThrow('unfinished');
  const session=await mixerSession(p,'en',{allowPartial:true});
  expect(session.production.partial).toBe(true);
  expect(session.production.omitted).toEqual(['hours@en']);
  expect(session.files).toHaveLength(1);
});
test('retained audio is checked again before opening the mixer',async()=>{
  const p=await ready(production()),digest=await productionDigest(p);
  new Uint8Array(p.cells[0].takes[0].bytes)[50]=1;
  expect(await productionDigest(p)).not.toBe(digest);
  await expect(mixerSession(p,'en')).rejects.toThrow('digest');
});
test('alternate takes remain selectable and a new render cannot erase previous takes',async()=>{
  let p=await ready(production()),[request]=await renderRequests(p,['library@en']);
  p=await acceptTake(p,request,{id:'alternate',bytes:wav(16000),decode});
  expect((await mixerSession(p,'en')).production.chapters[0].frames).toBe(16000);
  p=selectTake(p,'library@en','take-1');
  expect((await mixerSession(p,'en')).production.chapters[0].frames).toBe(8000);
  expect(p.cells[0].takes).toHaveLength(2);
});
