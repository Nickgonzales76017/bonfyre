import {test,expect} from 'bun:test';
import {publicationInput,publicationPacket,prepareResearchInputs,savePreparedResearch,researchDigest} from '../publication-research.mjs';
const input=(id='one',body='The exact retained passage.')=>publicationInput([id,'Publication '+id,body,'https://example.test/'+id,'feed','receipt:'+id,'Test excerpt','excerpt','2026-10-20']);
const sourceRecord=async value=>({...value,sha256:await researchDigest(value.body),coordinate:'UTF-16 offsets into exact original text'});
const prepare=(inputs=[input()],existing=null)=>prepareResearchInputs({inputs,title:'A review',existing,sourceRecord});
function memory(){let row=null,writes=0,lose=false;return {get writes(){return writes;},loseReply(){lose=true;},readVersion:async()=>structuredClone(row),saveLocal:async(key,value,expectedRevision)=>{if((row?.revision??0)!==expectedRevision)throw Error('Conflict');row={key,revision:expectedRevision+1,value:structuredClone(value)};writes++;if(lose){lose=false;throw Error('Lost response');}return structuredClone(row);},replace(value){row=value;}};}
test('preparation retains exact source text and provenance without authored findings',async()=>{
 const p=await prepare([input('one','A passage with 🧭 and\nnewlines.'),input('two')]);
 expect(p.change.value.sources).toHaveLength(2);expect(p.change.value.findings).toEqual([]);
 expect(p.change.value.sources[0].body).toBe('A passage with 🧭 and\nnewlines.');
 expect(p.change.value.sources[0].publication).toEqual({occurrence:'one',sourceRef:'feed',receipt:'receipt:one',rightsBasis:'Test excerpt',retention:'excerpt',publishedAt:'2026-10-20'});
 expect(p.change.value.selectedSource).toBe(p.added[0]);expect(p.change.expectedRevision).toBe(0);
});
test('source packets reject invalid identities, addresses and oversized pages',()=>{
 expect(publicationPacket([1,[],0])).toEqual({inputs:[],cursor:0});
 const row=['one','Title','Text','javascript:alert(1)','feed','receipt','terms','excerpt',''];
 expect(()=>publicationInput(row)).toThrow('address');row[3]='https://user:pass@example.test';expect(()=>publicationInput(row)).toThrow('address');row[3]='https://example.test';
 expect(()=>publicationPacket([1,[row,row],0])).toThrow('repeats');expect(()=>publicationPacket([1,Array(25).fill(row),0])).toThrow('incomplete');expect(()=>publicationPacket([1,[], -1])).toThrow('incomplete');
});
test('empty passages and repeated sources cannot become research evidence',async()=>{
 await expect(prepare([input('one','   ')])).rejects.toThrow('no retained excerpt');await expect(prepare([input(),input()])).rejects.toThrow('only once');
 await expect(prepare(Array.from({length:51},(_,i)=>input(String(i))))).rejects.toThrow('one to 50');
});
test('append preserves authored findings, title, tab and selections; matching input is idempotent',async()=>{
 const p=await prepare(),store=memory();let row=await savePreparedResearch(store,p);
 row.value.findings=[{id:'finding',statement:'An authored finding',evidence:[{source:row.value.sources[0].id}]}];row.value.tab='matrix';row.value.selectedFinding='finding';row.value.title='My authored title';store.replace(row);
 const addition=await prepare([input(),input('two')],row);expect(addition.added).toHaveLength(1);expect(addition.unchanged).toHaveLength(1);
 expect(addition.change.value.findings).toEqual(row.value.findings);expect(addition.change.value.title).toBe('My authored title');expect(addition.change.value.tab).toBe('matrix');expect(addition.change.value.selectedSource).toBe(row.value.selectedSource);
 row=await savePreparedResearch(store,addition);expect(row.revision).toBe(2);await savePreparedResearch(store,await prepare([input('two')],row));expect(store.writes).toBe(2);
});
test('changed source content, receipts and damaged receiving sources require explicit correction',async()=>{
 const store=memory(),row=await savePreparedResearch(store,await prepare());
 await expect(prepare([input('one','Corrected text')],row)).rejects.toThrow('changed excerpt');
 await expect(prepare([{...input(),receipt:'another-receipt'}],row)).rejects.toThrow('changed excerpt');
 row.value.sources[0].body='Damaged';await expect(prepare([input('two')],row)).rejects.toThrow('digest');
});
test('lost acknowledgement reconciles the exact saved record without a second write',async()=>{
 const store=memory(),p=await prepare();store.loseReply();const row=await savePreparedResearch(store,p);expect(row.revision).toBe(1);expect(store.writes).toBe(1);expect(await savePreparedResearch(store,p)).toEqual(row);expect(store.writes).toBe(1);
});
test('reviewed preimages, tombstones and preview fingerprints prevent accidental overwrite',async()=>{
 const store=memory(),p=await prepare();p.change.value.title='Tampered';await expect(savePreparedResearch(store,p)).rejects.toThrow('prepared review changed');expect(store.writes).toBe(0);
 const row=await savePreparedResearch(store,await prepare()),next=await prepare([input('two')],row);
 store.replace({...row,revision:2,value:{...row.value,title:'Another author'}});await expect(savePreparedResearch(store,next)).rejects.toThrow('receiving review changed');
 store.replace({...row,value:{...row.value,title:'Changed without revision'}});await expect(savePreparedResearch(store,next)).rejects.toThrow('receiving review changed');
 store.replace({...row,deleted:true,value:null});await expect(savePreparedResearch(store,next)).rejects.toThrow('receiving review changed');expect(store.writes).toBe(1);
});

// The consumer receives nothing until every exact selected input was re-read.
import {verifyPublicationSelection} from '../publication-picker.mjs';
const endpoint='/apps/ws-'+ 'a'.repeat(40)+'/_records';
const selection=(items=[input()],page={query:'',source:'',cursor:0,snapshot:''})=>items.map(input=>({input,page}));
test('adoption reads one current page for multiple excerpts and returns independent snapshots',async()=>{
 const items=[input(),input('two')],picked=selection(items);let reads=0;
 const current=await verifyPublicationSelection(endpoint,picked,{readPage:async()=>{reads++;return{inputs:items,cursor:0};}});
 expect(reads).toBe(1);expect(current).toEqual(items);current[0].body='Editor change';expect(picked[0].input.body).toBe('The exact retained passage.');
});
test('changed text, citation, terms, receipt, identity or source subject prevents adoption',async()=>{
 for(const field of ['body','title','url','rightsBasis','retention','receipt','publishedAt','sourceSubject','sourceRef','id']){
  const changed={...input(),[field]:'Changed'};
  await expect(verifyPublicationSelection(endpoint,selection(),{readPage:async()=>({inputs:[changed],cursor:0})})).rejects.toThrow('changed or is no longer available');
 }
 await expect(verifyPublicationSelection(endpoint,selection(),{readPage:async()=>({inputs:[],cursor:0})})).rejects.toThrow('no longer available');
});
test('retained adoption pins the reviewed page generation and refuses another snapshot',async()=>{
 const page={source:'newsdesk',query:'needle',cursor:24,snapshot:'a'.repeat(64)},calls=[];
 expect(await verifyPublicationSelection(endpoint,selection([input()],page),{readPage:async(endpoint,options)=>{calls.push(options);return {inputs:[input()],cursor:0,snapshot:page.snapshot};}})).toEqual([input()]);
 expect(calls[0]).toMatchObject(page);
 await expect(verifyPublicationSelection(endpoint,selection([input()],page),{readPage:async()=>({inputs:[input()],cursor:0,snapshot:'b'.repeat(64)})})).rejects.toThrow('changed');
});
test('failed, cancelled or partially verified selections retain their review and cannot be adopted',async()=>{
 const picked=selection(),before=structuredClone(picked);
 await expect(verifyPublicationSelection(endpoint,picked,{readPage:async()=>{throw Error('Owner unavailable');}})).rejects.toThrow('Owner unavailable');expect(picked).toEqual(before);
 const controller=new AbortController();await expect(verifyPublicationSelection(endpoint,picked,{signal:controller.signal,readPage:async()=>{controller.abort();return{inputs:[input()],cursor:0};}})).rejects.toThrow();expect(picked).toEqual(before);
 const second={input:input('two'),page:{query:'second'}};
 await expect(verifyPublicationSelection(endpoint,[...picked,second],{readPage:async(endpoint,{query})=>({inputs:query?[]:[input()],cursor:0})})).rejects.toThrow('no longer available');
 await expect(verifyPublicationSelection(endpoint,Array(51).fill(picked[0]))).rejects.toThrow('one to 50');
});

import {inspectPublicationInput} from '../publication-picker.mjs';
test('saved source inspection finds the exact identity across pages and separates changed from unavailable',async()=>{
 const original=input();let reads=0;
 expect((await inspectPublicationInput(endpoint,original,{readPage:async(endpoint,{cursor})=>{reads++;return cursor?{inputs:[original],cursor:0}:{inputs:[input('other')],cursor:24};}})).state).toBe('current');expect(reads).toBe(2);
 expect((await inspectPublicationInput(endpoint,original,{readPage:async()=>({inputs:[{...original,receipt:'corrected-receipt'}],cursor:0})})).state).toBe('changed');
 expect((await inspectPublicationInput(endpoint,original,{readPage:async()=>({inputs:[],cursor:0})})).state).toBe('unavailable');
});
test('saved retained-source checks pin one generation and never call failure or a truncated search withdrawal',async()=>{
 const original={...input(),receipt:'public-material:feed/one;body-sha256:'+ 'c'.repeat(64)},calls=[];
 await expect(inspectPublicationInput(endpoint,original,{readPage:async(endpoint,options)=>{calls.push(options);return {inputs:[],cursor:24,snapshot:options.cursor?'b'.repeat(64):'a'.repeat(64)};}})).rejects.toThrow('changed during');
 expect(calls[0].source).toBe('feed');expect(calls[1].snapshot).toBe('a'.repeat(64));
 await expect(inspectPublicationInput(endpoint,input(),{readPage:async()=>{throw Error('Owner refused');}})).rejects.toThrow('Owner refused');
 let reads=0;await expect(inspectPublicationInput(endpoint,input(),{readPage:async()=>({inputs:[],cursor:++reads})})).rejects.toThrow('bounded read');expect(reads).toBe(16);
 await expect(inspectPublicationInput(endpoint,input(),{readPage:async()=>({inputs:[],cursor:4})})).rejects.toThrow('repeated a page');
});
test('cancelled source checks do not report current source state',async()=>{
 const abort=new AbortController();await expect(inspectPublicationInput(endpoint,input(),{signal:abort.signal,readPage:async()=>{abort.abort();return {inputs:[input()],cursor:0};}})).rejects.toThrow();
});
