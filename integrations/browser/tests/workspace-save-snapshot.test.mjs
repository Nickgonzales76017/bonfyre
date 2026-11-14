// Test the existing MessagePack carrier's save boundary, not a replacement store.
// The fake HTTP owner below records exact requests; native-owner tests run separately.
import {test, expect} from 'bun:test';
import {deferred} from './browser-fixture.mjs';
import {openDestinationStore} from '../workspace-store.mjs';
import {encode, decode} from '../workspace-msgpack.mjs';
import {prepareRecord, restoreRecord} from '../workspace-value.mjs';

const endpoint='/apps/ws-'+ 'a'.repeat(40)+'/_records';
function fixture({firstGate=null,loseFirstReply=false}={}){
  const calls=[],records=new Map(),receipts=new Map(); let writes=0,epoch=0;
  const request=async(address,options)=>{
    const operation=new URL(address).searchParams.get('operation');
    if(operation==='get'){
      const key=new URL(address).searchParams.get('prefix'),row=records.get(key);
      return new Response(encode([1,epoch,false,'',row?[[key,row.revision,row.body]]:[]]),{headers:{'Content-Type':'application/msgpack'}});
    }
    if(operation!=='write')throw Error('Unexpected operation '+operation);
    const packet=decode(options.body);calls.push(packet);writes++;
    if(firstGate&&writes===1)await firstGate.promise;
    const [,requestId,rows]=packet;
    if(receipts.has(requestId))return new Response(receipts.get(requestId),{headers:{'Content-Type':'application/msgpack'}});
    for(const [key,revision] of rows)if((records.get(key)?.revision??0)!==revision)return new Response('Stale revision',{status:409});
    const kept=rows.map(([key,revision,mode,body])=>{
      if(mode===1)return [key,revision,records.get(key)?.body??null];
      records.set(key,{revision:revision+1,body:mode===2?null:body});
      return [key,revision+1,mode===2?null:body];
    });
    epoch++;
    const answer=encode([1,epoch,false,'',kept]);receipts.set(requestId,answer);
    if(loseFirstReply&&writes===1)throw Error('Reply lost after native commit');
    return new Response(answer,{headers:{'Content-Type':'application/msgpack'}});
  };
  return {store:openDestinationStore(endpoint,{request,origin:'https://workspace.example.test'}),calls,records,
    async value(key){return restoreRecord(records.get(key).body,()=>{throw Error('No fixture assets');});},
    async seed(key,value,revision=1){records.set(key,{revision,body:await prepareRecord(value).encodeWith(()=>{throw Error('No fixture assets');})});}};
}

test('saveLocal captures reviewed nested values and bytes before queueing',async()=>{
  const f=fixture(),value={segments:[{text:'reviewed'}],bytes:new Uint8Array([1,2])};
  const pending=f.store.saveLocal('draft:one',value,0);
  value.segments[0].text='later edit';value.bytes[0]=9;
  await pending;
  expect((await f.value('draft:one')).segments[0].text).toBe('reviewed');
  expect([...((await f.value('draft:one')).bytes)]).toEqual([1,2]);
});

test('batch captures keys, revisions, modes and values from the reviewed call',async()=>{
  const f=fixture(),change={key:'draft:one',expectedRevision:0,value:{title:'reviewed'}};
  const pending=f.store.saveLocalBatch([change]);
  change.key='draft:other';change.expectedRevision=7;change.value.title='later edit';
  await pending;
  expect(f.records.has('draft:other')).toBe(false);
  expect((await f.value('draft:one')).title).toBe('reviewed');
});

test('a queued save retains its original input while another write is pending',async()=>{
  const gate=deferred(),f=fixture({firstGate:gate});
  const first=f.store.saveLocal('draft:first',{title:'first'},0),value={title:'second reviewed'};
  const second=f.store.saveLocal('draft:second',value,0);
  value.title='changed while waiting';gate.resolve();
  await Promise.all([first,second]);
  expect((await f.value('draft:second')).title).toBe('second reviewed');
  expect(f.calls.map(p=>p[2][0][0])).toEqual(['draft:first','draft:second']);
});

test('check-only dependencies cannot turn into writes after review',async()=>{
  const f=fixture();await f.seed('source:one',{title:'source'});
  const check={key:'source:one',expectedRevision:1,checkOnly:true};
  const pending=f.store.saveLocalBatch([check,{key:'draft:one',expectedRevision:0,value:{title:'draft'}}]);
  check.checkOnly=false;check.value={title:'unreviewed replacement'};
  await pending;
  expect(f.records.get('source:one').revision).toBe(1);
  expect((await f.value('source:one')).title).toBe('source');
});

test('invalid input is refused before any native write and does not poison the queue',async()=>{
  const f=fixture();const invalid=f.store.saveLocal('draft:bad',{fn:()=>{}},0);
  await expect(invalid).rejects.toThrow();expect(f.calls.length).toBe(0);
  await f.store.saveLocal('draft:good',{title:'good'},0);
  expect((await f.value('draft:good')).title).toBe('good');
});

test('explicit stale revisions are refused without an automatic alternate write',async()=>{
  const f=fixture();await f.seed('draft:one',{title:'existing'},2);
  await expect(f.store.saveLocal('draft:one',{title:'stale'},1)).rejects.toMatchObject({status:409});
  expect(f.calls.length).toBe(1);expect((await f.value('draft:one')).title).toBe('existing');
});

test('a retry after a lost commit reply retains the request identity and one revision',async()=>{
  const f=fixture({loseFirstReply:true}),value={title:'reviewed'};
  await expect(f.store.saveLocal('draft:one',value,0)).rejects.toThrow('Reply lost');
  const row=await f.store.saveLocal('draft:one',value,0);
  expect(row.revision).toBe(1);expect(f.calls[0][1]).toBe(f.calls[1][1]);
  expect(f.records.get('draft:one').revision).toBe(1);
});

test('Buffer inputs snapshot bytes rather than retain mutable slice views',async()=>{
  const f=fixture(),value={bytes:Buffer.from([4,5])};
  const pending=f.store.saveLocal('draft:buffer',value,0);
  value.bytes[0]=9;
  await pending;
  expect([...((await f.value('draft:buffer')).bytes)]).toEqual([4,5]);
});
