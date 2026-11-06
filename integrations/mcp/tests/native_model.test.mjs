import {afterEach,expect,test} from 'bun:test';
import {chmodSync,mkdtempSync,rmSync,symlinkSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {createNativeModelOwner,readNativeModelBindings,nativeModelFromEnvironment} from '../native_model.mjs';
import {encode} from '../../browser/workspace-msgpack.mjs';
import {readLlamaServerPacket} from '../../estate/llama_generation.mjs';

const roots=[];afterEach(()=>{for(const root of roots.splice(0))rmSync(root,{recursive:true,force:true});});
const digest=value=>createHash('sha256').update(value).digest('hex');
const status={schema:'bonfyre.moe.residency.v2',resident:true,pid:99,grant_id:4,model:'/private/models/small.gguf',
  candidates:[{model:'small.gguf',usable:true,min_bytes:1048576}],choices:1};
function fixture(overrides={}) {
  const calls=[];let residence={...status},lease={lease:7,model_path:status.model,preferred:true,context:4096},reply='Native response';
  const fetchImpl=async(url,options={})=>{
    calls.push({url,options});const path=new URL(url).pathname;
    if(overrides.fetch)return overrides.fetch(url,options,calls);
    if(path==='/bonfyre/residency')return Response.json(residence);
    if(path==='/bonfyre/lease')return Response.json(new URL(url).searchParams.has('release')?{lease:7,released:true}:lease);
    if(path==='/v1/chat/completions')return Response.json({choices:[{message:{content:reply}}],usage:{prompt_tokens:10,completion_tokens:3,total_tokens:13}});
    throw Error('unexpected owner path');
  };
  const owner=createNativeModelOwner({fetchImpl,...overrides});
  return {owner,calls,setResidency:value=>{residence={...residence,...value};},setLease:value=>{lease={...lease,...value};},setReply:value=>{reply=value;},
    async generate(options={}){const model=await owner.inspect('moe:small.gguf');return owner.generate({model,prompt:'Local input',maxTokens:32,timeoutMs:10000},
      {beforeDispatch:async()=>{},onAttempt:async()=>{},...options});}};
}

test('inventory is read-only and catalogue joins require an explicit exact owner binding',async()=>{
  const f=fixture(),models=await f.owner.capabilities();expect(models).toHaveLength(1);expect(models[0].catalogId).toBeNull();
  expect(f.calls).toHaveLength(1);expect(f.calls[0].url).toEndWith('/bonfyre/residency');
  const exact=fixture({catalogBindings:{'small.gguf':'registered-small'}});expect((await exact.owner.capabilities())[0].catalogId).toBe('registered-small');
  expect(()=>createNativeModelOwner({catalogBindings:{'../small.gguf':'wrong'}})).toThrow();
});
test('native configuration cannot route account prompts off the fixed local owner',()=>{
  for(const serverUrl of ['https://127.0.0.1:7000','http://example.com:7000','http://user:secret@127.0.0.1:7000','http://127.0.0.1:7000/path','http://127.0.0.1:7000/?redirect=1'])
    expect(()=>createNativeModelOwner({serverUrl})).toThrow();
});
test('one exact leased model invokes the existing generation adapter and releases its lease',async()=>{
  const f=fixture();let authority=0,attempt;
  const value=await f.generate({beforeDispatch:async()=>{authority++;},onAttempt:async witness=>{attempt=witness;}});
  expect(value).toMatchObject({text:'Native response',digest:digest('Native response'),bytes:15,usage:{prompt_tokens:10,completion_tokens:3,total_tokens:13}});
  expect(attempt).toMatchObject({provider:'bonfyre-moe',checkpoint:'small.gguf',leaseId:'7',pid:99,grantId:'4'});expect(authority).toBe(2);
  const posts=f.calls.filter(x=>new URL(x.url).pathname==='/v1/chat/completions');expect(posts).toHaveLength(1);
  expect(JSON.parse(posts[0].options.body)).toMatchObject({model:'small.gguf',max_tokens:32,stream:false,temperature:0});
  expect(posts[0].options.redirect).toBe('error');expect(f.calls.at(-1).url).toEndWith('/bonfyre/lease?release=7');
});
test('owner fallback cannot silently substitute a different model or receive the prompt',async()=>{
  const f=fixture();f.setLease({model_path:'/private/models/other.gguf',preferred:false});
  await expect(f.generate()).rejects.toMatchObject({code:'admission',modelAttempted:false});
  expect(f.calls.filter(x=>new URL(x.url).pathname==='/v1/chat/completions')).toHaveLength(0);
  expect(f.calls.at(-1).url).toEndWith('/bonfyre/lease?release=7');
});
test('lease requires a live matching native memory grant before dispatch',async()=>{
  const f=fixture();f.setResidency({grant_id:0});await expect(f.generate()).rejects.toMatchObject({code:'unavailable',modelAttempted:false});
  expect(f.calls.filter(x=>new URL(x.url).pathname==='/v1/chat/completions')).toHaveLength(0);
});
test('native residency changes after a physical request preserve uncertainty and never retry',async()=>{
  let inference=false;
  const f=fixture({fetch:async(url,options)=>{
    const path=new URL(url).pathname;
    if(path==='/bonfyre/residency')return Response.json({...status,pid:inference?100:99});
    if(path==='/bonfyre/lease')return Response.json({lease:7,model_path:status.model,preferred:true,context:4096});
    inference=true;return Response.json({choices:[{message:{content:'A result with uncertain residency'}}]});
  }});
  await expect(f.generate()).rejects.toMatchObject({modelAttempted:true,reconciliationRequired:true});
  expect(f.calls.filter(x=>new URL(x.url).pathname==='/v1/chat/completions')).toHaveLength(1);
});
test('response bytes are bounded during the stream and an oversized body is cancelled',async()=>{
  let cancelled=false;const response=new Response(new ReadableStream({pull(controller){controller.enqueue(new Uint8Array(1024));},cancel(){cancelled=true;}}));
  await expect(readLlamaServerPacket(response,1500)).rejects.toThrow('byte bound');expect(cancelled).toBe(true);
  await expect(readLlamaServerPacket(new Response('x',{headers:{'content-length':'999999'}}),100)).rejects.toThrow('byte bound');
});
test('oversized retained model output becomes an uncertain attempt and releases the exact lease',async()=>{
  const f=fixture();f.setReply('x'.repeat(65537));await expect(f.generate()).rejects.toMatchObject({modelAttempted:true,reconciliationRequired:true});
  expect(f.calls.at(-1).url).toEndWith('/bonfyre/lease?release=7');
});
test('caller cancellation reaches the current native inference without an extra request',async()=>{
  const controller=new AbortController();let began;const started=new Promise(resolve=>{began=resolve;});
  const f=fixture({fetch:async(url,options)=>{
    const path=new URL(url).pathname;
    if(path==='/bonfyre/residency')return Response.json(status);
    if(path==='/bonfyre/lease')return Response.json({lease:7,model_path:status.model,preferred:true,context:4096});
    began();return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('Aborted','AbortError')),{once:true}));
  }});
  const work=f.generate({signal:controller.signal});await started;controller.abort();
  await expect(work).rejects.toMatchObject({modelAttempted:true,reconciliationRequired:true});
  expect(f.calls.filter(x=>new URL(x.url).pathname==='/v1/chat/completions')).toHaveLength(1);
});
test('native binding packet is bounded, exact, stable and never follows a symlink',async()=>{
  const root=mkdtempSync(join(tmpdir(),'native-model-bindings-'));roots.push(root);const file=join(root,'bindings.msgpack');
  writeFileSync(file,encode([1,{bindings:[{checkpoint:'small.gguf',catalogId:'catalog-small'}]}]),{mode:0o600});
  expect(readNativeModelBindings(file)).toEqual({'small.gguf':'catalog-small'});
  const alias=join(root,'alias.msgpack');symlinkSync(file,alias);expect(()=>readNativeModelBindings(alias)).toThrow();
  chmodSync(file,0o666);expect(()=>readNativeModelBindings(file)).toThrow();chmodSync(file,0o600);
  writeFileSync(file,encode([1,{bindings:[{checkpoint:'small.gguf',catalogId:'one'},{checkpoint:'small.gguf',catalogId:'two'}]}]));
  expect(()=>readNativeModelBindings(file)).toThrow();
  await expect(nativeModelFromEnvironment({BONFYRE_MODEL_BINDINGS_FILE:alias}).capabilities()).rejects.toMatchObject({code:'unavailable'});
});
