// Shared save transport against actual isolated Auth, CMS and WebAddress owners.
// The tiny editor package is a fixture; these are not full product or deployment tests.
import {test, expect, beforeAll, afterAll} from 'bun:test';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {workspaceFixture} from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {retainWorkspaceFiles} from '../../../scripts/materialize_workspace_package.mjs';
import {openDestinationStore} from '../workspace-store.mjs';
let temp,f,binding;
const names=['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'];
const previous=Object.fromEntries(names.map(key=>[key,process.env[key]]));
beforeAll(async()=>{
  temp=mkdtempSync(join(tmpdir(),'workspace-save-native-'));const root=join(temp,'packages');
  const html=Buffer.from('<!doctype html><html lang="en"><head><title>Isolated transport fixture</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"></head><body><p>Native transport test fixture, not a production application.</p></body></html>');
  const release=await retainWorkspaceFiles(new Map([['catalogue/index.html',html],['product-room/index.html',html]]),{root});
  process.env.BONFYRE_WORKSPACE_RELEASE=release.id;process.env.BONFYRE_WORKSPACE_PACKAGES=root;
  f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');
},60000);
afterAll(async()=>{
  await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});
  for(const key of names)if(previous[key]===undefined)delete process.env[key];else process.env[key]=previous[key];
});

test('the real owner receives the reviewed nested data and original binary inputs',async()=>{
  const store=f.store(binding),audio=new Uint8Array([10,11,12]).buffer;
  const value={name:'Snapshot',items:[{quantity:2}],bytes:Buffer.from([4,5]),audio};
  const pending=store.saveLocal('draft:catalogue:snapshot',value,0);
  value.items[0].quantity=9;value.bytes[0]=99;new Uint8Array(audio)[0]=88;
  const written=await pending,other=await f.store(binding,f.writer).readVersion('draft:catalogue:snapshot');
  expect(written.revision).toBe(1);expect(other.value.items[0].quantity).toBe(2);
  expect([...other.value.bytes]).toEqual([4,5]);expect([...new Uint8Array(other.value.audio)]).toEqual([10,11,12]);
});

test('a native check-only dependency stays unchanged after its caller mutates the descriptor',async()=>{
  const store=f.store(binding),check={key:'draft:catalogue:snapshot',expectedRevision:1,checkOnly:true};
  const pending=store.saveLocalBatch([check,{key:'review:catalogue:snapshot',expectedRevision:0,value:{name:'Reviewed result'}}]);
  check.checkOnly=false;check.value={name:'Unreviewed source replacement'};
  await pending;
  const source=await f.store(binding,f.writer).readVersion(check.key);
  expect(source.revision).toBe(1);expect(source.value.name).toBe('Snapshot');
  expect((await store.readVersion('review:catalogue:snapshot')).value.name).toBe('Reviewed result');
});

test('native reader and unrelated-account writes remain refused',async()=>{
  await expect(f.store(binding,f.reader).saveLocal('draft:catalogue:snapshot',{name:'reader'},1)).rejects.toMatchObject({status:403});
  await expect(f.store(binding,f.other).saveLocal('draft:catalogue:snapshot',{name:'other'},1)).rejects.toMatchObject({status:403});
  expect((await f.store(binding).readVersion('draft:catalogue:snapshot')).revision).toBe(1);
});

test('a competing native revision cannot be overwritten by a stale reviewed save',async()=>{
  const key='draft:catalogue:conflict',store=f.store(binding);
  await store.saveLocal(key,{name:'one'},0);await f.store(binding,f.writer).saveLocal(key,{name:'two'},1);
  await expect(store.saveLocal(key,{name:'stale'},1)).rejects.toMatchObject({status:409});
  expect((await store.readVersion(key)).value.name).toBe('two');
});

test('native commit with a lost reply is reconciled and replayed without a second revision',async()=>{
  let lost=false;const key='draft:catalogue:lost-reply',value={name:'One accepted write'};
  const store=openDestinationStore(binding.endpoint,{origin:f.base,request:async(url,options)=>{
    const response=await f.raw(url,f.owner,options);
    if(new URL(url).searchParams.get('operation')==='write'&&response.ok&&!lost){lost=true;await response.arrayBuffer();throw Error('Reply lost after commit');}
    return response;
  }});
  await expect(store.saveLocal(key,value,0)).rejects.toThrow('Reply lost');
  expect((await f.store(binding,f.writer).readVersion(key)).revision).toBe(1);
  expect((await store.saveLocal(key,value,0)).revision).toBe(1);
  expect((await f.store(binding,f.writer).readVersion(key)).revision).toBe(1);
});

test('native deletion retains its tombstone and an unchanged second delete is a no-op',async()=>{
  const key='draft:catalogue:removed',store=f.store(binding);
  await store.saveLocal(key,{name:'temporary fixture'},0);await store.deleteLocal(key);
  expect(await store.readVersion(key)).toMatchObject({revision:2,deleted:true});
  await store.deleteLocal(key);expect(await store.readVersion(key)).toMatchObject({revision:2,deleted:true});
});
