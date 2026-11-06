import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {workspaceFixture} from './workspace_fixture.mjs';
import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../../../scripts/materialize_authored_workspace.mjs';
import {publishSetups} from '../../../scripts/publish_experience_setups.mjs';
import {digest,prepareRecord,blobLimit,blobChunk} from '../../../integrations/browser/workspace-value.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
let temp,f,binding,personal,bytes,hash;
const prior={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
beforeAll(async()=>{
  temp=mkdtempSync(join(tmpdir(),'native-typed-bytes-'));const pack=await materializeAuthoredWorkspace({root:join(temp,'packages')});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  f=await workspaceFixture();publishSetups({records:authoredWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});binding=await f.install('experience.connected-instruments.workspace','institution:workspace-team');personal=await f.install('experience.connected-instruments.workspace','self');
  bytes=new Uint8Array(16*1024*1024);for(let n=0;n<bytes.length;n++)bytes[n]=n%251;hash=await digest(bytes);
},60000);
afterAll(async()=>{await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});for(const [key,value]of [['BONFYRE_WORKSPACE_RELEASE',prior.release],['BONFYRE_WORKSPACE_PACKAGES',prior.packages]])if(value===undefined)delete process.env[key];else process.env[key]=value;});
const key=name=>'draft:instruments:schema:'+name;
async function rawWrite(to,profile){return f.raw(to.endpoint+'?operation=write',f.owner,{method:'POST',headers:{'Content-Type':'application/msgpack'},body:encode([1,crypto.randomUUID(),[[key(crypto.randomUUID()),0,0,encode(profile)]]])});}
test('native CMS retains and restores a 16 MiB Uint8Array through the existing chunked blob owner',async()=>{
  const row=await f.store(binding,f.writer).saveLocal(key('large-database'),{kind:'schema',title:'Large typed database',bytes,selected:null,positions:{},acceptedSQL:[]},0);expect(row.revision).toBe(1);expect(row.value.bytes).toBeInstanceOf(Uint8Array);expect(await digest(row.value.bytes)).toBe(hash);
  const reopened=await f.store(binding,f.reader).readVersion(row.key);expect(reopened.value.bytes).toBeInstanceOf(Uint8Array);expect(reopened.value.bytes.byteLength).toBe(bytes.length);expect(await digest(reopened.value.bytes)).toBe(hash);
  const kept=f.sql.query('SELECT bytes,complete FROM _workspace_blob WHERE digest=?').get(hash);expect(kept).toEqual({bytes:bytes.length,complete:1});expect(f.sql.query('SELECT MAX(length(body)) n FROM _workspace_body').get().n).toBeLessThan(1024);
  const listing=await f.raw('/apps/'+binding.id+'/_work');expect(listing.status).toBe(200);expect(await listing.text()).toContain('Large typed database');
},90000);
test('the same bytes deduplicate within a destination while another destination cannot borrow their reference',async()=>{
  const before=f.sql.query('SELECT count(*) n FROM _workspace_blob').get().n;await f.store(binding).saveLocal(key('same-body'),{kind:'schema',title:'Same binary body',bytes},0);expect(f.sql.query('SELECT count(*) n FROM _workspace_blob').get().n).toBe(before);
  const profile=[4,{kind:'schema',bytes:null},[[['bytes'],hash,bytes.length,'',null,null,2]]];expect((await rawWrite(personal,profile)).status).toBe(400);expect((await f.store(personal).readLocal()).length).toBe(0);
  const foreign=await f.raw(personal.endpoint+'?operation=blob-get&prefix='+hash+'&cursor=0');expect(foreign.status).toBe(409);
  await expect(f.store(binding,f.reader).saveLocal(key('reader-write'),{kind:'schema',bytes},0)).rejects.toThrow();
},60000);
test('native value validation rejects forged profiles, binary metadata and missing or incomplete asset bodies',async()=>{
  const ref=[['bytes'],hash,bytes.length,'',null,null,2],plain={kind:'schema',bytes:null};
  const invalid=[[3,plain,[ref]],[2,plain,[ref]],[5,plain,[ref]],...[3,-1].map(kind=>[4,plain,[[...ref.slice(0,6),kind]]]),...[['application/octet-stream',null,null],['','fake.sqlite',1],['',null,1]].map(([mime,name,modified])=>[4,plain,[[ref[0],hash,bytes.length,mime,name,modified,2]]]),[4,plain,[[ref[0],'0'.repeat(64),bytes.length,'',null,null,2]]],[4,plain,[[ref[0],hash,blobLimit+1,'',null,null,2]]]];
  for(const profile of invalid)expect((await rawWrite(binding,profile)).status).toBe(400);
  const incomplete=new Uint8Array(blobChunk+1).fill(17),incompleteHash=await digest(incomplete),response=await f.raw(binding.endpoint+'?operation=blob-put',f.owner,{method:'POST',headers:{'Content-Type':'application/msgpack'},body:encode([1,incompleteHash,incomplete.length,0,incomplete.slice(0,blobChunk)])});expect(response.status).toBe(200);expect(decode(new Uint8Array(await response.arrayBuffer()))[4]).toBe(false);
  expect((await rawWrite(binding,[4,plain,[[ref[0],incompleteHash,incomplete.length,'',null,null,2]]])).status).toBe(400);
},60000);
test('the upgraded owner still reads and writes v2 inline bytes and v3 ArrayBuffers without changing types',async()=>{
  for(const value of [new Uint8Array([7,8,9]),new Uint8Array([7,8,9]).buffer]){
    const profile=decode(await prepareRecord({bytes:value}).encodeWith(async blob=>digest(await blob.arrayBuffer())));expect(profile[0]).toBe(value instanceof ArrayBuffer?3:2);
    const row=await f.store(binding).saveLocal(key(crypto.randomUUID()),{kind:'schema',title:'Existing value profile',bytes:value},0);expect(row.value.bytes instanceof ArrayBuffer).toBe(value instanceof ArrayBuffer);expect([...new Uint8Array(row.value.bytes)]).toEqual([7,8,9]);
  }
});
