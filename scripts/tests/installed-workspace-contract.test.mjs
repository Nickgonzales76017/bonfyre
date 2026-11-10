// The exact installed clients against isolated native Auth/CMS/WebAddress owners.
// This tests the storage contract, not each domain editor or a live account write.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,rmSync,mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {tmpdir,homedir} from 'node:os';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {workspaceFixture} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from '../workspace_material.mjs';
const fixture=JSON.parse(readFileSync(new URL('./fixtures/installed-workspace-contract.json',import.meta.url),'utf8'));
const workspaces=fixture.workspaces,hash=b=>createHash('sha256').update(b).digest('hex');
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(key=>[key,process.env[key]]));
let temp,f;const installed=new Map(),clients=new Map();
beforeAll(async()=>{
 expect(workspaces).toHaveLength(11);expect(new Set(workspaces.map(w=>w.family)).size).toBe(10);
 temp=mkdtempSync(join(tmpdir(),'installed-store-contract-'));const packages=join(temp,'packages');
 for(const release of new Set(workspaces.map(w=>w.release))){
  const files=await loadWorkspaceFiles(join(homedir(),'.bonfyre/workspace-packages'),release);
  expect((await retainWorkspaceMaterial(files,{root:packages})).id).toBe(release);
  // Import the retained store and its sibling module closure, not today's client.
  const selected=workspaces.find(w=>w.release===release),prefix=dirname(selected.store);
  expect(hash(files.get(selected.store))).toBe(selected.storeSha256);
  for(const[name,body]of files)if(dirname(name)===prefix&&name.endsWith('.mjs')){
   const path=join(temp,'clients',release,name);mkdirSync(dirname(path),{recursive:true});writeFileSync(path,body);
  }
  clients.set(release,(await import(pathToFileURL(join(temp,'clients',release,selected.store)).href)).openDestinationStore);
 }
 process.env.BONFYRE_WORKSPACE_RELEASE=workspaces[0].release;process.env.BONFYRE_WORKSPACE_PACKAGES=packages;f=await workspaceFixture();
 for(const w of workspaces){
  f.pack(w.pack,'Isolated contract: '+w.family);f.add('pack_demand',{pack:w.pack,demand_kind:'workspace',needs:w.family,preferred_sources:w.release,fields:w.entry,status:'published'});
  for(const fields of w.prefixes)f.add('pack_demand',{pack:w.pack,demand_kind:'workspace-record',needs:w.family,fields,status:'published'});
  installed.set(w.pack,await f.install(w.pack,'institution:workspace-team'));
 }
},90000);
afterAll(async()=>{try{await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});}finally{for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;}});
const store=(w,user=f.owner,request)=>{
 const binding=installed.get(w.pack);return clients.get(w.release)(binding.endpoint,{origin:f.base,request:request??((url,options)=>f.raw(url,user,options))});
};
const keyFor=(w,suffix)=>w.prefixes[0]+'contract-'+w.pack.split('.')[1]+'-'+suffix;
const value=w=>({title:'Isolated storage contract',family:w.family,nested:{count:1,labels:['reviewed','retained']},bytes:new Uint8Array([0,17,128,255]),attachment:new Blob(['Retained contract material for '+w.pack],{type:'text/plain'})});
for(const w of workspaces){
 test(w.pack+': create, fresh read, edit, stale refusal, tombstone and explicit restoration share one revision sequence',async()=>{
  const owner=store(w),stale=store(w,f.writer),key=keyFor(w,'lifecycle'),original=value(w);
  expect((await owner.saveLocal(key,original,0)).revision).toBe(1);
  const first=await stale.readVersion(key);expect(first.value.bytes).toEqual(original.bytes);expect(await first.value.attachment.text()).toBe(await original.attachment.text());
  const edited={...original,nested:{...original.nested,count:2}};expect((await owner.saveLocal(key,edited,1)).revision).toBe(2);
  await expect(stale.saveLocal(key,{...original,title:'Stale edit'},1)).rejects.toMatchObject({status:409});
  await expect(stale.deleteLocal(key)).rejects.toMatchObject({status:409});
  expect((await store(w,f.reader).readVersion(key)).value.nested.count).toBe(2);
  await owner.deleteLocal(key);expect(await store(w).readVersion(key)).toEqual({key,revision:3,deleted:true,value:null});
  expect((await store(w).readLocal(w.prefixes[0])).some(r=>r.key===key)).toBe(false);
  await expect(store(w).saveLocal(key,original,0)).rejects.toMatchObject({status:409});
  await expect(store(w).saveLocal(key,original)).rejects.toThrow('removed');
  expect((await store(w).saveLocal(key,edited,3)).revision).toBe(4);
  const restored=await store(w,f.reader).readVersion(key);expect(restored.revision).toBe(4);expect(restored.value.nested.count).toBe(2);expect(await restored.value.attachment.text()).toBe(await original.attachment.text());
 },15000);
 test(w.pack+': atomic batches, lost replies and current access use the same native owner',async()=>{
  const a=keyFor(w,'batch-a'),b=keyFor(w,'batch-b'),owner=store(w),original=value(w);
  await owner.saveLocal(a,original,0);
  await expect(owner.saveLocalBatch([{key:a,value:{...original,title:'Stale batch'},expectedRevision:0},{key:b,value:original,expectedRevision:0}])).rejects.toMatchObject({status:409});
  expect(await owner.readVersion(b)).toBeNull();expect((await owner.readVersion(a)).revision).toBe(1);
  const committed=await owner.saveLocalBatch([{key:a,value:{...original,title:'Atomic update'},expectedRevision:1},{key:b,value:original,expectedRevision:0}]);expect(committed.map(r=>r.revision)).toEqual([2,1]);
  let writes=0;const uncertain=store(w,f.owner,async(url,options)=>{const response=await f.raw(url,f.owner,options);if(new URL(url).searchParams.get('operation')==='write'){writes++;await response.arrayBuffer();throw Error('Isolated lost acknowledgement');}return response;});
  const lost=keyFor(w,'lost');await expect(uncertain.saveLocal(lost,original,0)).rejects.toThrow('lost acknowledgement');
  expect((await store(w).readVersion(lost)).revision).toBe(1);expect(writes).toBe(1);
  await expect(store(w,f.other).readVersion(a)).rejects.toMatchObject({status:403});
  await expect(store(w,f.reader).saveLocal(a,original,2)).rejects.toMatchObject({status:403});
  const foreign=w.family==='source-campaign'?'shipment-batch:foreign':'source-campaign:foreign';await expect(owner.saveLocal(foreign,original,0)).rejects.toThrow();
 },15000);
}
test('catalogue and room planning edit one destination-owned object and retain its attachment once',async()=>{
 const catalogue=workspaces.find(w=>w.entry==='catalogue/'),room=workspaces.find(w=>w.entry==='product-room/'),a=store(catalogue),b=store(room,f.writer),key='fixture-plan:contract-shared-object';
 const before=f.sql.query('SELECT count(*) AS n FROM _workspace_blob').get().n;
 const attachment=new Blob(['Shared room object material '+crypto.randomUUID()],{type:'text/plain'});await a.saveLocal(key,{title:'Same room plan',attachment},0);
 const seen=await b.readVersion(key);expect(seen.revision).toBe(1);expect(await seen.value.attachment.text()).toBe(await attachment.text());
 await b.saveLocal(key,{...seen.value,title:'Room editor revision'},seen.revision);expect((await a.readVersion(key)).value.title).toBe('Room editor revision');
 expect(f.sql.query('SELECT count(*) AS n FROM _workspace_blob').get().n).toBe(before+1);
 await b.deleteLocal(key);expect((await a.readVersion(key)).deleted).toBe(true);
},15000);
test('revoking write access refuses every already-open family client without changing its records',async()=>{
 const entries=workspaces.map(w=>({w,client:store(w,f.writer),key:keyFor(w,'lifecycle')}));
 for(const e of entries)e.before=await e.client.readVersion(e.key);
 f.grant(f.writer.id,'workspace-team','read');
 try{for(const {w,client,key,before}of entries){await expect(client.saveLocal(key,{...before.value,title:'Revoked edit'},before.revision)).rejects.toMatchObject({status:403});expect((await store(w).readVersion(key)).revision).toBe(before.revision);}}
 finally{f.grant(f.writer.id,'workspace-team','write');}
},30000);
