import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,writeFileSync,readFileSync,existsSync,readdirSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import {acquisitionStore} from '../acquisition-store.mjs';

const validate=value=>{if(!Array.isArray(value.lots)||value.lots.length>130||value.lots.some(x=>typeof x.title!=='string'||!Number.isSafeInteger(x.units)||x.units<1))throw Error('Invalid manifest.');return value;};
const value=title=>({lots:[{title,units:12}]});
async function isolated(fn){const home=mkdtempSync(join(tmpdir(),'bonfyre-acquisition-'));try{return await fn(acquisitionStore(home,{validate}),home);}finally{rmSync(home,{recursive:true,force:true});}}
function errorStatus(fn){try{fn();}catch(error){return error.status;}throw Error('Expected refusal.');}
function rows(home,sql){const db=new Database(join(home,'work.db'),{readonly:true});try{return db.query(sql).all();}finally{db.close();}}

test('native acquisition snapshots survive restart and isolate organization revisions',()=>isolated((store,home)=>{
  expect(store.read('alpha')).toBeNull();expect(existsSync(join(home,'work.db'))).toBe(false);
  expect(store.write('alpha','user:1',0,value('Alpha'))).toEqual({revision:1,value:value('Alpha')});
  expect(store.write('bravo','user:2',0,value('Bravo')).revision).toBe(1);
  expect(store.write('alpha','user:3',1,value('Alpha updated')).revision).toBe(2);
  const reopened=acquisitionStore(home,{validate});
  expect(reopened.read('alpha')).toEqual({revision:2,value:value('Alpha updated')});
  expect(reopened.read('bravo')).toEqual({revision:1,value:value('Bravo')});
  expect(rows(home,'select mission_id,operator_id,family from workgraph_nodes')).toEqual([
    {mission_id:'acquisition-workspace-alpha',operator_id:'queue.acquisition-workspace',family:'acquisition-workspace'},
    {mission_id:'acquisition-workspace-bravo',operator_id:'queue.acquisition-workspace',family:'acquisition-workspace'}]);
}));

test('input bounds and stale revisions refuse without creating or replacing drafts',()=>isolated((store,home)=>{
  expect(errorStatus(()=>store.write('alpha','user:1',0,{lots:[{title:'Broken',units:-1}]}))).toBe(400);
  expect(errorStatus(()=>store.write('../alpha','user:1',0,value('A')))).toBe(400);
  expect(errorStatus(()=>store.write('alpha','',0,value('A')))).toBe(400);
  expect(errorStatus(()=>store.write('alpha','user:1',-1,value('A')))).toBe(400);
  expect(errorStatus(()=>store.write('alpha','user:1',0,{...value('A'),padding:'x'.repeat(2*1024*1024)}))).toBe(400);
  expect(errorStatus(()=>store.write('alpha','user:1',0,{...value('A'),invalid:NaN}))).toBe(400);
  expect(existsSync(join(home,'work.db'))).toBe(false);
  store.write('alpha','user:1',0,value('Kept'));
  expect(errorStatus(()=>store.write('alpha','user:2',0,value('Stale')))).toBe(409);
  expect(store.read('alpha').value).toEqual(value('Kept'));
  expect(rows(home,'select revision from workgraph_checkpoints')).toEqual([{revision:1}]);
}));

test('hydration operates on a copy and its result is validated and bounded',()=>isolated((_,home)=>{
  const input=value('Legacy'),store=acquisitionStore(home,{validate,hydrate:s=>{s.version??=1;return s;}});
  expect(store.write('alpha','user:1',0,input).value.version).toBe(1);expect(input.version).toBeUndefined();
  expect(store.read('alpha').value.version).toBe(1);
  const bad=acquisitionStore(home,{validate,hydrate:s=>{s.lots[0].units=-1;return s;}});
  expect(errorStatus(()=>bad.write('alpha','user:1',1,input))).toBe(400);
}));

test('tampered or missing artifact cannot be loaded or silently replaced',()=>isolated((store,home)=>{
  store.write('alpha','user:1',0,value('Original'));
  const artifact=new URL(rows(home,'select artifact_uri from workgraph_checkpoints')[0].artifact_uri),bytes=readFileSync(artifact);
  writeFileSync(artifact,bytes.toString().replace('Original','Tampered'));
  expect(errorStatus(()=>store.read('alpha'))).toBe(503);
  expect(errorStatus(()=>store.write('alpha','user:1',1,value('Replacement')))).toBe(503);
  writeFileSync(artifact,bytes);expect(store.read('alpha').value).toEqual(value('Original'));
  rmSync(artifact);expect(errorStatus(()=>store.read('alpha'))).toBe(503);
}));

test('checkpoint URI and mission identity cannot cross organizations',()=>isolated((store,home)=>{
  store.write('alpha','user:1',0,value('Private'));store.write('bravo','user:2',0,value('Other'));
  const db=new Database(join(home,'work.db'));
  try{db.run("update workgraph_checkpoints set artifact_uri=(select artifact_uri from workgraph_checkpoints where mission_id='acquisition-workspace-alpha') where mission_id='acquisition-workspace-bravo'");}finally{db.close();}
  expect(errorStatus(()=>store.read('bravo'))).toBe(503);
  const db2=new Database(join(home,'work.db'));try{db2.run("update workgraph_nodes set family='training-forge' where mission_id='acquisition-workspace-alpha'");}finally{db2.close();}
  expect(errorStatus(()=>store.read('alpha'))).toBe(503);
}));

test('expired native lease can be recovered without resetting saved revisions',()=>isolated((_,home)=>{
  const store=acquisitionStore(home,{validate,leaseMs:100});store.write('alpha','user:1',0,value('Before'));
  const db=new Database(join(home,'work.db'));try{db.run('update workgraph_nodes set lease_expires_at_ms=1');}finally{db.close();}
  expect(store.write('alpha','user:1',1,value('After')).revision).toBe(2);
  expect(rows(home,'select attempt from workgraph_nodes')[0].attempt).toBe(2);
  expect(store.read('alpha').value).toEqual(value('After'));
}));

async function compete(home,revision){
  const module=resolve(import.meta.dir,'../acquisition-store.mjs'),gate=join(home,`gate-${revision}`);
  const children=[0,1].map(index=>{
    const script=`import {existsSync,writeFileSync} from 'node:fs'; import {acquisitionStore} from ${JSON.stringify(module)};
      const store=acquisitionStore(${JSON.stringify(home)},{validate:${validate.toString()}});
      writeFileSync(${JSON.stringify(`${gate}-${index}`)},'ready');
      while(!existsSync(${JSON.stringify(gate)})) await new Promise(r=>setTimeout(r,5));
      try { console.log(JSON.stringify(store.write('alpha','user:${index+1}',${revision},{lots:[{title:'writer-${index}',units:12}]}))); }
      catch(error){console.log(JSON.stringify({status:error.status,message:error.message}));}`;
    return Bun.spawn([process.execPath,'--eval',script],{stdout:'pipe',stderr:'pipe'});
  });
  const deadline=Date.now()+10000;
  while(![0,1].every(i=>existsSync(`${gate}-${i}`))){if(Date.now()>deadline)throw Error('Child initialization timed out.');await new Promise(r=>setTimeout(r,10));}
  writeFileSync(gate,'go');
  return Promise.all(children.map(async child=>{const output=await new Response(child.stdout).text(),stderr=await new Response(child.stderr).text();expect(await child.exited).toBe(0);if(!output)throw Error(stderr);return JSON.parse(output);}));
}

test('competing native first saves create one stream and admit only one revision',()=>isolated(async(store,home)=>{
  const result=await compete(home,0);
  expect(result.filter(x=>x.revision===1)).toHaveLength(1);expect(result.filter(x=>x.status===409)).toHaveLength(1);
  expect(rows(home,'select count(*) as n from workgraph_nodes')[0].n).toBe(1);
  expect(rows(home,'select count(*) as n from missions')[0].n).toBe(1);
  expect(store.read('alpha').value).toEqual(result.find(x=>x.revision===1).value);
  const next=await compete(home,1);expect(next.filter(x=>x.revision===2)).toHaveLength(1);expect(next.filter(x=>x.status===409)).toHaveLength(1);
  expect(rows(home,'select revision from workgraph_checkpoints order by revision')).toEqual([{revision:1},{revision:2}]);
}));

test('missing native owner refuses instead of falling back to local canonical storage',()=>isolated((_,home)=>{
  const store=acquisitionStore(home,{validate,binary:join(home,'missing-native-owner')});
  expect(errorStatus(()=>store.write('alpha','user:1',0,value('A')))).toBe(503);
  expect(existsSync(join(home,'work.db'))).toBe(false);
  expect(readdirSync(join(home,'acquisition','alpha'))).toHaveLength(0);
}));

test('lost native database refuses fresh writes while snapshot history remains',()=>isolated((store,home)=>{
  store.write('alpha','user:1',0,value('Saved'));
  for(const suffix of ['','-wal','-shm'])rmSync(join(home,`work.db${suffix}`),{force:true});
  expect(errorStatus(()=>store.read('alpha'))).toBe(503);
  expect(errorStatus(()=>store.write('bravo','user:2',0,value('New')))).toBe(503);
  expect(existsSync(join(home,'work.db'))).toBe(false);
}));
