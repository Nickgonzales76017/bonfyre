import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readdirSync,readFileSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import {execFileSync} from 'node:child_process';
import {requestStore} from '../training-requests.mjs';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio'),program=material.creators[0].routines[0];
const brief=()=>({id:crypto.randomUUID(),creatorId:'forge',programId:program.id,title:'Dumbbells only',details:'Please adapt this routine for dumbbells and a 30-minute session.'});
function isolated(fn){const home=mkdtempSync(join(tmpdir(),'bonfyre-training-work-'));try{return fn(requestStore(home,material),home);}finally{rmSync(home,{recursive:true,force:true});}}
test('native request retry keeps one mission and rejects changed briefs or invalid studio programs',()=>isolated((store,home)=>{
  const input=brief(),a=store.submit(input),b=store.submit(input);expect(a.state).toBe('ready');expect(b.accessToken).toBe(a.accessToken);
  const db=new Database(join(home,'work.db'),{readonly:true});expect(db.query('select count(*) as n from workgraph_nodes').get().n).toBe(1);db.close();
  expect(()=>store.submit({...input,details:'Different'})).toThrow('different brief');expect(()=>store.submit({...brief(),creatorId:'still'})).toThrow('studio');
  expect(()=>store.submit({...brief(),history:[]})).toThrow('Unexpected');
}));
test('member receipt cannot read another request and operator list is studio-scoped',()=>isolated(store=>{
  const a=store.submit(brief()),b=store.submit(brief());expect(()=>store.read(a.id,b.accessToken)).toThrow('receipt');expect(()=>store.read(a.id,'')).toThrow('receipt');
  expect(store.list('forge')).toHaveLength(2);expect(store.list('still')).toHaveLength(0);expect(JSON.stringify(store.read(a.id,a.accessToken))).not.toContain(a.accessToken);
}));
test('request can be claimed and delivered through native WorkGraph across adapter restarts',()=>isolated((store,home)=>{
  const a=store.submit(brief());expect(store.accept(a.id,'coach-one').state).toBe('running');expect(()=>store.accept(a.id,'coach-two')).toThrow();
  const reopened=requestStore(home,material);expect(reopened.read(a.id,a.accessToken).state).toBe('running');expect(reopened.renew(a.id).state).toBe('running');
  const routine=structuredClone(program);routine.name='Dumbbells in 30 minutes';routine.member='private';routine.exercises[0].sets[0].done=true;
  const done=reopened.deliver(a.id,routine);expect(done.state).toBe('complete');expect(done.delivery.routine.name).toBe(routine.name);expect(done.delivery.routine.member).toBeUndefined();expect(done.delivery.routine.exercises[0].sets[0].done).toBe(false);
  expect(requestStore(home,material).read(a.id,a.accessToken).delivery.sha256).toBe(done.delivery.sha256);expect(()=>store.deliver(a.id,routine)).toThrow('Claim');
}));
test('a stale claim cannot publish delivery and tampered output is refused',()=>isolated((store,home)=>{
  const a=store.submit(brief());store.accept(a.id,'coach');const path=join(home,a.id,'claim.json'),claim=JSON.parse(readFileSync(path));writeFileSync(path,JSON.stringify({...claim,claim_token:'0'.repeat(64)}));
  expect(()=>store.deliver(a.id,program)).toThrow();expect(store.read(a.id,a.accessToken).delivery).toBeNull();writeFileSync(path,JSON.stringify(claim));store.deliver(a.id,program);
  const db=new Database(join(home,'work.db'),{readonly:true}),node=db.query('select output_uri from workgraph_nodes').get();db.close();const file=new URL(node.output_uri),delivery=JSON.parse(readFileSync(file));delivery.routine.name='Changed';writeFileSync(file,JSON.stringify(delivery));
  expect(()=>store.read(a.id,a.accessToken)).toThrow('changed');
}));
test('claiming a named request does not take another request and cannot cross a family filter',()=>isolated((store,home)=>{
  const a=store.submit(brief()),b=store.submit(brief());
  expect(()=>execFileSync('cmd/BonfyreQueue/bonfyre-queue',['claim','training-request-'+b.id,'job','coach','--family','training-still','--db',join(home,'work.db')],{stdio:'pipe'})).toThrow();
  store.accept(b.id,'coach');expect(store.read(a.id,a.accessToken).state).toBe('ready');expect(store.read(b.id,b.accessToken).state).toBe('running');
}));
test('HTTP request writes require same-origin JSON and receipt reads never expose the token',async()=>{
  const home=mkdtempSync(join(tmpdir(),'bonfyre-training-http-'));try{
    const store=requestStore(home,material),handler=trainingHandler(material,4190,{requests:store}),input=brief();
    const post=origin=>new Request('http://127.0.0.1:4190/api/training/requests',{method:'POST',headers:{host:'127.0.0.1:4190',origin,'content-type':'application/json'},body:JSON.stringify(input)});
    expect((await handler(post('https://other.example'))).status).toBe(403);expect(store.list('forge')).toEqual([]);
    const created=await (await handler(post('http://127.0.0.1:4190'))).json();expect(created.state).toBe('ready');
    const read=token=>new Request(`http://127.0.0.1:4190/api/training/requests/${input.id}`,{headers:{host:'127.0.0.1:4190','x-request-receipt':token}});
    expect((await handler(read('bad'))).status).toBe(404);const response=await handler(read(created.accessToken));expect(response.status).toBe(200);expect(await response.text()).not.toContain(created.accessToken);
  }finally{rmSync(home,{recursive:true,force:true});}
});
