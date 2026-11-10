import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {Database} from 'bun:sqlite';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
import {requestStore} from '../training-requests.mjs';
import {publishingStore,programInstitution} from '../training-publishing.mjs';
import {creatorAuth,creatorInstitution} from '../training-creator.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio');
async function isolated(fn){
  const home=mkdtempSync(join(tmpdir(),'training-publishing-'));let publishing;
  const requests=requestStore(join(home,'requests'),material,{programResolver:(studio,id)=>publishing.list(studio,{publicOnly:true}).find(p=>p.routine.id===id)?.routine});publishing=publishingStore(join(home,'cms'),material,requests);
  const request=requests.submit({id:crypto.randomUUID(),creatorId:'forge',programId:material.creators[0].routines[0].id,title:'PRIVATE REQUEST TITLE',details:'PRIVATE MEMBER BRIEF'});requests.accept(request.id,'coach');requests.deliver(request.id,material.creators[0].routines[0]);
  const input=()=>({publicationId:crypto.randomUUID(),programId:crypto.randomUUID(),expectedVersion:0,requestId:request.id,title:'Travel strength',description:'Three movements for a short strength session.'});
  try{await fn({home,requests,publishing,request,input});}finally{rmSync(home,{recursive:true,force:true});}
}
test('native CMS publication is versioned, studio-scoped and excludes private request coordinates',()=>isolated(({home,publishing,request,input})=>{
  const submitted={...input(),description:'Three movements.\nShort rest periods.'},result=publishing.publish('forge','actor:publisher',submitted);expect(result.version).toBe(1);expect(result.status).toBe('published');expect(result.description).toBe(submitted.description);
  const publicRows=publishing.list('forge',{publicOnly:true});expect(publicRows).toHaveLength(1);expect(JSON.stringify(publicRows)).not.toContain(request.id);expect(JSON.stringify(publicRows)).not.toContain('PRIVATE');expect(JSON.stringify(publicRows)).not.toContain('actor:publisher');expect(publicRows[0].routine.exercises[0].id).toMatch(/^exercise:[a-f0-9]{40}$/);expect(publishing.list('still',{publicOnly:true})).toEqual([]);
  const db=new Database(join(home,'cms/catalogue.db'),{readonly:true}),row=db.query('select namespace,version,origin_request,publisher_actor from training_program_release').get();db.close();expect(row).toEqual({namespace:'training-forge',version:1,origin_request:request.id,publisher_actor:'actor:publisher'});
  expect(publishing.publish('forge','actor:publisher',submitted).releaseId).toBe(result.releaseId);expect(publishing.list('forge')).toHaveLength(1);
  expect(publishingStore(join(home,'cms'),material,{creatorRead(){throw Error('Read-only reopen');}}).list('forge',{publicOnly:true})).toEqual(publicRows);
  expect(()=>publishing.publish('forge','actor:publisher',{...submitted,title:'Changed retry'})).toThrow('different');
}));
test('new versions preserve adopted copies; withdrawing the latest release cannot expose an older one',()=>isolated(({home,publishing,requests,input})=>{
  const first=input(),a=publishing.publish('forge','actor:publisher',first),adopted=structuredClone(a.routine);
  const second={...first,publicationId:crypto.randomUUID(),expectedVersion:1,title:'Travel strength, longer rests'},b=publishing.publish('forge','actor:publisher',second);
  expect(b.version).toBe(2);expect(b.routine.id).not.toBe(a.routine.id);expect(adopted.source.version).toBe('1');expect(adopted.name).toBe(first.title);expect(publishing.list('forge',{publicOnly:true})[0].version).toBe(2);
  expect(()=>publishing.publish('forge','actor:publisher',{...second,publicationId:crypto.randomUUID()})).toThrow('newer');
  const db=new Database(join(home,'cms/catalogue.db'),{readonly:true});expect(db.query('select count(*) as n from training_program_release').get().n).toBe(2);db.close();
  publishing.withdraw('forge',b.releaseId);expect(publishing.list('forge',{publicOnly:true})).toEqual([]);expect(publishing.list('forge')[0].status).toBe('withdrawn');expect(adopted.name).toBe(first.title);
  expect(publishing.publish('forge','actor:publisher',second).status).toBe('withdrawn');expect(publishing.list('forge',{publicOnly:true})).toEqual([]);
  expect(()=>publishing.withdraw('still',a.releaseId)).toThrow('not found');
  expect(()=>publishing.publish('still','actor:publisher',{...first,publicationId:crypto.randomUUID(),programId:crypto.randomUUID()})).toThrow('not found');
}));
test('published releases accept follow-up requests with the exact public version and snapshot',()=>isolated(({publishing,requests,input})=>{
  const released=publishing.publish('forge','actor:publisher',input()),follow=requests.submit({id:crypto.randomUUID(),creatorId:'forge',programId:released.routine.id,title:'Another version',details:'Please adapt this published program.'});
  expect(follow.brief.programVersion).toBe('1');requests.accept(follow.id,'coach');expect(requests.draftRead(follow.id,'coach').sourceProgram.name).toBe(released.title);
  publishing.withdraw('forge',released.releaseId);expect(requests.draftRead(follow.id,'coach').sourceProgram.name).toBe(released.title);
  expect(()=>requests.submit({id:crypto.randomUUID(),creatorId:'forge',programId:released.routine.id,title:'Late',details:'Withdrawn program'})).toThrow('Choose a program');
}));
test('native uniqueness and digest checks reject conflicting or altered releases',()=>isolated(({home,publishing,input})=>{
  const released=publishing.publish('forge','actor:publisher',input()),db=new Database(join(home,'cms/catalogue.db'));
  const columns=db.query('pragma table_info(training_program_release)').all().map(c=>c.name).filter(c=>c!=='id').map(c=>'"'+c+'"').join(',');
  expect(()=>db.run(`insert into training_program_release(${columns}) select ${columns} from training_program_release`)).toThrow('UNIQUE');
  db.run('update training_program_release set title=? where id=?',['Changed outside publisher',released.releaseId]);db.close();expect(()=>publishing.list('forge',{publicOnly:true})).toThrow('changed');
}));
test('publishing requires its own current grant and never accepts a request from another studio',()=>isolated(async({home,requests,publishing,input})=>{
  const db=join(home,'auth.db'),native=(command,args=[],stdin='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input:stdin,encoding:'utf8',stdio:'pipe'}));
  native('http-signup',[],JSON.stringify({email:'coach@example.test',password:'publication-test-password',name:'Coach'}));native('actor-bind',['--user-id','1','--actor','actor:publisher']);native('institution-grant',['--user-id','1','--institution',creatorInstitution('forge'),'--role','write']);
  const auth=creatorAuth(db),handler=trainingHandler(material,4190,{requests,publishing,auth}),origin='http://127.0.0.1:4190';let cookie='';
  const call=(path,body,headers={})=>handler(new Request(origin+'/api/training/creator'+path,{method:body===undefined?'GET':'POST',headers:{host:'127.0.0.1:4190',origin,cookie,'content-type':'application/json',...headers},body:body===undefined?undefined:JSON.stringify(body)}));
  const login=await call('/login',{email:'coach@example.test',password:'publication-test-password'});cookie=login.headers.get('set-cookie').split(';')[0];
  expect((await call('/studios/forge/programs',input())).status).toBe(403);expect(publishing.list('forge')).toEqual([]);
  native('institution-grant',['--user-id','1','--institution',programInstitution('forge'),'--role','read']);expect((await call('/studios/forge/programs')).status).toBe(200);expect((await call('/studios/forge/programs',input())).status).toBe(403);
  native('institution-grant',['--user-id','1','--institution',programInstitution('forge'),'--role','write']);expect((await call('/studios/forge/programs',input(),{origin:'https://elsewhere.test'})).status).toBe(403);
  const release=await (await call('/studios/forge/programs',input())).json();expect(release.version).toBe(1);
  expect((await call('/studios/still/programs',input())).status).toBe(403);
  let releaseBody;const stream=new ReadableStream({start(controller){releaseBody=()=>{native('institution-revoke',['--user-id','1','--institution',programInstitution('forge')]);controller.enqueue(new TextEncoder().encode(JSON.stringify(input())));controller.close();};}});
  const pending=handler(new Request(origin+'/api/training/creator/studios/forge/programs',{method:'POST',headers:{host:'127.0.0.1:4190',origin,cookie,'content-type':'application/json'},body:stream,duplex:'half'}));releaseBody();expect((await pending).status).toBe(403);
  expect((await call(`/studios/forge/programs/${release.releaseId}/withdraw`,{})).status).toBe(403);expect(publishing.list('forge',{publicOnly:true})).toHaveLength(1);
  const publicResult=await handler(new Request(origin+'/api/training/programs/forge',{headers:{host:'127.0.0.1:4190'}}));expect(publicResult.status).toBe(200);expect(await publicResult.text()).not.toContain('origin_request');
}));
