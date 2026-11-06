import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {trainingHandler,trainingMaterial} from '../training-workspace.mjs';
import {creatorAuth,creatorInstitution} from '../training-creator.mjs';
import {requestStore} from '../training-requests.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio'),origin='http://127.0.0.1:4190',prefix='/api/training/creator';
async function isolated(fn){
  const home=mkdtempSync(join(tmpdir(),'training-creator-')),db=join(home,'auth.db');
  const native=(command,args=[],input='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input,encoding:'utf8',stdio:'pipe'}));
  const auth=creatorAuth(db),store=requestStore(join(home,'requests'),material),handler=trainingHandler(material,4190,{auth,requests:store});
  const req=(path,body,cookie='',headers={})=>handler(new Request(origin+prefix+path,{method:body===undefined?'GET':'POST',headers:{host:'127.0.0.1:4190',origin,'content-type':'application/json',cookie,...headers},body:body===undefined?undefined:JSON.stringify(body)}));
  const account=async(name,role='write',studio='forge',bind=true)=>{
    const email=name+'@example.test',password='test-creator-password';native('http-signup',[],JSON.stringify({email,password,name}));
    const login=native('http-login',[],JSON.stringify({email,password})),id=login.user.id;
    if(bind)native('actor-bind',['--user-id',String(id),'--actor','actor:'+name]);
    if(role)native('institution-grant',['--user-id',String(id),'--institution',creatorInstitution(studio),'--role',role]);
    const response=await req('/login',{email,password}),cookie=response.headers.get('set-cookie')?.split(';')[0]??'';
    return {id,cookie,response};
  };
  const submit=(studio='forge')=>store.submit({id:crypto.randomUUID(),creatorId:studio,programId:material.creators.find(c=>c.id===studio).routines[0].id,title:'Travel routine',details:'Please prepare a routine for a hotel room.'});
  try{await fn({home,db,native,auth,store,req,account,submit});}finally{rmSync(home,{recursive:true,force:true});}
}
test('creator sign-in uses native actor binding and private cookie; a session alone grants no studio',()=>isolated(async({account,req})=>{
  const unbound=await account('unbound',null,'forge',false);expect(unbound.response.status).toBe(403);
  const member=await account('member',null);expect(member.response.status).toBe(200);expect(member.response.headers.get('set-cookie')).toContain('HttpOnly; SameSite=Strict');expect(await member.response.text()).not.toContain('bfy_');
  const session=await (await req('/session',undefined,member.cookie)).json();expect(session.actor).toBe('actor:member');expect(session.studios).toEqual([]);
  expect((await req('/studios/forge/requests',undefined,member.cookie)).status).toBe(403);
  expect((await req('/session')).status).toBe(401);
}));
test('read permission cannot mutate, write cannot cross studios, and request IDs cannot bypass studio checks',()=>isolated(async({account,req,submit,store})=>{
  const forge=submit(),still=submit('still'),reader=await account('reader','read'),writer=await account('writer');
  const list=await (await req('/studios/forge/requests',undefined,reader.cookie)).json();expect(list).toHaveLength(1);expect(list[0].id).toBe(forge.id);expect(JSON.stringify(list)).not.toContain(forge.accessToken);
  expect((await req(`/studios/forge/requests/${forge.id}/accept`,{},reader.cookie)).status).toBe(403);
  expect((await req('/studios/still/requests',undefined,writer.cookie)).status).toBe(403);
  expect((await req(`/studios/forge/requests/${still.id}/accept`,{},writer.cookie)).status).toBe(404);
  expect(store.read(still.id,still.accessToken).state).toBe('ready');
  expect((await req(`/studios/forge/requests/${forge.id}/accept`,{},writer.cookie,{origin:'https://elsewhere.test'})).status).toBe(403);
  expect((await req(`/studios/forge/requests/${forge.id}/accept`,{worker:'spoofed'},writer.cookie)).status).toBe(400);
  expect(store.read(forge.id,forge.accessToken).state).toBe('ready');
}));
test('native claim is bound to the signed-in actor and only that coach can renew or deliver',()=>isolated(async({account,req,submit,store})=>{
  const a=await account('coach-a'),b=await account('coach-b'),request=submit(),path=`/studios/forge/requests/${request.id}`;
  const accepted=await (await req(path+'/accept',{},a.cookie)).json(),actor=await (await req('/session',undefined,a.cookie)).json();expect(accepted.state).toBe('running');expect(accepted.worker).toBe(actor.worker);
  expect((await req(path+'/renew',{},b.cookie)).status).toBe(400);
  expect((await req(path+'/deliver',{routine:material.creators[0].routines[0]},b.cookie)).status).toBe(400);
  expect(store.read(request.id,request.accessToken).state).toBe('running');
  expect((await req(path+'/renew',{},a.cookie)).status).toBe(200);
  const draft=await (await req(path+'/draft',{routine:material.creators[0].routines[0],expectedRevision:0},a.cookie)).json();expect(draft.revision).toBe(1);
  const delivered=await (await req(path+'/deliver',{revision:draft.revision},a.cookie)).json();expect(delivered.state).toBe('complete');expect(store.read(request.id,request.accessToken).delivery.sha256).toBe(delivered.delivery.sha256);
}));
test('draft APIs require the assigned coach and native revision checks apply to saving and delivery',()=>isolated(async({account,req,submit,store})=>{
  const coach=await account('draft-coach'),reader=await account('draft-reader','read'),other=await account('draft-other'),request=submit(),path=`/studios/forge/requests/${request.id}`;
  await req(path+'/accept',{},coach.cookie);
  expect((await req(path+'/draft',undefined,reader.cookie)).status).toBe(403);
  expect((await req(path+'/draft',undefined,other.cookie)).status).toBe(400);
  const first=await (await req(path+'/draft',undefined,coach.cookie)).json();expect(first.revision).toBe(0);expect(first.sourceProgram.name).toBe(material.creators[0].routines[0].name);
  const routine=structuredClone(first.sourceProgram);routine.name='Draft one';
  expect((await req(path+'/draft',{routine,expectedRevision:0},coach.cookie)).status).toBe(200);
  routine.name='Draft two';expect((await req(path+'/draft',{routine,expectedRevision:1},coach.cookie)).status).toBe(200);
  expect((await req(path+'/draft',{routine,expectedRevision:0},coach.cookie)).status).toBe(409);
  expect((await req(path+'/deliver',{revision:1},coach.cookie)).status).toBe(409);
  expect(store.read(request.id,request.accessToken).delivery).toBeNull();
  const saved=await (await req(path+'/draft',undefined,coach.cookie)).json();expect(saved.routine.name).toBe('Draft two');expect(saved.revision).toBe(2);
  expect((await req(path+'/deliver',{revision:2},coach.cookie)).status).toBe(200);
}));
test('current grants, actor binding and session revocation are checked on every request',()=>isolated(async({account,req,submit,native,store})=>{
  const coach=await account('coach'),request=submit(),path=`/studios/forge/requests/${request.id}/accept`;
  native('institution-revoke',['--user-id',String(coach.id),'--institution',creatorInstitution('forge')]);expect((await req(path,{},coach.cookie)).status).toBe(403);
  native('institution-grant',['--user-id',String(coach.id),'--institution',creatorInstitution('forge'),'--role','write']);
  native('actor-revoke',['--user-id',String(coach.id)]);expect((await req(path,{},coach.cookie)).status).toBe(403);
  native('actor-bind',['--user-id',String(coach.id),'--actor','actor:coach']);expect((await req('/session',undefined,coach.cookie)).status).toBe(200);
  const logout=await req('/logout',{},coach.cookie);expect(logout.status).toBe(200);expect(logout.headers.get('set-cookie')).toContain('Max-Age=0');expect((await req(path,{},coach.cookie)).status).toBe(403);
  expect(store.read(request.id,request.accessToken).state).toBe('ready');
}));
test('permissions are rechecked after a streamed request body arrives',()=>isolated(async({account,submit,native,auth,store})=>{
  const coach=await account('streamed'),request=submit();let release;
  const body=new ReadableStream({start(controller){release=()=>{native('institution-revoke',['--user-id',String(coach.id),'--institution',creatorInstitution('forge')]);controller.enqueue(new TextEncoder().encode('{}'));controller.close();};}});
  const handler=trainingHandler(material,4190,{auth,requests:store}),pending=handler(new Request(origin+prefix+`/studios/forge/requests/${request.id}/accept`,{method:'POST',headers:{host:'127.0.0.1:4190',origin,'content-type':'application/json',cookie:coach.cookie},body,duplex:'half'}));
  release();expect((await pending).status).toBe(403);expect(store.read(request.id,request.accessToken).state).toBe('ready');
}));

test('studio administration uses native grants, stays in its scope and revokes existing sessions immediately',()=>isolated(async({account,req,native})=>{
  const admin=await account('administrator','admin'),member=await account('new-coach',null),writer=await account('non-admin');
  const path='/studios/forge/team/requests';
  const session=await (await req('/session',undefined,admin.cookie)).json();
  expect(session.userId).toBe(admin.id);expect(session.studios[0].adminScopes).toEqual(['requests']);
  expect((await req(path,undefined,writer.cookie)).status).toBe(403);
  expect((await req('/studios/still/team/requests',undefined,admin.cookie)).status).toBe(403);
  expect((await req('/studios/forge/team/programs',undefined,admin.cookie)).status).toBe(403);
  expect((await req(path,{email:'new-coach@example.test',role:'write',institution:'bonfyre'},admin.cookie)).status).toBe(400);
  expect((await req(path,{email:'new-coach@example.test',role:'write'},admin.cookie,{origin:'https://elsewhere.test'})).status).toBe(403);
  expect((await req(path,{email:'new-coach@example.test',role:'write'},admin.cookie)).status).toBe(200);
  expect((await req('/studios/forge/requests',undefined,member.cookie)).status).toBe(200);
  expect((await req('/studios/forge/programs',undefined,member.cookie)).status).toBe(403);
  const list=await (await req(path,undefined,admin.cookie)).json();
  expect(list.members.find(m=>m.user_id===member.id).role).toBe('write');
  expect(JSON.stringify(list)).not.toContain('bfy_');expect(JSON.stringify(list)).not.toContain('password');
  expect((await req(path,{email:'new-coach@example.test',role:'none'},admin.cookie)).status).toBe(200);
  expect((await req('/studios/forge/requests',undefined,member.cookie)).status).toBe(403);
  expect((await req(path,{email:'administrator@example.test',role:'none'},admin.cookie)).status).toBe(400);
  native('institution-revoke',['--user-id',String(admin.id),'--institution',creatorInstitution('forge')]);
  expect((await req(path,undefined,admin.cookie)).status).toBe(403);
}));
test('team administrator permission is rechecked after a streamed body',()=>isolated(async({account,req,native,auth})=>{
  const admin=await account('stream-admin','admin'),member=await account('stream-member',null);
  const handler=trainingHandler(material,4190,{auth,requests:{}});
  const stream=new ReadableStream({start(controller){controller.enqueue(new TextEncoder().encode('{"email":"stream-member@example.test",'));setTimeout(()=>{native('institution-revoke',['--user-id',String(admin.id),'--institution',creatorInstitution('forge')]);controller.enqueue(new TextEncoder().encode('"role":"write"}'));controller.close();},10);}});
  const response=await handler(new Request(origin+prefix+'/studios/forge/team/requests',{method:'POST',headers:{host:'127.0.0.1:4190',origin,'content-type':'application/json',cookie:admin.cookie},body:stream,duplex:'half'}));
  expect(response.status).toBe(403);expect((await req('/studios/forge/requests',undefined,member.cookie)).status).toBe(403);
}));

test('reviewed team changes reject stale direct grants and preserve existing expiry',()=>isolated(async({account,req,native})=>{
  const admin=await account('review-admin','admin'),member=await account('review-member',null);
  const path='/studios/forge/team/requests',email='review-member@example.test';
  const change=(role,expected)=>req(path,{email,role,expected},admin.cookie);
  expect((await change('read',null)).status).toBe(200);
  expect((await change('admin',null)).status).toBe(409);
  const read={role:'read',expires_at:null};
  expect((await change('write',read)).status).toBe(200);
  expect((await change('none',read)).status).toBe(409);
  expect((await req('/studios/forge/requests',undefined,member.cookie)).status).toBe(200);
  native('institution-grant',['--user-id',String(member.id),'--institution',creatorInstitution('forge'),'--role','write','--expires-at','2090-01-01T00:00:00Z']);
  expect((await change('read',{role:'write',expires_at:null})).status).toBe(409);
  expect((await change('read',{role:'write',expires_at:'2090-01-01T00:00:00Z'})).status).toBe(200);
  let listing=await (await req(path,undefined,admin.cookie)).json();
  expect(listing.members.find(m=>m.user_id===member.id).expires_at).toBe('2090-01-01T00:00:00Z');
  expect((await change('none',{role:'read',expires_at:'2090-01-01T00:00:00Z'})).status).toBe(200);
  expect((await change('write',null)).status).toBe(200);
  for(const expected of [{role:'read'},{role:'read',expires_at:''},{role:'none',expires_at:null},{role:'read',expires_at:null,extra:true},[]])expect((await change('write',expected)).status).toBe(400);
  listing=await (await req(path,undefined,admin.cookie)).json();
  expect(listing.members.find(m=>m.user_id===member.id).role).toBe('write');
}));
