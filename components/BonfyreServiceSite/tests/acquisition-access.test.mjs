import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {acquisitionHandler} from '../acquisition-access.mjs';
import {accountAuth} from '../account-auth.mjs';

const origin='https://workspace.example.test',prefix='/api/acquisition',path='/organizations/north/draft';
const organizations=[{id:'north',name:'North acquisition',institution:'acquisition-north'},{id:'south',name:'South acquisition',institution:'acquisition-south'}];
const failure=(status,message)=>Object.assign(Error(message),{status});
async function isolated(fn){
  const home=mkdtempSync(join(tmpdir(),'acquisition-access-')),db=join(home,'auth.db');
  const native=(command,args=[],input='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input,encoding:'utf8',stdio:'pipe'}));
  const auth=accountAuth(db),rows=new Map(),writes=[];
  const store={read:id=>structuredClone(rows.get(id)??null),write(id,actor,expectedRevision,value){
    if((rows.get(id)?.revision??0)!==expectedRevision)throw failure(409,'A newer draft exists.');
    if(typeof value.title!=='string'||!value.title.trim())throw failure(400,'Enter a title.');
    const record={revision:expectedRevision+1,value:structuredClone(value)};rows.set(id,record);writes.push({id,actor});return structuredClone(record);
  }};
  const handler=acquisitionHandler({auth,store,organizations});
  const req=(route,body,cookie='',headers={},method=body===undefined?'GET':'POST',target=handler)=>target(new Request(origin+prefix+route,{method,headers:{origin,'content-type':'application/json',cookie,...headers},body:body===undefined?undefined:JSON.stringify(body)}));
  const account=async(name,role='write',org='north',bind=true)=>{
    const email=name+'@example.test',password='acquisition-test-password';native('http-signup',[],JSON.stringify({email,password,name}));
    const signed=native('http-login',[],JSON.stringify({email,password})),id=signed.user.id;
    if(bind)native('actor-bind',['--user-id',String(id),'--actor','actor:'+name]);
    if(role)native('institution-grant',['--user-id',String(id),'--institution','acquisition-'+org,'--role',role]);
    const response=await req('/login',{email,password}),cookie=response.headers.get('set-cookie')?.split(';')[0]??'';
    return {id,email,password,cookie,response};
  };
  const stream=async(cookie,change,target=handler)=>{
    const body=new ReadableStream({start(controller){controller.enqueue(new TextEncoder().encode('{"expectedRevision":0,'));setTimeout(()=>{change();controller.enqueue(new TextEncoder().encode('"value":{"title":"Shared lot"}}'));controller.close();},10);}});
    return target(new Request(origin+prefix+path,{method:'POST',headers:{origin,'content-type':'application/json',cookie},body,duplex:'half'}));
  };
  try{await fn({auth,store,writes,rows,native,handler,req,account,stream});}finally{rmSync(home,{recursive:true,force:true});}
}

test('native account sign-in needs an actor, returns a private scoped cookie, and grants no organization itself',()=>isolated(async({account,req})=>{
  const unbound=await account('unbound',null,'north',false);expect(unbound.response.status).toBe(403);
  const member=await account('member',null);expect(member.response.status).toBe(200);
  const cookie=member.response.headers.get('set-cookie');expect(cookie).toContain('Path=/api/acquisition; HttpOnly; SameSite=Strict;');expect(cookie).toContain('; Secure');
  const login=await member.response.json();expect(login).toEqual({actor:'actor:member',userId:member.id});expect(JSON.stringify(login)).not.toContain('bfy_');
  const session=await (await req('/session',undefined,member.cookie)).json();expect(session.organizations).toEqual([]);expect(session.profile).toEqual({id:member.id,name:'member',email:member.email});
  expect((await req(path,undefined,member.cookie)).status).toBe(403);expect((await req('/session')).status).toBe(401);
}));

test('organization roles use declared native institutions and isolate drafts and write authority',()=>isolated(async({account,req,writes})=>{
  const writer=await account('writer'),reader=await account('reader','read'),admin=await account('admin','admin');
  expect((await (await req('/session',undefined,reader.cookie)).json()).organizations).toEqual([{id:'north',name:'North acquisition',canRead:true,canWrite:false,canAdmin:false}]);
  expect((await (await req('/session',undefined,admin.cookie)).json()).organizations[0].canAdmin).toBe(true);
  expect(await (await req(path,undefined,writer.cookie)).json()).toBeNull();
  const input={expectedRevision:0,value:{title:'Shared lot'}};
  expect((await req(path,input,reader.cookie)).status).toBe(403);
  expect((await req('/organizations/south/draft',input,writer.cookie)).status).toBe(403);
  expect((await req('/organizations/missing/draft',undefined,writer.cookie)).status).toBe(403);
  expect((await req(path,{...input,institution:'acquisition-south'},writer.cookie)).status).toBe(400);
  expect((await req(path,input,writer.cookie)).status).toBe(200);expect(writes).toEqual([{id:'north',actor:'actor:writer'}]);
  expect(await (await req(path,undefined,reader.cookie)).json()).toEqual({revision:1,value:{title:'Shared lot'}});
  expect((await req(path,input,writer.cookie)).status).toBe(409);
  expect((await req(path,{expectedRevision:1,value:{title:'Updated'}},writer.cookie)).status).toBe(200);
}));

test('native grant, actor and session revocation take effect on existing cookies',()=>isolated(async({account,req,native})=>{
  const writer=await account('revoke');
  native('institution-revoke',['--user-id',String(writer.id),'--institution','acquisition-north']);
  expect((await req(path,undefined,writer.cookie)).status).toBe(403);
  native('institution-grant',['--user-id',String(writer.id),'--institution','acquisition-north','--role','write']);
  native('actor-revoke',['--user-id',String(writer.id)]);expect((await req('/session',undefined,writer.cookie)).status).toBe(401);
  native('actor-bind',['--user-id',String(writer.id),'--actor','actor:revoke']);
  const logout=await req('/logout',{},writer.cookie);expect(logout.status).toBe(200);expect(logout.headers.get('set-cookie')).toContain('Max-Age=0');
  expect((await req('/session',undefined,writer.cookie)).status).toBe(401);
}));

test('streamed saves recheck native grants after the request body arrives',()=>isolated(async({account,native,stream,writes})=>{
  const writer=await account('stream');
  const response=await stream(writer.cookie,()=>native('institution-revoke',['--user-id',String(writer.id),'--institution','acquisition-north']));
  expect(response.status).toBe(403);expect(writes).toHaveLength(0);
}));

test('streamed saves reject actor changes even when the account still has write access',()=>isolated(async({account,native,stream,writes})=>{
  const writer=await account('actor-change');
  const response=await stream(writer.cookie,()=>native('actor-bind',['--user-id',String(writer.id),'--actor','actor:replacement']));
  expect(response.status).toBe(403);expect(writes).toHaveLength(0);
}));

test('streamed saves compare user identity as well as actor identity',()=>isolated(async({account,auth,store,stream,writes})=>{
  const writer=await account('user-change');let changed=false;
  const altered={...auth,principal(token){const p=auth.principal(token);return {...p,userId:p.userId+(changed?1:0)};}};
  const handler=acquisitionHandler({auth:altered,store,organizations});
  expect((await stream(writer.cookie,()=>{changed=true;},handler)).status).toBe(403);expect(writes).toHaveLength(0);
}));

test('write routes require same-origin JSON, one cookie, exact envelopes and supported methods',()=>isolated(async({account,req,writes,handler})=>{
  const writer=await account('validation'),input={expectedRevision:0,value:{title:'Valid'}};
  for(const headers of [{origin:'https://elsewhere.test'},{origin:''}])expect((await req(path,input,writer.cookie,headers)).status).toBe(403);
  expect((await req(path,input,writer.cookie,{'content-type':'text/plain'})).status).toBe(415);
  expect((await req(path,input,writer.cookie+'; '+writer.cookie)).status).toBe(401);
  expect((await req('/login',{email:writer.email,password:writer.password},writer.cookie+'; '+writer.cookie)).status).toBe(401);
  for(const value of [null,[],{}, {...input,actor:'spoofed'},{...input,expectedRevision:-1},{...input,expectedRevision:1.5},{...input,expectedRevision:Number.MAX_SAFE_INTEGER+1},{...input,value:[]},{...input,value:null}])expect((await req(path,value,writer.cookie)).status).toBe(400);
  expect((await req(path,{expectedRevision:0,value:{title:''}},writer.cookie)).status).toBe(400);
  expect((await req('/login',undefined,writer.cookie)).status).toBe(405);
  expect((await req('/session',{},writer.cookie)).status).toBe(405);
  expect((await req(path,undefined,writer.cookie,{},'DELETE')).status).toBe(405);
  expect((await req('/login/extra',{},writer.cookie)).status).toBe(404);
  expect((await handler(new Request(origin+'/api/acquisition'))).status).toBe(404);
  expect(await handler(new Request(origin+'/api/acquisition-other/session'))).toBeNull();
  expect((await req(path+'/',undefined,writer.cookie)).status).toBe(404);expect(writes).toHaveLength(0);
}));

test('JSON byte limits apply to streamed input and oversized login attempts',()=>isolated(async({account,req,handler,writes})=>{
  const writer=await account('limits');
  expect((await req('/login',{email:'x'.repeat(9000),password:'invalid'})).status).toBe(413);
  expect((await req(path,{expectedRevision:0,value:{title:'x'.repeat(2*1024*1024)}},writer.cookie)).status).toBe(413);
  const response=await handler(new Request(origin+prefix+path,{method:'POST',headers:{origin,'content-type':'application/json',cookie:writer.cookie},body:'{"expectedRevision":0,"value":',duplex:'half'}));
  expect(response.status).toBe(400);expect(writes).toHaveLength(0);
}));

test('login rate limit is bounded without exposing credentials',()=>isolated(async({req})=>{
  for(let i=0;i<10;i++)expect((await req('/login',{})).status).toBe(400);
  const response=await req('/login',{email:'private@example.test',password:'private-password'});expect(response.status).toBe(429);expect(response.headers.get('retry-after')).toBe('60');
  const text=await response.text();expect(text).not.toContain('private');
}));

test('registry validation fails at startup for ambiguous or invalid scopes',()=>{
  for(const registry of [null,{},[{id:'../north',name:'North',institution:'native'}],[{id:'north',name:'',institution:'native'}],[{id:'north',name:'North',institution:'native_scope'}],[organizations[0],organizations[0]],[organizations[0],{...organizations[1],institution:organizations[0].institution}]])expect(()=>acquisitionHandler({organizations:registry})).toThrow();
});

test('registry entries are copied and profiles expose only account fields',()=>isolated(async({account,auth,store,req})=>{
  const writer=await account('registry'),registry=structuredClone(organizations);
  const handler=acquisitionHandler({auth:{...auth,profile(token){return {...auth.profile(token),password:'hidden-password',session:{token}};}},store,organizations:registry});
  registry[0].institution='acquisition-south';registry[0].name='Changed';registry.push({id:'extra',name:'Extra',institution:'acquisition-north'});
  const response=await req('/session',undefined,writer.cookie,{},'GET',handler),body=await response.text();
  expect(response.status).toBe(200);expect(body).not.toContain('hidden-password');expect(body).not.toContain('bfy_');
  expect(JSON.parse(body).organizations).toEqual([{id:'north',name:'North acquisition',canRead:true,canWrite:true,canAdmin:false}]);
}));

test('streamed saves reject a natively revoked session before persisting',()=>isolated(async({account,auth,stream,writes})=>{
  const writer=await account('stream-session');
  expect((await stream(writer.cookie,()=>auth.logout(writer.cookie.split('=')[1]))).status).toBe(401);expect(writes).toHaveLength(0);
}));

test('missing services and unexpected service errors fail closed without leaking details',async()=>{
  const unavailable=acquisitionHandler({organizations});expect((await unavailable(new Request(origin+prefix+'/session'))).status).toBe(503);
  const token='bfy_'+'a'.repeat(64),auth={principal(){throw Error('secret database path');}},handler=acquisitionHandler({auth,store:{},organizations});
  const response=await handler(new Request(origin+prefix+'/session',{headers:{cookie:'aurekai_acquisition='+token}}));expect(response.status).toBe(500);expect(await response.text()).not.toContain('secret');
});

test('missing actor identity and mismatched profile are refused despite a valid cookie',()=>isolated(async({account,auth,store,req})=>{
  const writer=await account('identity');
  for(const p of [{actor:'',userId:writer.id},{actor:'actor:identity'},{actor:'actor:identity',userId:0}]){
    const handler=acquisitionHandler({auth:{...auth,principal:()=>p},store,organizations});expect((await req('/session',undefined,writer.cookie,{},'GET',handler)).status).toBe(401);
  }
  const handler=acquisitionHandler({auth:{...auth,profile:()=>({id:writer.id+1,name:'Other',email:'other@example.test'})},store,organizations});expect((await req('/session',undefined,writer.cookie,{},'GET',handler)).status).toBe(401);
}));
