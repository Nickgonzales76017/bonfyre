import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {spawn} from 'node:child_process';
import {mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';

// A local Management API fixture. No real Supabase API, billing or credentials.
const binary=resolve(process.argv[2]||'./bonfyre-provider-estate');
const dir=mkdtempSync(join(tmpdir(),'bonfyre-supabase-customer-'));
const envFile=join(dir,'held.env'),wrongFile=join(dir,'wrong.env');
writeFileSync(envFile,'SUPABASE_ACCESS_TOKEN=fixture-held-token\n',{mode:0o600});
writeFileSync(wrongFile,'SUPABASE_ACCESS_TOKEN=bad-token\n',{mode:0o600});
const projects=new Map();
for(let i=0;i<24;i++)projects.set('foreign-'+i,{
  id:'foreign-'+i,name:'aurekai-c-'+(i?'foreign-'+i:'alice'),
  organization_id:'org-other',status:'ACTIVE_HEALTHY'});
projects.set('removed-self',{id:'removed-self',name:'aurekai-c-old',
  organization_id:'org-held',status:'REMOVED'});
let created=0,lastDbPassword='';
const reply=(res,status,data)=>{const raw=JSON.stringify(data);
  res.writeHead(status,{'content-type':'application/json'});res.end(raw);};
const server=createServer((req,res)=>{
 const chunks=[];req.on('data',c=>chunks.push(c));req.on('end',()=>{
  try{
   if(req.headers.authorization!=='Bearer fixture-held-token')return reply(res,401,{error:'unauthorized'});
   const url=new URL(req.url,'http://127.0.0.1');
   if(req.method==='GET'&&url.pathname==='/organizations')
     return reply(res,200,[{id:'org-other',slug:'other'},{id:'org-held',slug:'held'}]);
   if(req.method==='GET'&&url.pathname==='/projects')
     return reply(res,200,[...projects.values()]);
   if(req.method==='POST'&&url.pathname==='/projects'){
     const body=JSON.parse(Buffer.concat(chunks).toString('utf8'));
     assert.equal(body.organization_id,'org-held');
     assert.equal(body.region,'us-east-1');
     assert.match(body.name,/^aurekai-c-[a-z0-9-]+$/);
     assert.match(body.db_pass,/^[a-f0-9]{48}$/);
     lastDbPassword=body.db_pass;
     if([...projects.values()].some(p=>p.name===body.name&&p.organization_id==='org-held'&&p.status!=='REMOVED'))
       return reply(res,409,{error:'duplicate'});
     const id='held-'+(++created);
     projects.set(id,{id,name:body.name,organization_id:'org-held',status:'ACTIVE_HEALTHY'});
     return reply(res,201,{id,status:'COMING_UP'});
   }
   if(req.method==='DELETE'&&url.pathname.startsWith('/projects/')){
     const id=url.pathname.split('/').at(-1),p=projects.get(id);
     if(!p||p.organization_id!=='org-held')return reply(res,404,{error:'missing'});
     projects.delete(id);return reply(res,204,{});
   }
   return reply(res,404,{error:'missing'});
  }catch(e){reply(res,400,{error:'fixture-invalid-request',reason:e.message});}
 });});
const call=(verb,customer,extra={})=>new Promise((resolveCall,reject)=>{
 const args=['customer',verb,'--provider','supabase','--customer',customer,'--env',extra.envFile||envFile,'--api',extra.api||base];
 const child=spawn(binary,args,{env:{...process.env,
   BONFYRE_SUPABASE_HOLDING_ORG:extra.pin===undefined?'org-held':extra.pin,
   BONFYRE_PROVIDER_CUSTOMER_MAX:extra.cap===undefined?'20':extra.cap}});
 let stdout='',stderr='';child.stdout.on('data',c=>stdout+=c);child.stderr.on('data',c=>stderr+=c);
 child.once('error',reject);child.once('close',code=>resolveCall({code,stdout,stderr}));
});
let base='';
try{
 await new Promise(resolveListen=>server.listen(0,'127.0.0.1',resolveListen));
 base='http://127.0.0.1:'+server.address().port;
 const withoutPin=await call('show','alice',{pin:''});
 assert.notEqual(withoutPin.code,0,'ambiguous provider organization must fail closed');
 const wrongPin=await call('show','alice',{pin:'org-unknown'});
 assert.notEqual(wrongPin.code,0,'unknown organization must not be selected');
 assert.equal((await call('show','alice')).code,3);
 const first=await call('open','alice');
 assert.equal(first.code,0,first.stderr);
 assert.equal(JSON.parse(first.stdout).id,'held-1');
 assert.equal(created,1,'other org projects must not exhaust this org ceiling');
 assert(!first.stdout.includes(lastDbPassword)&&!first.stdout.includes('fixture-held-token'),'secret disclosed');
 assert.equal((await call('open','alice')).code,0);
 assert.equal(created,1,'idempotent open must not double-create');
 assert.equal((await call('show','alice')).code,0);
 assert.equal((await call('open','bob',{cap:'1'})).code,4,'ceiling scoped to held org');
 assert.equal((await call('open','alice',{cap:'1'})).code,0,'existing customer not blocked');
 const close=await call('close','alice');
 assert.equal(close.code,0,close.stderr);
 assert.equal(JSON.parse(close.stdout).state,'closed');
 assert.equal((await call('show','alice')).code,3);
 assert.equal((await call('open','bob',{cap:'1'})).code,0,'closed slot should be reusable');
 assert.equal(created,2);
 assert.equal((await call('open','bad;name')).code,2);
 assert.notEqual((await call('open','mallory',{envFile:wrongFile})).code,0);
 assert.equal(created,2,'refused request must not create project');
 assert.equal(projects.size,26,'foreign and removed projects must be untouched');
 console.log('customer supabase: PASS (tenant scope, pin, idempotence, ceiling, access, revoke)');
} finally {await new Promise(done=>server.close(done));rmSync(dir,{recursive:true,force:true});}
