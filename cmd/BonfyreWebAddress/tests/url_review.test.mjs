import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,rmSync,existsSync,symlinkSync,readFileSync,readdirSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_WEB_ADDRESS_TEST_BINARY||join(root,'cmd/BonfyreWebAddress/bonfyre-web-address');
const auth=join(root,'cmd/BonfyreAuth/bonfyre-auth');
const password='native-url-review-fixture-password';
const escape=s=>s.replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;').replaceAll('>','&gt;');
async function fixture(fn){
 const home=mkdtempSync(join(tmpdir(),'url-review-')),db=join(home,'matrix.db'),adb=join(home,'auth.db');
 const reservation=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('reserved')});const port=reservation.port;await reservation.stop(true);
 mkdirSync(join(home,'bin'));symlinkSync(join(root,'cmd/BonfyreProject/bonfyre-project'),join(home,'bin/bonfyre-project'));
 const env={...process.env,BONFYRE_CMS_DB:join(home,'absent.db'),BONFYRE_AUTH_BINARY:auth,BONFYRE_BIN_DIR:join(home,'bin'),BONFYRE_FOREIGN_ROOT:join(home,'foreign')};
 const run=(bin,args)=>{const p=Bun.spawnSync([bin,...args],{env});if(p.exitCode)throw Error(p.stderr.toString());return p.stdout.toString();};
 const authRun=(...a)=>JSON.parse(run(auth,['--db',adb,...a]));
 let server;
 try{
  run(binary,['bootstrap-owned','--db',db]);
  for(const tier of ['free','pro'])authRun('signup','--email',`${tier}@review.test`,'--name',tier,'--password',password,'--tier',tier);
  const tokens=Object.fromEntries(['free','pro'].map(tier=>[tier,authRun('login','--email',`${tier}@review.test`,'--password',password).session.token]));
  server=Bun.spawn([binary,'serve','--db',db,'--auth-db',adb,'--port',String(port),'--site-root',join(root,'site/matrix')],{env,stdout:'ignore',stderr:'ignore'});
  const base=`http://127.0.0.1:${port}`;
  for(let i=0;i<80;i++){try{if((await fetch(base+'/healthz')).ok)break;}catch{} await Bun.sleep(50);}
  const request=async(path,token,options={})=>{const r=await fetch(base+path,{...options,redirect:'manual',headers:{Host:'aurekai.work',Accept:'text/html',...(token?{Authorization:`Bearer ${token}`} : {}),...options.headers}});return {status:r.status,headers:r.headers,body:await r.text()};};
  await fn({request,tokens,home});
 }finally{server?.kill();if(server)await server.exited;rmSync(home,{recursive:true,force:true});}
}

test('a pasted URL opens a review without inventing a discovery candidate or fetching its body',()=>fixture(async({request,tokens,home})=>{
 const url='https://example.test/path?q=one&two=three',path='/studio/?'+new URLSearchParams({foreign_url:url});
 const anon=await request(path);expect(anon.status).toBe(200);expect(anon.body).toContain('Review a public address');expect(anon.body).toContain(escape(url));
 expect(anon.body).toContain('Sign in to continue');expect(anon.body).not.toContain('Candidate route refused');expect(anon.body).not.toContain('hx-post="/studio/v1/foreign/observe"');
 const read=await request(path,tokens.free);expect(read.body).toContain('requires write access');expect(read.body).not.toContain('Read this address');
 const write=await request(path,tokens.pro);expect(write.body).toContain('Read this address');expect(write.body).toContain('action="/studio/v1/foreign/observe" method="post"');
 expect(write.body).toMatch(/name="twin_id" value="url:[a-f0-9]{64}"/);expect(write.body).toContain(`name="url" value="${escape(url)}"`);
 expect(existsSync(join(home,'foreign'))).toBe(false);
 for(const [token,status] of [[null,401],[tokens.free,403]]){
  const refused=await request('/studio/v1/foreign/observe',token,{method:'POST',body:new URLSearchParams({twin_id:'fixture',url})});expect(refused.status).toBe(status);
 }
 expect(existsSync(join(home,'foreign'))).toBe(false);
}));

test('candidate coordinates retain their exact-pair check and invalid URLs get no observation form',()=>fixture(async({request,tokens})=>{
 for(const occurrence of ['external:missing','']){
  const result=await request('/studio/?'+new URLSearchParams({foreign_url:'https://example.test/',foreign_occurrence:occurrence}),tokens.pro);
  expect(result.body).toContain('Candidate route refused');expect(result.body).not.toContain('Read this address');
 }
 for(const url of ['file:///private','javascript:alert(1)','https://example.test/\r\nInjected: yes']){
  const result=await request('/studio/?'+new URLSearchParams({foreign_url:url}),tokens.pro);
  expect(result.body).not.toContain('hx-post="/studio/v1/foreign/observe"');
 }
 const url='https://example.test/?q="<script>"&x=one';
 const result=await request('/studio/?'+new URLSearchParams({foreign_url:url}),tokens.pro);
 expect(result.body).toContain(escape(url));expect(result.body).not.toContain('q="<script>');
}));

test('sign-in preserves the complete review URL across success, failure and no-script referral',()=>fixture(async({request})=>{
 const url='https://example.test/?q='+('a&b='.repeat(180));
 const next='/studio/?'+new URLSearchParams({foreign_url:url});
 expect(next.length).toBeGreaterThan(512);
 const review=await request(next);expect(review.body).toContain(`name="next" value="${escape(next)}"`);
 const form=new URLSearchParams({email:'pro@review.test',password:'wrong',next});
 const failed=await request('/login',null,{method:'POST',body:form});expect(failed.status).toBe(303);
 const retry=new URL(failed.headers.get('location'),'https://aurekai.work');expect(retry.searchParams.get('next')).toBe(next);expect(retry.searchParams.get('failed')).toBe('1');
 form.set('password',password);
 const success=await request('/login',null,{method:'POST',body:form});expect(success.status).toBe(303);expect(success.headers.get('location')).toBe(next);
 form.delete('next');
 const referral=await request('/login',null,{method:'POST',body:form,headers:{Referer:'https://aurekai.work/login?'+new URLSearchParams({next})}});
 expect(referral.status).toBe(303);expect(referral.headers.get('location')).toBe(next);
}));


test('the authorized review form reaches the native observer and retains the selected body',()=>fixture(async({request,tokens,home})=>{
 const hits=[];
 const source=Bun.serve({hostname:'127.0.0.1',port:0,fetch:r=>{const path=new URL(r.url).pathname;hits.push(path);return new Response(path==='/robots.txt'?'User-agent: *\nAllow: /\n':'Native URL review source.\n',{headers:{'Content-Type':'text/plain'}});}});
 try{
  const url=`http://127.0.0.1:${source.port}/source`;
  const review=await request('/studio/?'+new URLSearchParams({foreign_url:url}),tokens.pro);
  const twin=review.body.match(/name="twin_id" value="(url:[a-f0-9]{64})"/)?.[1];expect(twin).toBeTruthy();expect(hits).toEqual([]);
  const result=await request('/studio/v1/foreign/observe',tokens.pro,{method:'POST',body:new URLSearchParams({twin_id:twin,url})});
  expect(result.status).toBe(200);expect(hits).toContain('/source');
  const bodies=readdirSync(join(home,'foreign/bodies'));expect(bodies).toHaveLength(1);
  expect(readFileSync(join(home,'foreign/bodies',bodies[0]),'utf8')).toBe('Native URL review source.\n');
  const receipts=readdirSync(join(home,'foreign/receipts'));expect(receipts).toHaveLength(1);
  expect(readFileSync(join(home,'foreign/receipts',receipts[0]),'utf8')).toContain(url);
 }finally{await source.stop(true);}
}));
