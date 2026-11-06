import {test,expect} from 'bun:test';
import {packSetupFixture} from './pack_setup_fixture.mjs';

/* The four hosts share one Auth owner, not a cookie. Every receiving host
 * starts and consumes an independent single-use ceremony. */
test('cross-territory login hands off through WORK without disclosing the original session',async()=>{
 const f=await packSetupFixture();
 try{
  const owner=await f.login('territory-handoff-fixture@example.test');
  const request=async(path,host,cookie)=>{
   const r=await fetch(new URL(path,f.base),{redirect:'manual',headers:{Host:host,Accept:'text/html',...(cookie?{Cookie:cookie}:{})},signal:AbortSignal.timeout(8000)});
   return {status:r.status,location:r.headers.get('location'),cookie:r.headers.get('set-cookie'),body:await r.text()};
  };
  const work=await request('/session/continue?next=/account','aurekai.work');
  expect(work.status).toBe(303);expect(work.location).toStartWith('/login?next=');
  const workExisting=await request('/session/continue?next=/account','aurekai.work',owner.cookie);
  expect(workExisting.status).toBe(303);expect(workExisting.location).toBe('/account');
  for(const territory of ['life','live','shop']){
   const host='aurekai.'+territory;
   const first=await request('/session/continue?next=/account',host);
   expect(first.status).toBe(303);
   const workUrl=new URL(first.location);
   expect(workUrl.hostname).toBe('aurekai.work');
   const state=workUrl.searchParams.get('state');
   expect(state).toMatch(/^[a-f0-9]{64}$/);
   const cookies=(first.cookie||'').split(/,(?=\s*__Host-)/).map(s=>s.split(';')[0].trim()).filter(Boolean);
   expect(cookies.length).toBe(2);
   const authorized=await request(workUrl.pathname+workUrl.search,'aurekai.work',owner.cookie);
   expect(authorized.status).toBe(303);
   const destination=new URL(authorized.location);
   expect(destination.hostname).toBe(host);
   expect(destination.searchParams.get('code')).toMatch(/^[a-f0-9]{64}$/);
   const path=destination.pathname+destination.search;
   const complete=await request(path,host,cookies.join('; '));
   expect(complete.status).toBe(303);expect(complete.location).toBe('/account');
   expect(complete.cookie).toContain('bonfyre_session=');
   expect(complete.cookie).not.toContain(owner.cookie.split('=')[1]);
   const received=complete.cookie.split(';')[0];
   const navigation=await request('/account/navigation',host,received);
   expect(navigation.status).toBe(200);
   expect(navigation.body).toContain('data-session-state="signed-in"');
   expect(navigation.body).not.toContain(owner.cookie.split('=')[1]);
   const repeat=await request(path,host,cookies.join('; '));
   expect(repeat.status).not.toBe(303);
   const wrong=await request(path,territory==='life'?'aurekai.shop':'aurekai.life',cookies.join('; '));
   expect(wrong.status).not.toBe(303);
  }
  const nav=await request('/account/navigation','aurekai.life');
  expect(nav.status).toBe(200);expect(nav.body).toContain('/session/continue?next=');
 }finally{await f.close();}
},60000);

test('ordinary territory switching supports every directed pair and preserves identity, scope and expiry',async()=>{
 const f=await packSetupFixture();
 try{
  const owner=await f.login('every-territory@example.test');
  const original=owner.cookie.split('=')[1],deadline=new Date(Date.now()+20*60000).toISOString().replace(/\.\d+Z$/,'Z');
  f.authsql.query('UPDATE sessions SET expires_at=? WHERE token=?').run(deadline,original);
  const request=async(url,host,cookie='')=>{
   const r=await f.request(url,{host,cookie});return {...r,location:r.headers.get('location'),cookies:r.headers.getSetCookie()};
  };
  for(const source of ['work','life','live','shop'])for(const dest of ['work','life','live','shop']){
   if(source===dest)continue;
   const from='aurekai.'+source,to='aurekai.'+dest,next='/apps?context=self#saved';
   const depart=await request('/session/switch?'+new URLSearchParams({to:dest,next}),from,owner.cookie);
   expect(depart.status).toBe(303);const receiver=new URL(depart.location);expect(receiver.hostname).toBe(to);
   expect(receiver.searchParams.get('source')).toBe(source);
   const begin=await request(receiver.pathname+receiver.search,to);expect(begin.status).toBe(303);
   const origin=new URL(begin.location);expect(origin.hostname).toBe(from);
   expect(begin.cookies.every(c=>!c.includes('Domain=')&&c.includes('Secure')&&c.includes('HttpOnly'))).toBe(true);
   const authorize=await request(origin.pathname+origin.search,from,owner.cookie);expect(authorize.status).toBe(303);
   expect(authorize.location).not.toContain(original);
   const callback=new URL(authorize.location),nonce=begin.cookies.map(c=>c.split(';')[0]).join('; ');
   const finish=await request(callback.pathname+callback.search,to,nonce);expect(finish.status).toBe(303);expect(finish.location).toBe(next);
   const session=finish.cookies.find(c=>c.startsWith('bonfyre_session='));expect(session).toContain('Domain=.aurekai.'+dest);
   const token=session.split(';')[0].split('=')[1];expect(token).not.toBe(original);
   const retained=f.authsql.query('SELECT user_id,scope,expires_at FROM sessions WHERE token=?').get(token);
   expect(retained.user_id).toBe(owner.id);expect(retained.expires_at<=deadline).toBe(true);
   expect(retained.scope).toBe(f.authsql.query('SELECT scope FROM sessions WHERE token=?').get(original).scope);
   expect((await request(callback.pathname+callback.search,to,nonce)).status).toBe(401);
  }
  for(const query of ['to=evil&next=%2F','to=life&next=https%3A%2F%2Fevil.test%2F','to=life&to=shop&next=%2F']){
   expect((await request('/session/switch?'+query,'aurekai.work',owner.cookie)).status).toBe(400);
  }
  const foreign=await request('/session/switch?to=life&next=%2F','evil.test',owner.cookie);expect(foreign.status).toBe(400);
  const absent=await request('/session/switch?to=shop&next=%2Faccount','aurekai.life');expect(absent.location).toBe('https://aurekai.shop/account');
  expect(f.authsql.query('SELECT COUNT(*) AS n FROM institution_grants').get().n).toBe(0);
 }finally{await f.close();}
},60000);

test('profile continuations require a registered same-territory Matrix address and never replace a different signed-in account',async()=>{
 const f=await packSetupFixture();try{
  const owner=await f.login('profile-departure@example.test'),other=await f.login('profile-recipient@example.test');
  const request=async(url,host,cookie='')=>{const r=await f.request(url,{host,cookie});return {...r,location:r.headers.get('location'),cookies:r.headers.getSetCookie()};};
  const next='https://profile-fixture.aurekai.life/';
  expect((await request('/session/switch?'+new URLSearchParams({to:'life',next}),'aurekai.work',owner.cookie)).status).toBe(400);
  f.run(f.web,['domain','--db',f.home+'/matrix.db','--host','profile-fixture.aurekai.life','--territory','life','--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile-continuation']);
  const depart=await request('/session/switch?'+new URLSearchParams({to:'life',next}),'aurekai.work',owner.cookie);
  expect(depart.status).toBe(303);const receiver=new URL(depart.location);
  const begin=await request(receiver.pathname+receiver.search,receiver.hostname),origin=new URL(begin.location);
  const issued=await request(origin.pathname+origin.search,origin.hostname,owner.cookie),callback=new URL(issued.location);
  const nonce=begin.cookies.map(c=>c.split(';')[0]).join('; ');
  const before=f.authsql.query('SELECT COUNT(*) AS n FROM sessions').get().n;
  const conflict=await request(callback.pathname+callback.search,callback.hostname,nonce+'; '+other.cookie);
  expect(conflict.location).toBe(next);expect(conflict.cookies.some(c=>c.startsWith('bonfyre_session='))).toBe(false);
  expect(f.authsql.query('SELECT COUNT(*) AS n FROM sessions').get().n).toBe(before);
  const finish=await request(callback.pathname+callback.search,callback.hostname,nonce);
  expect(finish.status).toBe(303);expect(finish.location).toBe(next);
  for(const bad of ['https://profile-fixture.aurekai.life.evil.test/','https://user@profile-fixture.aurekai.life/','https://profile-fixture.aurekai.life:444/','https://unregistered.aurekai.life/']){
   expect((await request('/session/switch?'+new URLSearchParams({to:'life',next:bad}),'aurekai.work',owner.cookie)).status).toBe(400);
  }
 }finally{await f.close();}
},30000);
