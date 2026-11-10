// Run with Bun from any working directory; uses the actual checkout binaries.
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {readFileSync,writeFileSync} from 'node:fs';
import {Database} from 'bun:sqlite';
import {fixture,root,password,decode,hidden} from './auth_continuation_fixture.mjs';
const f=await fixture(),checks=[];
const check=(label,value)=>{assert.ok(value,label);checks.push(label);console.log('PASS '+label);};
const location=r=>r.headers.get('location');
const cookie=r=>r.headers.get('set-cookie')?.split(';')[0]??'';
const queryNext=r=>new URL(location(r),'https://aurekai.life').searchParams.get('next');
const count=(table)=>{const db=new Database(f.authdb,{readonly:true});try{return db.query("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table)?db.query(`SELECT count(*) AS n FROM ${table}`).get().n:0;}finally{db.close();}};
async function register(name,next){return f.request('/signup',{form:{name,email:name+'@example.test',password,next}});}
async function login(name,next,pw=password,host='aurekai.life'){return f.request('/login',{host,form:{email:name+'@example.test',password:pw,next}});}
try{
  const unit=join(f.home,'native-parser-test');
  f.run('/usr/bin/cc',['-std=c11','-Wall','-Wextra','-Werror',join(root,'cmd/BonfyreWebAddress/tests/auth_continuation_test.c'),'-o',unit]);
  check('native parser and form-lowering assertions pass',f.run(unit,[]).includes('PASS'));
  const target=f.path+'?mode=compare&term=a%20b#review';
  for(const host of ['aurekai.life','aurekai.live','aurekai.shop','aurekai.work']){
    const r=await f.request('/login?next='+encodeURIComponent(target),{host});
    check(host+' keeps the complete continuation in its fallback form',r.status===200&&hidden(r.body)===target&&decode(r.body).includes('/signup?next='+encodeURIComponent(target)));
    check(host+' uses native forms without the old browser redirect',!r.body.includes('safeNext()')&&!r.body.includes('JSON.stringify({email, password})')&&!r.body.includes(' novalidate'));
  }
  const created=await register('first',target);
  check('signup retains the selected address but creates no session',created.status===303&&queryNext(created)===target&&!cookie(created)&&count('sessions')===0);
  check('signup creates no institutional grants',count('institution_grants')===0);
  const duplicate=await register('first',target);
  check('failed signup retains continuation without a cookie',duplicate.status===303&&queryNext(duplicate)===target&&location(duplicate).startsWith('/signup?failed=1')&&!cookie(duplicate));
  const notice=await f.request(location(created));
  check('created-account status and continuation reach the next form',notice.body.includes('Account created. Sign in to continue.')&&hidden(notice.body)===target);
  const failed=await login('first',target,'incorrect-fixture-password');
  check('failed login retains the complete destination',failed.status===303&&queryNext(failed)===target&&!cookie(failed));
  const signed=await login('first',target),first=cookie(signed);
  check('successful login returns to the exact item, query and fragment',signed.status===303&&location(signed)===target&&first.startsWith('bonfyre_session=bfy_'));
  check('native cookie remains HttpOnly, Secure and territory-scoped',/Domain=\.aurekai.life;.*Secure; HttpOnly; SameSite=Lax/.test(signed.headers.get('set-cookie')));
  const empty=await f.request('/saved',{cookie:first});
  check('signing in never automatically saves or adopts the target',!empty.body.includes('City opens a new library'));
  const save=await f.request('/o/save',{cookie:first,form:{item:f.item,return:f.path}});
  check('the returned person explicitly saves through the existing native owner',save.status===303&&(await f.request('/saved',{cookie:first})).body.includes('City opens a new library'));
  await register('second',target);const second=cookie(await login('second',target));
  check('a second new account cannot see the first person’s saved work',!(await f.request('/saved',{cookie:second})).body.includes('City opens a new library'));
  await f.request('/o/save',{cookie:second,form:{item:f.item,return:f.path}});
  await f.request('/o/save',{cookie:second,form:{item:f.item,return:f.path}});
  {const db=new Database(f.db,{readonly:true});try{const rows=db.query('SELECT actor,item_ref FROM saved_item').all();check('two people continue from one public item into independent, replay-safe saved records',rows.length===2&&new Set(rows.map(row=>row.actor)).size===2&&rows.every(row=>row.item_ref===f.item));}finally{db.close();}}
  await f.request('/o/unsave',{cookie:first,form:{item:f.item,return:'/saved'}});
  check('one person removing their saved copy leaves the other copy intact',(await f.request('/saved',{cookie:second})).body.includes('City opens a new library')&&!(await f.request('/saved',{cookie:first})).body.includes('City opens a new library'));
  writeFileSync(f.source,'<?xml version="1.0"?><rss><channel></channel></rss>');
  check('source withdrawal hides the public item but preserves the permitted saved copy',(await f.request(f.path)).status===404&&(await f.request(f.path,{cookie:second})).body.includes('You saved this'));
  writeFileSync(f.source,f.rss);
  for(const next of ['//evil.test','/%2fevil.test','/a/%2e%2e/private','/a%0d%0aHeader:x','https://jrss.aurekai.work.evil.test/','https://u@jrss.aurekai.work/'])check('unsafe destination refused: '+next,location(await login('first',next))==='/account');
  const ambiguous=await f.request('/login',{form:'email=first%40example.test&password='+password+'&next=%2Fsaved&next=%2Felsewhere'});
  check('duplicate continuation fields cannot select a destination',location(ambiguous)==='/account');
  const work='https://jrss.aurekai.work/review?item=one#details';
  check('named WORK continuation is preserved within WORK',location(await login('first',work,password,'aurekai.work'))===work);
  check('WORK continuation is not transported from LIFE',location(await login('first',work))==='/account');
  f.authored();
  for(const route of ['/login','/signup']){
    const r=await f.request(route+'?next='+encodeURIComponent(target)),aperture=decode(r.body.match(/hx-get="([^\"]*\/_aperture\/form[^\"]*)"/)?.[1]??'');
    check('CMS-authored '+route+' retains its published page',r.status===200&&r.body.includes('Authored '));
    check('CMS-authored '+route+' carries continuation into the native action fragment',new URL(aperture,'https://aurekai.life').searchParams.get('next')===target);
    const fragment=await f.request(aperture);
    check('CMS '+route+' form and alternate account action carry the same destination',fragment.status===200&&hidden(fragment.body)===target&&fragment.body.includes('?next='+encodeURIComponent(target)));
  }
  const api=await f.request('/login',{form:JSON.stringify({email:'first@example.test',password}),headers:{Accept:'application/json','Content-Type':'application/json'}});
  check('existing explicit machine login contract remains available',api.status===200&&JSON.parse(api.body).session?.token?.startsWith('bfy_'));
  const logout=await f.request('/logout',{cookie:second,form:{}});
  check('native logout revokes the already-issued continuation session',logout.status===200&&(await f.request('/saved',{cookie:second})).status===303);
  const log=readFileSync(f.log,'utf8');check('test server logs contain neither passwords nor session tokens',!log.includes(password)&&!/bfy_[a-f0-9]{64}/.test(log));
  console.log(`AUTH_CONTINUATION_PASS\t${checks.length}\tnative_sha256\t${f.fingerprint}`);
}finally{await f.close();}
